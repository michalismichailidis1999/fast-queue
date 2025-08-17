#include "../../header_files/queue_management/CompactionHandler.h"

CompactionHandler::CompactionHandler(Controller* controller, QueueManager* qm, MessagesHandler* mh, SegmentLockManager* lock_manager, ClusterMetadataApplyHandler* cmah, FileHandler* fh, QueueSegmentFilePathMapper* pm, Logger* logger, Settings* settings) {
	this->controller = controller;
	this->qm = qm;
	this->mh = mh;
	this->lock_manager = lock_manager;
	this->cmah = cmah;
	this->fh = fh;
	this->pm = pm;
	this->logger = logger;
	this->settings = settings;
}

void CompactionHandler::compact_closed_segments(std::atomic_bool* should_terminate) {
	std::vector<std::string> queue_names;

	while (!should_terminate->load()) {
		try
		{
			queue_names.clear();

			this->qm->get_all_queue_names(&queue_names);

			for (const std::string& queue_name : queue_names)
				this->handle_queue_partitions_segment_compaction(queue_name, should_terminate);
		}
		catch (const std::exception& ex)
		{
			std::string err_msg = "Error occured during segment compaction. Reason: " + std::string(ex.what());
			this->logger->log_error(err_msg);
		}

		std::this_thread::sleep_for(std::chrono::milliseconds(CHECK_FOR_COMPACTION));
	}
}

void CompactionHandler::handle_queue_partitions_segment_compaction(const std::string& queue_name, std::atomic_bool* should_terminate) {
	if (queue_name == CLUSTER_METADATA_QUEUE_NAME) return;

	std::shared_ptr<Queue> queue = this->qm->get_queue(queue_name);

	if (queue == nullptr || queue->get_metadata()->get_cleanup_policy() != CleanupPolicyType::COMPACT_SEGMENTS) return;

	if (!should_terminate->load() && !this->continue_compaction(queue.get())) return;

	unsigned int partitions = queue->get_metadata()->get_partitions();

	for (unsigned int i = 0; i < partitions; i++) {
		if (!should_terminate->load() && !this->continue_compaction(queue.get())) return;

		std::shared_ptr<Partition> partition = queue->get_partition(i);

		if (partition == nullptr) continue;

		unsigned int segments_checked_count = 20;

		while (this->handle_partition_oldest_segment_compaction(partition.get()) && segments_checked_count > 0)
			segments_checked_count--;
	}
}

bool CompactionHandler::handle_partition_oldest_segment_compaction(Partition* partition) {
	this->existing_keys.clear();

	unsigned long long segment_id = partition->get_smallest_uncompacted_segment_id();

	if (partition->get_current_segment_id() == segment_id) return false;

	bool is_internal_queue = Helper::is_internal_queue(partition->get_queue_name());

	std::string segment_path = this->pm->get_file_path(
		partition->get_queue_name(),
		segment_id,
		is_internal_queue ? -1 : partition->get_partition_id()
	);

	std::string index_path = this->pm->get_file_path(
		partition->get_queue_name(),
		segment_id,
		is_internal_queue ? -1 : partition->get_partition_id(),
		true
	);

	bool data_exists = this->fh->check_if_exists(segment_path);
	bool index_exists = this->fh->check_if_exists(index_path);

	if (!data_exists || !index_exists) {
		this->logger->log_error(
			"Skipping compaction in queue's " 
			+ partition->get_queue_name() 
			+ " partition's " 
			+ std::to_string(partition->get_partition_id()) 
			+ " segment " 
			+ std::to_string(segment_id)
			+ ". Segment's data or index file does not exist"
		);

		return false;
	}

	std::string segment_key = this->pm->get_file_key(
		partition->get_queue_name(),
		segment_id,
		is_internal_queue ? -1 : partition->get_partition_id()
	);

	std::string index_key = this->pm->get_file_key(
		partition->get_queue_name(),
		segment_id,
		is_internal_queue ? -1 : partition->get_partition_id(),
		true
	);

	std::unique_ptr<char> segment_bytes = std::unique_ptr<char>(new char[SEGMENT_METADATA_TOTAL_BYTES]);

	this->fh->read_from_file(segment_key, segment_path, SEGMENT_METADATA_TOTAL_BYTES, 0, segment_bytes.get());

	PartitionSegment segment = PartitionSegment(segment_bytes.get(), segment_key, segment_path);
	segment.set_index(index_key, index_path);

	if (!segment.get_is_read_only()) return false;

	if (segment.is_segment_compacted()) {
		std::lock_guard<std::shared_mutex> partition_lock(partition->mut);
		partition->smallest_uncompacted_segment_id = segment_id + 1;
		return true;
	}

	bool success = true;

	std::shared_ptr<PartitionSegment> compacted_segment = nullptr;
	std::shared_ptr<PartitionSegment> prev_compacted_segment = nullptr;

	// Shared lock can be used since data will only be read and written to different file
	this->lock_manager->lock_segment(partition, &segment);

	try
	{
		auto tup_res = this->compact_segment(partition, &segment);
		compacted_segment = std::get<0>(tup_res);
		prev_compacted_segment = std::get<1>(tup_res);
	}
	catch (const CorruptionException& ex) {
		success = false;
		std::string err_msg = "Error occured during segment compaction. Reason: " + std::string(ex.what());
		this->logger->log_error(err_msg);
	}
	catch (const std::exception& ex)
	{
		success = false;
		std::string err_msg = "Error occured during segment compaction. Reason: " + std::string(ex.what());
		this->logger->log_error(err_msg);
	}

	this->lock_manager->release_segment_lock(partition, &segment);

	if (!success) return false;

	this->lock_manager->lock_segment(partition, &segment, true);

	if(prev_compacted_segment != nullptr) 
		this->lock_manager->lock_segment(partition, prev_compacted_segment.get(), true);

	try
	{
		this->fh->delete_dir_or_file(segment.get_index_path(), segment.get_index_key());
		this->fh->delete_dir_or_file(segment.get_segment_path(), segment.get_segment_key());

		if (prev_compacted_segment != nullptr) {
			this->fh->delete_dir_or_file(prev_compacted_segment.get()->get_index_path(), prev_compacted_segment.get()->get_index_key());
			this->fh->delete_dir_or_file(prev_compacted_segment.get()->get_segment_path(), prev_compacted_segment.get()->get_segment_key());
		}

		this->fh->rename_file(
			compacted_segment.get()->get_index_key(),
			compacted_segment.get()->get_index_path(),
			segment.get_index_path()
		);

		this->fh->rename_file(
			compacted_segment.get()->get_segment_key(),
			compacted_segment.get()->get_segment_path(),
			segment.get_segment_path()
		);

		std::lock_guard<std::shared_mutex> lock(partition->mut);
		partition->smallest_uncompacted_segment_id = segment_id + 1;
	}
	catch (const std::exception& ex)
	{
		success = false;
		std::string err_msg = "Error occured during segment compaction. Reason: " + std::string(ex.what());
		this->logger->log_error(err_msg);
	}

	this->lock_manager->release_segment_lock(partition, &segment, true);

	if (prev_compacted_segment != nullptr)
		this->lock_manager->release_segment_lock(partition, prev_compacted_segment.get(), true);

	return success;
}

std::tuple<std::shared_ptr<PartitionSegment>, std::shared_ptr<PartitionSegment>> CompactionHandler::compact_segment(Partition* partition, PartitionSegment* segment) {
	std::shared_ptr<PartitionSegment> write_segment = this->initialize_compacted_segment_write_locations(partition, segment);

	this->compact_segment(partition, segment, write_segment);

	std::shared_ptr<PartitionSegment> prev_compacted_segment = this->get_prev_compacted_segment(partition, segment->get_id() - 1);

	if (prev_compacted_segment != nullptr) {
		try
		{
			this->lock_manager->lock_segment(partition, prev_compacted_segment.get());

			this->compact_segment(partition, prev_compacted_segment.get(), write_segment);
		}
		catch (const std::exception& ex)
		{
			this->lock_manager->release_segment_lock(partition, prev_compacted_segment.get());
			throw ex;
		}

		this->lock_manager->release_segment_lock(partition, prev_compacted_segment.get());
	}

	this->existing_keys.clear();

	return std::tuple<std::shared_ptr<PartitionSegment>, std::shared_ptr<PartitionSegment>>(write_segment, prev_compacted_segment);
}

void CompactionHandler::compact_segment(Partition* partition, PartitionSegment* segment, std::shared_ptr<PartitionSegment> write_segment) {
	unsigned int batch_size = READ_MESSAGES_BATCH_SIZE;

	unsigned long long read_pos = SEGMENT_METADATA_TOTAL_BYTES;

	std::unique_ptr<char> read_batch = std::unique_ptr<char>(new char[batch_size]);
	
	unsigned int message_bytes = 0;

	std::string key = "";
	unsigned int key_size = 0;

	unsigned int count = 0;

	for (unsigned int i = 0; i < 2; i++) {
		unsigned int offset = 0;

		while (true) {
			unsigned long bytes_read = this->fh->read_from_file(
				segment->get_segment_key(),
				segment->get_segment_path(),
				batch_size,
				read_pos,
				read_batch.get()
			);

			if (bytes_read == 0) break;

			while (offset < bytes_read - MESSAGE_TOTAL_BYTES) {
				memcpy_s(&message_bytes, TOTAL_METADATA_BYTES, read_batch.get() + offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);

				if (offset + message_bytes > bytes_read) break;

				unsigned int key_offset = this->get_key_offset(partition->get_queue_name(), read_batch.get() + offset);

				memcpy_s(&key_size, MESSAGE_KEY_SIZE, read_batch.get() + offset + MESSAGE_KEY_OFFSET, MESSAGE_KEY_SIZE);

				if (i == 0 && (key_size == 0 || key_offset == 0)) {
					offset += message_bytes;
					continue;
				}

				key = std::string(read_batch.get() + offset + key_offset, key_size);

				if (i == 0) {
					bool key_exists = this->existing_keys.find(key) != this->existing_keys.end();
					count = this->existing_keys[key];

					if (!key_exists)
						this->existing_keys[key] = 1;
					else if (count > 0)
						this->existing_keys[key]++;

					offset += message_bytes;
					continue;
				}

				if (key_size == 0 || key_offset == 0) {
					this->mh->save_messages(partition, read_batch.get() + offset, message_bytes, write_segment);
					offset += message_bytes;
					continue;
				}

				count = this->existing_keys[key];

				if(count > 0) this->existing_keys[key]--;

				if (count == 1)
					this->mh->save_messages(partition, read_batch.get() + offset, message_bytes, write_segment);

				offset += message_bytes;
			}

			if (bytes_read < batch_size) break;

			if (message_bytes > batch_size) {
				batch_size = message_bytes;
				read_batch = std::unique_ptr<char>(new char[batch_size]);
			}
			else if(batch_size != READ_MESSAGES_BATCH_SIZE && message_bytes != batch_size){
				batch_size = READ_MESSAGES_BATCH_SIZE;
				read_batch = std::unique_ptr<char>(new char[batch_size]);
			}

			read_pos += offset;
		}

		read_pos = SEGMENT_METADATA_TOTAL_BYTES;

		if (batch_size != READ_MESSAGES_BATCH_SIZE) {
			batch_size = READ_MESSAGES_BATCH_SIZE;
			read_batch = std::unique_ptr<char>(new char[batch_size]);
		}
	}
}

unsigned int CompactionHandler::get_key_offset(const std::string& queue_name, void* message_data) {
	bool is_internal_queue = Helper::is_internal_queue(queue_name);

	if (!is_internal_queue) return MESSAGE_TOTAL_BYTES;

	CommandType type = CommandType::NONE;

	memcpy_s(&type, COMMAND_TYPE_SIZE, (char*)message_data + COMMAND_TYPE_OFFSET, COMMAND_TYPE_SIZE);

	switch (type) {
		case CommandType::CREATE_QUEUE: return CQ_COMMAND_TOTAL_BYTES;
		case CommandType::DELETE_QUEUE: return DQ_COMMAND_TOTAL_BYTES;
		case CommandType::ALTER_PARTITION_ASSIGNMENT: return PA_COMMAND_TOTAL_BYTES;
		case CommandType::ALTER_PARTITION_LEADER_ASSIGNMENT: return PLA_COMMAND_TOTAL_BYTES;
		case CommandType::REGISTER_DATA_NODE: return RDN_COMMAND_TOTAL_BYTES;
		case CommandType::UNREGISTER_DATA_NODE: return UDN_COMMAND_TOTAL_BYTES;
		default: return 0;
	}
}

bool CompactionHandler::continue_compaction(Queue* queue) {
	if (queue->get_metadata()->get_status() == Status::ACTIVE) return true;

	this->logger->log_info("Queue " + queue->get_metadata()->get_name() + " is not active. Skipping compaction check");

	return false;
}

std::shared_ptr<PartitionSegment> CompactionHandler::initialize_compacted_segment_write_locations(Partition* partition, PartitionSegment* segment) {
	std::string compacted_segment_path = this->pm->get_compacted_file_path(
		partition->get_queue_name(),
		segment->get_id(),
		partition->get_partition_id()
	);

	std::string compacted_segment_key = this->pm->get_compacted_file_key(
		partition->get_queue_name(),
		segment->get_id(),
		partition->get_partition_id()
	);

	std::string compacted_index_path = this->pm->get_compacted_file_path(
		partition->get_queue_name(),
		segment->get_id(),
		partition->get_partition_id(),
		true
	);

	std::string compacted_index_key = this->pm->get_compacted_file_key(
		partition->get_queue_name(),
		segment->get_id(),
		partition->get_partition_id(),
		true
	);

	this->fh->delete_dir_or_file(compacted_index_path, compacted_index_key);
	this->fh->delete_dir_or_file(compacted_segment_path, compacted_segment_key);

	segment->set_to_compacted();

	this->fh->create_new_file(
		compacted_segment_path,
		SEGMENT_METADATA_TOTAL_BYTES,
		std::get<1>(segment->get_metadata_bytes()).get(),
		compacted_segment_key,
		true
	);

	std::shared_ptr<BTreeNode> write_node = std::shared_ptr<BTreeNode>(new BTreeNode(PageType::LEAF));

	this->fh->create_new_file(
		compacted_index_path,
		INDEX_PAGE_SIZE,
		std::get<0>(write_node.get()->get_page_bytes()).get(),
		compacted_index_key,
		true
	);

	std::shared_ptr<PartitionSegment> write_segment = std::shared_ptr<PartitionSegment>(
		new PartitionSegment(segment->get_id(), compacted_segment_key, compacted_segment_path)
	);

	write_segment.get()->set_index(compacted_index_key, compacted_index_path);
	write_segment.get()->set_to_compacted();
	write_segment.get()->set_to_read_only();
	write_segment.get()->set_is_for_compaction();
	write_segment.get()->set_last_index_page_offset(segment->get_last_index_page_offset());
	write_segment.get()->set_last_message_offset(segment->get_last_message_offset());
	write_segment.get()->set_last_message_timestamp(segment->get_last_message_timestamp());

	return write_segment;
}

std::shared_ptr<PartitionSegment> CompactionHandler::get_prev_compacted_segment(Partition* partition, unsigned long long prev_segment_id) {
	if (prev_segment_id == 0) return nullptr;

	std::string compacted_segment_path = this->pm->get_file_path(
		partition->get_queue_name(),
		prev_segment_id,
		partition->get_partition_id()
	);

	std::string compacted_segment_key = this->pm->get_file_key(
		partition->get_queue_name(),
		prev_segment_id,
		partition->get_partition_id()
	);

	std::string compacted_index_path = this->pm->get_file_path(
		partition->get_queue_name(),
		prev_segment_id,
		partition->get_partition_id(),
		true
	);

	std::string compacted_index_key = this->pm->get_file_key(
		partition->get_queue_name(),
		prev_segment_id,
		partition->get_partition_id(),
		true
	);

	if (!this->fh->check_if_exists(compacted_segment_path) || !this->fh->check_if_exists(compacted_index_path))
		return nullptr;

	std::unique_ptr<char> metadata = std::unique_ptr<char>(new char[SEGMENT_METADATA_TOTAL_BYTES]);

	this->fh->read_from_file(
		compacted_segment_key,
		compacted_segment_path,
		SEGMENT_METADATA_TOTAL_BYTES,
		0,
		metadata.get()
	);

	std::shared_ptr<PartitionSegment> segment = std::shared_ptr<PartitionSegment>(new PartitionSegment(metadata.get(), compacted_segment_key, compacted_segment_path));

	segment.get()->set_index(compacted_index_key, compacted_index_path);

	return segment;
}