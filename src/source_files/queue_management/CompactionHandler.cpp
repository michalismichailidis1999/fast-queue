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

	this->bf = std::unique_ptr<BloomFilter>(new BloomFilter(BLOOM_FILTER_BIT_ARRAY_SIZE, BLOOM_FILTER_HASH_FUNCTIONS_COUNT));
}

void CompactionHandler::compact_closed_segments(std::atomic_bool* should_terminate) {
	std::vector<std::string> queue_names;

	while (!(*should_terminate)) {
		queue_names.clear();

		this->qm->get_all_queue_names(&queue_names);

		for (const std::string& queue_name : queue_names)
			this->handle_queue_partitions_segment_compaction(queue_name, should_terminate);

		std::this_thread::sleep_for(std::chrono::milliseconds(CHECK_FOR_COMPACTION));
	}
}

void CompactionHandler::handle_queue_partitions_segment_compaction(const std::string& queue_name, std::atomic_bool* should_terminate) {
	std::shared_ptr<Queue> queue = this->qm->get_queue(queue_name);

	if (queue == nullptr || queue->get_metadata()->get_cleanup_policy() != CleanupPolicyType::COMPACT_SEGMENTS) return;

	if (!(*should_terminate) && this->continue_compaction(queue.get())) return;

	unsigned int partitions = queue->get_metadata()->get_partitions();

	for (unsigned int i = 0; i < partitions; i++) {
		if (!(*should_terminate) && this->continue_compaction(queue.get())) return;

		Partition* partition = queue->get_partition(i);
		unsigned int segments_checked_count = 20;

		while (this->handle_partition_oldest_segment_compaction(partition) && segments_checked_count > 0)
			segments_checked_count--;
	}
}

bool CompactionHandler::handle_partition_oldest_segment_compaction(Partition* partition) {
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
		return true;
	}

	std::unique_ptr<char> segment_bytes = std::unique_ptr<char>(new char[SEGMENT_METADATA_TOTAL_BYTES]);

	std::string segment_key = this->pm->get_file_path(
		partition->get_queue_name(),
		segment_id,
		is_internal_queue ? -1 : partition->get_partition_id()
	);

	std::string index_key = this->pm->get_file_key(
		partition->get_queue_name(),
		segment_id,
		true
	);

	this->fh->read_from_file(segment_key, segment_path, SEGMENT_METADATA_TOTAL_BYTES, 0, segment_bytes.get());

	PartitionSegment segment = PartitionSegment(segment_bytes.get(), segment_key, segment_path);

	if (!segment.get_is_read_only() || segment.is_segment_compacted()) return false;

	bool success = true;

	std::shared_ptr<PartitionSegment> compacted_segment = nullptr;

	try
	{
		// Shared lock can be used since data will only be read and written to different file
		this->lock_manager->lock_segment(partition, &segment);

		if (is_internal_queue) this->compact_internal_segment(&segment);
		else compacted_segment = this->compact_segment(partition, &segment);
	}
	catch (const CorruptionException& ex) {
		success = false;
		std::string err_msg = "Error occured during segment compaction. Reason: " + std::string(ex.what());
		this->logger->log_error(err_msg);
		// TODO: Mark corruption for fix
	}
	catch (const std::exception& ex)
	{
		success = false;
		std::string err_msg = "Error occured during segment compaction. Reason: " + std::string(ex.what());
		this->logger->log_error(err_msg);
	}

	this->lock_manager->release_segment_lock(partition, &segment);

	if (!success) return false;

	try
	{
		this->lock_manager->lock_segment(partition, &segment, true);

		if (is_internal_queue) {
			auto tup_res = this->controller->get_compacted_cluster_metadata()->get_metadata_bytes();

			this->fh->write_to_file(
				this->pm->get_cluster_metadata_compaction_key(),
				this->pm->get_cluster_metadata_compaction_path(),
				std::get<0>(tup_res),
				0,
				std::get<1>(tup_res).get(),
				true
			);
		}

		this->fh->delete_dir_or_file(segment.get_index_path(), segment.get_index_key());
		this->fh->delete_dir_or_file(segment.get_segment_path(), segment.get_segment_key());

		if (!is_internal_queue) {
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
		}

		std::lock_guard<std::shared_mutex> lock(partition->mut);
		partition->smallest_uncompacted_segment_id++;
	}
	catch (const std::exception& ex)
	{
		success = false;
		std::string err_msg = "Error occured during segment compaction. Reason: " + std::string(ex.what());
		this->logger->log_error(err_msg);
	}

	this->lock_manager->release_segment_lock(partition, &segment, true);

	return success;
}

std::shared_ptr<PartitionSegment> CompactionHandler::compact_segment(Partition* partition, PartitionSegment* segment, std::shared_ptr<PartitionSegment> write_segment, std::shared_ptr<BTreeNode> write_node) {
	if (write_segment == nullptr) {
		this->bf->reset();

		auto write_tup_res = this->initialize_compacted_segment_write_locations(partition, segment);

		write_segment = std::get<0>(write_tup_res);
		write_node = std::get<1>(write_tup_res);
	}

	std::unique_ptr<char> page_data = std::unique_ptr<char>(new char [INDEX_PAGE_SIZE]);

	std::shared_ptr<BTreeNode> current_last_node = this->find_index_node_with_last_message(segment, page_data.get());

	if (current_last_node == nullptr)
		throw std::exception("Something went wrong during index page with last maessage retrieval");

	int current_row_index = !segment->is_segment_compacted()
		? current_last_node.get()->get_total_rows_count() - 1
		: 0;

	int step = !segment->is_segment_compacted() ? -1 : 1;

	while (current_last_node != nullptr) {
		this->parse_index_node_rows(partition, segment, write_segment, current_last_node.get(), !segment->is_segment_compacted());
		current_last_node = this->find_index_node_with_last_message(segment, page_data.get(), current_last_node.get());
	}

	// TODO: Compact now prev compacted segment to this segment if prev segment exists

	return write_segment;
}

void CompactionHandler::compact_internal_segment(PartitionSegment* segment) {
	this->cmah->apply_commands_from_segment(
		this->controller->get_compacted_cluster_metadata(),
		segment->get_id()
	);
}

bool CompactionHandler::continue_compaction(Queue* queue) {
	if (queue->get_metadata()->get_status() == Status::ACTIVE) return true;

	this->logger->log_info("Queue " + queue->get_metadata()->get_name() + " is not active. Skipping compaction check");

	return false;
}

bool CompactionHandler::ignore_message(void* message) {
	unsigned int key_size = 0;

	memcpy_s(&key_size, MESSAGE_KEY_LENGTH_SIZE, (char*)message + MESSAGE_KEY_LENGTH_OFFSET, MESSAGE_KEY_LENGTH_SIZE);

	std::string key = std::string((char*)message + MESSAGE_KEY_OFFSET, key_size);

	if (!this->bf->has((void*)key.c_str(), key_size)) return false;

	// TODO: Create a key index for each queue with compaction and check there if key exists

	return false;
}

std::tuple<std::shared_ptr<PartitionSegment>, std::shared_ptr<BTreeNode>> CompactionHandler::initialize_compacted_segment_write_locations(Partition* partition, PartitionSegment* segment) {
	std::string compacted_segment_path = this->pm->get_compacted_file_path(
		partition->get_queue_name(),
		segment->get_id(),
		partition->get_partition_id()
	);

	std::string compacted_segment_key = this->pm->get_compacted_file_path(
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

	std::string compacted_index_key = this->pm->get_compacted_file_path(
		partition->get_queue_name(),
		segment->get_id(),
		partition->get_partition_id(),
		true
	);

	this->fh->delete_dir_or_file(compacted_index_path);
	this->fh->delete_dir_or_file(compacted_segment_path);

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

	return std::tuple<std::shared_ptr<PartitionSegment>, std::shared_ptr<BTreeNode>>(
		write_segment, write_node
	);
}

std::shared_ptr<BTreeNode> CompactionHandler::find_index_node_with_last_message(PartitionSegment* segment, void* page_data) {
	std::shared_ptr<BTreeNode> current_last_node = nullptr;

	unsigned int page_offset = 0;

	while (true) {
		unsigned long bytes_read = this->fh->read_from_file(
			segment->get_index_key(),
			segment->get_index_path(),
			INDEX_PAGE_SIZE,
			page_offset,
			page_data
		);

		if (bytes_read != INDEX_PAGE_SIZE)
			throw CorruptionException("Corrupted index file");

		current_last_node = std::shared_ptr<BTreeNode>(new BTreeNode(page_data));

		if (current_last_node.get()->get_page_type() == PageType::LEAF) break;

		if (!segment->is_segment_compacted() && current_last_node.get()->get_next_page_offset() != 0)
			page_offset = current_last_node.get()->get_next_page_offset();
		else
			page_offset = segment->is_segment_compacted() 
				? current_last_node.get()->get_first_child()->val_pos 
				: current_last_node.get()->get_last_child()->val_pos;
	}

	return current_last_node;
}

std::shared_ptr<BTreeNode> CompactionHandler::find_index_node_with_last_message(PartitionSegment* segment, void* page_data, BTreeNode* current_last_node) {
	std::shared_ptr<BTreeNode> next_last_node = nullptr;
	
	if (
		segment->is_segment_compacted() 
		&& current_last_node->get_next_page_offset() == 0 
		&& current_last_node->get_parent_offset() == 0
	) return nullptr;

	if (
		!segment->is_segment_compacted()
		&& current_last_node->get_prev_page_offset() == 0
		&& current_last_node->get_parent_offset() == 0
	) return nullptr;

	if (
		(segment->is_segment_compacted()
		&& current_last_node->get_next_page_offset() == 0
		) || (!segment->is_segment_compacted()
			&& current_last_node->get_prev_page_offset() == 0
		)
	) {
		unsigned long bytes_read = this->fh->read_from_file(
			segment->get_index_key(),
			segment->get_index_path(),
			INDEX_PAGE_SIZE,
			current_last_node->get_parent_offset(),
			page_data
		);

		if (bytes_read != INDEX_PAGE_SIZE)
			throw CorruptionException("Corrupted index file");

		next_last_node = std::shared_ptr<BTreeNode>(new BTreeNode(page_data));
	}

	if (next_last_node.get()->get_page_type() == PageType::LEAF) return next_last_node;

	unsigned long bytes_read = this->fh->read_from_file(
		segment->get_index_key(),
		segment->get_index_path(),
		INDEX_PAGE_SIZE,
		segment->is_segment_compacted() 
			? current_last_node->get_first_child()->val_pos 
			: current_last_node->get_last_child()->val_pos,
		page_data
	);

	if (bytes_read != INDEX_PAGE_SIZE)
		throw CorruptionException("Corrupted index file");

	next_last_node = std::shared_ptr<BTreeNode>(new BTreeNode(page_data));

	return next_last_node;
}

void CompactionHandler::parse_index_node_rows(Partition* partition, PartitionSegment* read_segment, std::shared_ptr<PartitionSegment> write_segment, BTreeNode* node, bool reverse_order) {
	int current_row_index = reverse_order
		? node->get_total_rows_count() - 1
		: 0;

	int step = reverse_order ? -1 : 1;

	unsigned int last_read_pos = 0;

	std::unique_ptr<char> read_batch = std::unique_ptr<char>(new char[READ_MESSAGES_BATCH_SIZE]);

	unsigned int last_message_offset = 0;
	unsigned int last_message_bytes = 0;
	unsigned int message_bytes = 0;
	unsigned int offset = 0;

	unsigned long long message_id = 0;

	std::unordered_map<unsigned int, unsigned int> messages_to_keep;
	unsigned int ignore_count = 0;

	unsigned int prev_kept_key = 0;
	unsigned int non_ignored_streak = 0;

	Partition dummy_partition = Partition(partition->get_partition_id(), partition->get_queue_name());
	dummy_partition.set_active_segment(write_segment);

	while (
		(reverse_order && current_row_index > 0)
		|| (!reverse_order && current_row_index < node->get_total_rows_count())
	) {
		unsigned long bytes_read = this->fh->read_from_file(
			read_segment->get_segment_key(),
			read_segment->get_segment_path(),
			READ_MESSAGES_BATCH_SIZE,
			last_read_pos == 0 ? node->get_nth_child(current_row_index)->val_pos : last_read_pos,
			read_batch.get()
		);

		while (offset + message_bytes < READ_MESSAGES_BATCH_SIZE) {
			memcpy_s(&message_bytes, TOTAL_METADATA_BYTES, (char*)read_batch.get() + offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);

			if (offset + message_bytes > READ_MESSAGES_BATCH_SIZE) break;

			if (!Helper::has_valid_checksum((char*)read_batch.get() + offset)) {
				memcpy_s(&message_id, MESSAGE_ID_SIZE, (char*)read_batch.get() + offset + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);
				std::string err_msg = "Message with offset " + std::to_string(message_id) + " was corrupted";
				throw CorruptionException(err_msg);
			}

			if (!this->ignore_message((char*)read_batch.get() + offset)) {
				non_ignored_streak++;

				if (non_ignored_streak > 1 && prev_kept_key > 0) messages_to_keep[prev_kept_key] += message_bytes;
				else {
					messages_to_keep[offset] = message_bytes;
					prev_kept_key = offset;
				}
			}
			else {
				ignore_count++;
				non_ignored_streak = 0;
				prev_kept_key = 0;
			}

			last_message_offset = offset;
			last_message_bytes = message_bytes;
			offset += message_bytes;
		}

		if (ignore_count == 0)
			this->mh->save_messages(&dummy_partition, read_batch.get(), READ_MESSAGES_BATCH_SIZE - (READ_MESSAGES_BATCH_SIZE - last_message_offset + last_message_bytes));
		else
			for(auto& iter : messages_to_keep)
				this->mh->save_messages(&dummy_partition, read_batch.get() + iter.first, iter.second);

		if (bytes_read >= READ_MESSAGES_BATCH_SIZE) {
			last_read_pos = last_message_offset + last_message_bytes;
			continue;
		}

		current_row_index += step;
		last_read_pos = 0;
	}
}