#include "../../header_files/queue_management/CompactionHandler.h"

CompactionHandler::CompactionHandler(QueueManager* qm, SegmentLockManager* lock_manager, FileHandler* fh, QueueSegmentFilePathMapper* pm, Logger* logger, Settings* settings) {
	this->qm = qm;
	this->lock_manager = lock_manager;
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

	if (queue == nullptr || !queue->get_metadata()->has_segment_compaction()) return;

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

	try
	{
		// Shared lock can be used since data will only be read and written to different file
		this->lock_manager->lock_segment(partition, &segment);

		if (is_internal_queue) this->compact_internal_segment(&segment);
		else this->compact_segment(partition, &segment);
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

		// TODO: Delete original segment and rename compacted segment to original's segment name

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

void CompactionHandler::compact_segment(Partition* partition, PartitionSegment* segment) {
	this->bf->reset();

	auto write_tup_res = this->initialize_compacted_segment_write_locations(partition, segment);

	std::unique_ptr<char> current_last_index_page = std::unique_ptr<char>(new char [INDEX_PAGE_SIZE]);
	std::unique_ptr<BTreeNode> current_last_node = nullptr;

	unsigned int page_offset = 0;

	while (true) {
		unsigned long bytes_read = this->fh->read_from_file(
			segment->get_index_key(),
			segment->get_index_path(),
			INDEX_PAGE_SIZE,
			page_offset,
			current_last_index_page.get()
		);

		if (bytes_read != INDEX_PAGE_SIZE)
			throw CorruptionException("Corrupted index file");

		current_last_node = std::unique_ptr<BTreeNode>(new BTreeNode(current_last_index_page.get()));

		if (current_last_node.get()->get_page_type() == PageType::LEAF) break;

		if (current_last_node.get()->get_next_page_offset() != 0)
			page_offset = current_last_node.get()->get_next_page_offset();
		else
			page_offset = current_last_node.get()->get_last_child()->val_pos;
	}

	unsigned int last_read_pointer = 0;

	while (current_last_node != nullptr) {
		// TODO: Complete logic here
	}
}

void CompactionHandler::compact_internal_segment(PartitionSegment* segment) {
	// TODO: Complete logic here
}

bool CompactionHandler::continue_compaction(Queue* queue) {
	if (queue->get_metadata()->get_status() == Status::ACTIVE) return true;

	this->logger->log_info("Queue " + queue->get_metadata()->get_name() + " is not active. Skipping compaction check");

	return false;
}

bool CompactionHandler::ignore_message(void* message) {
	if (!this->bf->has(NULL, 0)) return false;

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

	return std::tuple<std::shared_ptr<PartitionSegment>, std::shared_ptr<BTreeNode>>(
		write_segment, write_node
	);
}