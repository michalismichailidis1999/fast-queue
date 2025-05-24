#include "../../header_files/queue_management/CompactionHandler.h"

CompactionHandler::CompactionHandler(QueueManager* qm, SegmentLockManager* lock_manager, FileHandler* fh, QueueSegmentFilePathMapper* pm, Logger* logger, Settings* settings) {
	this->qm = qm;
	this->lock_manager = lock_manager;
	this->fh = fh;
	this->pm = pm;
	this->logger = logger;
	this->settings = settings;

	this->bf = std::unique_ptr<BloomFilter>(new BloomFilter(COMPACTION_BLOOM_FILTER_SIZE));
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

		this->compact_segment(&segment);
	}
	catch (const std::exception& ex)
	{
		success = false;
		std::string err_msg = "Error occured during segment compaction. Reason: " + std::string(ex.what());
		this->logger->log_error(err_msg);
	}

	this->lock_manager->release_segment_lock(partition, &segment);

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

void CompactionHandler::compact_segment(PartitionSegment* segment) {
	// TODO: Add logic here
}

bool CompactionHandler::continue_compaction(Queue* queue) {
	if (queue->get_metadata()->get_status() == Status::ACTIVE) return true;

	this->logger->log_info("Queue " + queue->get_metadata()->get_name() + " is not active. Skipping compaction check");

	return false;
}