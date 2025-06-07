#include "../../header_files/queue_management/RetentionHandler.h"

RetentionHandler::RetentionHandler(QueueManager* qm, SegmentLockManager* lock_manager, FileHandler* fh, QueueSegmentFilePathMapper* pm, Util* util, Logger* logger, Settings* settings) {
	this->qm = qm;
	this->lock_manager = lock_manager;
	this->fh = fh;
	this->pm = pm;
	this->util = util;
	this->logger = logger;
	this->settings = settings;
}

void RetentionHandler::remove_expired_segments(std::atomic_bool* should_terminate) {
	std::vector<std::string> queue_names;

	while (!(*should_terminate)) {
		if (this->settings->get_retention_ms() == 0) {
			std::this_thread::sleep_for(std::chrono::milliseconds(this->settings->get_retention_worker_wait_ms()));
			continue;
		}

		queue_names.clear();

		this->qm->get_all_queue_names(&queue_names);

		for (const std::string& queue_name : queue_names)
			this->handle_queue_partitions_segment_retention(queue_name, should_terminate);

		std::this_thread::sleep_for(std::chrono::milliseconds(this->settings->get_retention_worker_wait_ms()));
	}
}

void RetentionHandler::handle_queue_partitions_segment_retention(const std::string& queue_name, std::atomic_bool* should_terminate) {
	if (Helper::is_internal_queue(queue_name)) return;

	std::shared_ptr<Queue> queue = this->qm->get_queue(queue_name);

	if (queue == nullptr || queue.get()->get_metadata()->get_cleanup_policy() != CleanupPolicyType::DELETE_SEGMENTS) return;

	if (!(*should_terminate) && this->continue_retention(queue.get())) return;

	unsigned int partitions = queue->get_metadata()->get_partitions();

	for (unsigned int i = 0; i < partitions; i++) {
		if (!(*should_terminate) && this->continue_retention(queue.get())) return;

		Partition* partition = queue->get_partition(i);
		unsigned int segments_checked_count = 20;

		while (this->handle_partition_oldest_segment_retention(partition) && segments_checked_count > 0)
			segments_checked_count--;
	}
}

// Returns true if segment for retention is not found or segment was deleted due to retention policy
bool RetentionHandler::handle_partition_oldest_segment_retention(Partition* partition) {
	unsigned long long segment_id = partition->get_smallest_segment_id();

	if (partition->get_current_segment_id() == segment_id) return false;

	std::string segment_path = this->pm->get_file_path(
		partition->get_queue_name(),
		segment_id,
		partition->get_partition_id()
	);

	std::string index_path = this->pm->get_file_path(
		partition->get_queue_name(),
		segment_id,
		partition->get_partition_id(),
		true
	);

	bool data_exists = this->fh->check_if_exists(segment_path);
	bool index_exists = this->fh->check_if_exists(index_path);

	if (!data_exists && !index_exists) return true;

	std::unique_ptr<char> segment_bytes = std::unique_ptr<char>(new char[SEGMENT_METADATA_TOTAL_BYTES]);

	std::string segment_key = this->pm->get_file_path(
		partition->get_queue_name(),
		segment_id,
		partition->get_partition_id()
	);

	std::string index_key = this->pm->get_file_key(
		partition->get_queue_name(),
		segment_id,
		partition->get_partition_id(),
		true
	);

	this->fh->read_from_file(segment_key, segment_path, SEGMENT_METADATA_TOTAL_BYTES, 0, segment_bytes.get());

	PartitionSegment segment = PartitionSegment(segment_bytes.get(), segment_key, segment_path);

	if (!segment.get_is_read_only() || !this->util->has_timeframe_expired(segment.get_last_message_timestamp(), this->settings->get_retention_ms())) 
		return false;

	bool success = true;

	try
	{
		this->lock_manager->lock_segment(partition, &segment, true);

		this->fh->delete_dir_or_file(index_path, index_key);
		this->fh->delete_dir_or_file(segment_path, segment_key);

		std::lock_guard<std::shared_mutex> lock(partition->mut);

		unsigned long long current_smallest_segment_id = segment_id;
		partition->smallest_segment_id = segment_id + 1;

		if (current_smallest_segment_id == partition->smallest_uncompacted_segment_id)
			partition->smallest_uncompacted_segment_id = current_smallest_segment_id + 1;
	}
	catch (const std::exception& ex)
	{
		success = false;
		std::string err_msg = "Error occured during segment retention. Reason: " + std::string(ex.what());
		this->logger->log_error(err_msg);
	}

	this->lock_manager->release_segment_lock(partition, &segment, true);

	return success;
}

bool RetentionHandler::continue_retention(Queue* queue) {
	if (queue->get_metadata()->get_status() == Status::ACTIVE) return true;

	this->logger->log_info("Queue " + queue->get_metadata()->get_name() + " is not active. Skipping retention check");

	return false;
}