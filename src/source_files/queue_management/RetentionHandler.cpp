#include "../../header_files/queue_management/RetentionHandler.h"

RetentionHandler::RetentionHandler(QueueManager* qm, FileHandler* fh, QueueSegmentFilePathMapper* pm, Logger* logger, Settings* settings) {
	this->qm = qm;
	this->fh = fh;
	this->pm = pm;
	this->fh = fh;
	this->logger = logger;
	this->settings = settings;
}

void RetentionHandler::remove_expired_segments(std::atomic_bool* should_terminate) {
	std::vector<std::string> queue_names;

	while (!(*should_terminate)) {
		queue_names.clear();

		this->qm->get_all_queue_names(&queue_names);

		for (const std::string& queue_name : queue_names)
			this->handle_queue_partitions_segment_retention(queue_name, should_terminate);

		std::this_thread::sleep_for(std::chrono::milliseconds(5000));
	}
}

void RetentionHandler::handle_queue_partitions_segment_retention(const std::string& queue_name, std::atomic_bool* should_terminate) {
	std::shared_ptr<Queue> queue = this->qm->get_queue(queue_name);

	if (!(*should_terminate) && this->continue_retention(queue.get())) return;

	unsigned int partitions = queue->get_metadata()->get_partitions();

	for (unsigned int i = 0; i < partitions; i++) {
		if (!(*should_terminate) && this->continue_retention(queue.get())) return;

		Partition* partition = queue->get_partition(i);

		while (this->handle_partition_oldest_segment_retention(partition)) {
			std::lock_guard<std::shared_mutex> lock(partition->mut);

			unsigned long long current_smallest_segment_id = partition->smallest_segment_id;
			partition->smallest_segment_id = current_smallest_segment_id + 1;

			if (current_smallest_segment_id == partition->smallest_uncompacted_segment_id)
				partition->smallest_uncompacted_segment_id = current_smallest_segment_id + 1;
		}
	}
}

// Returns true if segment for retention is not found or segment was deleted due to retention policy
bool RetentionHandler::handle_partition_oldest_segment_retention(Partition* partition) {
	bool is_internal_queue = Helper::is_internal_queue(partition->get_queue_name());

	std::string segment_path = this->pm->get_file_path(
		partition->get_queue_name(),
		partition->get_smallest_segment_id(),
		is_internal_queue ? -1 : partition->get_partition_id()
	);

	std::string index_path = this->pm->get_file_path(
		partition->get_queue_name(),
		partition->get_smallest_segment_id(),
		is_internal_queue ? -1 : partition->get_partition_id(),
		true
	);

	// TODO: Fill logic here
	bool data_exists = this->fh->check_if_exists(segment_path);
	bool index_exists = this->fh->check_if_exists(index_path);

	if (!data_exists && !index_exists) return true;

	return false;
}

bool RetentionHandler::continue_retention(Queue* queue) {
	if (queue->get_metadata()->get_status() == Status::ACTIVE) return true;

	this->logger->log_info("Queue " + queue->get_metadata()->get_name() + " is not active. Skipping retention check");

	return false;
}