#include "../../header_files/queue_management/QueueManager.h"

QueueManager::QueueManager(FileHandler* fh, QueueSegmentFilePathMapper* pm, Logger* logger) {
	this->fh = fh;
	this->pm = pm;
	this->logger = logger;
}

void QueueManager::add_queue(std::shared_ptr<Queue> queue) {
	std::lock_guard<std::mutex> lock(this->mut);
	this->queues[queue.get()->get_metadata()->get_name()] = queue;
}

bool QueueManager::has_queue(const std::string& queue_name) {
	std::lock_guard<std::mutex> lock(this->mut);
	return this->queues.find(queue_name) != this->queues.end();
}

std::shared_ptr<Queue> QueueManager::get_queue(const std::string& queue_name) {
	std::lock_guard<std::mutex> lock(this->mut);

	if (this->queues.find(queue_name) == this->queues.end()) return nullptr;

	return this->queues[queue_name];
}

void QueueManager::remove_queue(const std::string& queue_name) {
	std::lock_guard<std::mutex> lock(this->mut);
	this->queues.erase(queue_name);
}

void QueueManager::get_all_queue_names(std::vector<std::string>* queues_list) {
	std::lock_guard<std::mutex> lock(this->mut);

	for (auto iter : this->queues)
		(*queues_list).push_back(iter.first);
}

void QueueManager::create_queue(QueueMetadata* metadata) {
	std::string queue_path = this->pm->get_queue_folder_path(metadata->get_name());

	if (!this->fh->check_if_exists(queue_path))
		this->fh->create_directory(queue_path);

	std::string queue_metadata_path = this->pm->get_metadata_file_path(metadata->get_name());

	if (!this->fh->check_if_exists(queue_metadata_path)) {
		std::tuple<long, std::shared_ptr<char>> bytes_tup = metadata->get_metadata_bytes();

		this->fh->create_new_file(
			queue_metadata_path,
			std::get<0>(bytes_tup),
			std::get<1>(bytes_tup).get(),
			"",
			true
		);
	}

	std::shared_ptr<QueueMetadata> new_metadata = std::shared_ptr<QueueMetadata>(
		new QueueMetadata(metadata->get_name(), metadata->get_partitions(), metadata->get_replication_factor())
	);

	std::shared_ptr<Queue> queue = std::shared_ptr<Queue>(new Queue(new_metadata));

	for (int i = 0; i < metadata->get_partitions(); i++) {
		std::string partition_path = this->pm->get_partition_folder_path(metadata->get_name(), i);

		if (partition_path != "" && !this->fh->check_if_exists(partition_path))
			this->fh->create_directory(partition_path);

		std::shared_ptr<Partition> partition = std::shared_ptr<Partition>(new Partition(i, metadata->get_name()));

		queue->add_partition(partition);

		std::string segment_path = this->pm->get_file_path(metadata->get_name(), 1, i);

		std::shared_ptr<PartitionSegment> segment = std::shared_ptr<PartitionSegment>(new PartitionSegment(1, segment_path));

		partition->set_active_segment(segment);

		if (!this->fh->check_if_exists(segment_path))
		{
			std::tuple<long, std::shared_ptr<char>> bytes_tup = segment.get()->get_metadata_bytes();

			this->fh->create_new_file(
				segment_path,
				std::get<0>(bytes_tup),
				std::get<1>(bytes_tup).get(),
				this->pm->get_file_key(metadata->get_name(), 1),
				true
			);
		}
	}

	if (!this->has_queue(metadata->get_name()))
		this->add_queue(queue);
}

void QueueManager::delete_queue(const std::string& queue_name) {

}