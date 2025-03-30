#include "Queue.h"

Queue::Queue(std::shared_ptr<QueueMetadata> metadata) {
	this->metadata = metadata;
}

QueueMetadata* Queue::get_metadata() {
	return this->metadata.get();
}

void Queue::add_partition(std::shared_ptr<Partition> partition) {
	std::lock_guard<std::mutex> lock(this->mut);
	this->partitions[partition.get()->get_partition_id()] = partition;
}

Partition* Queue::get_partition(unsigned int partition_id) {
	std::lock_guard<std::mutex> lock(this->mut);

	if (this->partitions.find(partition_id) == this->partitions.end()) return nullptr;

	return this->partitions[partition_id].get();
}