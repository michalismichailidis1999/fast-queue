#include "../../header_files/queue_management/Partition.h"

Partition::Partition(unsigned int partition_id, const std::string& queue_name) {
	this->queue_name = queue_name;
	this->partition_id = partition_id;
	this->current_segment_id = 0;
}

const std::string& Partition::get_queue_name() {
	return this->queue_name;
}

unsigned int Partition::get_partition_id() {
	return this->partition_id;
}

unsigned long long Partition::get_current_segment_id() {
	std::lock_guard<std::mutex> lock(this->mut);
	return this->current_segment_id;
}

void Partition::set_current_segment_id(unsigned long long current_segment_id) {
	std::lock_guard<std::mutex> lock(this->mut);
	this->current_segment_id = current_segment_id;
}

PartitionSegment* Partition::get_active_segment() {
	std::lock_guard<std::mutex> lock(this->mut);
	return this->active_segment.get();
}

void Partition::set_active_segment(std::shared_ptr<PartitionSegment> segment) {
	std::lock_guard<std::mutex> lock(this->mut);
	this->active_segment = segment;
	this->current_segment_id = segment->get_id();
}