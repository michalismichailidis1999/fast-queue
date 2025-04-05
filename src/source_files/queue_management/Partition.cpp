#include "../../header_files/queue_management/Partition.h"

Partition::Partition(unsigned int partition_id) {
	this->partition_id = partition_id;
	this->current_segment = 0;
	this->oldest_segment = 0;
	this->largest_message_offset = 0;
}

int Partition::get_partition_id() {
	return this->partition_id;
}

unsigned long long Partition::get_current_segment() {
	std::lock_guard<std::mutex> lock(this->mut);
	return this->current_segment;
}

void Partition::set_current_segment(unsigned long long current_segment) {
	std::lock_guard<std::mutex> lock(this->mut);
	this->current_segment = current_segment;
}

unsigned long long Partition::get_oldest_segment() {
	std::lock_guard<std::mutex> lock(this->mut);
	return this->oldest_segment;
}

void Partition::set_oldest_segment(unsigned long long oldest_segment) {
	std::lock_guard<std::mutex> lock(this->mut);
	this->oldest_segment = oldest_segment;
}

PartitionSegment* Partition::get_active_segment() {
	std::lock_guard<std::mutex> lock(this->mut);
	return this->active_segment.get();
}

unsigned long long Partition::get_new_message_offset() {
	std::lock_guard<std::mutex> lock(this->mut);
	return ++this->largest_message_offset;
}

void Partition::set_largest_message_offset(unsigned long long largest_message_offset) {
	std::lock_guard<std::mutex> lock(this->mut);
	this->largest_message_offset = largest_message_offset;
}

void Partition::set_active_segment(std::shared_ptr<PartitionSegment> active_segment) {
	std::lock_guard<std::mutex> lock(this->mut);
	this->active_segment = active_segment;
	this->current_segment = this->active_segment.get()->get_id();
}