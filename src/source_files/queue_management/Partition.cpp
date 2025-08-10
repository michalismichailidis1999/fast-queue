#include "../../header_files/queue_management/Partition.h"

Partition::Partition(unsigned int partition_id, const std::string& queue_name) {
	this->queue_name = queue_name;
	this->partition_id = partition_id;
	this->current_segment_id = 0;
	this->smallest_segment_id = 0;
	this->smallest_uncompacted_segment_id = 0;
	this->last_message_offset = 0;
	this->last_replicated_offset = 0;
	this->message_map_key = "";
	this->message_map_path = "";
	this->offsets_key = "";
	this->offsets_path = "";
	this->consumer_offset_updates_count = 0;
}

const std::string& Partition::get_queue_name() {
	return this->queue_name;
}

unsigned int Partition::get_partition_id() {
	return this->partition_id;
}

unsigned long long Partition::get_current_segment_id() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->current_segment_id;
}

void Partition::set_current_segment_id(unsigned long long current_segment_id) {
	std::lock_guard<std::shared_mutex> lock(this->mut);
	this->current_segment_id = current_segment_id;
}

PartitionSegment* Partition::get_active_segment() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->active_segment.get();
}

std::shared_ptr<PartitionSegment> Partition::get_active_segment_ref() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->active_segment;
}

std::shared_ptr<PartitionSegment> Partition::set_active_segment(std::shared_ptr<PartitionSegment> segment) {
	std::lock_guard<std::shared_mutex> lock(this->mut);
	std::shared_ptr<PartitionSegment> old_active_segment = this->active_segment;
	this->active_segment = segment;
	this->current_segment_id = segment->get_id();
	return old_active_segment;
}

void Partition::set_message_map(const std::string& message_map_key, const std::string& message_map_path) {
	std::lock_guard<std::shared_mutex> lock(this->mut);
	this->message_map_key = message_map_key;
	this->message_map_path = message_map_path;
}

void Partition::set_offsets(const std::string& offsets_key, const std::string& offsets_path) {
	std::lock_guard<std::shared_mutex> lock(this->mut);
	this->offsets_key = offsets_key;
	this->offsets_path = offsets_path;
}

const std::string& Partition::get_message_map_key() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->message_map_key;
}

const std::string& Partition::get_message_map_path() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->message_map_path;
}

const std::string& Partition::get_offsets_key() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->offsets_key;
}

const std::string& Partition::get_offsets_path() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->offsets_path;
}

void Partition::set_smallest_uncompacted_segment_id(unsigned long long segment_id) {
	std::lock_guard<std::shared_mutex> lock(this->mut);
	this->smallest_uncompacted_segment_id = segment_id;
}

unsigned long long Partition::get_smallest_uncompacted_segment_id() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->smallest_uncompacted_segment_id;
}

void Partition::set_smallest_segment_id(unsigned long long segment_id) {
	std::lock_guard<std::shared_mutex> lock(this->mut);
	this->smallest_segment_id = segment_id;
}

unsigned long long Partition::get_smallest_segment_id() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->smallest_segment_id;
}

unsigned long long Partition::get_next_message_offset() {
	std::lock_guard<std::shared_mutex> lock(this->mut);
	return ++this->last_message_offset;
}

unsigned long long Partition::get_message_offset() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->last_message_offset;
}

void Partition::set_last_message_offset(unsigned long long last_message_offset) {
	std::lock_guard<std::shared_mutex> lock(this->mut);
	this->last_message_offset = last_message_offset;
}

unsigned long long Partition::get_last_replicated_offset() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->last_replicated_offset;

}

void Partition::set_last_replicated_offset(unsigned long long last_replicated_offset) {
	std::lock_guard<std::shared_mutex> lock(this->mut);
	this->last_replicated_offset = last_replicated_offset;
}

void Partition::add_consumer(std::shared_ptr<Consumer> consumer) {
	std::lock_guard<std::shared_mutex> lock(this->consumers_mut);
	this->consumers[consumer.get()->get_id()] = consumer;
}

std::shared_ptr<Consumer> Partition::get_consumer(unsigned long long consumer_id) {
	std::shared_lock<std::shared_mutex> lock(this->consumers_mut);
	return this->consumers[consumer_id];
}

void Partition::remove_consumer(unsigned long long consumer_id) {
	std::lock_guard<std::shared_mutex> lock(this->consumers_mut);
	this->consumers.erase(consumer_id);
}

std::vector<std::shared_ptr<Consumer>> Partition::get_all_consumers() {
	std::lock_guard<std::shared_mutex> lock(this->consumers_mut);

	std::vector<std::shared_ptr<Consumer>> consumers;

	for (auto& iter : this->consumers)
		consumers.emplace_back(iter.second);

	return consumers;
}

unsigned int Partition::increase_consumers_offset_update_count() {
	std::lock_guard<std::shared_mutex> lock(this->consumers_mut);
	return ++this->consumer_offset_updates_count;
}

void Partition::init_consumers_offset_update_count() {
	std::lock_guard<std::shared_mutex> lock(this->consumers_mut);
	this->consumer_offset_updates_count = 0;
}

void Partition::set_consumer_offsets_flushed_bytes(unsigned int consumer_offsets_flushed_bytes) {
	std::lock_guard<std::shared_mutex> lock(this->consumers_mut);
	this->consumer_offsets_flushed_bytes = consumer_offsets_flushed_bytes;
}

unsigned int Partition::get_consumer_offsets_flushed_bytes() {
	std::shared_lock<std::shared_mutex> lock(this->consumers_mut);
	return this->consumer_offsets_flushed_bytes;
}