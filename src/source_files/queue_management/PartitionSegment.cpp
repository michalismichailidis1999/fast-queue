#include "../../header_files/queue_management/PartitionSegment.h"

PartitionSegment::PartitionSegment(unsigned long long id, std::string segment_path) {
	this->id = id;
	this->total_messages = 0;
	this->newest_message_timestamp = 0;
	this->oldest_message_timestamp = 0;
	this->first_message_offset = 0;
	this->last_message_offset = 0;
	this->compacted = false;
	this->is_read_only = false;
	this->segment_path = segment_path ;
}

PartitionSegment::PartitionSegment(unsigned long long id, void* metadata, std::string segment_path) {
	if (metadata == NULL)
		throw std::exception("Partition metadata was NULL");

	this->id = id;
	this->segment_path = segment_path;

	memcpy_s(&this->total_messages, SEGMENT_TOTAL_MESSAGES_SIZE, (char*)metadata + SEGMENT_TOTAL_MESSAGES_OFFSET, SEGMENT_TOTAL_MESSAGES_SIZE);
	memcpy_s(&this->newest_message_timestamp, SEGMENT_NEWEST_MESSAGE_TMSTMP_SIZE, (char*)metadata + SEGMENT_NEWEST_MESSAGE_TMSTMP_OFFSET, SEGMENT_NEWEST_MESSAGE_TMSTMP_SIZE);
	memcpy_s(&this->oldest_message_timestamp, SEGMENT_OLDEST_MESSAGE_TMSTMP_SIZE, (char*)metadata + SEGMENT_OLDEST_MESSAGE_TMSTMP_OFFSET, SEGMENT_OLDEST_MESSAGE_TMSTMP_SIZE);
}

unsigned long long PartitionSegment::get_id() {
	return this->id;
}

int PartitionSegment::get_total_messages() {
	std::lock_guard<std::mutex> lock(this->mut);
	return this->total_messages;
}

void PartitionSegment::increase_total_messages(int new_messages) {
	std::lock_guard<std::mutex> lock(this->mut);
	this->total_messages += new_messages;
}

long long PartitionSegment::get_newest_message_timestamp() {
	std::lock_guard<std::mutex> lock(this->mut);
	return this->newest_message_timestamp;
}

void PartitionSegment::set_newest_message_timestamp(long long timestamp) {
	std::lock_guard<std::mutex> lock(this->mut);
	this->newest_message_timestamp = timestamp;
}

long long PartitionSegment::get_oldest_message_timestamp() {
	std::lock_guard<std::mutex> lock(this->mut);
	return this->oldest_message_timestamp;
}

void PartitionSegment::set_oldest_message_timestamp(long long timestamp) {
	std::lock_guard<std::mutex> lock(this->mut);
	this->oldest_message_timestamp = timestamp;
}

void PartitionSegment::set_to_compacted() {
	std::lock_guard<std::mutex> lock(this->mut);
	this->compacted = true;
}

bool PartitionSegment::is_segment_compacted() {
	std::lock_guard<std::mutex> lock(this->mut);
	return this->compacted;
}

void PartitionSegment::set_to_read_only() {
	std::lock_guard<std::mutex> lock(this->mut);
	this->is_read_only = true;;
}

bool PartitionSegment::get_is_read_only() {
	std::lock_guard<std::mutex> lock(this->mut);
	return this->is_read_only;
}

std::tuple<int, std::shared_ptr<char>> PartitionSegment::get_metadata_bytes() {
	std::lock_guard<std::mutex> lock(this->mut);

	std::shared_ptr<char> bytes = std::shared_ptr<char>(new char[SEGMENT_METADATA_TOTAL_BYTES]);

	memcpy_s(bytes.get() + SEGMENT_ID_OFFSET, SEGMENT_ID_SIZE, &this->id, SEGMENT_ID_SIZE);
	memcpy_s(bytes.get() + SEGMENT_TOTAL_MESSAGES_OFFSET, SEGMENT_TOTAL_MESSAGES_SIZE, &this->total_messages, SEGMENT_TOTAL_MESSAGES_SIZE);
	memcpy_s(bytes.get() + SEGMENT_NEWEST_MESSAGE_TMSTMP_OFFSET, SEGMENT_NEWEST_MESSAGE_TMSTMP_SIZE, &this->newest_message_timestamp, SEGMENT_NEWEST_MESSAGE_TMSTMP_SIZE);
	memcpy_s(bytes.get() + SEGMENT_OLDEST_MESSAGE_TMSTMP_OFFSET, SEGMENT_OLDEST_MESSAGE_TMSTMP_SIZE, &this->oldest_message_timestamp, SEGMENT_OLDEST_MESSAGE_TMSTMP_SIZE);

	return std::tuple<int, std::shared_ptr<char>>(SEGMENT_METADATA_TOTAL_BYTES, bytes);
}