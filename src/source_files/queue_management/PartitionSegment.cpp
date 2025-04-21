#include "../../header_files/queue_management/PartitionSegment.h"

PartitionSegment::PartitionSegment(unsigned long long id, std::string segment_path) {
	this->id = id;
	this->newest_message_timestamp = 0;
	this->oldest_message_timestamp = 0;
	this->newest_message_offset = 0;
	this->oldest_message_offset = 0;
	this->compacted = false;
	this->is_read_only = false;
	this->segment_path = segment_path ;
}

PartitionSegment::PartitionSegment(void* metadata, std::string segment_path) {
	if (metadata == NULL)
		throw std::exception("Partition metadata was NULL");

	this->segment_path = segment_path;

	memcpy_s(&this->id, SEGMENT_ID_SIZE, (char*)metadata + SEGMENT_ID_OFFSET, SEGMENT_ID_SIZE);
	memcpy_s(&this->oldest_message_timestamp, SEGMENT_OLDEST_MESSAGE_TMSTMP_SIZE, (char*)metadata + SEGMENT_OLDEST_MESSAGE_TMSTMP_OFFSET, SEGMENT_OLDEST_MESSAGE_TMSTMP_SIZE);
	memcpy_s(&this->newest_message_timestamp, SEGMENT_NEWEST_MESSAGE_TMSTMP_SIZE, (char*)metadata + SEGMENT_NEWEST_MESSAGE_TMSTMP_OFFSET, SEGMENT_NEWEST_MESSAGE_TMSTMP_SIZE);
	memcpy_s(&this->oldest_message_offset, SEGMENT_OLDEST_MESSAGE_OFF_SIZE, (char*)metadata + SEGMENT_OLDEST_MESSAGE_OFF_OFFSET, SEGMENT_OLDEST_MESSAGE_OFF_SIZE);
	memcpy_s(&this->newest_message_offset, SEGMENT_NEWEST_MESSAGE_OFF_SIZE, (char*)metadata + SEGMENT_NEWEST_MESSAGE_OFF_OFFSET, SEGMENT_NEWEST_MESSAGE_OFF_SIZE);

	memcpy_s(&this->is_read_only, SEGMENT_IS_READ_ONLY_SIZE, (char*)metadata + SEGMENT_IS_READ_ONLY_OFFSET, SEGMENT_IS_READ_ONLY_SIZE);
	memcpy_s(&this->compacted, SEGMENT_IS_COMPACTED_SIZE, (char*)metadata + SEGMENT_IS_COMPACTED_OFFSET, SEGMENT_IS_COMPACTED_SIZE);
}

unsigned long long PartitionSegment::get_id() {
	return this->id;
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

unsigned long long PartitionSegment::get_newest_message_offset() {
	std::lock_guard<std::mutex> lock(this->mut);
	return this->newest_message_offset;
}

void PartitionSegment::set_newest_message_offset(unsigned long long offset) {
	std::lock_guard<std::mutex> lock(this->mut);
	this->newest_message_offset = offset;
}

unsigned long long PartitionSegment::get_oldest_message_offset() {
	std::lock_guard<std::mutex> lock(this->mut);
	return this->oldest_message_offset;
}

void PartitionSegment::set_oldest_message_offset(unsigned long long offset) {
	std::lock_guard<std::mutex> lock(this->mut);
	this->oldest_message_offset = offset;
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

long PartitionSegment::add_written_bytes(long bytes) {
	std::lock_guard<std::mutex> lock(this->mut);
	this->total_written_bytes += bytes;
	return this->total_written_bytes;
}

std::tuple<long, std::shared_ptr<char>> PartitionSegment::get_metadata_bytes() {
	std::lock_guard<std::mutex> lock(this->mut);

	std::shared_ptr<char> bytes = std::shared_ptr<char>(new char[SEGMENT_METADATA_TOTAL_BYTES]);

	Helper::add_common_metadata_values((void*)(bytes.get()), SEGMENT_METADATA_TOTAL_BYTES, ObjectType::METADATA);

	memcpy_s(bytes.get() + SEGMENT_ID_OFFSET, SEGMENT_ID_SIZE, &this->id, SEGMENT_ID_SIZE);
	memcpy_s(bytes.get() + SEGMENT_OLDEST_MESSAGE_TMSTMP_OFFSET, SEGMENT_OLDEST_MESSAGE_TMSTMP_SIZE, &this->oldest_message_timestamp, SEGMENT_OLDEST_MESSAGE_TMSTMP_SIZE);
	memcpy_s(bytes.get() + SEGMENT_NEWEST_MESSAGE_TMSTMP_OFFSET, SEGMENT_NEWEST_MESSAGE_TMSTMP_SIZE, &this->newest_message_timestamp, SEGMENT_NEWEST_MESSAGE_TMSTMP_SIZE);
	memcpy_s(bytes.get() + SEGMENT_OLDEST_MESSAGE_OFF_OFFSET, SEGMENT_OLDEST_MESSAGE_OFF_SIZE, &this->oldest_message_offset, SEGMENT_OLDEST_MESSAGE_OFF_SIZE);
	memcpy_s(bytes.get() + SEGMENT_NEWEST_MESSAGE_OFF_OFFSET, SEGMENT_NEWEST_MESSAGE_OFF_SIZE, &this->newest_message_offset, SEGMENT_NEWEST_MESSAGE_OFF_SIZE);
	memcpy_s(bytes.get() + SEGMENT_IS_READ_ONLY_OFFSET, SEGMENT_IS_READ_ONLY_SIZE, &this->is_read_only, SEGMENT_IS_READ_ONLY_SIZE);
	memcpy_s(bytes.get() + SEGMENT_IS_COMPACTED_OFFSET, SEGMENT_IS_COMPACTED_SIZE, &this->compacted, SEGMENT_IS_READ_ONLY_SIZE);

	return std::tuple<long, std::shared_ptr<char>>(SEGMENT_METADATA_TOTAL_BYTES, bytes);
}