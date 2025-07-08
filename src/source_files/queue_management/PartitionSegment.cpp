#include "../../header_files/queue_management/PartitionSegment.h"

PartitionSegment::PartitionSegment(unsigned long long id, const std::string& segment_key, const std::string& segment_path) {
	this->id = id;
	this->last_message_timestamp = 0;
	this->last_message_offset = 0;
	this->compacted = false;
	this->is_read_only = false;
	this->segment_key = segment_key;
	this->segment_path = segment_path;
	this->index_key = "";
	this->index_path = "";
	this->total_written_bytes = 0;
}

PartitionSegment::PartitionSegment(void* metadata, const std::string& segment_key, const std::string& segment_path) {
	if (metadata == NULL)
		throw std::exception("Partition metadata was NULL");

	this->segment_key = segment_key;
	this->segment_path = segment_path;
	this->index_key = "";
	this->index_path = "";
	this->total_written_bytes = 0;

	memcpy_s(&this->id, SEGMENT_ID_SIZE, (char*)metadata + SEGMENT_ID_OFFSET, SEGMENT_ID_SIZE);
	memcpy_s(&this->last_message_timestamp, SEGMENT_LAST_MESSAGE_TMSTMP_SIZE, (char*)metadata + SEGMENT_LAST_MESSAGE_TMSTMP_OFFSET, SEGMENT_LAST_MESSAGE_TMSTMP_SIZE);
	memcpy_s(&this->last_message_offset, SEGMENT_LAST_MESSAGE_OFF_SIZE, (char*)metadata + SEGMENT_LAST_MESSAGE_OFF_OFFSET, SEGMENT_LAST_MESSAGE_OFF_SIZE);
	memcpy_s(&this->is_read_only, SEGMENT_IS_READ_ONLY_SIZE, (char*)metadata + SEGMENT_IS_READ_ONLY_OFFSET, SEGMENT_IS_READ_ONLY_SIZE);
	memcpy_s(&this->compacted, SEGMENT_IS_COMPACTED_SIZE, (char*)metadata + SEGMENT_IS_COMPACTED_OFFSET, SEGMENT_IS_COMPACTED_SIZE);
	memcpy_s(&this->last_index_page_offset, SEGMENT_LAST_INDEX_PAGE_OFFSET_SIZE, (char*)metadata + SEGMENT_LAST_INDEX_PAGE_OFFSET_OFFSET, SEGMENT_LAST_INDEX_PAGE_OFFSET_SIZE);
}

unsigned long long PartitionSegment::get_id() {
	return this->id;
}

const std::string& PartitionSegment::get_segment_key() {
	return this->segment_key;
}

const std::string& PartitionSegment::get_segment_path() {
	return this->segment_path;
}

const std::string& PartitionSegment::get_index_key() {
	return this->index_key;
}

const std::string& PartitionSegment::get_index_path() {
	return this->index_path;
}

void PartitionSegment::set_index(const std::string& index_key, const std::string& index_path) {
	this->index_key = index_key;
	this->index_path = index_path;
}

long long PartitionSegment::get_last_message_timestamp() {
	std::lock_guard<std::mutex> lock(this->mut);
	return this->last_message_timestamp;
}

void PartitionSegment::set_last_message_timestamp(long long timestamp) {
	std::lock_guard<std::mutex> lock(this->mut);
	this->last_message_timestamp = timestamp;
}

unsigned long long PartitionSegment::get_last_message_offset() {
	std::lock_guard<std::mutex> lock(this->mut);
	return this->last_message_offset;
}
void PartitionSegment::set_last_message_offset(unsigned long long offset) {
	std::lock_guard<std::mutex> lock(this->mut);
	this->last_message_offset = offset;
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

unsigned int PartitionSegment::get_last_index_page_offset(bool increase_before_get) {
	std::lock_guard<std::mutex> lock(this->mut);
	if (increase_before_get) this->last_index_page_offset++;
	return this->last_index_page_offset;
}

void PartitionSegment::set_last_index_page_offset(unsigned int last_index_page_offset) {
	std::lock_guard<std::mutex> lock(this->mut);
	this->last_index_page_offset = last_index_page_offset;
}

unsigned long long PartitionSegment::add_written_bytes(unsigned long bytes) {
	std::lock_guard<std::mutex> lock(this->mut);
	this->total_written_bytes += bytes;
	return this->total_written_bytes;
}

void PartitionSegment::set_total_written_bytes(unsigned long long total_written_bytes) {
	std::lock_guard<std::mutex> lock(this->mut);
	this->total_written_bytes = total_written_bytes;
}

std::tuple<long, std::shared_ptr<char>> PartitionSegment::get_metadata_bytes() {
	std::lock_guard<std::mutex> lock(this->mut);

	std::shared_ptr<char> bytes = std::shared_ptr<char>(new char[SEGMENT_METADATA_TOTAL_BYTES]);

	memcpy_s(bytes.get() + SEGMENT_ID_OFFSET, SEGMENT_ID_SIZE, &this->id, SEGMENT_ID_SIZE);
	memcpy_s(bytes.get() + SEGMENT_LAST_MESSAGE_TMSTMP_OFFSET, SEGMENT_LAST_MESSAGE_TMSTMP_SIZE, &this->last_message_timestamp, SEGMENT_LAST_MESSAGE_TMSTMP_SIZE);
	memcpy_s(bytes.get() + SEGMENT_LAST_MESSAGE_OFF_OFFSET, SEGMENT_LAST_MESSAGE_OFF_SIZE, &this->last_message_offset, SEGMENT_LAST_MESSAGE_OFF_SIZE);
	memcpy_s(bytes.get() + SEGMENT_IS_READ_ONLY_OFFSET, SEGMENT_IS_READ_ONLY_SIZE, &this->is_read_only, SEGMENT_IS_READ_ONLY_SIZE);
	memcpy_s(bytes.get() + SEGMENT_IS_COMPACTED_OFFSET, SEGMENT_IS_COMPACTED_SIZE, &this->compacted, SEGMENT_IS_READ_ONLY_SIZE);
	memcpy_s(bytes.get() + SEGMENT_LAST_INDEX_PAGE_OFFSET_OFFSET, SEGMENT_LAST_INDEX_PAGE_OFFSET_SIZE, &this->last_index_page_offset, SEGMENT_LAST_INDEX_PAGE_OFFSET_SIZE);

	Helper::add_common_metadata_values((void*)(bytes.get()), SEGMENT_METADATA_TOTAL_BYTES, ObjectType::METADATA);

	return std::tuple<long, std::shared_ptr<char>>(SEGMENT_METADATA_TOTAL_BYTES, bytes);
}