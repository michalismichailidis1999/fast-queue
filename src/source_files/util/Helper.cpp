#include "../../header_files/util/Helper.h"

void Helper::add_common_metadata_values(void* metadata, long total_bytes) {
	memcpy_s((char*)metadata + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES, &total_bytes, TOTAL_METADATA_BYTES);
	memcpy_s((char*)metadata + VERSION_SIZE_OFFSET, VERSION_SIZE, &VERSION_INT_FORMAT, VERSION_SIZE);

	unsigned long long checksum = crc32(
		crc32(0L, Z_NULL, 0), 
		reinterpret_cast<const Bytef*>((char*)metadata + COMMON_METADATA_TOTAL_BYTES),
		total_bytes - COMMON_METADATA_TOTAL_BYTES
	);

	memcpy_s((char*)metadata + CHECKSUM_OFFSET, CHECKSUM_SIZE, &checksum, CHECKSUM_SIZE);
}

void Helper::update_checksum(void* metadata, long total_bytes) {
	unsigned long long checksum = crc32(
		crc32(0L, Z_NULL, 0),
		reinterpret_cast<const Bytef*>((char*)metadata + COMMON_METADATA_TOTAL_BYTES),
		total_bytes - COMMON_METADATA_TOTAL_BYTES
	);

	memcpy_s((char*)metadata + CHECKSUM_OFFSET, CHECKSUM_SIZE, &checksum, CHECKSUM_SIZE);
}

void Helper::add_message_metadata_values(void* metadata, unsigned long long message_id, unsigned long long timestamp, unsigned int key_size, const char* key, unsigned int key_offset, unsigned long long leader_id) {
	memcpy_s((char*)metadata + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE, &message_id, MESSAGE_ID_SIZE);
	memcpy_s((char*)metadata + MESSAGE_TIMESTAMP_OFFSET, MESSAGE_TIMESTAMP_SIZE, &timestamp, MESSAGE_TIMESTAMP_SIZE);

	bool is_active = true;

	memcpy_s((char*)metadata + MESSAGE_IS_ACTIVE_OFFSET, MESSAGE_IS_ACTIVE_SIZE, &is_active, MESSAGE_IS_ACTIVE_SIZE);
	memcpy_s((char*)metadata + MESSAGE_LEADER_ID_OFFSET, MESSAGE_LEADER_ID_SIZE, &leader_id, MESSAGE_LEADER_ID_SIZE);

	if (key != NULL && key_size > 0)
	{
		memcpy_s((char*)metadata + MESSAGE_KEY_OFFSET, MESSAGE_KEY_SIZE, &key_size, MESSAGE_KEY_SIZE);
		memcpy_s((char*)metadata + key_offset, key_size, key, key_size);
	}
}

void Helper::retrieve_message_metadata_values(void* metadata, unsigned long long* message_id, unsigned long long* timestamp) {
	memcpy_s(message_id, MESSAGE_ID_SIZE, (char*)metadata + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);
	memcpy_s(timestamp, MESSAGE_TIMESTAMP_SIZE, (char*)metadata + MESSAGE_TIMESTAMP_OFFSET, MESSAGE_TIMESTAMP_SIZE);
}

bool Helper::has_valid_checksum(void* metadata) {
	unsigned long long metadata_checksum = 0;
	long total_bytes = 0;

	memcpy_s(&total_bytes, TOTAL_METADATA_BYTES, (char*)metadata + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);
	memcpy_s(&metadata_checksum, CHECKSUM_SIZE, (char*)metadata + CHECKSUM_OFFSET, CHECKSUM_SIZE);

	unsigned long long checksum = crc32(
		crc32(0L, Z_NULL, 0),
		reinterpret_cast<const Bytef*>((char*)metadata + COMMON_METADATA_TOTAL_BYTES),
		total_bytes - COMMON_METADATA_TOTAL_BYTES
	);

	return metadata_checksum == checksum;
}

bool Helper::is_internal_queue(const std::string& queue_name) {
	return queue_name == CLUSTER_METADATA_QUEUE_NAME || queue_name == TRANSACTIONS_QUEUE_NAME;
}

unsigned int Helper::abs(int val) {
	return val < 0 ? val * -1 : val;
}

unsigned long long Helper::get_min(unsigned long long val1, unsigned long long val2) {
	return val1 <= val2 ? val1 : val2;
}