#include "../../header_files/util/Helper.h"

void Helper::add_common_metadata_values(void* metadata, long total_bytes, ObjectType type) {
	memcpy_s((char*)metadata + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES, &total_bytes, TOTAL_METADATA_BYTES);
	memcpy_s((char*)metadata + VERSION_SIZE_OFFSET, VERSION_SIZE, &VERSION_INT_FORMAT, VERSION_SIZE);
	memcpy_s((char*)metadata + OBJECT_TYPE_OFFSET, OBJECT_TYPE_SIZE, &type, OBJECT_TYPE_SIZE);

	unsigned long long checksum = crc32(
		crc32(0L, Z_NULL, 0), 
		reinterpret_cast<const Bytef*>((char*)metadata + COMMON_METADATA_TOTAL_BYTES),
		total_bytes - COMMON_METADATA_TOTAL_BYTES
	);

	memcpy_s((char*)metadata + CHECKSUM_OFFSET, CHECKSUM_SIZE, &checksum, CHECKSUM_SIZE);
}

void Helper::add_message_metadata_values(void* metadata, unsigned long long message_id, unsigned long long timestamp, unsigned int key_size, const char* key) {
	memcpy_s((char*)metadata + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE, &message_id, MESSAGE_ID_SIZE);
	memcpy_s((char*)metadata + MESSAGE_TIMESTAMP_OFFSET, MESSAGE_TIMESTAMP_SIZE, &timestamp, MESSAGE_TIMESTAMP_SIZE);

	if (key != NULL && key_size > 0)
	{
		memcpy_s((char*)metadata + MESSAGE_KEY_LENGTH_OFFSET, MESSAGE_KEY_LENGTH_SIZE, &key_size, MESSAGE_KEY_LENGTH_SIZE);
		memcpy_s((char*)metadata + MESSAGE_KEY_OFFSET, MESSAGE_KEY_SIZE, &key, MESSAGE_KEY_SIZE);
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
	return queue_name == CLUSTER_METADATA_QUEUE_NAME;
}