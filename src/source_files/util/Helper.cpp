#include "../../header_files/util/Helper.h"

void Helper::add_common_metadata_values(void* metadata, long total_bytes) {
	memcpy_s((char*)metadata + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES, &total_bytes, TOTAL_METADATA_BYTES);
	memcpy_s((char*)metadata + VERSION_SIZE_OFFSET, VERSION_SIZE, &VERSION_INT_FORMAT, VERSION_SIZE);

	unsigned long long checksum = crc32(
		crc32(0L, Z_NULL, 0), 
		reinterpret_cast<const Bytef*>(metadata), 
		total_bytes
	);

	memcpy_s((char*)metadata + CHECKSUM_OFFSET, CHECKSUM_SIZE, &checksum, CHECKSUM_SIZE);
}

bool Helper::has_valid_checksum(void* metadata) {
	unsigned long long metadata_checksum = 0;
	long total_bytes = 0;

	memcpy_s(&total_bytes, TOTAL_METADATA_BYTES, (char*)metadata + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);
	memcpy_s(&metadata_checksum, CHECKSUM_SIZE, (char*)metadata + CHECKSUM_OFFSET, CHECKSUM_SIZE);

	unsigned long long checksum = crc32(
		crc32(0L, Z_NULL, 0),
		reinterpret_cast<const Bytef*>(metadata),
		total_bytes
	);

	return metadata_checksum == checksum;
}