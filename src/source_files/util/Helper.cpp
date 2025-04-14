#include "../../header_files/util/Helper.h"

void Helper::add_common_metadata_values(void* metadata, int total_bytes) {
	memcpy_s((char*)metadata + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES, &total_bytes, TOTAL_METADATA_BYTES);
	memcpy_s((char*)metadata + VERSION_SIZE_OFFSET, VERSION_SIZE, &VERSION_INT_FORMAT, VERSION_SIZE);
}