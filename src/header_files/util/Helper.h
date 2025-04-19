#pragma once
#include <zlib.h>
#include <string>
#include "../Constants.h"

class Helper {
public:
	static void add_common_metadata_values(void* metadata, long total_bytes);
	static bool has_valid_checksum(void* metadata);
	static bool is_internal_queue(const std::string& queue_name);
};