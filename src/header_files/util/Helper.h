#pragma once
#include <zlib.h>
#include <string>
#include "../Constants.h"

class Helper {
public:
	static void add_common_metadata_values(void* metadata, long total_bytes);
	static void add_message_metadata_values(void* metadata, unsigned long long message_id, unsigned long long timestamp);
	static void retrieve_message_metadata_values(void* metadata, unsigned long long* message_id, unsigned long long* timestamp);
	static bool has_valid_checksum(void* metadata);
	static bool is_internal_queue(const std::string& queue_name);
};