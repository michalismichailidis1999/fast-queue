#pragma once
#include <zlib.h>
#include <string>
#include "../Constants.h"
#include "../Enums.h"

#include "../__linux/memcpy_s.h"

class Helper {
public:
	static void add_common_metadata_values(void* metadata, long total_bytes);
	static void update_checksum(void* metadata, long total_bytes);
	static void add_message_metadata_values(void* metadata, unsigned long long message_id, unsigned long long timestamp, unsigned int key_size = 0, const char* key = NULL, unsigned int key_offset = MESSAGE_TOTAL_BYTES, unsigned long long leader_id = 0);
	static void retrieve_message_metadata_values(void* metadata, unsigned long long* message_id, unsigned long long* timestamp);
	static bool has_valid_checksum(void* metadata);
	static bool is_internal_queue(const std::string& queue_name);
	static unsigned int abs(int val);
	static unsigned long long get_min(unsigned long long val1, unsigned long long val2);
	static unsigned long long get_max(unsigned long long val1, unsigned long long val2);
	static bool starts_with(const std::string& value, const std::string& prefix);
};