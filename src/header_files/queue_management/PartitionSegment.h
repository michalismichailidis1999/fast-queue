#pragma once
#include <chrono>
#include <memory>
#include <mutex>
#include "../Constants.h"
#include "../util/Helper.h"

class PartitionSegment {
private:
	unsigned long long id;

	long long newest_message_timestamp;
	long long oldest_message_timestamp;

	unsigned long long newest_message_offset;
	unsigned long long oldest_message_offset;

	bool compacted;

	bool is_read_only;

	std::string segment_path;

	std::mutex mut;
public:
	PartitionSegment(unsigned long long id, std::string segment_path);
	PartitionSegment(void* metadata, std::string segment_path);

	unsigned long long get_id();

	long long get_newest_message_timestamp();
	void set_newest_message_timestamp(long long timestamp);

	long long get_oldest_message_timestamp();
	void set_oldest_message_timestamp(long long timestamp);

	unsigned long long get_newest_message_offset();
	void set_newest_message_offset(unsigned long long offset);

	unsigned long long get_oldest_message_offset();
	void set_oldest_message_offset(unsigned long long offset);

	void set_to_compacted();
	bool is_segment_compacted();

	void set_to_read_only();
	bool get_is_read_only();

	std::tuple<long, std::shared_ptr<char>> get_metadata_bytes();
};
