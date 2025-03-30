#pragma once
#include <chrono>
#include <memory>
#include <mutex>

class PartitionSegment {
private:
	unsigned long long id;
	int total_messages;
	long long newest_message_timestamp;
	long long oldest_message_timestamp;

	unsigned long long first_message_offset;
	unsigned long long last_message_offset;

	bool compacted;

	bool is_read_only;

	std::string segment_path;

	std::mutex mut;
public:
	PartitionSegment(unsigned long long id, std::string segment_path);
	PartitionSegment(unsigned long long id, void* metadata, std::string segment_path);

	unsigned long long get_id();

	int get_total_messages();
	void increase_total_messages(int new_messages);

	long long get_newest_message_timestamp();
	void set_newest_message_timestamp(long long timestamp);

	long long get_oldest_message_timestamp();
	void set_oldest_message_timestamp(long long timestamp);

	void set_to_compacted();
	bool is_segment_compacted();

	void set_to_read_only();
	bool get_is_read_only();

	std::tuple<int, std::shared_ptr<char>> get_metadata_bytes();
};
