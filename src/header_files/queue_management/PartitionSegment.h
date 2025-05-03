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

	unsigned int last_index_page_offset;

	unsigned int last_index_non_leaf_offset;

	std::string segment_key;
	std::string segment_path;

	std::string index_key;
	std::string index_path;

	unsigned long total_written_bytes;

	std::mutex mut;
public:
	PartitionSegment(unsigned long long id, const std::string& segment_key, const std::string& segment_path);
	PartitionSegment(void* metadata, const std::string& segment_key, const std::string& segment_path);

	unsigned long long get_id();

	const std::string& get_segment_key();
	const std::string& get_segment_path();

	const std::string& get_index_key();
	const std::string& get_index_path();

	void set_index(const std::string& index_key, const std::string& index_path);

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

	unsigned int get_last_index_page_offset(bool increase_before_get = false);
	void set_last_index_page_offset(unsigned int last_index_page_offset);

	unsigned int get_last_index_non_leaf_offset();
	void set_last_index_non_leaf_offset(unsigned int last_index_non_leaf_offset);

	unsigned long add_written_bytes(unsigned long bytes);

	std::tuple<long, std::shared_ptr<char>> get_metadata_bytes();
};
