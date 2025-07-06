#pragma once
#include <memory>
#include <shared_mutex>
#include <unordered_set>
#include <string>
#include "./PartitionSegment.h"
#include "../Constants.h"

class Partition {
private:
	std::string queue_name;

	unsigned int partition_id;

	std::shared_ptr<PartitionSegment> active_segment;
	unsigned long long current_segment_id;

	unsigned long long smallest_uncompacted_segment_id;
	unsigned long long smallest_segment_id;

	std::string message_map_key;
	std::string message_map_path;

	unsigned long long last_message_offset;

	std::shared_mutex mut;
public:
	Partition(unsigned int partition_id, const std::string& queue_name);

	const std::string& get_queue_name();

	unsigned int get_partition_id();

	unsigned long long get_current_segment_id();
	void set_current_segment_id(unsigned long long current_segment_id);

	void set_message_map(const std::string& message_map_key, const std::string& message_map_path);

	void set_smallest_uncompacted_segment_id(unsigned long long segment_id);
	unsigned long long get_smallest_uncompacted_segment_id();

	void set_smallest_segment_id(unsigned long long segment_id);
	unsigned long long get_smallest_segment_id();

	unsigned long long get_next_message_offset();
	void set_last_message_offset(unsigned long long last_message_offset);

	const std::string& get_message_map_key();
	const std::string& get_message_map_path();

	PartitionSegment* get_active_segment();
	std::shared_ptr<PartitionSegment> set_active_segment(std::shared_ptr<PartitionSegment> segment);

	friend class RetentionHandler;
	friend class CompactionHandler;
};