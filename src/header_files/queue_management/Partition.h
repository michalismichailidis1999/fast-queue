#pragma once
#include <memory>
#include <mutex>
#include <map>
#include <string>
#include "./PartitionSegment.h"
#include "../Constants.h"

class Partition {
private:
	std::string queue_name;

	unsigned int partition_id;
	unsigned long long current_segment;
	unsigned long long oldest_segment;

	unsigned long long largest_message_offset;

	std::shared_ptr<PartitionSegment> active_segment;

	std::mutex mut;
public:
	Partition(unsigned int partition_id, const std::string& queue_name);

	const std::string& get_queue_name();

	unsigned int get_partition_id();

	unsigned long long get_current_segment();
	void set_current_segment(unsigned long long current_segment);

	unsigned long long get_oldest_segment();
	void set_oldest_segment(unsigned long long oldest_segment);

	unsigned long long get_new_message_offset();
	void set_largest_message_offset(unsigned long long largest_message_offset);

	PartitionSegment* get_active_segment();
	void set_active_segment(std::shared_ptr<PartitionSegment> segment);
};