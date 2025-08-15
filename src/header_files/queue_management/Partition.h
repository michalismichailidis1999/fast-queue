#pragma once
#include <vector>
#include <memory>
#include <shared_mutex>
#include <unordered_set>
#include <unordered_map>
#include <string>
#include "./PartitionSegment.h"
#include "./messages_management/Consumer.h"
#include "../Constants.h"

#include "../__linux/memcpy_s.h"

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

	std::string offsets_key;
	std::string offsets_path;

	unsigned long long last_message_offset;

	unsigned long long last_replicated_offset;

	unsigned int consumer_offsets_flushed_bytes;

	unsigned int consumer_offset_updates_count;
	std::unordered_map<unsigned long long, std::shared_ptr<Consumer>> consumers;

	std::shared_mutex mut;
	std::shared_mutex consumers_mut;
public:
	Partition(unsigned int partition_id, const std::string& queue_name);

	const std::string& get_queue_name();

	unsigned int get_partition_id();

	unsigned long long get_current_segment_id();
	void set_current_segment_id(unsigned long long current_segment_id);

	void set_message_map(const std::string& message_map_key, const std::string& message_map_path);
	void set_offsets(const std::string& offsets_key, const std::string& offsets_path);

	void set_smallest_uncompacted_segment_id(unsigned long long segment_id);
	unsigned long long get_smallest_uncompacted_segment_id();

	void set_smallest_segment_id(unsigned long long segment_id);
	unsigned long long get_smallest_segment_id();

	unsigned long long get_next_message_offset();
	unsigned long long get_message_offset();
	void set_last_message_offset(unsigned long long last_message_offset);

	unsigned long long get_last_replicated_offset();
	void set_last_replicated_offset(unsigned long long last_replicated_offset);

	const std::string& get_message_map_key();
	const std::string& get_message_map_path();

	const std::string& get_offsets_key();
	const std::string& get_offsets_path();

	PartitionSegment* get_active_segment();
	std::shared_ptr<PartitionSegment> get_active_segment_ref();
	std::shared_ptr<PartitionSegment> set_active_segment(std::shared_ptr<PartitionSegment> segment);

	void add_consumer(std::shared_ptr<Consumer> consumer);
	std::shared_ptr<Consumer> get_consumer(unsigned long long consumer_id);
	void remove_consumer(unsigned long long consumer_id);
	std::vector<std::shared_ptr<Consumer>> get_all_consumers();

	unsigned int increase_consumers_offset_update_count();
	void init_consumers_offset_update_count();

	void set_consumer_offsets_flushed_bytes(unsigned int consumer_offsets_flushed_bytes);
	unsigned int get_consumer_offsets_flushed_bytes();

	friend class RetentionHandler;
	friend class CompactionHandler;
	friend class MessageOffsetAckHandler;
};