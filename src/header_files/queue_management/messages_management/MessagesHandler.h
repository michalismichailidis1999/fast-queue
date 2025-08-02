#pragma once
#include <memory>
#include <mutex>
#include "../../file_management/DiskFlusher.h"
#include "../../file_management/DiskReader.h"
#include "../../file_management/QueueSegmentFilePathMapper.h"
#include "../../util/Helper.h"
#include "../Partition.h"
#include "../SegmentLockManager.h"
#include "./SegmentAllocator.h"
#include "../../Settings.h"
#include "./index_management/BPlusTreeIndexHandler.h"
#include "./index_management/SegmentMessageMap.h"
#include "../../logging/Logger.h"
#include "../../exceptions/CurruptionException.h"
#include "../../requests_management/Requests.h"
#include "../../util/Util.h"

class MessagesHandler {
private:
	DiskFlusher* disk_flusher;
	DiskReader* disk_reader;
	QueueSegmentFilePathMapper* pm;
	SegmentAllocator* sa;
	SegmentMessageMap* smm;
	SegmentLockManager* lock_manager;
	BPlusTreeIndexHandler* index_handler;
	Util* util;
	Settings* settings;
	Logger* logger;

	std::string cluster_metadata_file_key;
	std::string cluster_metadata_file_path;

	std::unordered_map<std::string, unsigned int> remaining_bytes;
	std::mutex remaining_bytes_mut;

	void update_cluster_metadata_index_value(unsigned long long index_value, unsigned int index_size, unsigned int index_pos);

	std::string get_queue_partition_key(Partition* partition);
	unsigned long remove_from_partition_remaining_bytes(const std::string& queue_partition_key, unsigned int bytes_written);

	void set_last_message_id_and_timestamp(PartitionSegment* segment, void* messages, unsigned int total_bytes);

	unsigned int get_message_offset(void* read_batch, unsigned int batch_size, unsigned long long message_id, bool* message_found, bool message_can_have_larger_id = false);

	unsigned int get_last_message_offset_from_batch(void* read_batch, unsigned int batch_size, bool only_active_messages = false);

	unsigned int get_second_last_message_offset_from_batch(void* read_batch, unsigned int batch_size, unsigned int starting_offset, unsigned int ending_offset);

	std::shared_ptr<PartitionSegment> get_next_segment_to_remove_messages(Partition* partition, unsigned long long current_segment_id);

	std::tuple<std::shared_ptr<char>, unsigned int, unsigned int, unsigned int, unsigned int> get_next_available_message(Partition* partition, PartitionSegment* segment, long long current_pos, void* current_batch, unsigned int current_batch_size, unsigned long long message_id);

	std::shared_ptr<char> get_last_active_message_less_than_message_id(void* current_batch, unsigned int current_batch_size, unsigned long long message_id);
public:
	MessagesHandler(DiskFlusher* disk_flusher, DiskReader* disk_reader, QueueSegmentFilePathMapper* pm, SegmentAllocator* sa, SegmentMessageMap* smm, SegmentLockManager* lock_manager, BPlusTreeIndexHandler* index_handler, Util* util, Settings* settings, Logger* logger);

	bool save_messages(Partition* partition, void* messages, unsigned int total_bytes, std::shared_ptr<PartitionSegment> segment_to_write = nullptr, bool cache_messages = true);

	bool save_messages(Partition* partition, ProduceMessagesRequest* request, bool cache_messages = true, bool has_replication = false);

	void update_cluster_metadata_commit_index(unsigned long long commit_index);

	void update_cluster_metadata_last_applied(unsigned long long last_applied);

	std::tuple<std::shared_ptr<char>, unsigned int, unsigned int, unsigned int, unsigned int> read_partition_messages(Partition* partition, unsigned long long read_from_message_id, unsigned int maximum_messages_to_read = 0, bool get_prev_available = false, bool get_next_available = false);

	bool remove_messages_after_message_id(Partition* partition, unsigned long long message_id);
};