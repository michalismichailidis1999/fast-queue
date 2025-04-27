#pragma once
#include <memory>
#include <mutex>
#include "../../file_management/DiskFlusher.h"
#include "../../file_management/DiskReader.h"
#include "../../file_management/QueueSegmentFilePathMapper.h"
#include "../../util/Helper.h"
#include "../Partition.h"
#include "./SegmentAllocator.h"
#include "../../Settings.h"
#include "./index_management/BPlusTreeIndexHandler.h"

class MessagesHandler {
private:
	DiskFlusher* disk_flusher;
	DiskReader* disk_reader;
	QueueSegmentFilePathMapper* pm;
	SegmentAllocator* sa;
	BPlusTreeIndexHandler* index_handler;
	Settings* settings;

	std::unordered_map<std::string, unsigned long> remaining_bytes;
	std::mutex remaining_bytes_mut;

	void update_cluster_metadata_index_value(unsigned long long index_value, unsigned long index_size, unsigned long index_pos);

	std::string get_queue_partition_key(Partition* partition);
	unsigned long remove_from_partition_remaining_bytes(const std::string& queue_partition_key, unsigned long bytes_written);
public:
	MessagesHandler(DiskFlusher* disk_flusher, DiskReader* disk_reader, QueueSegmentFilePathMapper* pm, SegmentAllocator* sa, BPlusTreeIndexHandler* index_handler, Settings* settings);

	void save_messages(Partition* partition, void* messages, unsigned long total_bytes);

	void update_cluster_metadata_commit_index(unsigned long long commit_index);

	void update_cluster_metadata_last_applied(unsigned long long last_applied);
};