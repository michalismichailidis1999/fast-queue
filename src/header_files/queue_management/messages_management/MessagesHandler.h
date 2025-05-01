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

	std::string cluster_metadata_file_key;
	std::string cluster_metadata_file_path;

	std::unordered_map<std::string, unsigned int> remaining_bytes;
	std::mutex remaining_bytes_mut;

	void update_cluster_metadata_index_value(unsigned long long index_value, unsigned int index_size, unsigned int index_pos);

	std::string get_queue_partition_key(Partition* partition);
	unsigned long remove_from_partition_remaining_bytes(const std::string& queue_partition_key, unsigned int bytes_written);
public:
	MessagesHandler(DiskFlusher* disk_flusher, DiskReader* disk_reader, QueueSegmentFilePathMapper* pm, SegmentAllocator* sa, BPlusTreeIndexHandler* index_handler, Settings* settings);

	void save_messages(Partition* partition, void* messages, unsigned int total_bytes);

	void update_cluster_metadata_commit_index(unsigned long long commit_index);

	void update_cluster_metadata_last_applied(unsigned long long last_applied);
};