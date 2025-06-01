#pragma once
#include <vector>
#include <map>
#include <memory>
#include <thread>
#include <chrono>
#include <future>
#include "../Settings.h"
#include "../Constants.h"
#include "../Enums.h"
#include "./FileHandler.h"
#include "../logging/Logger.h"
#include "../util/Helper.h"
#include "../queue_management/PartitionSegment.h"
#include "../queue_management/messages_management/index_management/BTreeNode.h"

// will handle memory allocations (like buffers that need to store data for client requests etc.)
// will handle also flushing to disk and deallocating data for new messages
class DiskFlusher {
private:
	FileHandler* fh;
	Logger* logger;
	Settings* settings;

	unsigned long long bytes_to_flush; // how many bytes have been allocated and need to be flushed to disk

	// Mutexes
	std::mutex flush_mut;
	std::condition_variable flush_cond;

	std::atomic_bool* should_terminate;

	unsigned long long write_data_to_file(const std::string& key, const std::string& path, void* data, unsigned long total_bytes, long long pos = -1, bool flush_immediatelly = false);
public:
	DiskFlusher(FileHandler* fh, Logger* logger, Settings* settings, std::atomic_bool* should_terminate);

	void flush_to_disk_periodically();

	unsigned long long append_data_to_end_of_file(const std::string& key, const std::string& path, void* data, unsigned long total_bytes, bool flush_immediatelly = false);

	void write_data_to_specific_file_location(const std::string& key, const std::string& path, void* data, unsigned long total_bytes, long long pos, bool flush_immediatelly = false);

	void flush_metadata_updates_to_disk(PartitionSegment* segment);

	void flush_new_metadata_to_disk(PartitionSegment* segment, const std::string& prev_segment_key, const std::string& prev_index_key);

	void flush_metadata_updates_to_disk(const std::string& key, const std::string& path, void* data, unsigned long total_bytes, unsigned long pos);
};