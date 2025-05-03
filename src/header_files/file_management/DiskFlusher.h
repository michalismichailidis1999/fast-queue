#pragma once
#include <vector>
#include <map>
#include <memory>
#include <thread>
#include <chrono>
#include <future>
#include "../Settings.h"
#include "../Constants.h"
#include "./FileHandler.h"
#include "../logging/Logger.h"
#include "../util/Helper.h"
#include "../queue_management/QueueManager.h"

// will handle memory allocations (like buffers that need to store data for client requests etc.)
// will handle also flushing to disk and deallocating data for new messages
class DiskFlusher {
private:
	QueueManager* qm;
	FileHandler* fh;
	Logger* logger;
	Settings* settings;

	unsigned long bytes_to_flush; // how many bytes have been allocated and need to be flushed to disk

	// Mutexes
	std::mutex flush_mut;
	std::condition_variable flush_cond;

	std::atomic_bool* should_terminate;
public:
	DiskFlusher(QueueManager* qm, FileHandler* fh, Logger* logger, Settings* settings, std::atomic_bool* should_terminate);

	void flush_to_disk_periodically(int milliseconds);

	unsigned int flush_data_to_disk(const std::string& key, const std::string& path, void* data, unsigned long total_bytes, long long pos = -1, bool is_internal_queue = false);
};