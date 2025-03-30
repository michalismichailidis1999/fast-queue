#pragma once
#include <vector>
#include <map>
#include <memory>
#include "Settings.h"
#include "FileHandler.h"
#include "Logger.h"
#include "QueueSegmentFilePathMapper.h"
#include "QueueManager.h"

// will handle memory allocations (like buffers that need to store data for client requests etc.)
// will handle also flushing to disk and deallocating data for new messages
class DiskFlusher {
private:
	QueueManager* qm;
	FileHandler* fh;
	QueueSegmentFilePathMapper* pm;
	Logger* logger;
	Settings* settings;

	long bytes_to_flush; // how many bytes have been allocated and need to be flushed to disk

	// Unmutable data
	long max_allocated_bytes_threshold;

	// Mutexes
	std::mutex flush_mut;
	std::condition_variable flush_cond;

	std::atomic_bool* should_terminate;

	std::map<std::string, std::shared_ptr<std::vector<std::shared_ptr<void>>>> queues_messages;

	void flush_data_to_disk();
public:
	DiskFlusher(QueueManager* qm, FileHandler* fh, QueueSegmentFilePathMapper* pm, Logger* logger, Settings* settings, std::atomic_bool* should_terminate);

	void flush_to_disk_periodically(int milliseconds);

	void store_messages_for_disk_flushing(const std::string& queue_name, std::vector<std::shared_ptr<void>>* messages, long total_bytes);
};