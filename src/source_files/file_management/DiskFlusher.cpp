#include <thread>
#include <chrono>
#include <future>
#include "DiskFlusher.h"
#include "Constants.cpp"

DiskFlusher::DiskFlusher(QueueManager* qm, FileHandler* fh, QueueSegmentFilePathMapper* pm, Logger* logger, Settings* settings, std::atomic_bool* should_terminate) {
	this->qm = qm;
	this->fh = fh;
	this->pm = pm;
	this->logger = logger;
	this->settings = settings;

	this->bytes_to_flush = 0;
	this->max_allocated_bytes_threshold = settings->get_max_cached_memory();
	this->should_terminate = should_terminate;
}

// will run only once in a detached seperate thread
void DiskFlusher::flush_to_disk_periodically(int milliseconds) {
	while (!(*this->should_terminate)) {
		std::unique_lock<std::mutex> lock(this->flush_mut);

		this->flush_cond.wait_for(
			lock, 
			std::chrono::milliseconds(milliseconds), 
			[this] { return this->bytes_to_flush >= this->max_allocated_bytes_threshold; }
		);

		if (this->bytes_to_flush == 0) continue;

		this->flush_data_to_disk();
	}
}

void DiskFlusher::flush_data_to_disk() {
	this->logger->log_info("Flushing to disk...");
	std::this_thread::sleep_for(std::chrono::milliseconds(1000));
	this->logger->log_info("Flushing finished");
}

void DiskFlusher::store_messages_for_disk_flushing(const std::string& queue_name, std::vector<std::shared_ptr<void>>* messages, long total_bytes) {
	std::unique_lock<std::mutex> lock(this->flush_mut);

	if (this->queues_messages.find(queue_name) == this->queues_messages.end())
		this->queues_messages[queue_name] = std::make_shared<std::vector<std::shared_ptr<void>>>();

	this->queues_messages[queue_name]->insert(this->queues_messages[queue_name]->end(), messages->begin(), messages->end());

	this->bytes_to_flush += total_bytes;

	if (this->bytes_to_flush >= this->max_allocated_bytes_threshold)
		this->flush_cond.notify_one();
}