#include "../../header_files/file_management/DiskFlusher.h"

DiskFlusher::DiskFlusher(QueueManager* qm, FileHandler* fh, Logger* logger, Settings* settings, std::atomic_bool* should_terminate) {
	this->qm = qm;
	this->fh = fh;
	this->logger = logger;
	this->settings = settings;

	this->bytes_to_flush = 0;
	this->should_terminate = should_terminate;
}

// will run only once in a detached seperate thread
void DiskFlusher::flush_to_disk_periodically(int milliseconds) {
	while (!(*this->should_terminate)) {
		std::unique_lock<std::mutex> lock(this->flush_mut);

		this->flush_cond.wait_for(
			lock, 
			std::chrono::milliseconds(milliseconds), 
			[this] { return this->bytes_to_flush >= this->settings->get_max_cached_memory(); }
		);

		if (this->bytes_to_flush == 0) continue;

		this->fh->flush_output_streams();

		this->bytes_to_flush = 0;
	}
}

void DiskFlusher::flush_data_to_disk(const std::string& key, const std::string& path, void* data, long total_bytes, long pos, bool flush_immediatelly) {
	std::unique_lock<std::mutex> lock(this->flush_mut);

	this->fh->write_to_file(
		key,
		path,
		total_bytes,
		pos,
		data,
		flush_immediatelly
	);

	this->bytes_to_flush += total_bytes;

	if (this->bytes_to_flush >= this->settings->get_max_cached_memory())
		this->flush_cond.notify_one();
}