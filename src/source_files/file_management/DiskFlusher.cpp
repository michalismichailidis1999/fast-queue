#include "../../header_files/file_management/DiskFlusher.h"

DiskFlusher::DiskFlusher(FileHandler* fh, Logger* logger, Settings* settings, std::atomic_bool* should_terminate) {
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

unsigned int DiskFlusher::append_data_to_end_of_file(const std::string& key, const std::string& path, void* data, unsigned long total_bytes, bool is_internal_queue, bool flush_immediatelly) {
	return this->write_data_to_file(key, path, data, total_bytes, -1, is_internal_queue, flush_immediatelly);
}

void DiskFlusher::write_data_to_specific_file_location(const std::string& key, const std::string& path, void* data, unsigned long total_bytes, long long pos, bool is_internal_queue, bool flush_immediatelly) {
	this->write_data_to_file(key, path, data, total_bytes, pos, is_internal_queue, flush_immediatelly);
}

void DiskFlusher::flush_metadata_updates_to_disk(PartitionSegment* segment, bool is_internal_queue) {
	this->write_data_to_file(
		segment->get_segment_key(),
		segment->get_segment_path(),
		std::get<1>(segment->get_metadata_bytes()).get(),
		SEGMENT_METADATA_TOTAL_BYTES,
		0,
		is_internal_queue,
		true
	);
}

void DiskFlusher::flush_new_metadata_to_disk(PartitionSegment* segment, const std::string& prev_segment_key, const std::string& prev_index_key, bool is_internal_queue) {
	this->fh->create_new_file(
		segment->get_segment_path(),
		SEGMENT_METADATA_TOTAL_BYTES,
		std::get<1>(segment->get_metadata_bytes()).get(),
		segment->get_segment_key(),
		true,
		is_internal_queue
	);

	this->fh->create_new_file(
		segment->get_index_path(),
		SEGMENT_METADATA_TOTAL_BYTES,
		std::get<0>(BTreeNode(PageType::LEAF).get_page_bytes()).get(),
		segment->get_index_key(),
		true,
		is_internal_queue
	);

	this->fh->close_file(prev_segment_key);
	this->fh->close_file(prev_index_key);
}

void DiskFlusher::flush_metadata_updates_to_disk(const std::string& key, const std::string& path, void* data, unsigned long total_bytes, unsigned long pos, bool is_internal_queue) {
	this->write_data_to_file(
		key,
		path,
		data,
		total_bytes,
		pos,
		is_internal_queue,
		true
	);
}

unsigned int DiskFlusher::write_data_to_file(const std::string& key, const std::string& path, void* data, unsigned long total_bytes, long long pos, bool is_internal_queue, bool flush_immediatelly) {
	std::unique_lock<std::mutex> lock(this->flush_mut);

	unsigned int begin_written_pos = this->fh->write_to_file(
		key,
		path,
		total_bytes,
		pos,
		data,
		flush_immediatelly,
		is_internal_queue
	);

	if (flush_immediatelly) return 0;

	this->bytes_to_flush += total_bytes;

	if (this->bytes_to_flush >= this->settings->get_max_cached_memory())
		this->flush_cond.notify_one();

	return begin_written_pos;
}