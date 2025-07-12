#include "../../header_files/file_management/DiskFlusher.h"

DiskFlusher::DiskFlusher(FileHandler* fh, CacheHandler* ch, Logger* logger, Settings* settings, std::atomic_bool* should_terminate) {
	this->fh = fh;
	this->ch = ch;
	this->logger = logger;
	this->settings = settings;

	this->bytes_to_flush = 0;
	this->should_terminate = should_terminate;
}

// will run only once in a detached seperate thread
void DiskFlusher::flush_to_disk_periodically() {
	while (!(*this->should_terminate)) {
		std::unique_lock<std::mutex> lock(this->flush_mut);

		this->flush_cond.wait_for(
			lock, 
			std::chrono::milliseconds(this->settings->get_flush_to_disk_after_ms()), 
			[this] { return this->bytes_to_flush >= this->settings->get_max_cached_memory(); }
		);

		if (this->bytes_to_flush == 0) continue;

		try
		{
			this->fh->flush_output_streams();
			this->ch->clear_unflushed_data_cache();

			this->bytes_to_flush = 0;
		}
		catch (const std::exception& ex)
		{
			std::string err_msg = "Error occured while flushing data to disk periodically. Reason: " + std::string(ex.what());
			this->logger->log_error(err_msg);
		}
	}
}

unsigned long long DiskFlusher::append_data_to_end_of_file(const std::string& key, const std::string& path, void* data, unsigned long total_bytes, bool flush_immediatelly, bool data_is_messages, CacheKeyInfo* cache_key_info) {
	if (cache_key_info != NULL) this->cache_data(data, total_bytes, flush_immediatelly, data_is_messages, cache_key_info);
	
	return this->write_data_to_file(key, path, data, total_bytes, -1, flush_immediatelly);
}

void DiskFlusher::write_data_to_specific_file_location(const std::string& key, const std::string& path, void* data, unsigned long total_bytes, long long pos, bool flush_immediatelly, bool data_is_messages, CacheKeyInfo* cache_key_info) {
	if (cache_key_info != NULL) this->cache_data(data, total_bytes, flush_immediatelly, data_is_messages, cache_key_info);
	
	this->write_data_to_file(key, path, data, total_bytes, pos, flush_immediatelly);
}

void DiskFlusher::flush_metadata_updates_to_disk(PartitionSegment* segment, bool flush_immediatelly) {
	this->write_data_to_file(
		segment->get_segment_key(),
		segment->get_segment_path(),
		std::get<1>(segment->get_metadata_bytes()).get(),
		SEGMENT_METADATA_TOTAL_BYTES,
		0,
		flush_immediatelly
	);
}

void DiskFlusher::flush_new_metadata_to_disk(PartitionSegment* segment, const std::string& prev_segment_key, const std::string& prev_index_key) {
	this->fh->create_new_file(
		segment->get_segment_path(),
		SEGMENT_METADATA_TOTAL_BYTES,
		std::get<1>(segment->get_metadata_bytes()).get(),
		segment->get_segment_key(),
		true
	);

	this->fh->create_new_file(
		segment->get_index_path(),
		INDEX_PAGE_SIZE,
		std::get<0>(BTreeNode(PageType::LEAF).get_page_bytes()).get(),
		segment->get_index_key(),
		true
	);

	this->fh->close_file(prev_segment_key);
	this->fh->close_file(prev_index_key);
}

void DiskFlusher::flush_metadata_updates_to_disk(const std::string& key, const std::string& path, void* data, unsigned long total_bytes, unsigned long pos) {
	this->write_data_to_file(
		key,
		path,
		data,
		total_bytes,
		pos,
		true
	);
}

unsigned long long DiskFlusher::write_data_to_file(const std::string& key, const std::string& path, void* data, unsigned long total_bytes, long long pos, bool flush_immediatelly) {
	std::unique_lock<std::mutex> lock(this->flush_mut);

	unsigned long long begin_written_pos = this->fh->write_to_file(
		key,
		path,
		total_bytes,
		pos,
		data,
		flush_immediatelly
	);

	if (flush_immediatelly) return begin_written_pos;

	this->bytes_to_flush += total_bytes;

	if (this->bytes_to_flush >= this->settings->get_max_cached_memory())
		this->flush_cond.notify_one();

	return begin_written_pos;
}

bool DiskFlusher::path_exists(const std::string& path) {
	return this->fh->check_if_exists(path);
}

void DiskFlusher::cache_data(void* data, unsigned long total_bytes, bool flush_immediatelly, bool data_is_messages, CacheKeyInfo* cache_key_info) {
	if (!data_is_messages) {
		this->ch->cache_index_page(
			CacheHandler::get_index_page_cache_key(
				cache_key_info->queue_name,
				cache_key_info->partition,
				cache_key_info->segment_id,
				cache_key_info->page_offset
			),
			data,
			!flush_immediatelly
		);

		return;
	}
	
	std::vector<std::string> keys;

	unsigned int message_bytes = 0;
	unsigned long long message_id = 0;
	unsigned long offset = 0;

	while (offset < total_bytes) {
		memcpy_s(&message_bytes, TOTAL_METADATA_BYTES, (char*)data + offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);
		memcpy_s(&message_id, MESSAGE_ID_SIZE, (char*)data + offset + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);

		keys.emplace_back(
			CacheHandler::get_message_cache_key(
				cache_key_info->queue_name,
				cache_key_info->partition,
				cache_key_info->segment_id,
				message_id
			)
		);

		offset += message_bytes;
	}

	this->ch->cache_messages(&keys, data, total_bytes, !flush_immediatelly);
}