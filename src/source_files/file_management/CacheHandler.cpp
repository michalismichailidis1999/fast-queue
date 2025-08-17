#include "../../header_files/file_management/CacheHandler.h"

CacheHandler::CacheHandler(Util* util, Settings* settings) {
	this->util = util;
	this->settings = settings;

	this->cache_search_count = 0;

	this->messages_cache = new Cache<std::string, std::shared_ptr<char>>(MAXIMUM_CACHED_MESSAGES, "", nullptr);
	this->index_pages_cache = new Cache<std::string, std::shared_ptr<char>>(MAXIMUM_CACHED_INDEX_PAGES, "", nullptr);
}

std::shared_ptr<char> CacheHandler::get_message(const std::string& key) {
	std::shared_lock<std::shared_mutex> lock(this->mut);

	if (this->cache_search_count++ >= LAZY_KEY_EXPIRATION_COUNTER) {
		lock.unlock();

		this->remove_expired_keys();

		lock.lock();
	} else if (this->keys_insertion_time.find(key) != this->keys_insertion_time.end()
		&& this->util->has_timeframe_expired(this->keys_insertion_time[key], CACHE_KEY_TTL_MILLI))
		return nullptr;

	if (this->unflushed_data_cache.find(key) != this->unflushed_data_cache.end()) 
		return std::get<0>(this->unflushed_data_cache[key]);

	std::shared_ptr<char> message = this->messages_cache->get(key);

	return message;
}

std::shared_ptr<char> CacheHandler::get_index_page(const std::string& key) {
	std::shared_lock<std::shared_mutex> lock(this->mut);

	if (this->cache_search_count++ >= LAZY_KEY_EXPIRATION_COUNTER) {
		lock.unlock();

		this->remove_expired_keys();

		lock.lock();
	}
	else if (this->keys_insertion_time.find(key) != this->keys_insertion_time.end()
		&& this->util->has_timeframe_expired(this->keys_insertion_time[key], CACHE_KEY_TTL_MILLI))
		return nullptr;
	
	if (this->unflushed_data_cache.find(key) != this->unflushed_data_cache.end())
		return std::get<0>(this->unflushed_data_cache[key]);

	std::shared_ptr<char> index_page = this->index_pages_cache->get(key);

	return index_page;
}

void CacheHandler::cache_messages(std::vector<std::string>* keys, void* messages, unsigned int messages_bytes, bool is_unflushed_data) {
	std::lock_guard<std::shared_mutex> lock(this->mut);

	unsigned int message_bytes = 0;

	unsigned int offset = 0;
	unsigned int i = 0;

	while (offset < messages_bytes) {
		memcpy_s(&message_bytes, TOTAL_METADATA_BYTES, (char*)messages + offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);

		std::shared_ptr<char> message = std::shared_ptr<char>(new char[message_bytes]);

		memcpy_s(message.get(), message_bytes, (char*)messages + offset, message_bytes);

		std::string key = (*keys)[i];

		if (is_unflushed_data) {
			this->unflushed_data_cache[key] = std::tuple<std::shared_ptr<char>, bool>(message, true);
			this->messages_cache->remove(key);
			this->keys_insertion_time.erase(key);
		}
		else {
			this->messages_cache->put(key, message);
			this->keys_insertion_time[key] = this->util->get_current_time_milli().count();
		}

		offset += message_bytes;
		i++;
	}
}

void CacheHandler::cache_index_page(const std::string& key, void* page_data, bool is_unflushed_data) {
	std::lock_guard<std::shared_mutex> lock(this->mut);

	std::shared_ptr<char> page = std::shared_ptr<char>(new char [INDEX_PAGE_SIZE]);

	memcpy_s(page.get(), INDEX_PAGE_SIZE, page_data, INDEX_PAGE_SIZE);

	if (is_unflushed_data) {
		this->unflushed_data_cache[key] = std::tuple<std::shared_ptr<char>, bool>(page, false);
		this->index_pages_cache->remove(key);
		this->keys_insertion_time.erase(key);
	}
	else {
		this->index_pages_cache->put(key, page);
		this->keys_insertion_time[key] = this->util->get_current_time_milli().count();
	}
}

// Transition recently flushed data to LRU cache before clearing it from unflushed_data_cache map
void CacheHandler::clear_unflushed_data_cache() {
	std::lock_guard<std::shared_mutex> lock(this->mut);

	for (auto& iter : this->unflushed_data_cache) {
		if (std::get<1>(iter.second)) this->messages_cache->put(iter.first, std::get<0>(iter.second));
		else this->index_pages_cache->put(iter.first, std::get<0>(iter.second));

		this->keys_insertion_time[iter.first] = this->util->get_current_time_milli().count();
	}

	this->unflushed_data_cache.clear();
}

void CacheHandler::remove_expired_keys() {
	std::lock_guard<std::shared_mutex> lock(this->mut);

	std::unordered_set<std::string> expired_keys;

	// TODO: Add cache key duration in settings

	for (auto& iter : this->keys_insertion_time)
		if (this->util->has_timeframe_expired(iter.second, CACHE_KEY_TTL_MILLI))
			expired_keys.insert(iter.first);

	for (auto& key : expired_keys) {
		this->keys_insertion_time.erase(key);
		this->messages_cache->remove(key);
		this->index_pages_cache->remove(key);
	}

	this->cache_search_count = 0;
}

std::string CacheHandler::get_message_cache_key(const std::string& queue_name, int partition, unsigned long long segment_id, unsigned long long message_id) {
	return queue_name + "_" + std::to_string(partition) + "_" + std::to_string(segment_id) + "_m_" + std::to_string(message_id);
}

std::string CacheHandler::get_index_page_cache_key(const std::string& queue_name, int partition, unsigned long long segment_id, unsigned long long page_offset, bool compaction_segment) {
	return queue_name + "_" + std::to_string(partition) + "_" + std::to_string(segment_id) + "_i_" + std::to_string(page_offset) + (compaction_segment ? "_c" : "");
}