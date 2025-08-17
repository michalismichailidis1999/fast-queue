#pragma once
#include <string>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <tuple>
#include <vector>
#include "../Settings.h"
#include "../Constants.h"
#include "../util/Cache.h"
#include "../util/Util.h"

#include "../__linux/memcpy_s.h"

struct CacheKeyInfo {
	std::string queue_name;
	int partition;
	unsigned long long segment_id;
	unsigned long long page_offset;
};

class CacheHandler {
private:
	Util* util;
	Settings* settings;

	std::atomic_uint cache_search_count;

	std::unordered_map<std::string, std::tuple<std::shared_ptr<char>, bool>> unflushed_data_cache;

	Cache<std::string, std::shared_ptr<char>>* messages_cache;
	Cache<std::string, std::shared_ptr<char>>* index_pages_cache;

	std::unordered_map<std::string, long long> keys_insertion_time;

	std::shared_mutex mut;

	void remove_expired_keys();
public:
	CacheHandler(Util* util, Settings* settings);

	std::shared_ptr<char> get_message(const std::string& key);

	std::shared_ptr<char> get_index_page(const std::string& key);

	void cache_messages(std::vector<std::string>* keys, void* messages, unsigned int messages_bytes, bool is_unflushed_data = false);

	void cache_index_page(const std::string& key, void* page_data, bool is_unflushed_data = false);

	void clear_unflushed_data_cache();

	static std::string get_message_cache_key(const std::string& queue_name, int partition, unsigned long long segment_id, unsigned long long message_id);

	static std::string get_index_page_cache_key(const std::string& queue_name, int partition, unsigned long long segment_id, unsigned long long page_offset, bool compaction_segment = false);
};