#pragma once
#include <cstdint>
#include <map>
#include <unordered_set>
#include <mutex>

#include "../__linux/memcpy_s.h"

template <typename K, typename V>
class CacheNode {
public:
	K key;
	V value;
	CacheNode* prev;
	CacheNode* next;

	CacheNode() {
		this->prev = NULL;
		this->next = NULL;
	}

	CacheNode(K key, V value) {
		this->key = key;
		this->value = value;
		this->prev = NULL;
		this->next = NULL;
	}
};

template <typename K, typename V>
class Cache {
private:
	uint32_t capacity;
	V empty_value;
	std::map<K, CacheNode<K, V>*> cache;
	CacheNode<K, V>* left;
	CacheNode<K, V>* right;

	std::mutex cache_mut;

	void remove(CacheNode<K, V>* node, bool delete_node) {
		CacheNode<K, V>* prev = node->prev;
		CacheNode<K, V>* next = node->next;

		prev->next = next;
		next->prev = prev;

		if (delete_node)
			delete node;
	}

	void insert(CacheNode<K, V>* node) {
		CacheNode<K, V>* prev = this->left;
		CacheNode<K, V>* next = this->left->next;
		prev->next = node;
		next->prev = node;
		node->next = next;
		node->prev = prev;
	}

	V get_value(K key) {
		if (this->cache.find(key) != this->cache.end()) {
			CacheNode<K, V>* node = this->cache[key];
			this->remove(node, false);
			this->insert(node);
			return node->value;
		}

		return this->empty_value;
	}

	V put_value(K key, V value) {
		CacheNode<K, V>* node;

		V old_removed_value = this->empty_value;

		if (this->cache.find(key) != this->cache.end()) {
			node = this->cache[key];
			this->remove(node, false);
		}
		else {
			node = new CacheNode<K, V>(key, value);
			this->cache[key] = node;
		}

		if (this->cache.size() > this->capacity) {
			CacheNode<K, V>* to_remove = this->right->prev;
			old_removed_value = to_remove->value;
			this->cache.erase(to_remove->key);
			this->remove(to_remove, true);
		}

		this->insert(node);

		return old_removed_value;
	}

public:
	Cache() {}

	Cache(uint32_t capacity, K empty_key, V empty_value) {
		this->capacity = capacity;
		this->empty_value = empty_value;
		this->left = new CacheNode<K, V>(empty_key, empty_value);
		this->right = new CacheNode<K, V>(empty_key, empty_value);
		this->left->next = right;
		this->right->prev = left;
	}

	bool key_exists(K key) {
		std::lock_guard<std::mutex> lock(this->cache_mut);
		return this->cache.find(key) != this->cache.end();
	}

	V get(K key) {
		std::lock_guard<std::mutex> lock(this->cache_mut);
		return this->get_value(key);
	}

	V put(K key, V value) {
		std::lock_guard<std::mutex> lock(this->cache_mut);
		return this->put_value(key, value);
	}

	V put_and_get(K key, V value) {
		std::lock_guard<std::mutex> lock(this->cache_mut);
		this->put_value(key, value);
		return this->get_value(key);
	}

	void remove(K key) {
		std::lock_guard<std::mutex> lock(this->cache_mut);

		if (this->cache.find(key) != this->cache.end()) {
			CacheNode<K, V>* node = this->cache[key];
			this->cache.erase(key);
			this->remove(node, true);
		}
	}

	void find_matching_keys(K key_prefix, std::function<bool(K, K)> comp, std::unordered_set<K>* matching_keys) {
		std::lock_guard<std::mutex> lock(this->cache_mut);

		for (auto& iter : this->cache)
			if (comp(key_prefix, iter.first))
				matching_keys->insert(iter.first);
	}
};