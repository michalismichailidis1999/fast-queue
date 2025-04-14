#pragma once
#include <cstdint>
#include <map>
#include <mutex>

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

	bool put_value(K key, V value, bool update_with_new_if_found = false) {
		CacheNode<K, V>* node;

		bool is_new = true;

		if (this->cache.find(key) != this->cache.end()) {
			node = this->cache[key];
			this->remove(node, update_with_new_if_found);

			if (update_with_new_if_found) {
				node = new CacheNode<K, V>(key, value);
				this->cache[key] = node;
			}
			else is_new = false;
		}
		else {
			node = new CacheNode<K, V>(key, value);
			this->cache[key] = node;
		}

		if (this->cache.size() > this->capacity) {
			CacheNode<K, V>* to_remove = this->right->prev;
			this->cache.erase(to_remove->key);
			this->remove(to_remove, true);
		}

		this->insert(node);

		return is_new;
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

	bool put(K key, V value, bool update_with_new_if_found = false) {
		std::lock_guard<std::mutex> lock(this->cache_mut);

		return this->put_value(key, value, update_with_new_if_found);
	}

	V put_and_get(K key, V value, bool update_with_new_if_found = false) {
		std::lock_guard<std::mutex> lock(this->cache_mut);

		this->put_value(key, value, update_with_new_if_found);

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
};