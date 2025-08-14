#pragma once
#include <vector>
#include <mutex>
#include <unordered_map>
#include <functional>

#include "../__linux/memcpy_s.h"

template <typename T, typename K>
class IndexedHeap {
private:
	std::vector<T> heap; // The heap elements (priorities)
	std::unordered_map<K, int> indexToHeap; // Maps user index to heap index
	std::unordered_map<int, K> heapToIndex; // Maps heap index to user index
	std::function<bool(T, T)> comparator;

	std::mutex mut;

    T null_value;
    K null_index;

	void heapifyUp(int pos) {
		while (pos > 0) {
			int parent = (pos - 1) / 2;

			if (!this->comparator(this->heap[pos], this->heap[parent])) break;

			this->swap(pos, parent);
			pos = parent;
		}
	}

	void heapifyDown(int pos) {
		int n = this->heap.size();

		while (true) {
			int left = 2 * pos + 1;
            int right = 2 * pos + 2;
			int extreme = pos;

			if (left < n && this->comparator(this->heap[left], this->heap[extreme]))
				extreme = left;
			else if (right < n && this->comparator(this->heap[right], this->heap[extreme]))
				extreme = right;

			if (extreme == pos) break;

			this->swap(pos, extreme);

			pos = extreme;
		}
	}

	void swap(int i, int j) {
		std::swap(this->heap[i], this->heap[j]);
		std::swap(this->heapToIndex[i], this->heapToIndex[j]);
        this->indexToHeap[this->heapToIndex[i]] = i;
        this->indexToHeap[this->heapToIndex[j]] = j;
	}

public:
    IndexedHeap() {}

	IndexedHeap(std::function<bool(T, T)> comp, T default_null_value, K default_null_index) : 
        comparator(comp),
        null_value(default_null_value),
        null_index(default_null_index) {}

    void insert(K user_index, T value) {
        std::unique_lock<std::mutex> lock(this->mut);

        if (this->indexToHeap.find(user_index) != this->indexToHeap.end()) {
            lock.unlock();
            this->update(user_index, value);
            return;
        }

        this->heap.emplace_back(value);

        int pos = this->heap.size() - 1;

        this->indexToHeap[user_index] = pos;
        this->heapToIndex[pos] = user_index;
        this->heapifyUp(pos);
    }

    void update(K user_index, T newValue) {
        std::lock_guard<std::mutex> lock(this->mut);

        if (this->indexToHeap.find(user_index) == this->indexToHeap.end()) return;

        int pos = this->indexToHeap[user_index];

        T oldValue = this->heap[pos];

        this->heap[pos] = newValue;

        if (this->comparator(newValue, oldValue)) heapifyUp(pos);
        else this->heapifyDown(pos);
    }

    std::tuple<T, K> extractTopElement() {
        auto res = this->extractTopKValues(1);

        if (res.size() == 0) return std::tuple<T, K>(this->null_value, this->null_index);

        return res[0];
    }

    std::vector<std::tuple<T, K>> extractTopKValues(int k) {
        std::lock_guard<std::mutex> lock(this->mut);

        std::vector<std::tuple<T, K>> top_k_elements;

        if (this->heap.empty()) return top_k_elements;

        while (k > 0) {
            T extremeValue = this->heap[0];
            K extremeIndex = this->heapToIndex[0];
            this->swap(0, this->heap.size() - 1);

            this->heap.pop_back();
            this->indexToHeap.erase(extremeIndex);
            this->heapToIndex.erase(this->heap.size());

            if (!this->heap.empty()) this->heapifyDown(0);

            top_k_elements.emplace_back(extremeValue, extremeIndex);

            if (this->heap.empty()) return top_k_elements;

            k--;
        }

        return top_k_elements;
    }

    T remove(K user_index) {
        std::lock_guard<std::mutex> lock(this->mut);

        if (this->heap.empty()) return this->null_value;

        int pos = this->indexToHeap[user_index];
        T value = this->heap[pos];
        this->swap(pos, heap.size() - 1);

        this->heap.pop_back();
        this->indexToHeap.erase(user_index);
        this->heapToIndex.erase(heap.size());

        if (pos < this->heap.size()) {
            this->heapifyDown(pos);
            this->heapifyUp(pos);
        }

        return value;
    }

    T get(K user_index) {
        std::lock_guard<std::mutex> lock(this->mut);

        if (this->heap.empty()) return this->null_value;

        if (this->indexToHeap.find(user_index) == this->indexToHeap.end()) return this->null_value;

        return this->heap[this->indexToHeap[user_index]];
    }

    std::mutex* get_lock() { return &this->mut; }

    bool is_null_value(T value) { return value == this->null_value; }

    bool is_null_index(K index) { return index == this->null_index; }
};