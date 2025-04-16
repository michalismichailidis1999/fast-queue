#pragma once
#include <mutex>
#include <atomic>
#include <unordered_map>
#include <unordered_set>
#include "../generic/IndexedHeap.h"
#include "./Commands.h"
#include "../queue_management/QueueMetadata.h"

class ClusterMetadata {
private:
	std::atomic<unsigned long long> metadata_version;

	std::atomic_int leader_id;

	IndexedHeap<int, int>* nodes_partition_counts;
	IndexedHeap<int, int>* nodes_leader_partition_counts;

	// nodes_partitions_mut used for all the below maps
	std::unordered_map<int, std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<std::unordered_set<int>>>>> nodes_partitions;

	std::unordered_map<std::string, std::shared_ptr<std::unordered_map<int, std::shared_ptr<std::unordered_set<int>>>>> owned_partitions;

	std::unordered_map<std::string, std::shared_ptr<std::unordered_map<int, int>>> partition_leader_nodes;
	// ======================================================

	std::unordered_map<std::string, std::shared_ptr<QueueMetadata>> queues;

	std::mutex nodes_partitions_mut;
	std::mutex queues_mut;

public:
	ClusterMetadata(int node_id);
	ClusterMetadata();

	void init_node_partitions(int node_id);

	void set_leader_id(int leader_id);
	int get_leader_id();

	void add_queue_metadata(std::shared_ptr<QueueMetadata> queue_metadata);
	QueueMetadata* get_queue_metadata(const std::string& queue_name);
	void remove_queue_metadata(const std::string& queue_name);

	void apply_command(Command* command);

	void copy_from(ClusterMetadata* obj);

	friend class Controller;
};