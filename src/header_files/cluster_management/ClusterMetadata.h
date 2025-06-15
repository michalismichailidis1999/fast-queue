#pragma once
#include <mutex>
#include <atomic>
#include <unordered_map>
#include <unordered_set>
#include <tuple>
#include <memory>
#include "../util/IndexedHeap.h"
#include "./Commands.h"
#include "../queue_management/QueueMetadata.h"
#include "../exceptions/CurruptionException.h"

class ClusterMetadata {
private:
	std::atomic<unsigned long long> metadata_version;
	std::atomic<unsigned long long> current_term;

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

	void apply_create_queue_command(CreateQueueCommand* command);
	void apply_delete_queue_command(DeleteQueueCommand* command);
	void apply_partition_assignment_command(PartitionAssignmentCommand* command);
	void apply_partition_leader_assignment_command(PartitionLeaderAssignmentCommand* command);

public:
	ClusterMetadata(int node_id);
	ClusterMetadata(void* metadata);
	ClusterMetadata();

	void init_node_partitions(int node_id);
	void remove_node_partitions(int node_id);
	bool has_node_partitions(int node_id);

	void set_leader_id(int leader_id);
	int get_leader_id();

	unsigned long long get_current_version();
	unsigned long long get_current_term();

	void add_queue_metadata(std::shared_ptr<QueueMetadata> queue_metadata);
	QueueMetadata* get_queue_metadata(const std::string& queue_name);
	void remove_queue_metadata(const std::string& queue_name);

	void apply_command(Command* command);

	void copy_from(ClusterMetadata* obj);

	void fill_from_metadata(void* metadata);

	std::tuple<unsigned int, std::shared_ptr<char>> get_metadata_bytes();

	friend class Controller;
};