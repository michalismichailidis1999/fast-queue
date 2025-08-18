#pragma once
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <unordered_map>
#include <unordered_set>
#include <tuple>
#include <memory>
#include "../util/IndexedHeap.h"
#include "./Commands.h"
#include "../queue_management/QueueMetadata.h"
#include "../exceptions/CurruptionException.h"

#include "../__linux/memcpy_s.h"

class ClusterMetadata {
private:
	std::atomic<unsigned long long> metadata_version;
	std::atomic<unsigned long long> current_term;

	std::atomic_int leader_id;

	IndexedHeap<int, int>* nodes_partition_counts;
	IndexedHeap<int, int>* nodes_leader_partition_counts;
	IndexedHeap<int, unsigned long long>* consumers_partition_counts;
	IndexedHeap<int, unsigned long long>* consumers_partition_counts_inverse;

	// nodes_partitions_mut used for all the below maps
	std::unordered_map<int, std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<std::unordered_set<int>>>>> nodes_partitions;

	std::unordered_map<std::string, std::shared_ptr<std::unordered_map<int, std::shared_ptr<std::unordered_set<int>>>>> owned_partitions;

	std::unordered_map<std::string, std::shared_ptr<std::unordered_map<int, int>>> partition_leader_nodes;

	std::unordered_map<std::string, std::shared_ptr<std::unordered_map<int, unsigned long long>>> queue_partition_leader_ids;

	std::unordered_map<std::string, std::shared_ptr<std::unordered_map<int, std::shared_ptr<std::unordered_set<int>>>>> lagging_followers;

	unsigned long long last_queue_partition_leader_id;
	// ======================================================

	unsigned long long last_consumer_id;

	std::unordered_map<
		std::string, 
		std::shared_ptr<
			std::unordered_map<
				std::string, 
				std::shared_ptr<std::unordered_map<int, unsigned long long>>
			>
		>
	> partition_consumers;

	std::unordered_map<unsigned long long, bool> consumers_consume_init_point;

	std::unordered_map<std::string, std::shared_ptr<QueueMetadata>> queues;

	std::shared_mutex nodes_partitions_mut;
	std::mutex queues_mut;
	std::shared_mutex consumers_mut;

	void apply_create_queue_command(CreateQueueCommand* command);
	void apply_delete_queue_command(DeleteQueueCommand* command);
	void apply_partition_assignment_command(PartitionAssignmentCommand* command);
	void apply_partition_leader_assignment_command(PartitionLeaderAssignmentCommand* command);
	void apply_register_consuer_group_command(RegisterConsumerGroupCommand* command);
	void apply_unregister_consuer_group_command(UnregisterConsumerGroupCommand* command);

public:
	ClusterMetadata();
	ClusterMetadata(void* metadata);

	void init_node_partitions(int node_id);
	bool has_node_partitions(int node_id);

	void set_leader_id(int leader_id);
	int get_leader_id();

	unsigned long long get_current_version();
	unsigned long long get_current_term();

	void add_queue_metadata(std::shared_ptr<QueueMetadata> queue_metadata);
	QueueMetadata* get_queue_metadata(const std::string& queue_name);
	void remove_queue_metadata(const std::string& queue_name);

	void apply_command(Command* command, bool with_lock = true);

	void copy_from(ClusterMetadata* obj);

	int get_partition_leader(const std::string& queue, int partition);

	std::shared_mutex* get_partitions_mut();

	std::shared_ptr<std::unordered_map<int, int>> get_queue_partition_leaders(const std::string& queue_name);

	unsigned long long get_queue_partition_leader_id(const std::string& queue_name, int partition);

	friend class Controller;
};