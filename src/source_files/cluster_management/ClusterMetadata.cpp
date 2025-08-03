#include "../../header_files/cluster_management/ClusterMetadata.h"

ClusterMetadata::ClusterMetadata() {
	this->metadata_version = 0;
	this->current_term = 0;
	this->leader_id = 0;
	this->last_consumer_id = 0;

	this->nodes_partition_counts = new IndexedHeap<int, int>([](int a, int b) { return a < b; }, 0, 0);
	this->nodes_leader_partition_counts = new IndexedHeap<int, int>([](int a, int b) { return a < b; }, 0, 0);
	this->consumers_partition_counts = new IndexedHeap<int, unsigned long long>([](int a, int b) { return a < b; }, 0, 0);
	this->consumers_partition_counts_inverse = new IndexedHeap<int, unsigned long long>([](int a, int b) { return a > b; }, 0, 0);
}

void ClusterMetadata::set_leader_id(int leader_id) {
	this->leader_id = leader_id;
}

int ClusterMetadata::get_leader_id() {
	return this->leader_id;
}

unsigned long long ClusterMetadata::get_current_version() {
	return this->metadata_version;
}

unsigned long long ClusterMetadata::get_current_term() {
	return this->current_term;
}

void ClusterMetadata::add_queue_metadata(std::shared_ptr<QueueMetadata> queue_metadata) {
	std::lock_guard<std::mutex> lock(this->queues_mut);
	this->queues[queue_metadata->get_name()] = queue_metadata;
}

QueueMetadata* ClusterMetadata::get_queue_metadata(const std::string& queue_name) {
	std::lock_guard<std::mutex> lock(this->queues_mut);
	return this->queues[queue_name].get();
}

void ClusterMetadata::remove_queue_metadata(const std::string& queue_name) {
	std::lock_guard<std::mutex> lock(this->queues_mut);
	this->queues.erase(queue_name);
}

void ClusterMetadata::init_node_partitions(int node_id) {
	std::lock_guard<std::mutex> lock(this->nodes_partitions_mut);

	if (this->nodes_partitions.find(node_id) == this->nodes_partitions.end()) {
		this->nodes_partitions[node_id] = std::make_shared<std::unordered_map<std::string, std::shared_ptr<std::unordered_set<int>>>>();
		this->nodes_partition_counts->insert(node_id, 0);
		this->nodes_leader_partition_counts->insert(node_id, 0);
	}
}

bool ClusterMetadata::has_node_partitions(int node_id) {
	std::lock_guard<std::mutex> lock(this->nodes_partitions_mut);
	return this->nodes_partitions.find(node_id) != this->nodes_partitions.end();
}

void ClusterMetadata::apply_command(Command* command) {
	std::lock_guard<std::mutex> lock(this->nodes_partitions_mut);

	this->metadata_version = command->get_metadata_version();
	this->current_term = command->get_term();

	switch (command->get_command_type())
	{
	case CommandType::CREATE_QUEUE: {
		CreateQueueCommand* command_info = static_cast<CreateQueueCommand*>(command->get_command_info());
		this->apply_create_queue_command(command_info);
		return;
	}
	case CommandType::DELETE_QUEUE: {
		DeleteQueueCommand* command_info = static_cast<DeleteQueueCommand*>(command->get_command_info());
		this->apply_delete_queue_command(command_info);
		return;
	}
	case CommandType::ALTER_PARTITION_ASSIGNMENT: {
		PartitionAssignmentCommand* command_info = static_cast<PartitionAssignmentCommand*>(command->get_command_info());
		this->apply_partition_assignment_command(command_info);
		return;
	}
	case CommandType::ALTER_PARTITION_LEADER_ASSIGNMENT: {
		PartitionLeaderAssignmentCommand* command_info = static_cast<PartitionLeaderAssignmentCommand*>(command->get_command_info());
		this->apply_partition_leader_assignment_command(command_info);
		return;
	}
	default:
		return;
	}
}

void ClusterMetadata::apply_create_queue_command(CreateQueueCommand* command) {
	std::shared_ptr<QueueMetadata> queue_metadata = std::shared_ptr<QueueMetadata>(
		new QueueMetadata(
			command->get_queue_name(),
			command->get_partitions(),
			command->get_replication_factor()
		)
	);

	this->add_queue_metadata(queue_metadata);
}

void ClusterMetadata::apply_delete_queue_command(DeleteQueueCommand* command) {
	this->remove_queue_metadata(command->get_queue_name());
}

void ClusterMetadata::apply_partition_assignment_command(PartitionAssignmentCommand* command) {
	int node_id = command->get_to_node();

	auto node_queues = this->nodes_partitions[node_id];

	auto owned_partitions = this->owned_partitions[command->get_queue_name()];

	if (node_queues == nullptr) {
		node_queues = std::make_shared<std::unordered_map<std::string, std::shared_ptr<std::unordered_set<int>>>>();
		this->nodes_partitions[node_id] = node_queues;
	}

	if (owned_partitions == nullptr) {
		owned_partitions = std::make_shared<std::unordered_map<int, std::shared_ptr<std::unordered_set<int>>>>();
		this->owned_partitions[command->get_queue_name()] = owned_partitions;
	}

	auto partition_owners = (*(owned_partitions.get()))[command->get_partition()];

	if (partition_owners == nullptr) {
		partition_owners = std::make_shared<std::unordered_set<int>>();
		(*(owned_partitions.get()))[command->get_partition()] = partition_owners;
	}

	auto node_partitions = (*(node_queues.get()))[command->get_queue_name()];

	if (node_partitions == nullptr) {
		node_partitions = std::make_shared<std::unordered_set<int>>();
		(*(node_queues.get()))[command->get_queue_name()] = node_partitions;
	}

	node_partitions->insert(command->get_partition());

	int partitions_count = this->nodes_partition_counts->get(node_id);

	this->nodes_partition_counts->update(node_id, partitions_count + 1);

	partition_owners->insert(node_id);

	if (command->get_from_node() <= 0) return;

	node_id = command->get_from_node();

	node_queues = this->nodes_partitions[node_id];

	node_partitions = (*(node_queues.get()))[command->get_queue_name()];

	node_partitions->erase(command->get_partition());

	if (node_partitions->size() == 0)
		node_queues.get()->erase(command->get_queue_name());

	partition_owners->erase(node_id);

	partitions_count = this->nodes_partition_counts->get(node_id);

	this->nodes_partition_counts->update(node_id, partitions_count - 1);
}

void ClusterMetadata::apply_partition_leader_assignment_command(PartitionLeaderAssignmentCommand* command) {
	auto partitions_leaders = this->partition_leader_nodes[command->get_queue_name()];

	if (partitions_leaders == nullptr) {
		partitions_leaders = std::make_shared<std::unordered_map<int, int>>();
		this->partition_leader_nodes[command->get_queue_name()] = partitions_leaders;
	}

	int node_id = command->get_new_leader();

	int lead_partitions_count = this->nodes_leader_partition_counts->get(node_id);

	(*(partitions_leaders.get()))[command->get_partition()] = node_id;

	this->nodes_leader_partition_counts->update(node_id, lead_partitions_count + 1);

	if (command->get_prev_leader() <= 0) return;

	node_id = command->get_prev_leader();

	lead_partitions_count = this->nodes_leader_partition_counts->get(node_id);

	this->nodes_leader_partition_counts->update(node_id, lead_partitions_count - 1);
}

void ClusterMetadata::copy_from(ClusterMetadata* obj) {
	this->leader_id.store(obj->leader_id.load());
	this->metadata_version.store(obj->metadata_version);
	this->current_term.store(obj->current_term);

	if(this->nodes_partition_counts != NULL)
		free(this->nodes_partition_counts);

	if(this->nodes_leader_partition_counts != NULL)
		free(this->nodes_leader_partition_counts);

	this->nodes_partition_counts = new IndexedHeap<int, int>([](int a, int b) { return a < b; }, 0, 0);
	this->nodes_leader_partition_counts = new IndexedHeap<int, int>([](int a, int b) { return a < b; }, 0, 0);
	this->consumers_partition_counts = new IndexedHeap<int, unsigned long long>([](int a, int b) { return a < b; }, 0, 0);
	this->consumers_partition_counts_inverse = new IndexedHeap<int, unsigned long long>([](int a, int b) { return a > b; }, 0, 0);

	for (auto& iter : obj->nodes_partitions) {
		auto partitions = std::make_shared<std::unordered_map<std::string, std::shared_ptr<std::unordered_set<int>>>>();

		int partitions_count = 0;

		this->nodes_partitions[iter.first] = partitions;

		for (auto& iter_two : *(iter.second.get())) {
			auto queue_partitions = std::make_shared<std::unordered_set<int>>();
			(*(partitions.get()))[iter_two.first] = queue_partitions;

			for (auto& queue_partition : *(iter_two.second.get())) {
				queue_partitions.get()->insert(queue_partition);
				partitions_count++;
			}
		}

		this->nodes_partition_counts->insert(iter.first, partitions_count);
	}

	for (auto& iter : obj->owned_partitions) {
		auto owned = std::make_shared<std::unordered_map<int, std::shared_ptr<std::unordered_set<int>>>>();

		this->owned_partitions[iter.first] = owned;

		for (auto& iter_two : *(iter.second.get())) {
			auto node_owners = std::make_shared<std::unordered_set<int>>();
			(*(owned.get()))[iter_two.first] = node_owners;

			for (auto& node_id : *(iter_two.second.get()))
				node_owners.get()->insert(node_id);
		}
	}

	for (auto& iter : obj->partition_leader_nodes) {
		auto queue_partition_leads = std::make_shared<std::unordered_map<int, int>>();

		this->partition_leader_nodes[iter.first] = queue_partition_leads;

		for (auto& iter_two : *(iter.second.get())) {
			(*(queue_partition_leads.get()))[iter_two.first] = iter_two.second;
			this->nodes_leader_partition_counts->insert(iter_two.second, this->nodes_leader_partition_counts->get(iter_two.second) + 1);
		}
	}
}

int ClusterMetadata::get_partition_leader(const std::string& queue, int partition) {
	std::lock_guard<std::mutex> lock(this->nodes_partitions_mut);

	if (this->partition_leader_nodes.find(queue) == this->partition_leader_nodes.end()) return -1;

	auto queue_partition_leads = this->partition_leader_nodes[queue];

	if (queue_partition_leads.get()->find(partition) == queue_partition_leads.get()->end()) return -1;

	return (*(queue_partition_leads.get()))[partition];
}

std::mutex* ClusterMetadata::get_partitions_mut() {
	return &this->nodes_partitions_mut;
}

std::shared_ptr<std::unordered_map<int, int>> ClusterMetadata::get_queue_partition_leaders(const std::string& queue_name) {
	return this->partition_leader_nodes[queue_name];
}