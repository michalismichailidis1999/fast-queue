#include "../../header_files/cluster_management/ClusterMetadata.h"

ClusterMetadata::ClusterMetadata() {
	this->metadata_version = 0;
	this->current_term = 0;
	this->leader_id = 0;
	this->last_consumer_id = 0;
	this->last_queue_partition_leader_id = 0;

	this->nodes_partition_counts = new IndexedHeap<int, int>([](int a, int b) { return a < b; }, 0, 0);
	this->nodes_leader_partition_counts = new IndexedHeap<int, int>([](int a, int b) { return a < b; }, 0, 0);
	this->consumers_partition_counts = new IndexedHeap<int, unsigned long long>([](int a, int b) { return a < b; }, 0, 0);
	this->consumers_partition_counts_inverse = new IndexedHeap<int, unsigned long long>([](int a, int b) { return a > b; }, 0, 0);
	this->nodes_transaction_groups_counts = new IndexedHeap<int, int>([](int a, int b) { return a < b; }, 0, 0);
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

void ClusterMetadata::set_current_version(unsigned long long metadata_version) {
	this->metadata_version = metadata_version;
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
	if (this->queues.find(queue_name) == this->queues.end()) return NULL;
	return this->queues[queue_name].get();
}

void ClusterMetadata::remove_queue_metadata(const std::string& queue_name) {
	std::lock_guard<std::mutex> lock(this->queues_mut);
	this->queues.erase(queue_name);
}

void ClusterMetadata::init_node_partitions(int node_id) {
	std::lock_guard<std::shared_mutex> lock(this->nodes_partitions_mut);

	if (this->nodes_partitions.find(node_id) == this->nodes_partitions.end()) {
		this->nodes_partitions[node_id] = std::make_shared<std::unordered_map<std::string, std::shared_ptr<std::unordered_set<int>>>>();
		this->nodes_partition_counts->insert(node_id, 0);
		this->nodes_leader_partition_counts->insert(node_id, 0);
	}
}

bool ClusterMetadata::has_node_partitions(int node_id) {
	std::shared_lock<std::shared_mutex> lock(this->nodes_partitions_mut);
	return this->nodes_partitions.find(node_id) != this->nodes_partitions.end();
}

void ClusterMetadata::apply_command(Command* command, bool with_lock, bool ignore_metadata_version_update) {
	if (!ignore_metadata_version_update)
		this->metadata_version = command->get_metadata_version();

	this->current_term = command->get_term();

	switch (command->get_command_type())
	{
	case CommandType::CREATE_QUEUE: {
		std::lock_guard<std::shared_mutex> lock(this->nodes_partitions_mut);
		CreateQueueCommand* command_info = static_cast<CreateQueueCommand*>(command->get_command_info());
		this->apply_create_queue_command(command_info);
		return;
	}
	case CommandType::DELETE_QUEUE: {
		std::lock_guard<std::shared_mutex> lock(this->nodes_partitions_mut);
		std::lock_guard<std::shared_mutex> lock2(this->consumers_mut);
		DeleteQueueCommand* command_info = static_cast<DeleteQueueCommand*>(command->get_command_info());
		this->apply_delete_queue_command(command_info);
		return;
	}
	case CommandType::ALTER_PARTITION_ASSIGNMENT: {
		std::lock_guard<std::shared_mutex> lock(this->nodes_partitions_mut);
		PartitionAssignmentCommand* command_info = static_cast<PartitionAssignmentCommand*>(command->get_command_info());
		this->apply_partition_assignment_command(command_info);
		return;
	}
	case CommandType::ALTER_PARTITION_LEADER_ASSIGNMENT: {
		std::lock_guard<std::shared_mutex> lock(this->nodes_partitions_mut);
		PartitionLeaderAssignmentCommand* command_info = static_cast<PartitionLeaderAssignmentCommand*>(command->get_command_info());
		this->apply_partition_leader_assignment_command(command_info);
		return;
	}
	case CommandType::REGISTER_CONSUMER_GROUP: {
		std::lock_guard<std::shared_mutex> lock(this->consumers_mut);
		RegisterConsumerGroupCommand* command_info = static_cast<RegisterConsumerGroupCommand*>(command->get_command_info());
		this->apply_register_consuer_group_command(command_info);
		return;
	}
	case CommandType::UNREGISTER_CONSUMER_GROUP: {
		UnregisterConsumerGroupCommand* command_info = static_cast<UnregisterConsumerGroupCommand*>(command->get_command_info());

		if (with_lock) {
			std::lock_guard<std::shared_mutex> lock(this->consumers_mut);
			this->apply_unregister_consuer_group_command(command_info);
		} else this->apply_unregister_consuer_group_command(command_info);
		
		return;
	}
	case CommandType::ADD_LAGGING_FOLLOWER: {
		AddLaggingFollowerCommand* command_info = static_cast<AddLaggingFollowerCommand*>(command->get_command_info());

		if (with_lock) {
			std::lock_guard<std::shared_mutex> lock(this->consumers_mut);
			this->apply_add_lagging_follower_command(command_info);
		}
		else this->apply_add_lagging_follower_command(command_info);

		return;
	}
	case CommandType::REMOVE_LAGGING_FOLLOWER: {
		RemoveLaggingFollowerCommand* command_info = static_cast<RemoveLaggingFollowerCommand*>(command->get_command_info());

		if (with_lock) {
			std::lock_guard<std::shared_mutex> lock(this->consumers_mut);
			this->apply_remove_lagging_follower_command(command_info);
		}
		else this->apply_remove_lagging_follower_command(command_info);

		return;
	}
	case CommandType::REGISTER_TRANSACTION_GROUP: {
		RegisterTransactionGroupCommand* command_info = static_cast<RegisterTransactionGroupCommand*>(command->get_command_info());

		this->apply_register_transaction_group_command(command_info);

		return;
	}
	case CommandType::UNREGISTER_TRANSACTION_GROUP: {
		UnregisterTransactionGroupCommand* command_info = static_cast<UnregisterTransactionGroupCommand*>(command->get_command_info());

		this->apply_unregister_transaction_group_command(command_info);

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
	this->lagging_followers.erase(command->get_queue_name());

	if (this->partition_consumers.find(command->get_queue_name()) != this->partition_consumers.end())
		for (auto& iter : *(this->partition_consumers[command->get_queue_name()].get()))
			for (auto& iter2 : *(iter.second.get()))
			{
				this->consumers_consume_init_point.erase(iter2.second);
				this->consumers_partition_counts->remove(iter2.second);
				this->consumers_partition_counts_inverse->remove(iter2.second);
			}

	this->partition_consumers.erase(command->get_queue_name());

	if (this->owned_partitions.find(command->get_queue_name()) != this->owned_partitions.end())
		for (auto& iter : *(this->owned_partitions[command->get_queue_name()].get()))
			for (int node_id : *(iter.second.get()))
				if (this->nodes_partitions.find(node_id) != this->nodes_partitions.end()) {
					this->nodes_partitions[node_id].get()->erase(command->get_queue_name());

					this->nodes_partition_counts->insert(
						node_id,
						this->nodes_partition_counts->get(node_id) - 1
					);
				}

	if (this->partition_leader_nodes.find(command->get_queue_name()) != this->partition_leader_nodes.end())
		for (auto& iter : *(this->partition_leader_nodes[command->get_queue_name()].get()))
			this->nodes_leader_partition_counts->insert(
				iter.second,
				this->nodes_leader_partition_counts->get(iter.second) - 1
			);

	this->owned_partitions.erase(command->get_queue_name());
	this->partition_leader_nodes.erase(command->get_queue_name());
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

	this->nodes_partition_counts->insert(node_id, partitions_count + 1);

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

	this->nodes_partition_counts->insert(node_id, partitions_count - 1);
}

void ClusterMetadata::apply_partition_leader_assignment_command(PartitionLeaderAssignmentCommand* command) {
	this->last_queue_partition_leader_id = command->get_leader_id();

	auto queue_partition_lead_ids = this->queue_partition_leader_ids[command->get_queue_name()];

	if (queue_partition_lead_ids == nullptr) {
		queue_partition_lead_ids = std::make_shared<std::unordered_map<int, unsigned long long>>();
		this->queue_partition_leader_ids[command->get_queue_name()] = queue_partition_lead_ids;
	}

	((*queue_partition_lead_ids.get()))[command->get_partition()] = command->get_leader_id();

	auto partitions_leaders = this->partition_leader_nodes[command->get_queue_name()];

	if (partitions_leaders == nullptr) {
		partitions_leaders = std::make_shared<std::unordered_map<int, int>>();
		this->partition_leader_nodes[command->get_queue_name()] = partitions_leaders;
	}

	int node_id = command->get_new_leader();

	int lead_partitions_count = this->nodes_leader_partition_counts->get(node_id);

	(*(partitions_leaders.get()))[command->get_partition()] = node_id;

	this->nodes_leader_partition_counts->insert(node_id, lead_partitions_count + 1);

	this->replicated_offets[command->get_queue_name() + "_" + std::to_string(command->get_partition()) + "_" + std::to_string(command->get_new_leader())] =
		std::make_shared<std::unordered_map<int, unsigned long long>>();

	if (command->get_prev_leader() <= 0) return;

	node_id = command->get_prev_leader();

	lead_partitions_count = this->nodes_leader_partition_counts->get(node_id);

	this->nodes_leader_partition_counts->insert(node_id, lead_partitions_count - 1);

	this->replicated_offets.erase(
		command->get_queue_name() + "_" + std::to_string(command->get_partition()) + "_" + std::to_string(command->get_prev_leader())
	);
}

void ClusterMetadata::apply_register_consuer_group_command(RegisterConsumerGroupCommand* command) {
	this->last_consumer_id = command->get_consumer_id();

	auto queue_consumer_groups = this->partition_consumers[command->get_queue_name()];

	if (queue_consumer_groups == nullptr) {
		queue_consumer_groups = std::make_shared<std::unordered_map<std::string,std::shared_ptr<std::unordered_map<int, unsigned long long>>>>();
		this->partition_consumers[command->get_queue_name()] = queue_consumer_groups;
	}

	auto group_consumers = (*(queue_consumer_groups.get()))[command->get_group_id()];

	if (group_consumers == nullptr) {
		group_consumers = std::make_shared<std::unordered_map<int, unsigned long long>>();
		(*(queue_consumer_groups.get()))[command->get_group_id()] = group_consumers;
	}

	(*(group_consumers.get()))[command->get_partition_id()] = command->get_consumer_id();

	this->consumers_partition_counts->insert(
		command->get_consumer_id(),
		this->consumers_partition_counts->get(command->get_consumer_id()) + 1
	);

	this->consumers_partition_counts_inverse->insert(
		command->get_consumer_id(),
		this->consumers_partition_counts_inverse->get(command->get_consumer_id()) + 1
	);

	this->consumers_consume_init_point[command->get_consumer_id()] = command->get_consume_from_beginning();

	if (command->get_stole_from_consumer() > 0) {
		int total_assigned_partitions = this->consumers_partition_counts->remove(command->get_stole_from_consumer());
		this->consumers_partition_counts_inverse->remove(command->get_stole_from_consumer());

		if (--total_assigned_partitions > 0) {
			this->consumers_partition_counts->insert(command->get_stole_from_consumer(), total_assigned_partitions);
			this->consumers_partition_counts_inverse->insert(command->get_stole_from_consumer(), total_assigned_partitions);
		}
	}
}

void ClusterMetadata::apply_unregister_consuer_group_command(UnregisterConsumerGroupCommand* command) {
	auto queue_consumer_groups = this->partition_consumers[command->get_queue_name()];

	if (queue_consumer_groups == nullptr) return;

	auto group_consumers = (*(queue_consumer_groups.get()))[command->get_group_id()];

	if (group_consumers == nullptr) return;

	group_consumers.get()->erase(command->get_partition_id());

	int total_assigned_partitions = this->consumers_partition_counts->remove(command->get_consumer_id());
	this->consumers_partition_counts_inverse->remove(command->get_consumer_id());

	if (--total_assigned_partitions > 0) {
		this->consumers_partition_counts->insert(command->get_consumer_id(), total_assigned_partitions);
		this->consumers_partition_counts_inverse->insert(command->get_consumer_id(), total_assigned_partitions);
	}

	this->consumers_consume_init_point.erase(command->get_consumer_id());
}

void ClusterMetadata::apply_add_lagging_follower_command(AddLaggingFollowerCommand* command) {
	auto queue_partitions_lag_followers = this->lagging_followers[command->get_queue_name()];

	if (queue_partitions_lag_followers == nullptr) {
		queue_partitions_lag_followers = std::make_shared<std::unordered_map<int, std::shared_ptr<std::unordered_set<int>>>>();
		this->lagging_followers[command->get_queue_name()] = queue_partitions_lag_followers;
	}

	auto partition_lag_followers = (*(queue_partitions_lag_followers.get()))[command->get_partition_id()];

	if (partition_lag_followers == nullptr) {
		partition_lag_followers = std::make_shared<std::unordered_set<int>>();
		(*(queue_partitions_lag_followers.get()))[command->get_partition_id()] = partition_lag_followers;
	}

	partition_lag_followers.get()->insert(command->get_node_id());
}

void ClusterMetadata::apply_remove_lagging_follower_command(RemoveLaggingFollowerCommand* command) {
	auto queue_partitions_lag_followers = this->lagging_followers[command->get_queue_name()];

	if (queue_partitions_lag_followers == nullptr) {
		this->lagging_followers.erase(command->get_queue_name());
		return;
	}

	auto partition_lag_followers = (*(queue_partitions_lag_followers.get()))[command->get_partition_id()];

	if (partition_lag_followers == nullptr) {
		queue_partitions_lag_followers.get()->erase(command->get_partition_id());
		return;
	}

	partition_lag_followers.get()->erase(command->get_node_id());
}

void ClusterMetadata::apply_register_transaction_group_command(RegisterTransactionGroupCommand* command) {
	std::lock_guard<std::shared_mutex> lock(this->transaction_groups_mut);

	auto nodes_assigned_transaction_groups = this->nodes_transaction_groups[command->get_node_id()];

	if (nodes_assigned_transaction_groups == nullptr) {
		nodes_assigned_transaction_groups = std::make_shared<std::unordered_map<unsigned long long, std::shared_ptr<std::unordered_set<std::string>>>>();
		this->nodes_transaction_groups[command->get_node_id()] = nodes_assigned_transaction_groups;
	}

	auto assigned_queues = std::make_shared<std::unordered_set<std::string>>();

	(*(nodes_assigned_transaction_groups.get()))[command->get_transaction_group_id()] = assigned_queues;

	for (const std::string& queue_name : *(command->get_registered_queues()))
		assigned_queues->insert(queue_name);

	this->nodes_transaction_groups_counts->insert(
		command->get_node_id(),
		this->nodes_transaction_groups_counts->get(command->get_node_id()) + 1
	);

	{
		std::lock_guard<std::mutex> ts_lock(this->transaction_ids_mut);
		this->transaction_ids[command->get_transaction_group_id()] = 0;
	}
}

void ClusterMetadata::apply_unregister_transaction_group_command(UnregisterTransactionGroupCommand* command) {
	std::lock_guard<std::shared_mutex> lock(this->transaction_groups_mut);

	if (this->nodes_transaction_groups.find(command->get_node_id()) == this->nodes_transaction_groups.end()) return;

	auto nodes_assigned_transaction_groups = this->nodes_transaction_groups[command->get_node_id()];

	if (nodes_assigned_transaction_groups.get()->find(command->get_transaction_group_id()) == nodes_assigned_transaction_groups.get()->end())
		return;

	nodes_assigned_transaction_groups.get()->erase(command->get_transaction_group_id());
	int count = this->nodes_transaction_groups_counts->remove(command->get_node_id());

	if (--count > 0)
		this->nodes_transaction_groups_counts->insert(command->get_node_id(), count);

	{
		std::lock_guard<std::mutex> ts_lock(this->transaction_ids_mut);
		this->transaction_ids.erase(command->get_transaction_group_id());
	}
}

void ClusterMetadata::copy_from(ClusterMetadata* obj) {
	std::lock_guard<std::shared_mutex> _lock1(obj->nodes_partitions_mut);
	std::lock_guard<std::shared_mutex> _lock2(obj->consumers_mut);

	std::lock_guard<std::shared_mutex> lock1(this->nodes_partitions_mut);
	std::lock_guard<std::shared_mutex> lock2(this->consumers_mut);

	this->leader_id.store(obj->leader_id.load());
	this->metadata_version.store(obj->metadata_version);
	this->current_term.store(obj->current_term);

	this->last_consumer_id = obj->last_consumer_id;
	this->last_queue_partition_leader_id = obj->last_queue_partition_leader_id;

	if(this->nodes_partition_counts != NULL)
		free(this->nodes_partition_counts);

	if(this->nodes_leader_partition_counts != NULL)
		free(this->nodes_leader_partition_counts);

	if (this->consumers_partition_counts != NULL)
		free(this->consumers_partition_counts);

	if (this->consumers_partition_counts_inverse != NULL)
		free(this->consumers_partition_counts_inverse);

	this->nodes_partition_counts = new IndexedHeap<int, int>([](int a, int b) { return a < b; }, 0, 0);
	this->nodes_leader_partition_counts = new IndexedHeap<int, int>([](int a, int b) { return a < b; }, 0, 0);
	this->consumers_partition_counts = new IndexedHeap<int, unsigned long long>([](int a, int b) { return a < b; }, 0, 0);
	this->consumers_partition_counts_inverse = new IndexedHeap<int, unsigned long long>([](int a, int b) { return a > b; }, 0, 0);

	this->nodes_partitions.clear();
	this->owned_partitions.clear();
	this->partition_leader_nodes.clear();
	this->partition_consumers.clear();
	this->consumers_consume_init_point.clear();
	this->queues.clear();

	for (auto& iter : obj->queues) {
		std::shared_ptr<QueueMetadata> queue = std::shared_ptr<QueueMetadata>(new QueueMetadata(
			iter.second.get()->get_name(),
			iter.second.get()->get_partitions(),
			iter.second.get()->get_replication_factor(),
			iter.second.get()->get_cleanup_policy()
		));

		this->queues[iter.first] = queue;
	}

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

	for (auto& iter : obj->queue_partition_leader_ids) {
		auto queue_partition_leader_ids = std::make_shared<std::unordered_map<int, unsigned long long>>();

		this->queue_partition_leader_ids[iter.first] = queue_partition_leader_ids;

		for (auto& iter_two : *(iter.second.get())) {
			(*(queue_partition_leader_ids.get()))[iter_two.first] = iter_two.second;
		}
	}

	for (auto& iter : obj->lagging_followers) {
		auto queue_partition_lagging_followers = std::make_shared<std::unordered_map<int, std::shared_ptr<std::unordered_set<int>>>>();

		this->lagging_followers[iter.first] = queue_partition_lagging_followers;

		for (auto& iter_two : *(iter.second.get())) {
			auto followers = std::make_shared<std::unordered_set<int>>();

			(*(iter.second.get()))[iter_two.first] = followers;

			for (int node_id : *(iter_two.second.get()))
				followers.get()->insert(node_id);
		}
	}

	for (auto& iter : obj->partition_consumers) {
		auto queue_consumer_groups = std::make_shared<std::unordered_map<std::string,std::shared_ptr<std::unordered_map<int, unsigned long long>>>>();

		this->partition_consumers[iter.first] = queue_consumer_groups;

		for (auto& iter_two : *(iter.second.get())) {
			auto consumer_groups = std::make_shared<std::unordered_map<int, unsigned long long>>();

			for (auto& iter_three : *(iter_two.second.get())) {
				(*(consumer_groups.get()))[iter_three.first] = iter_three.second;

				this->consumers_partition_counts->insert(
					iter_three.second,
					this->consumers_partition_counts->get(iter_three.second) + 1
				);

				this->consumers_partition_counts_inverse->insert(
					iter_three.second,
					this->consumers_partition_counts->get(iter_three.second) + 1
				);
			}

			(*(queue_consumer_groups.get()))[iter.first] = consumer_groups;
		}
	}

	for (auto& iter : obj->consumers_consume_init_point)
		this->consumers_consume_init_point[iter.first] = iter.second;
}

int ClusterMetadata::get_partition_leader(const std::string& queue, int partition) {
	std::shared_lock<std::shared_mutex> lock(this->nodes_partitions_mut);

	if (this->partition_leader_nodes.find(queue) == this->partition_leader_nodes.end()) return -1;

	auto queue_partition_leads = this->partition_leader_nodes[queue];

	if (queue_partition_leads == nullptr) return -1;

	if (queue_partition_leads.get()->find(partition) == queue_partition_leads.get()->end()) return -1;

	return (*(queue_partition_leads.get()))[partition];
}

bool ClusterMetadata::is_node_partition_owner(const std::string& queue, int partition, int node_id) {
	std::shared_lock<std::shared_mutex> lock(this->nodes_partitions_mut);

	if (this->owned_partitions.find(queue) == this->owned_partitions.end()) return false;

	auto partition_owners = this->owned_partitions[queue];

	if (partition_owners == nullptr) return false;

	if (partition_owners.get()->find(partition) == partition_owners.get()->end()) return false;

	auto partition_nodes = (*(partition_owners.get()))[partition];

	if (partition_nodes == nullptr) return false;

	return partition_nodes.get()->find(node_id) != partition_nodes.get()->end();
}

bool ClusterMetadata::is_follower_lagging(const std::string& queue, int partition, int follower_id) {
	if (this->lagging_followers.find(queue) == this->lagging_followers.end()) return false;

	auto queue_partition_lag_followers = this->lagging_followers[queue];

	if (queue_partition_lag_followers == nullptr) return false;

	if (queue_partition_lag_followers.get()->find(partition) == queue_partition_lag_followers.get()->end()) return false;

	auto partition_lag_followers = (*(queue_partition_lag_followers.get()))[partition];

	if (partition_lag_followers == nullptr) return false;

	return partition_lag_followers.get()->find(follower_id) != partition_lag_followers.get()->end();
}

std::shared_mutex* ClusterMetadata::get_partitions_mut() {
	return &this->nodes_partitions_mut;
}

std::shared_ptr<std::unordered_map<int, int>> ClusterMetadata::get_queue_partition_leaders(const std::string& queue_name) {
	if (this->partition_leader_nodes.find(queue_name) == this->partition_leader_nodes.end()) return nullptr;
	return this->partition_leader_nodes[queue_name];
}

unsigned long long ClusterMetadata::get_queue_partition_leader_id(const std::string& queue_name, int partition) {
	std::shared_lock<std::shared_mutex> lock(this->nodes_partitions_mut);

	if (this->queue_partition_leader_ids.find(queue_name) == this->queue_partition_leader_ids.end()) return 0;

	auto queue_partitions_lead_ids = this->queue_partition_leader_ids[queue_name];

	if (queue_partitions_lead_ids == nullptr) return 0;

	if (queue_partitions_lead_ids.get()->find(partition) == queue_partitions_lead_ids.get()->end()) return 0;

	return (*(queue_partitions_lead_ids.get()))[partition];
}

bool ClusterMetadata::check_if_follower_is_lagging(const std::string& queue, int partition, int follower_id) {
	std::shared_lock<std::shared_mutex> lock(this->nodes_partitions_mut);
	return this->is_follower_lagging(queue, partition, follower_id);
}