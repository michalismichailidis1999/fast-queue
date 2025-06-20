#include "../../header_files/cluster_management/ClusterMetadata.h"

ClusterMetadata::ClusterMetadata(int node_id) {
	this->metadata_version = 0;
	this->current_term = 0;
	this->leader_id = 0;

	this->nodes_partition_counts = new IndexedHeap<int, int>([](int a, int b) { return a < b; }, -1, -1);
	this->nodes_leader_partition_counts = new IndexedHeap<int, int>([](int a, int b) { return a < b; }, -1, -1);

	this->init_node_partitions(node_id);
}

ClusterMetadata::ClusterMetadata() {
	this->metadata_version = 0;
	this->current_term = 0;
	this->leader_id = 0;

	this->nodes_partition_counts = NULL;
	this->nodes_leader_partition_counts = NULL;
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

void ClusterMetadata::remove_node_partitions(int node_id) {
	std::lock_guard<std::mutex> lock(this->nodes_partitions_mut);

	if (this->nodes_partitions.find(node_id) != this->nodes_partitions.end()) {
		this->nodes_partitions.erase(node_id);
		this->nodes_partition_counts->remove(node_id);
		this->nodes_leader_partition_counts->remove(node_id);
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

	if (partitions_leaders == nullptr) return;

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

	this->nodes_partition_counts = new IndexedHeap<int, int>([](int a, int b) { return a < b; }, -1, -1);
	this->nodes_leader_partition_counts = new IndexedHeap<int, int>([](int a, int b) { return a < b; }, -1, -1);

	for (auto iter : obj->nodes_partitions) {
		auto partitions = std::make_shared<std::unordered_map<std::string, std::shared_ptr<std::unordered_set<int>>>>();

		int partitions_count = 0;

		this->nodes_partitions[iter.first] = partitions;

		for (auto iter_two : *(iter.second.get())) {
			auto queue_partitions = std::make_shared<std::unordered_set<int>>();
			(*(partitions.get()))[iter_two.first] = queue_partitions;

			for (auto queue_partition : *(iter_two.second.get())) {
				queue_partitions.get()->insert(queue_partition);
				partitions_count++;
			}
		}

		this->nodes_partition_counts->insert(iter.first, partitions_count);
	}
}

void ClusterMetadata::fill_from_metadata(void* metadata) {
	if (!Helper::has_valid_checksum(metadata))
		throw CorruptionException("Compacted cluster metadata was corrupted");

	std::lock_guard<std::mutex> lock(this->nodes_partitions_mut);

	int total_queues = 0;
	int total_assignments = 0;

	memcpy_s(&this->metadata_version, METADATA_VERSION_SIZE, (char*)metadata + METADATA_VERSION_OFFSET, METADATA_VERSION_SIZE);
	memcpy_s(&this->current_term, TERM_SIZE, (char*)metadata + TERM_OFFSET, TERM_SIZE);
	memcpy_s(&total_queues, TOTAL_QUEUES_SIZE, (char*)metadata + TOTAL_QUEUES_OFFSET, TOTAL_QUEUES_SIZE);
	memcpy_s(&total_assignments, TOTAL_PARTITION_ASSIGNMENTS_SIZE, (char*)metadata + TOTAL_PARTITION_ASSIGNMENTS_OFFSET, TOTAL_PARTITION_ASSIGNMENTS_SIZE);

	if(total_queues > total_assignments || (total_queues == 0 && total_assignments != 0))
		throw CorruptionException("Compacted cluster metadata was corrupted");

	long offset = TOTAL_PARTITION_ASSIGNMENTS_BYTES;

	for (unsigned int i = 0; i < total_queues; i++) {
		int queue_name_length = 0;
		int queue_total_assignments = 0;

		memcpy_s(&queue_name_length, PA_QUEUE_NAME_LENGTH_SIZE, (char*)metadata + PA_QUEUE_NAME_LENGTH_OFFSET + offset, PA_QUEUE_NAME_LENGTH_SIZE);
		memcpy_s(&queue_total_assignments, PA_QUEUE_TOTAL_ASSIGNMENTS_SIZE, (char*)metadata + PA_QUEUE_TOTAL_ASSIGNMENTS_OFFSET + offset, PA_QUEUE_TOTAL_ASSIGNMENTS_SIZE);

		std::string queue_name = std::string((char*)metadata + PA_QUEUE_NAME_OFFSET + offset, queue_name_length);

		offset += PA_QUEUE_TOTAL_BYTES;

		this->owned_partitions[queue_name] = std::make_shared<std::unordered_map<int, std::shared_ptr<std::unordered_set<int>>>>();
		this->partition_leader_nodes[queue_name] = std::make_shared<std::unordered_map<int, int>>();

		auto owned_parts = this->owned_partitions[queue_name];
		auto leads = this->partition_leader_nodes[queue_name];

		for (unsigned j = 0; j < queue_total_assignments; j++) {
			int partition_id = 0;
			int node_id = 0;
			bool is_lead = false;

			memcpy_s(&partition_id, PA_PARTITION_ID_SIZE, (char*)metadata + PA_PARTITION_ID_OFFSET + offset, PA_PARTITION_ID_SIZE);
			memcpy_s(&node_id, PA_NODE_ID_SIZE, (char*)metadata + PA_NODE_ID_OFFSET + offset, PA_NODE_ID_SIZE);
			memcpy_s(&is_lead, PA_IS_LEAD_SIZE, (char*)metadata + PA_IS_LEAD_OFFSET + offset, PA_IS_LEAD_SIZE);

			if(partition_id < 0 || node_id <= 0)
				throw CorruptionException("Compacted cluster metadata was corrupted (partition assignment)");

			if (owned_parts.get()->find(partition_id) == owned_parts.get()->end())
				(*(owned_parts.get()))[partition_id] = std::make_shared<std::unordered_set<int>>(node_id);

			if (this->nodes_partitions.find(node_id) == this->nodes_partitions.end())
				this->nodes_partitions[node_id] = std::make_shared<std::unordered_map<std::string, std::shared_ptr<std::unordered_set<int>>>>();

			auto node_parts = this->nodes_partitions[node_id];

			if (node_parts.get()->find(queue_name) == node_parts.get()->end())
				(*(node_parts.get()))[queue_name] = std::make_shared<std::unordered_set<int>>();

			(*(node_parts.get()))[queue_name].get()->insert(partition_id);

			int current_counts = this->nodes_partition_counts->get(node_id);
			this->nodes_partition_counts->insert(node_id, current_counts > 0 ? current_counts + 1 : 1);

			if (is_lead) {
				(*(leads.get()))[partition_id] = node_id;

				int current_lead_counts = this->nodes_leader_partition_counts->get(node_id);
				this->nodes_leader_partition_counts->insert(node_id, current_counts > 0 ? current_counts + 1 : 1);
			}

			offset += PA_NODE_ASSIGNMENT_TOTAL_BYTES;
		}
	}
}

std::tuple<unsigned int, std::shared_ptr<char>> ClusterMetadata::get_metadata_bytes() {
	std::lock_guard<std::mutex> lock(this->nodes_partitions_mut);

	int total_queues = this->owned_partitions.size();

	int total_assignments = 0;

	for (auto& iter : this->nodes_partitions)
		total_assignments += iter.second.get()->size();

	unsigned int total_bytes = TOTAL_PARTITION_ASSIGNMENTS_BYTES 
		+ total_queues * PA_QUEUE_TOTAL_BYTES 
		+ total_assignments * PA_NODE_ASSIGNMENT_TOTAL_BYTES;

	std::shared_ptr<char> bytes = std::shared_ptr<char>(new char[total_bytes]);

	Helper::add_common_metadata_values(bytes.get(), total_bytes, ObjectType::METADATA);

	memcpy_s(bytes.get() + METADATA_VERSION_OFFSET, METADATA_VERSION_SIZE, &this->metadata_version, METADATA_VERSION_SIZE);
	memcpy_s(bytes.get() + TERM_OFFSET, TERM_SIZE, &this->current_term, TERM_SIZE);
	memcpy_s(bytes.get() + TOTAL_QUEUES_OFFSET, TOTAL_QUEUES_SIZE, &total_queues, TOTAL_QUEUES_SIZE);
	memcpy_s(bytes.get() + TOTAL_PARTITION_ASSIGNMENTS_OFFSET, TOTAL_PARTITION_ASSIGNMENTS_SIZE, &total_assignments, TOTAL_PARTITION_ASSIGNMENTS_SIZE);

	long offset = TOTAL_PARTITION_ASSIGNMENTS_BYTES;

	for (auto& iter : this->owned_partitions) {
		unsigned int queue_name_length = iter.first.size();

		memcpy_s(bytes.get() + PA_QUEUE_NAME_LENGTH_OFFSET + offset, PA_QUEUE_NAME_LENGTH_SIZE, &queue_name_length, PA_QUEUE_NAME_LENGTH_SIZE);
		memcpy_s(bytes.get() + PA_QUEUE_NAME_OFFSET + offset, PA_QUEUE_NAME_SIZE, iter.first.c_str(), PA_QUEUE_NAME_SIZE);

		long current_offset = offset;
		offset += PA_QUEUE_TOTAL_BYTES;

		unsigned int total_queue_assignments = 0;

		for (auto& part_iter : *(iter.second.get())) {
			for (auto& node_id : *(part_iter.second.get())) {
				int partition_leader = (*(this->partition_leader_nodes[iter.first].get()))[part_iter.first];

				bool is_lead = partition_leader == node_id;

				memcpy_s(bytes.get() + PA_PARTITION_ID_OFFSET + offset, PA_PARTITION_ID_SIZE, &part_iter.first, PA_PARTITION_ID_SIZE);
				memcpy_s(bytes.get() + PA_NODE_ID_OFFSET + offset, PA_NODE_ID_SIZE, &node_id, PA_NODE_ID_SIZE);
				memcpy_s(bytes.get() + PA_IS_LEAD_OFFSET + offset, PA_IS_LEAD_SIZE, &is_lead, PA_IS_LEAD_SIZE);

				offset += PA_NODE_ASSIGNMENT_TOTAL_BYTES;
				total_queue_assignments++;
			}
		}

		memcpy_s(bytes.get() + PA_QUEUE_TOTAL_ASSIGNMENTS_OFFSET + current_offset, PA_QUEUE_TOTAL_ASSIGNMENTS_SIZE, &total_queue_assignments, PA_QUEUE_TOTAL_ASSIGNMENTS_SIZE);
	}

	return std::tuple<unsigned int, std::shared_ptr<char>>(total_bytes, bytes);
}

int ClusterMetadata::get_partition_leader(const std::string& queue, int partition) {
	std::lock_guard<std::mutex> lock(this->nodes_partitions_mut);

	if (this->partition_leader_nodes.find(queue) == this->partition_leader_nodes.end()) return -1;

	auto queue_partition_leads = this->partition_leader_nodes[queue];

	if (queue_partition_leads.get()->find(partition) == queue_partition_leads.get()->end()) return -1;

	return (*(queue_partition_leads.get()))[partition];
}