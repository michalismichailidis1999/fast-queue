#include "../../header_files/cluster_management/Commands.h"

Command::Command() {}

Command::Command(CommandType type, unsigned long long term, unsigned long long timestamp, std::shared_ptr<void> command_info) {
	this->term = term;
	this->type = type;
	this->metadata_version = 0;
	this->timestamp = timestamp;
	this->command_info = command_info;
}

Command::Command(void* metadata) {
	if (!Helper::has_valid_checksum(metadata))
		throw CorruptionException("Command metadata was corrupted");

	Helper::retrieve_message_metadata_values(metadata, &this->metadata_version, &this->timestamp);

	memcpy_s(&this->type, COMMAND_TYPE_SIZE, (char*)metadata + COMMAND_TYPE_OFFSET, COMMAND_TYPE_SIZE);
	memcpy_s(&this->term, COMMAND_TERM_SIZE, (char*)metadata + COMMAND_TERM_OFFSET, COMMAND_TERM_SIZE);

	switch (this->type) {
	case CommandType::CREATE_QUEUE:
		this->command_info = std::shared_ptr<CreateQueueCommand>(new CreateQueueCommand(metadata));
		break;
	case CommandType::ALTER_PARTITION_ASSIGNMENT:
		this->command_info = std::shared_ptr<PartitionAssignmentCommand>(new PartitionAssignmentCommand(metadata));
		break;
	case CommandType::ALTER_PARTITION_LEADER_ASSIGNMENT:
		this->command_info = std::shared_ptr<PartitionLeaderAssignmentCommand>(new PartitionLeaderAssignmentCommand(metadata));
		break;
	case CommandType::DELETE_QUEUE:
		this->command_info = std::shared_ptr<DeleteQueueCommand>(new DeleteQueueCommand(metadata));
		break;
	case CommandType::REGISTER_DATA_NODE:
		this->command_info = std::shared_ptr<RegisterDataNodeCommand>(new RegisterDataNodeCommand(metadata));
		break;
	case CommandType::UNREGISTER_DATA_NODE:
		this->command_info = std::shared_ptr<UnregisterDataNodeCommand>(new UnregisterDataNodeCommand(metadata));
		break;
	case CommandType::REGISTER_CONSUMER_GROUP:
		this->command_info = std::shared_ptr<RegisterConsumerGroupCommand>(new RegisterConsumerGroupCommand(metadata));
		break;
	case CommandType::UNREGISTER_CONSUMER_GROUP:
		this->command_info = std::shared_ptr<UnregisterConsumerGroupCommand>(new UnregisterConsumerGroupCommand(metadata));
		break;
	case CommandType::ADD_LAGGING_FOLLOWER:
		this->command_info = std::shared_ptr<AddLaggingFollowerCommand>(new AddLaggingFollowerCommand(metadata));
		break;
	case CommandType::REMOVE_LAGGING_FOLLOWER:
		this->command_info = std::shared_ptr<RemoveLaggingFollowerCommand>(new RemoveLaggingFollowerCommand(metadata));
		break;
	case CommandType::REGISTER_TRANSACTION_GROUP:
		this->command_info = std::shared_ptr<RegisterTransactionGroupCommand>(new RegisterTransactionGroupCommand(metadata));
		break;
	case CommandType::UNREGISTER_TRANSACTION_GROUP:
		this->command_info = std::shared_ptr<UnregisterTransactionGroupCommand>(new UnregisterTransactionGroupCommand(metadata));
		break;
	default:
		break;
	}
}

CommandType Command::get_command_type() {
	return this->type;
}

void* Command::get_command_info() {
	return this->command_info.get();
}

unsigned long long Command::get_timestamp() {
	return this->timestamp;
}

unsigned long long Command::get_metadata_version() {
	return this->metadata_version;
}

unsigned long long Command::get_term() {
	return this->term;
}

void Command::set_metadata_version(unsigned long long metadata_version) {
	this->metadata_version = metadata_version;
}

std::tuple<long, std::shared_ptr<char>> Command::get_metadata_bytes() {
	long total_bytes = 0;
	int size_dif = COMMAND_TOTAL_BYTES;

	unsigned int key_offset = 0;

	std::string key = "";

	switch (this->type) {
	case CommandType::CREATE_QUEUE:
		key = ((CreateQueueCommand*)(this->command_info.get()))->get_command_key();
		total_bytes = CQ_COMMAND_TOTAL_BYTES + key.size();
		size_dif = CQ_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES;
		key_offset = CQ_COMMAND_TOTAL_BYTES;
		break;
	case CommandType::ALTER_PARTITION_ASSIGNMENT:
		key = ((PartitionAssignmentCommand*)(this->command_info.get()))->get_command_key();
		total_bytes = PA_COMMAND_TOTAL_BYTES + key.size();
		size_dif = PA_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES;
		key_offset = PA_COMMAND_TOTAL_BYTES;
		break;
	case CommandType::ALTER_PARTITION_LEADER_ASSIGNMENT:
		key = ((PartitionLeaderAssignmentCommand*)(this->command_info.get()))->get_command_key();
		total_bytes = PLA_COMMAND_TOTAL_BYTES + key.size();
		size_dif = PLA_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES;
		key_offset = PLA_COMMAND_TOTAL_BYTES;
		break;
	case CommandType::DELETE_QUEUE:
		key = ((DeleteQueueCommand*)(this->command_info.get()))->get_command_key();
		total_bytes = DQ_COMMAND_TOTAL_BYTES + key.size();
		size_dif = DQ_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES;
		key_offset = DQ_COMMAND_TOTAL_BYTES;
		break;
	case CommandType::REGISTER_DATA_NODE:
		key = ((RegisterDataNodeCommand*)(this->command_info.get()))->get_command_key();
		total_bytes = RDN_COMMAND_TOTAL_BYTES + key.size();
		size_dif = RDN_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES;
		key_offset = RDN_COMMAND_TOTAL_BYTES;
		break;
	case CommandType::UNREGISTER_DATA_NODE:
		key = ((UnregisterDataNodeCommand*)(this->command_info.get()))->get_command_key();
		total_bytes = UDN_COMMAND_TOTAL_BYTES + key.size();
		size_dif = UDN_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES;
		key_offset = UDN_COMMAND_TOTAL_BYTES;
		break;
	case CommandType::REGISTER_CONSUMER_GROUP:
		key = ((RegisterConsumerGroupCommand*)(this->command_info.get()))->get_command_key();
		total_bytes = RCG_COMMAND_TOTAL_BYTES + key.size();
		size_dif = RCG_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES;
		key_offset = RCG_COMMAND_TOTAL_BYTES;
		break;
	case CommandType::UNREGISTER_CONSUMER_GROUP:
		key = ((UnregisterConsumerGroupCommand*)(this->command_info.get()))->get_command_key();
		total_bytes = UCG_COMMAND_TOTAL_BYTES + key.size();
		size_dif = UCG_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES;
		key_offset = UCG_COMMAND_TOTAL_BYTES;
		break;
	case CommandType::ADD_LAGGING_FOLLOWER:
		key = ((AddLaggingFollowerCommand*)(this->command_info.get()))->get_command_key();
		total_bytes = ALF_COMMAND_TOTAL_BYTES + key.size();
		size_dif = ALF_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES;
		key_offset = ALF_COMMAND_TOTAL_BYTES;
		break;
	case CommandType::REMOVE_LAGGING_FOLLOWER:
		key = ((RemoveLaggingFollowerCommand*)(this->command_info.get()))->get_command_key();
		total_bytes = RLF_COMMAND_TOTAL_BYTES + key.size();
		size_dif = RLF_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES;
		key_offset = RLF_COMMAND_TOTAL_BYTES;
		break;
	case CommandType::REGISTER_TRANSACTION_GROUP:
		key = ((RegisterTransactionGroupCommand*)(this->command_info.get()))->get_command_key();
		total_bytes = RTG_COMMAND_TOTAL_BYTES + key.size();

		for (const std::string& queue_name : *(((RegisterTransactionGroupCommand*)(this->command_info.get()))->get_registered_queues()))
			total_bytes += sizeof(int) + queue_name.size();

		size_dif = total_bytes - key.size() - COMMAND_TOTAL_BYTES;
		key_offset = total_bytes - key.size();
		break;
	case CommandType::UNREGISTER_TRANSACTION_GROUP:
		key = ((UnregisterTransactionGroupCommand*)(this->command_info.get()))->get_command_key();
		total_bytes = UTG_COMMAND_TOTAL_BYTES + key.size();
		size_dif = UTG_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES;
		key_offset = UTG_COMMAND_TOTAL_BYTES;
		break;
	default:
		return std::tuple<long, std::shared_ptr<char>>(0, nullptr);
	}

	std::shared_ptr<char> bytes = std::shared_ptr<char>(new char[total_bytes]);
	
	Helper::add_message_metadata_values(bytes.get(), this->metadata_version, this->timestamp, key.size(), key.c_str(), key_offset);

	memcpy_s(bytes.get() + COMMAND_TYPE_OFFSET, COMMAND_TYPE_SIZE, &this->type, COMMAND_TYPE_SIZE);
	memcpy_s(bytes.get() + COMMAND_TERM_OFFSET, COMMAND_TERM_SIZE, &this->term, COMMAND_TERM_SIZE);

	switch (this->type) {
	case CommandType::CREATE_QUEUE:
		memcpy_s(
			bytes.get() + COMMAND_TOTAL_BYTES,
			size_dif, 
			((CreateQueueCommand*)(this->command_info.get()))->get_metadata_bytes().get(), 
			size_dif
		);
		break;
	case CommandType::ALTER_PARTITION_ASSIGNMENT:
		memcpy_s(
			bytes.get() + COMMAND_TOTAL_BYTES,
			size_dif,
			((PartitionAssignmentCommand*)(this->command_info.get()))->get_metadata_bytes().get(),
			size_dif
		);
		break;
	case CommandType::ALTER_PARTITION_LEADER_ASSIGNMENT:
		memcpy_s(
			bytes.get() + COMMAND_TOTAL_BYTES,
			size_dif,
			((PartitionLeaderAssignmentCommand*)(this->command_info.get()))->get_metadata_bytes().get(),
			size_dif
		);
		break;
	case CommandType::DELETE_QUEUE:
		memcpy_s(
			bytes.get() + COMMAND_TOTAL_BYTES,
			size_dif,
			((DeleteQueueCommand*)(this->command_info.get()))->get_metadata_bytes().get(),
			size_dif
		);
		break;
	case CommandType::REGISTER_DATA_NODE:
		memcpy_s(
			bytes.get() + COMMAND_TOTAL_BYTES,
			size_dif,
			((RegisterDataNodeCommand*)(this->command_info.get()))->get_metadata_bytes().get(),
			size_dif
		);
		break;
	case CommandType::UNREGISTER_DATA_NODE:
		memcpy_s(
			bytes.get() + COMMAND_TOTAL_BYTES,
			size_dif,
			((UnregisterDataNodeCommand*)(this->command_info.get()))->get_metadata_bytes().get(),
			size_dif
		);
		break;
	case CommandType::REGISTER_CONSUMER_GROUP:
		memcpy_s(
			bytes.get() + COMMAND_TOTAL_BYTES,
			size_dif,
			((RegisterConsumerGroupCommand*)(this->command_info.get()))->get_metadata_bytes().get(),
			size_dif
		);
		break;
	case CommandType::UNREGISTER_CONSUMER_GROUP:
		memcpy_s(
			bytes.get() + COMMAND_TOTAL_BYTES,
			size_dif,
			((UnregisterConsumerGroupCommand*)(this->command_info.get()))->get_metadata_bytes().get(),
			size_dif
		);
		break;
	case CommandType::ADD_LAGGING_FOLLOWER:
		memcpy_s(
			bytes.get() + COMMAND_TOTAL_BYTES,
			size_dif,
			((AddLaggingFollowerCommand*)(this->command_info.get()))->get_metadata_bytes().get(),
			size_dif
		);
		break;
	case CommandType::REMOVE_LAGGING_FOLLOWER:
		memcpy_s(
			bytes.get() + COMMAND_TOTAL_BYTES,
			size_dif,
			((RemoveLaggingFollowerCommand*)(this->command_info.get()))->get_metadata_bytes().get(),
			size_dif
		);
		break;
	case CommandType::REGISTER_TRANSACTION_GROUP:
		memcpy_s(
			bytes.get() + COMMAND_TOTAL_BYTES,
			size_dif,
			((RegisterTransactionGroupCommand*)(this->command_info.get()))->get_metadata_bytes().get(),
			size_dif
		);
		break;
	case CommandType::UNREGISTER_TRANSACTION_GROUP:
		memcpy_s(
			bytes.get() + COMMAND_TOTAL_BYTES,
			size_dif,
			((UnregisterTransactionGroupCommand*)(this->command_info.get()))->get_metadata_bytes().get(),
			size_dif
		);
		break;
	default:
		return std::tuple<long, std::shared_ptr<char>>(0, nullptr);
	}

	Helper::add_common_metadata_values(bytes.get(), total_bytes);

	return std::tuple<long, std::shared_ptr<char>>(total_bytes, bytes);
}

// Create Queue Command

CreateQueueCommand::CreateQueueCommand(const std::string& queue_name, int partitions, int replication_factor) {
	this->queue_name = queue_name;
	this->partitions = partitions;
	this->replication_factor = replication_factor;
}

CreateQueueCommand::CreateQueueCommand(void* metadata) {
	int queue_name_size = 0;

	memcpy_s(&queue_name_size, CQ_COMMAND_QUEUE_NAME_LENGTH_SIZE, (char*)metadata + CQ_COMMAND_QUEUE_NAME_LENGTH_OFFSET, CQ_COMMAND_QUEUE_NAME_LENGTH_SIZE);
	
	this->queue_name = std::string((char*)metadata + CQ_COMMAND_QUEUE_NAME_OFFSET, queue_name_size);

	memcpy_s(&this->partitions, CQ_COMMAND_PARTITION_SIZE, (char*)metadata + CQ_COMMAND_PARTITION_OFFSET, CQ_COMMAND_PARTITION_SIZE);
	memcpy_s(&this->replication_factor, CQ_COMMAND_REPLICATION_SIZE, (char*)metadata + CQ_COMMAND_REPLICATION_OFFSET, CQ_COMMAND_REPLICATION_SIZE);
}

const std::string& CreateQueueCommand::get_queue_name() {
	return this->queue_name;
}

int CreateQueueCommand::get_partitions() {
	return this->partitions;
}

int CreateQueueCommand::get_replication_factor() {
	return this->replication_factor;
}

std::shared_ptr<char> CreateQueueCommand::get_metadata_bytes() {
	std::shared_ptr<char> bytes = std::shared_ptr<char>(new char[CQ_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES]);

	int queue_name_size = this->queue_name.size();

	memcpy_s(bytes.get() + CQ_COMMAND_QUEUE_NAME_LENGTH_OFFSET - COMMAND_TOTAL_BYTES, CQ_COMMAND_QUEUE_NAME_LENGTH_SIZE, &queue_name_size, CQ_COMMAND_QUEUE_NAME_LENGTH_SIZE);
	memcpy_s(bytes.get() + CQ_COMMAND_QUEUE_NAME_OFFSET - COMMAND_TOTAL_BYTES, queue_name_size, this->queue_name.c_str(), queue_name_size);
	memcpy_s(bytes.get() + CQ_COMMAND_PARTITION_OFFSET - COMMAND_TOTAL_BYTES, CQ_COMMAND_PARTITION_SIZE, &this->partitions, CQ_COMMAND_PARTITION_SIZE);
	memcpy_s(bytes.get() + CQ_COMMAND_REPLICATION_OFFSET - COMMAND_TOTAL_BYTES, CQ_COMMAND_REPLICATION_SIZE, &this->replication_factor, CQ_COMMAND_REPLICATION_SIZE);

	return bytes;
}

std::string CreateQueueCommand::get_command_key() {
	return "cq_" + this->queue_name;
}

// ================================================================

// Partition Assignment Command

PartitionAssignmentCommand::PartitionAssignmentCommand(const std::string& queue_name, int partition, int to_node, int from_node) {
	this->queue_name = queue_name;
	this->partition = partition;
	this->to_node = to_node;
	this->from_node = from_node;
}

PartitionAssignmentCommand::PartitionAssignmentCommand(void* metadata) {
	int queue_name_size = 0;

	memcpy_s(&queue_name_size, PA_COMMAND_QUEUE_NAME_LENGTH_SIZE, (char*)metadata + PA_COMMAND_QUEUE_NAME_LENGTH_OFFSET, PA_COMMAND_QUEUE_NAME_LENGTH_SIZE);

	this->queue_name = std::string((char*)metadata + PA_COMMAND_QUEUE_NAME_OFFSET, queue_name_size);

	memcpy_s(&this->partition, PA_COMMAND_PARTITION_SIZE, (char*)metadata + PA_COMMAND_PARTITION_OFFSET, PA_COMMAND_PARTITION_SIZE);
	memcpy_s(&this->to_node, PA_COMMAND_TO_NODE_SIZE, (char*)metadata + PA_COMMAND_TO_NODE_OFFSET, PA_COMMAND_TO_NODE_SIZE);
	memcpy_s(&this->from_node, PA_COMMAND_FROM_NODE_SIZE, (char*)metadata + PA_COMMAND_FROM_NODE_OFFSET, PA_COMMAND_FROM_NODE_SIZE);
}

const std::string& PartitionAssignmentCommand::get_queue_name() {
	return this->queue_name;
}

int PartitionAssignmentCommand::get_partition() {
	return this->partition;
}

int PartitionAssignmentCommand::get_to_node() {
	return this->to_node;
}

int PartitionAssignmentCommand::get_from_node() {
	return this->from_node;
}

std::shared_ptr<char> PartitionAssignmentCommand::get_metadata_bytes() {
	std::shared_ptr<char> bytes = std::shared_ptr<char>(new char[PA_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES]);

	int queue_name_size = this->queue_name.size();

	memcpy_s(bytes.get() + PA_COMMAND_QUEUE_NAME_LENGTH_OFFSET - COMMAND_TOTAL_BYTES, PA_COMMAND_QUEUE_NAME_LENGTH_SIZE, &queue_name_size, PA_COMMAND_QUEUE_NAME_LENGTH_SIZE);
	memcpy_s(bytes.get() + PA_COMMAND_QUEUE_NAME_OFFSET - COMMAND_TOTAL_BYTES, queue_name_size, this->queue_name.c_str(), queue_name_size);
	memcpy_s(bytes.get() + PA_COMMAND_PARTITION_OFFSET - COMMAND_TOTAL_BYTES, PA_COMMAND_PARTITION_SIZE, &this->partition, PA_COMMAND_PARTITION_SIZE);
	memcpy_s(bytes.get() + PA_COMMAND_TO_NODE_OFFSET - COMMAND_TOTAL_BYTES, PA_COMMAND_TO_NODE_SIZE, &this->to_node, PA_COMMAND_TO_NODE_SIZE);
	memcpy_s(bytes.get() + PA_COMMAND_FROM_NODE_OFFSET - COMMAND_TOTAL_BYTES, PA_COMMAND_FROM_NODE_SIZE, &this->from_node, PA_COMMAND_FROM_NODE_SIZE);

	return bytes;
}

std::string PartitionAssignmentCommand::get_command_key() {
	return "pa_" + this->queue_name + "_" + std::to_string(this->partition);
}

// ================================================================

// Partition Leader Assignment Command

PartitionLeaderAssignmentCommand::PartitionLeaderAssignmentCommand(const std::string& queue_name, int partition, unsigned long long leader_id, int new_leader, int prev_leader) {
	this->queue_name = queue_name;
	this->partition = partition;
	this->leader_id = leader_id;
	this->new_leader = new_leader;
	this->prev_leader = prev_leader;
}

PartitionLeaderAssignmentCommand::PartitionLeaderAssignmentCommand(void* metadata) {
	int queue_name_size = 0;

	memcpy_s(&queue_name_size, PLA_COMMAND_QUEUE_NAME_LENGTH_SIZE, (char*)metadata + PLA_COMMAND_QUEUE_NAME_LENGTH_OFFSET, PLA_COMMAND_QUEUE_NAME_LENGTH_SIZE);

	this->queue_name = std::string((char*)metadata + PLA_COMMAND_QUEUE_NAME_OFFSET, queue_name_size);

	memcpy_s(&this->partition, PLA_COMMAND_PARTITION_SIZE, (char*)metadata + PLA_COMMAND_PARTITION_OFFSET, PLA_COMMAND_PARTITION_SIZE);
	memcpy_s(&this->leader_id, PLA_COMMAND_LEADER_ID_SIZE, (char*)metadata + PLA_COMMAND_LEADER_ID_OFFSET, PLA_COMMAND_LEADER_ID_SIZE);
	memcpy_s(&this->new_leader, PLA_COMMAND_NEW_LEADER_SIZE, (char*)metadata + PLA_COMMAND_NEW_LEADER_OFFSET, PLA_COMMAND_NEW_LEADER_SIZE);
	memcpy_s(&this->prev_leader, PLA_COMMAND_PREV_LEADER_SIZE, (char*)metadata + PLA_COMMAND_PREV_LEADER_OFFSET, PLA_COMMAND_PREV_LEADER_SIZE);
}

const std::string& PartitionLeaderAssignmentCommand::get_queue_name() {
	return this->queue_name;
}

int PartitionLeaderAssignmentCommand::get_partition() {
	return this->partition;
}

unsigned long long PartitionLeaderAssignmentCommand::get_leader_id() {
	return this->leader_id;
}

int PartitionLeaderAssignmentCommand::get_new_leader() {
	return this->new_leader;
}

int PartitionLeaderAssignmentCommand::get_prev_leader() {
	return this->prev_leader;
}

std::shared_ptr<char> PartitionLeaderAssignmentCommand::get_metadata_bytes() {
	std::shared_ptr<char> bytes = std::shared_ptr<char>(new char[PLA_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES]);

	int queue_name_size = this->queue_name.size();

	memcpy_s(bytes.get() + PLA_COMMAND_QUEUE_NAME_LENGTH_OFFSET - COMMAND_TOTAL_BYTES, PLA_COMMAND_QUEUE_NAME_LENGTH_SIZE, &queue_name_size, PLA_COMMAND_QUEUE_NAME_LENGTH_SIZE);
	memcpy_s(bytes.get() + PLA_COMMAND_QUEUE_NAME_OFFSET - COMMAND_TOTAL_BYTES, queue_name_size, this->queue_name.c_str(), queue_name_size);
	memcpy_s(bytes.get() + PLA_COMMAND_PARTITION_OFFSET - COMMAND_TOTAL_BYTES, PLA_COMMAND_PARTITION_SIZE, &this->partition, PLA_COMMAND_PARTITION_SIZE);
	memcpy_s(bytes.get() + PLA_COMMAND_LEADER_ID_OFFSET - COMMAND_TOTAL_BYTES, PLA_COMMAND_LEADER_ID_SIZE, &this->leader_id, PLA_COMMAND_LEADER_ID_SIZE);
	memcpy_s(bytes.get() + PLA_COMMAND_NEW_LEADER_OFFSET - COMMAND_TOTAL_BYTES, PLA_COMMAND_NEW_LEADER_SIZE, &this->new_leader, PLA_COMMAND_NEW_LEADER_SIZE);
	memcpy_s(bytes.get() + PLA_COMMAND_PREV_LEADER_OFFSET - COMMAND_TOTAL_BYTES, PLA_COMMAND_PREV_LEADER_SIZE, &this->prev_leader, PLA_COMMAND_PREV_LEADER_SIZE);

	return bytes;
}

std::string PartitionLeaderAssignmentCommand::get_command_key() {
	return "pla_" + this->queue_name + "_" + std::to_string(this->partition);
}

// ================================================================

// Delete Queue Command

DeleteQueueCommand::DeleteQueueCommand(const std::string& queue_name) {
	this->queue_name = queue_name;
}

DeleteQueueCommand::DeleteQueueCommand(void* metadata) {
	int queue_name_size = 0;

	memcpy_s(&queue_name_size, DQ_COMMAND_QUEUE_NAME_LENGTH_SIZE, (char*)metadata + DQ_COMMAND_QUEUE_NAME_LENGTH_OFFSET, DQ_COMMAND_QUEUE_NAME_LENGTH_SIZE);

	this->queue_name = std::string((char*)metadata + DQ_COMMAND_QUEUE_NAME_OFFSET, queue_name_size);
}

const std::string& DeleteQueueCommand::get_queue_name() {
	return this->queue_name;
}

std::shared_ptr<char> DeleteQueueCommand::get_metadata_bytes() {
	std::shared_ptr<char> bytes = std::shared_ptr<char>(new char[DQ_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES]);

	int queue_name_size = this->queue_name.size();

	memcpy_s(bytes.get() + DQ_COMMAND_QUEUE_NAME_LENGTH_OFFSET - COMMAND_TOTAL_BYTES, DQ_COMMAND_QUEUE_NAME_LENGTH_SIZE, &queue_name_size, DQ_COMMAND_QUEUE_NAME_LENGTH_SIZE);
	memcpy_s(bytes.get() + DQ_COMMAND_QUEUE_NAME_OFFSET - COMMAND_TOTAL_BYTES, queue_name_size, this->queue_name.c_str(), queue_name_size);

	return bytes;
}

std::string DeleteQueueCommand::get_command_key() {
	return "dq_" + this->queue_name;
}

// ================================================================

// Register Data Node Command

RegisterDataNodeCommand::RegisterDataNodeCommand(int node_id, const std::string& address, int port, const std::string& external_address, int external_port) {
	this->node_id = node_id;
	this->address = address;
	this->port = port;
	this->external_address = external_address;
	this->external_port = external_port;
}

RegisterDataNodeCommand::RegisterDataNodeCommand(void* metadata) {
	memcpy_s(&this->node_id, RDN_COMMAND_NODE_ID_SIZE, (char*)metadata + RDN_COMMAND_NODE_ID_OFFSET, RDN_COMMAND_NODE_ID_SIZE);

	int address_lenth = 0;
	memcpy_s(&address_lenth, RDN_COMMAND_ADDRESS_LENGTH_SIZE, (char*)metadata + RDN_COMMAND_ADDRESS_LENGTH_OFFSET, RDN_COMMAND_ADDRESS_LENGTH_SIZE);

	this->address = std::string((char*)metadata + RDN_COMMAND_ADDRESS_OFFSET, address_lenth);

	memcpy_s(&this->port, RDN_COMMAND_PORT_SIZE, (char*)metadata + RDN_COMMAND_PORT_OFFSET, RDN_COMMAND_PORT_SIZE);

	int external_address_lenth = 0;
	memcpy_s(&external_address_lenth, RDN_COMMAND_EXT_ADDRESS_LENGTH_SIZE, (char*)metadata + RDN_COMMAND_EXT_ADDRESS_LENGTH_OFFSET, RDN_COMMAND_EXT_ADDRESS_LENGTH_SIZE);

	this->external_address = std::string((char*)metadata + RDN_COMMAND_EXT_ADDRESS_OFFSET, external_address_lenth);

	memcpy_s(&this->external_port, RDN_COMMAND_EXT_PORT_SIZE, (char*)metadata + RDN_COMMAND_EXT_PORT_OFFSET, RDN_COMMAND_EXT_PORT_SIZE);
}

int RegisterDataNodeCommand::get_node_id() {
	return this->node_id;
}

const std::string& RegisterDataNodeCommand::get_address() {
	return this->address;
}

int RegisterDataNodeCommand::get_port() {
	return this->port;
}

const std::string& RegisterDataNodeCommand::get_external_address() {
	return this->external_address;
}

int RegisterDataNodeCommand::get_external_port() {
	return this->external_port;
}

std::shared_ptr<char> RegisterDataNodeCommand::get_metadata_bytes() {
	std::shared_ptr<char> bytes = std::shared_ptr<char>(new char[RDN_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES]);

	memcpy_s(bytes.get() + RDN_COMMAND_NODE_ID_OFFSET - COMMAND_TOTAL_BYTES, RDN_COMMAND_NODE_ID_SIZE, &this->node_id, RDN_COMMAND_NODE_ID_SIZE);

	int address_length = this->address.size();
	int external_address_length = this->external_address.size();

	memcpy_s(bytes.get() + RDN_COMMAND_ADDRESS_LENGTH_OFFSET - COMMAND_TOTAL_BYTES, RDN_COMMAND_ADDRESS_LENGTH_SIZE, &address_length, RDN_COMMAND_ADDRESS_LENGTH_SIZE);
	memcpy_s(bytes.get() + RDN_COMMAND_ADDRESS_OFFSET - COMMAND_TOTAL_BYTES, address_length, this->address.c_str(), address_length);
	memcpy_s(bytes.get() + RDN_COMMAND_PORT_OFFSET - COMMAND_TOTAL_BYTES, RDN_COMMAND_PORT_SIZE, &this->port, RDN_COMMAND_PORT_SIZE);

	memcpy_s(bytes.get() + RDN_COMMAND_EXT_ADDRESS_LENGTH_OFFSET - COMMAND_TOTAL_BYTES, RDN_COMMAND_EXT_ADDRESS_LENGTH_SIZE, &external_address_length, RDN_COMMAND_EXT_ADDRESS_LENGTH_SIZE);
	memcpy_s(bytes.get() + RDN_COMMAND_EXT_ADDRESS_OFFSET - COMMAND_TOTAL_BYTES, external_address_length, this->external_address.c_str(), external_address_length);
	memcpy_s(bytes.get() + RDN_COMMAND_EXT_PORT_OFFSET - COMMAND_TOTAL_BYTES, RDN_COMMAND_EXT_PORT_SIZE, &this->external_port, RDN_COMMAND_EXT_PORT_SIZE);

	return bytes;
}

std::string RegisterDataNodeCommand::get_command_key() {
	return "rn_" + std::to_string(this->node_id);
}

// ================================================================

// Unregister Data Node Command

UnregisterDataNodeCommand::UnregisterDataNodeCommand(int node_id) {
	this->node_id = node_id;
}

UnregisterDataNodeCommand::UnregisterDataNodeCommand(void* metadata) {
	memcpy_s(&this->node_id, UDN_COMMAND_NODE_ID_SIZE, (char*)metadata + UDN_COMMAND_NODE_ID_OFFSET, UDN_COMMAND_NODE_ID_SIZE);
}

int UnregisterDataNodeCommand::get_node_id() {
	return this->node_id;
}

std::shared_ptr<char> UnregisterDataNodeCommand::get_metadata_bytes() {
	std::shared_ptr<char> bytes = std::shared_ptr<char>(new char[UDN_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES]);

	memcpy_s(bytes.get() + UDN_COMMAND_NODE_ID_OFFSET - COMMAND_TOTAL_BYTES, UDN_COMMAND_NODE_ID_SIZE, &this->node_id, UDN_COMMAND_NODE_ID_SIZE);

	return bytes;
}

std::string UnregisterDataNodeCommand::get_command_key() {
	return "un_" + std::to_string(this->node_id);
}

// ================================================================

// Register Consumer Group Command

RegisterConsumerGroupCommand::RegisterConsumerGroupCommand(const std::string& queue_name, int partition_id, const std::string& group_id, unsigned long long consumer_id, unsigned long long stole_from_consumer, bool consume_from_beginning) {
	this->queue_name = queue_name;
	this->partition_id = partition_id;
	this->group_id = group_id;
	this->consumer_id = consumer_id;
	this->stole_from_consumer = stole_from_consumer;
	this->consume_from_beginning = consume_from_beginning;
}

RegisterConsumerGroupCommand::RegisterConsumerGroupCommand(void* metadata) {
	int queue_name_length = 0;
	int group_id_length = 0;

	memcpy_s(&queue_name_length, RCG_COMMAND_QUEUE_NAME_LENGTH_SIZE, (char*)metadata + RCG_COMMAND_QUEUE_NAME_LENGTH_OFFSET, RCG_COMMAND_QUEUE_NAME_LENGTH_SIZE);
	memcpy_s(&group_id_length, RCG_COMMAND_GROUP_ID_LENGTH_SIZE, (char*)metadata + RCG_COMMAND_GROUP_ID_LENGTH_OFFSET, RCG_COMMAND_GROUP_ID_LENGTH_SIZE);

	this->queue_name = std::string((char*)metadata + RCG_COMMAND_QUEUE_NAME_OFFSET, queue_name_length);
	this->group_id = std::string((char*)metadata + RCG_COMMAND_GROUP_ID_OFFSET, group_id_length);

	memcpy_s(&this->partition_id, RCG_COMMAND_PARTITION_ID_SIZE, (char*)metadata + RCG_COMMAND_PARTITION_ID_OFFSET, RCG_COMMAND_PARTITION_ID_SIZE);
	memcpy_s(&this->consumer_id, RCG_COMMAND_CONSUMER_ID_SIZE, (char*)metadata + RCG_COMMAND_CONSUMER_ID_OFFSET, RCG_COMMAND_CONSUMER_ID_SIZE);
	memcpy_s(&this->stole_from_consumer, RCG_COMMAND_STOLE_FROM_CONSUMER_SIZE, (char*)metadata + RCG_COMMAND_STOLE_FROM_CONSUMER_OFFSET, RCG_COMMAND_STOLE_FROM_CONSUMER_SIZE);
	memcpy_s(&this->consume_from_beginning, RCG_COMMAND_CONSUME_FROM_BEGINNING_SIZE, (char*)metadata + RCG_COMMAND_CONSUME_FROM_BEGINNING_OFFSET, RCG_COMMAND_CONSUME_FROM_BEGINNING_SIZE);
}

const std::string& RegisterConsumerGroupCommand::get_queue_name() {
	return this->queue_name;
}

int RegisterConsumerGroupCommand::get_partition_id() {
	return this->partition_id;
}

const std::string& RegisterConsumerGroupCommand::get_group_id() {
	return this->group_id;
}

unsigned long long RegisterConsumerGroupCommand::get_consumer_id() {
	return this->consumer_id;
}

unsigned long long RegisterConsumerGroupCommand::get_stole_from_consumer() {
	return this->stole_from_consumer;
}

bool RegisterConsumerGroupCommand::get_consume_from_beginning() {
	return this->consume_from_beginning;
}

std::shared_ptr<char> RegisterConsumerGroupCommand::get_metadata_bytes() {
	std::shared_ptr<char> bytes = std::shared_ptr<char>(new char[RCG_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES]);

	int queue_name_length = this->queue_name.size();
	int group_id_size = this->group_id.size();

	memcpy_s(bytes.get() + RCG_COMMAND_QUEUE_NAME_LENGTH_OFFSET - COMMAND_TOTAL_BYTES, RCG_COMMAND_QUEUE_NAME_LENGTH_SIZE, &queue_name_length, RCG_COMMAND_QUEUE_NAME_LENGTH_SIZE);
	memcpy_s(bytes.get() + RCG_COMMAND_QUEUE_NAME_OFFSET - COMMAND_TOTAL_BYTES, queue_name_length, this->queue_name.c_str(), queue_name_length);

	memcpy_s(bytes.get() + RCG_COMMAND_GROUP_ID_LENGTH_OFFSET - COMMAND_TOTAL_BYTES, RCG_COMMAND_GROUP_ID_LENGTH_SIZE, &group_id_size, RCG_COMMAND_GROUP_ID_LENGTH_SIZE);
	memcpy_s(bytes.get() + RCG_COMMAND_GROUP_ID_OFFSET - COMMAND_TOTAL_BYTES, group_id_size, this->group_id.c_str(), group_id_size);

	memcpy_s(bytes.get() + RCG_COMMAND_PARTITION_ID_OFFSET - COMMAND_TOTAL_BYTES, RCG_COMMAND_PARTITION_ID_SIZE, &this->partition_id, RCG_COMMAND_PARTITION_ID_SIZE);
	memcpy_s(bytes.get() + RCG_COMMAND_CONSUMER_ID_OFFSET - COMMAND_TOTAL_BYTES, RCG_COMMAND_CONSUMER_ID_SIZE, &this->consumer_id, RCG_COMMAND_CONSUMER_ID_SIZE);
	memcpy_s(bytes.get() + RCG_COMMAND_STOLE_FROM_CONSUMER_OFFSET - COMMAND_TOTAL_BYTES, RCG_COMMAND_STOLE_FROM_CONSUMER_SIZE, &this->stole_from_consumer, RCG_COMMAND_STOLE_FROM_CONSUMER_SIZE);
	memcpy_s(bytes.get() + RCG_COMMAND_CONSUME_FROM_BEGINNING_OFFSET - COMMAND_TOTAL_BYTES, RCG_COMMAND_CONSUME_FROM_BEGINNING_SIZE, &this->consume_from_beginning, RCG_COMMAND_CONSUME_FROM_BEGINNING_SIZE);

	return bytes;
}

std::string RegisterConsumerGroupCommand::get_command_key() {
	return "rcg_" + this->group_id + "_q_" + queue_name + "_p_" + std::to_string(this->partition_id);
}

// ================================================================

// Register Consumer Group Command

UnregisterConsumerGroupCommand::UnregisterConsumerGroupCommand(const std::string& queue_name, int partition_id, const std::string& group_id, unsigned long long consumer_id) {
	this->queue_name = queue_name;
	this->partition_id = partition_id;
	this->group_id = group_id;
	this->consumer_id = consumer_id;
}

UnregisterConsumerGroupCommand::UnregisterConsumerGroupCommand(void* metadata) {
	int queue_name_length = 0;
	int group_id_length = 0;

	memcpy_s(&queue_name_length, UCG_COMMAND_QUEUE_NAME_LENGTH_SIZE, (char*)metadata + UCG_COMMAND_QUEUE_NAME_LENGTH_OFFSET, UCG_COMMAND_QUEUE_NAME_LENGTH_SIZE);
	memcpy_s(&group_id_length, UCG_COMMAND_GROUP_ID_LENGTH_SIZE, (char*)metadata + UCG_COMMAND_GROUP_ID_LENGTH_OFFSET, UCG_COMMAND_GROUP_ID_LENGTH_SIZE);

	this->queue_name = std::string((char*)metadata + UCG_COMMAND_QUEUE_NAME_OFFSET, queue_name_length);
	this->group_id = std::string((char*)metadata + UCG_COMMAND_GROUP_ID_OFFSET, group_id_length);

	memcpy_s(&this->partition_id, UCG_COMMAND_PARTITION_ID_SIZE, (char*)metadata + UCG_COMMAND_PARTITION_ID_OFFSET, UCG_COMMAND_PARTITION_ID_SIZE);
	memcpy_s(&this->consumer_id, UCG_COMMAND_CONSUMER_ID_SIZE, (char*)metadata + UCG_COMMAND_CONSUMER_ID_OFFSET, UCG_COMMAND_CONSUMER_ID_SIZE);
}

const std::string& UnregisterConsumerGroupCommand::get_queue_name() {
	return this->queue_name;
}

int UnregisterConsumerGroupCommand::get_partition_id() {
	return this->partition_id;
}

const std::string& UnregisterConsumerGroupCommand::get_group_id() {
	return this->group_id;
}

unsigned long long UnregisterConsumerGroupCommand::get_consumer_id() {
	return this->consumer_id;
}

std::shared_ptr<char> UnregisterConsumerGroupCommand::get_metadata_bytes() {
	std::shared_ptr<char> bytes = std::shared_ptr<char>(new char[RCG_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES]);

	int queue_name_length = this->queue_name.size();
	int group_id_size = this->group_id.size();

	memcpy_s(bytes.get() + UCG_COMMAND_QUEUE_NAME_LENGTH_OFFSET - COMMAND_TOTAL_BYTES, UCG_COMMAND_QUEUE_NAME_LENGTH_SIZE, &queue_name_length, UCG_COMMAND_QUEUE_NAME_LENGTH_SIZE);
	memcpy_s(bytes.get() + UCG_COMMAND_QUEUE_NAME_OFFSET - COMMAND_TOTAL_BYTES, queue_name_length, this->queue_name.c_str(), queue_name_length);

	memcpy_s(bytes.get() + UCG_COMMAND_GROUP_ID_LENGTH_OFFSET - COMMAND_TOTAL_BYTES, UCG_COMMAND_GROUP_ID_LENGTH_SIZE, &group_id_size, UCG_COMMAND_GROUP_ID_LENGTH_SIZE);
	memcpy_s(bytes.get() + UCG_COMMAND_GROUP_ID_OFFSET - COMMAND_TOTAL_BYTES, group_id_size, this->group_id.c_str(), group_id_size);

	memcpy_s(bytes.get() + UCG_COMMAND_PARTITION_ID_OFFSET - COMMAND_TOTAL_BYTES, UCG_COMMAND_PARTITION_ID_SIZE, &this->partition_id, UCG_COMMAND_PARTITION_ID_SIZE);
	memcpy_s(bytes.get() + UCG_COMMAND_CONSUMER_ID_OFFSET - COMMAND_TOTAL_BYTES, UCG_COMMAND_CONSUMER_ID_SIZE, &this->consumer_id, UCG_COMMAND_CONSUMER_ID_SIZE);

	return bytes;
}

std::string UnregisterConsumerGroupCommand::get_command_key() {
	return "ucg_" + this->group_id + "_q_" + queue_name + "_p_" + std::to_string(this->partition_id);
}

// ================================================================

// Add Lagging Follower Command

AddLaggingFollowerCommand::AddLaggingFollowerCommand(const std::string& queue_name, int partition_id, int node_id) {
	this->queue_name = queue_name;
	this->partition_id = partition_id;
	this->node_id = node_id;
}

AddLaggingFollowerCommand::AddLaggingFollowerCommand(void* metadata) {
	int queue_name_length = 0;

	memcpy_s(&queue_name_length, ALF_COMMAND_QUEUE_NAME_LENGTH_SIZE, (char*)metadata + ALF_COMMAND_QUEUE_NAME_LENGTH_OFFSET, ALF_COMMAND_QUEUE_NAME_LENGTH_SIZE);

	this->queue_name = std::string((char*)metadata + ALF_COMMAND_QUEUE_NAME_OFFSET, queue_name_length);

	memcpy_s(&this->partition_id, ALF_COMMAND_PARTITION_ID_SIZE, (char*)metadata + ALF_COMMAND_PARTITION_ID_OFFSET, ALF_COMMAND_PARTITION_ID_SIZE);
	memcpy_s(&this->node_id, ALF_COMMAND_NODE_ID_SIZE, (char*)metadata + ALF_COMMAND_NODE_ID_OFFSET, ALF_COMMAND_NODE_ID_SIZE);
}

const std::string& AddLaggingFollowerCommand::get_queue_name() {
	return this->queue_name;
}

int AddLaggingFollowerCommand::get_partition_id() {
	return this->partition_id;
}

int AddLaggingFollowerCommand::get_node_id() {
	return this->node_id;
}

std::shared_ptr<char> AddLaggingFollowerCommand::get_metadata_bytes() {
	std::shared_ptr<char> bytes = std::shared_ptr<char>(new char[ALF_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES]);

	int queue_name_length = this->queue_name.size();

	memcpy_s(bytes.get() + ALF_COMMAND_QUEUE_NAME_LENGTH_OFFSET - COMMAND_TOTAL_BYTES, ALF_COMMAND_QUEUE_NAME_LENGTH_SIZE, &queue_name_length, ALF_COMMAND_QUEUE_NAME_LENGTH_SIZE);
	memcpy_s(bytes.get() + ALF_COMMAND_QUEUE_NAME_OFFSET - COMMAND_TOTAL_BYTES, queue_name_length, this->queue_name.c_str(), queue_name_length);

	memcpy_s(bytes.get() + ALF_COMMAND_PARTITION_ID_OFFSET - COMMAND_TOTAL_BYTES, ALF_COMMAND_PARTITION_ID_SIZE, &this->partition_id, ALF_COMMAND_PARTITION_ID_SIZE);
	memcpy_s(bytes.get() + ALF_COMMAND_NODE_ID_OFFSET - COMMAND_TOTAL_BYTES, ALF_COMMAND_NODE_ID_SIZE, &this->node_id, ALF_COMMAND_NODE_ID_SIZE);

	return bytes;
}

std::string AddLaggingFollowerCommand::get_command_key() {
	return "alf_" + this->queue_name + "_p_" + std::to_string(this->partition_id) + "_n_" + std::to_string(this->node_id);
}

// ================================================================

// Remove Lagging Follower Command

RemoveLaggingFollowerCommand::RemoveLaggingFollowerCommand(const std::string& queue_name, int partition_id, int node_id) {
	this->queue_name = queue_name;
	this->partition_id = partition_id;
	this->node_id = node_id;
}

RemoveLaggingFollowerCommand::RemoveLaggingFollowerCommand(void* metadata) {
	int queue_name_length = 0;

	memcpy_s(&queue_name_length, RLF_COMMAND_QUEUE_NAME_LENGTH_SIZE, (char*)metadata + RLF_COMMAND_QUEUE_NAME_LENGTH_OFFSET, RLF_COMMAND_QUEUE_NAME_LENGTH_SIZE);

	this->queue_name = std::string((char*)metadata + RLF_COMMAND_QUEUE_NAME_OFFSET, queue_name_length);

	memcpy_s(&this->partition_id, RLF_COMMAND_PARTITION_ID_SIZE, (char*)metadata + RLF_COMMAND_PARTITION_ID_OFFSET, RLF_COMMAND_PARTITION_ID_SIZE);
	memcpy_s(&this->node_id, RLF_COMMAND_NODE_ID_SIZE, (char*)metadata + RLF_COMMAND_NODE_ID_OFFSET, RLF_COMMAND_NODE_ID_SIZE);
}

const std::string& RemoveLaggingFollowerCommand::get_queue_name() {
	return this->queue_name;
}

int RemoveLaggingFollowerCommand::get_partition_id() {
	return this->partition_id;
}

int RemoveLaggingFollowerCommand::get_node_id() {
	return this->node_id;
}

std::shared_ptr<char> RemoveLaggingFollowerCommand::get_metadata_bytes() {
	std::shared_ptr<char> bytes = std::shared_ptr<char>(new char[ALF_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES]);

	int queue_name_length = this->queue_name.size();

	memcpy_s(bytes.get() + RLF_COMMAND_QUEUE_NAME_LENGTH_OFFSET - COMMAND_TOTAL_BYTES, RLF_COMMAND_QUEUE_NAME_LENGTH_SIZE, &queue_name_length, RLF_COMMAND_QUEUE_NAME_LENGTH_SIZE);
	memcpy_s(bytes.get() + RLF_COMMAND_QUEUE_NAME_OFFSET - COMMAND_TOTAL_BYTES, queue_name_length, this->queue_name.c_str(), queue_name_length);

	memcpy_s(bytes.get() + RLF_COMMAND_PARTITION_ID_OFFSET - COMMAND_TOTAL_BYTES, RLF_COMMAND_PARTITION_ID_SIZE, &this->partition_id, RLF_COMMAND_PARTITION_ID_SIZE);
	memcpy_s(bytes.get() + RLF_COMMAND_NODE_ID_OFFSET - COMMAND_TOTAL_BYTES, RLF_COMMAND_NODE_ID_SIZE, &this->node_id, RLF_COMMAND_NODE_ID_SIZE);

	return bytes;
}

std::string RemoveLaggingFollowerCommand::get_command_key() {
	return "rlf_" + this->queue_name + "_p_" + std::to_string(this->partition_id) + "_n_" + std::to_string(this->node_id);
}

// ================================================================

// Register Transaction Group

RegisterTransactionGroupCommand::RegisterTransactionGroupCommand(int node_id, unsigned long long transaction_group_id, std::vector<std::string>* registered_queues) {
	this->node_id = node_id;
	this->transaction_group_id = transaction_group_id;
	this->registered_queues = std::vector<std::string>(registered_queues->size());

	int i = 0;
	for (const std::string& queue_name : *registered_queues) {
		this->registered_queues[i] = queue_name;
		i++;
	}
}

RegisterTransactionGroupCommand::RegisterTransactionGroupCommand(void* metadata) {
	memcpy_s(&this->node_id, RTG_COMMAND_NODE_ID_SIZE, (char*)metadata + RTG_COMMAND_NODE_ID_OFFSET, RTG_COMMAND_NODE_ID_SIZE);
	memcpy_s(&this->transaction_group_id, RTG_COMMAND_GROUP_ID_SIZE, (char*)metadata + RTG_COMMAND_GROUP_ID_OFFSET, RTG_COMMAND_GROUP_ID_SIZE);

	int total_registered_queues = 0;

	memcpy_s(&total_registered_queues, RTG_COMMAND_QUEUES_COUNT_SIZE, (char*)metadata + RTG_COMMAND_QUEUES_COUNT_OFFSET, RTG_COMMAND_QUEUES_COUNT_SIZE);

	this->registered_queues = std::vector<std::string>(total_registered_queues);

	int offset = RTG_COMMAND_TOTAL_BYTES;
	for (int i = 0; i < total_registered_queues; i++) {
		int queue_name_size = 0;

		memcpy_s(&queue_name_size, sizeof(int), (char*)metadata + offset, sizeof(int));

		this->registered_queues[i] = std::string((char*)metadata + offset + sizeof(int), queue_name_size);

		offset += sizeof(int) + queue_name_size;
	}
}

int RegisterTransactionGroupCommand::get_node_id() {
	return this->node_id;
}

unsigned long long RegisterTransactionGroupCommand::get_transaction_group_id() {
	return this->transaction_group_id;
}

std::vector<std::string>* RegisterTransactionGroupCommand::get_registered_queues() {
	return &this->registered_queues;
}

std::shared_ptr<char> RegisterTransactionGroupCommand::get_metadata_bytes() {
	unsigned int total_bytes = RTG_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES;

	for (const std::string& queue_name : this->registered_queues)
		total_bytes += sizeof(int) + queue_name.size();

	std::shared_ptr<char> bytes = std::shared_ptr<char>(new char[total_bytes]);

	memcpy_s(bytes.get() + RTG_COMMAND_NODE_ID_OFFSET - COMMAND_TOTAL_BYTES, RTG_COMMAND_NODE_ID_SIZE, &this->node_id, RTG_COMMAND_NODE_ID_SIZE);
	memcpy_s(bytes.get() + RTG_COMMAND_GROUP_ID_OFFSET - COMMAND_TOTAL_BYTES, RTG_COMMAND_GROUP_ID_SIZE, &this->transaction_group_id, RTG_COMMAND_GROUP_ID_SIZE);

	int total_queues = this->registered_queues.size();

	memcpy_s(bytes.get() + RTG_COMMAND_QUEUES_COUNT_OFFSET - COMMAND_TOTAL_BYTES, RTG_COMMAND_QUEUES_COUNT_SIZE, &total_queues, RTG_COMMAND_QUEUES_COUNT_SIZE);

	int offset = RTG_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES;

	for (const std::string& queue_name : this->registered_queues) {
		int queue_size = queue_name.size();

		memcpy_s(bytes.get() + offset, sizeof(int), &queue_size, sizeof(int));
		memcpy_s(bytes.get() + offset + sizeof(int), queue_size, queue_name.c_str(), queue_size);

		offset += sizeof(int) + queue_size;
	}

	return bytes;
}

std::string RegisterTransactionGroupCommand::get_command_key() {
	return "rtg_" + std::to_string(this->transaction_group_id);
}

// ================================================================

// Unegister Transaction Group

UnregisterTransactionGroupCommand::UnregisterTransactionGroupCommand(int node_id, unsigned long long transaction_group_id) {
	this->node_id = node_id;
	this->transaction_group_id = transaction_group_id;
}

UnregisterTransactionGroupCommand::UnregisterTransactionGroupCommand(void* metadata) {
	memcpy_s(&this->node_id, UTG_COMMAND_NODE_ID_SIZE, (char*)metadata + UTG_COMMAND_NODE_ID_OFFSET, UTG_COMMAND_NODE_ID_SIZE);
	memcpy_s(&this->transaction_group_id, UTG_COMMAND_GROUP_ID_SIZE, (char*)metadata + UTG_COMMAND_GROUP_ID_OFFSET, UTG_COMMAND_GROUP_ID_SIZE);
}

int UnregisterTransactionGroupCommand::get_node_id() {
	return this->node_id;
}

unsigned long long UnregisterTransactionGroupCommand::get_transaction_group_id() {
	return this->transaction_group_id;
}

std::shared_ptr<char> UnregisterTransactionGroupCommand::get_metadata_bytes() {
	std::shared_ptr<char> bytes = std::shared_ptr<char>(new char[UTG_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES]);

	memcpy_s(bytes.get() + UTG_COMMAND_NODE_ID_OFFSET - COMMAND_TOTAL_BYTES, UTG_COMMAND_NODE_ID_SIZE, &this->node_id, UTG_COMMAND_NODE_ID_SIZE);
	memcpy_s(bytes.get() + UTG_COMMAND_GROUP_ID_OFFSET - COMMAND_TOTAL_BYTES, UTG_COMMAND_GROUP_ID_SIZE, &this->transaction_group_id, UTG_COMMAND_GROUP_ID_SIZE);

	return bytes;
}

std::string UnregisterTransactionGroupCommand::get_command_key() {
	return "urtg_" + std::to_string(this->transaction_group_id);
}

// ================================================================