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

void Command::set_metadata_version(unsigned long long metadata_version) {
	this->metadata_version = metadata_version;
}

std::string Command::get_command_key() {
	switch (this->type) {
	case CommandType::CREATE_QUEUE:
		return ((CreateQueueCommand*)(this->command_info.get()))->get_command_key();
	case CommandType::ALTER_PARTITION_ASSIGNMENT:
		return ((PartitionAssignmentCommand*)(this->command_info.get()))->get_command_key();
	case CommandType::ALTER_PARTITION_LEADER_ASSIGNMENT:
		return ((PartitionLeaderAssignmentCommand*)(this->command_info.get()))->get_command_key();
	case CommandType::DELETE_QUEUE:
		return ((DeleteQueueCommand*)(this->command_info.get()))->get_command_key();
	default:
		return "";
	}
}

std::tuple<long, std::shared_ptr<char>> Command::get_metadata_bytes() {
	long total_bytes = 0;
	int size_dif = COMMAND_TOTAL_BYTES;

	switch (this->type) {
	case CommandType::CREATE_QUEUE:
		total_bytes = CQ_COMMAND_TOTAL_BYTES;
		size_dif = CQ_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES;
		break;
	case CommandType::ALTER_PARTITION_ASSIGNMENT:
		total_bytes = PA_COMMAND_TOTAL_BYTES;
		size_dif = PA_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES;
		break;
	case CommandType::ALTER_PARTITION_LEADER_ASSIGNMENT:
		total_bytes = PLA_COMMAND_TOTAL_BYTES;
		size_dif = PLA_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES;
		break;
	case CommandType::DELETE_QUEUE:
		total_bytes = DQ_COMMAND_TOTAL_BYTES;
		size_dif = DQ_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES;
		break;
	default:
		return std::tuple<long, std::shared_ptr<char>>(0, nullptr);
	}

	std::string command_key = this->get_command_key();

	std::shared_ptr<char> bytes = std::shared_ptr<char>(new char[total_bytes]);
	
	Helper::add_message_metadata_values(bytes.get(), this->metadata_version, this->timestamp, command_key.size(), command_key.c_str());

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
	default:
		return std::tuple<long, std::shared_ptr<char>>(0, nullptr);
	}

	Helper::add_common_metadata_values(bytes.get(), total_bytes, ObjectType::MESSAGE);

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

std::string CreateQueueCommand::get_command_key() {
	return "qc_" + this->queue_name + "_" + std::to_string(this->partitions) + "_" + std::to_string(this->replication_factor);
}

std::shared_ptr<char> CreateQueueCommand::get_metadata_bytes() {
	std::shared_ptr<char> bytes = std::shared_ptr<char>(new char[CQ_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES]);

	int queue_name_size = this->queue_name.size();

	memcpy_s(bytes.get() + CQ_COMMAND_QUEUE_NAME_LENGTH_OFFSET, CQ_COMMAND_QUEUE_NAME_LENGTH_SIZE, &queue_name_size, CQ_COMMAND_QUEUE_NAME_LENGTH_SIZE);
	memcpy_s(bytes.get() + CQ_COMMAND_QUEUE_NAME_OFFSET, queue_name_size, this->queue_name.c_str(), queue_name_size);
	memcpy_s(bytes.get() + CQ_COMMAND_PARTITION_OFFSET, CQ_COMMAND_PARTITION_SIZE, &this->partitions, CQ_COMMAND_PARTITION_SIZE);
	memcpy_s(bytes.get() + CQ_COMMAND_REPLICATION_OFFSET, CQ_COMMAND_REPLICATION_SIZE, &this->replication_factor, CQ_COMMAND_REPLICATION_SIZE);

	return bytes;
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

std::string PartitionAssignmentCommand::get_command_key() {
	return "pa_" + this->queue_name + "_" + std::to_string(this->partition) + "_to_" + std::to_string(this->to_node) + "_from_" + std::to_string(this->from_node);
}

std::shared_ptr<char> PartitionAssignmentCommand::get_metadata_bytes() {
	std::shared_ptr<char> bytes = std::shared_ptr<char>(new char[PA_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES]);

	int queue_name_size = this->queue_name.size();

	memcpy_s(bytes.get() + PA_COMMAND_QUEUE_NAME_LENGTH_OFFSET, PA_COMMAND_QUEUE_NAME_LENGTH_SIZE, &queue_name_size, PA_COMMAND_QUEUE_NAME_LENGTH_SIZE);
	memcpy_s(bytes.get() + PA_COMMAND_QUEUE_NAME_OFFSET, queue_name_size, this->queue_name.c_str(), queue_name_size);
	memcpy_s(bytes.get() + PA_COMMAND_PARTITION_OFFSET, PA_COMMAND_PARTITION_SIZE, &this->partition, PA_COMMAND_PARTITION_SIZE);
	memcpy_s(bytes.get() + PA_COMMAND_TO_NODE_OFFSET, PA_COMMAND_TO_NODE_SIZE, &this->to_node, PA_COMMAND_TO_NODE_SIZE);
	memcpy_s(bytes.get() + PA_COMMAND_FROM_NODE_OFFSET, PA_COMMAND_FROM_NODE_SIZE, &this->from_node, PA_COMMAND_FROM_NODE_SIZE);

	return bytes;
}

// ================================================================

// Partition Leader Assignment Command

PartitionLeaderAssignmentCommand::PartitionLeaderAssignmentCommand(const std::string& queue_name, int partition, int new_leader, int prev_leader) {
	this->queue_name = queue_name;
	this->partition = partition;
	this->new_leader = new_leader;
	this->prev_leader = prev_leader;
}

PartitionLeaderAssignmentCommand::PartitionLeaderAssignmentCommand(void* metadata) {
	int queue_name_size = 0;

	memcpy_s(&queue_name_size, PLA_COMMAND_QUEUE_NAME_LENGTH_SIZE, (char*)metadata + PLA_COMMAND_QUEUE_NAME_LENGTH_OFFSET, PLA_COMMAND_QUEUE_NAME_LENGTH_SIZE);

	this->queue_name = std::string((char*)metadata + PLA_COMMAND_QUEUE_NAME_OFFSET, queue_name_size);

	memcpy_s(&this->partition, PLA_COMMAND_PARTITION_SIZE, (char*)metadata + PLA_COMMAND_PARTITION_OFFSET, PLA_COMMAND_PARTITION_SIZE);
	memcpy_s(&this->new_leader, PLA_COMMAND_NEW_LEADER_SIZE, (char*)metadata + PLA_COMMAND_NEW_LEADER_OFFSET, PLA_COMMAND_NEW_LEADER_SIZE);
	memcpy_s(&this->prev_leader, PLA_COMMAND_PREV_LEADER_SIZE, (char*)metadata + PLA_COMMAND_PREV_LEADER_OFFSET, PLA_COMMAND_PREV_LEADER_SIZE);
}

const std::string& PartitionLeaderAssignmentCommand::get_queue_name() {
	return this->queue_name;
}

int PartitionLeaderAssignmentCommand::get_partition() {
	return this->partition;
}

int PartitionLeaderAssignmentCommand::get_new_leader() {
	return this->new_leader;
}

int PartitionLeaderAssignmentCommand::get_prev_leader() {
	return this->prev_leader;
}

std::string PartitionLeaderAssignmentCommand::get_command_key() {
	return "pla_" + this->queue_name + "_" + std::to_string(this->partition) + "_new_" + std::to_string(this->new_leader) + "_prev_" + std::to_string(this->prev_leader);
}

std::shared_ptr<char> PartitionLeaderAssignmentCommand::get_metadata_bytes() {
	std::shared_ptr<char> bytes = std::shared_ptr<char>(new char[PLA_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES]);

	int queue_name_size = this->queue_name.size();

	memcpy_s(bytes.get() + PLA_COMMAND_QUEUE_NAME_LENGTH_OFFSET, PLA_COMMAND_QUEUE_NAME_LENGTH_SIZE, &queue_name_size, PLA_COMMAND_QUEUE_NAME_LENGTH_SIZE);
	memcpy_s(bytes.get() + PLA_COMMAND_QUEUE_NAME_OFFSET, queue_name_size, this->queue_name.c_str(), queue_name_size);
	memcpy_s(bytes.get() + PLA_COMMAND_PARTITION_OFFSET, PLA_COMMAND_PARTITION_SIZE, &this->partition, PLA_COMMAND_PARTITION_SIZE);
	memcpy_s(bytes.get() + PLA_COMMAND_NEW_LEADER_OFFSET, PLA_COMMAND_NEW_LEADER_SIZE, &this->new_leader, PLA_COMMAND_NEW_LEADER_SIZE);
	memcpy_s(bytes.get() + PLA_COMMAND_PREV_LEADER_OFFSET, PLA_COMMAND_PREV_LEADER_SIZE, &this->prev_leader, PLA_COMMAND_PREV_LEADER_SIZE);

	return bytes;
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

std::string DeleteQueueCommand::get_command_key() {
	return "dc_" + this->queue_name;
}

std::shared_ptr<char> DeleteQueueCommand::get_metadata_bytes() {
	std::shared_ptr<char> bytes = std::shared_ptr<char>(new char[DQ_COMMAND_TOTAL_BYTES - COMMAND_TOTAL_BYTES]);

	int queue_name_size = this->queue_name.size();

	memcpy_s(bytes.get() + DQ_COMMAND_QUEUE_NAME_LENGTH_OFFSET, DQ_COMMAND_QUEUE_NAME_LENGTH_SIZE, &queue_name_size, DQ_COMMAND_QUEUE_NAME_LENGTH_SIZE);
	memcpy_s(bytes.get() + DQ_COMMAND_QUEUE_NAME_OFFSET, queue_name_size, this->queue_name.c_str(), queue_name_size);

	return bytes;
}

// ================================================================