#pragma once
#include "Commands.h"

Command::Command(CommandType type, unsigned long long metadata_version, unsigned long long timestamp, std::shared_ptr<void> command_info) {
	this->type = type;
	this->metadata_version = metadata_version;
	this->timestamp = timestamp;
	this->command_info = command_info;
}

CommandType Command::get_command_type() {
	return this->type;
}

void* Command::get_command_info() {
	return this->command_info.get();
}

unsigned long long Command::get_metadata_version() {
	return this->metadata_version;
}

unsigned long long Command::get_timestamp() {
	return this->timestamp;
}

CreateQueueCommand::CreateQueueCommand(const std::string& queue_name, int partitions, int replication_factor) {
	this->queue_name = queue_name;
	this->partitions = partitions;
	this->replication_factor = replication_factor;
}

CreateQueueCommand::CreateQueueCommand(void* metadata) {}

const std::string& CreateQueueCommand::get_queue_name() {
	return this->queue_name;
}

int CreateQueueCommand::get_partitions() {
	return this->partitions;
}

int CreateQueueCommand::get_replication_factor() {
	return this->replication_factor;
}

PartitionAssignmentCommand::PartitionAssignmentCommand(const std::string& queue_name, int partition, int to_node, int from_node) {
	this->queue_name = queue_name;
	this->partition = partition;
	this->to_node = to_node;
	this->from_node = from_node;
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

PartitionLeaderAssignmentCommand::PartitionLeaderAssignmentCommand(const std::string& queue_name, int partition, int new_leader, int prev_leader) {
	this->queue_name = queue_name;
	this->partition = partition;
	this->new_leader = new_leader;
	this->prev_leader = prev_leader;
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