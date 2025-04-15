#pragma once
#include <string>
#include <memory>
#include <vector>
#include "../util/Helper.h"
#include "../Enums.h"
#include "../Constants.h"

class Command {
private:
	CommandType type;
	unsigned long long term;
	unsigned long long metadata_version;
	unsigned long long timestamp;

	std::shared_ptr<void> command_info;
public:
	Command(CommandType type, unsigned long long metadata_version, unsigned long long timestamp, std::shared_ptr<void> command_info);

	Command(void* metadata);

	CommandType get_command_type();

	void* get_command_info();
	unsigned long long get_metadata_version();
	unsigned long long get_timestamp();

	std::tuple<long, std::shared_ptr<char>> get_metadata_bytes();
};

class CreateQueueCommand {
private:
	std::string queue_name;
	int partitions;
	int replication_factor;

public:
	CreateQueueCommand(const std::string& queue_name, int partitions, int replication_factor);

	CreateQueueCommand(void* metadata);

	const std::string& get_queue_name();

	int get_partitions();

	int get_replication_factor();

	std::shared_ptr<char> get_metadata_bytes();
};

class PartitionAssignmentCommand {
private:
	std::string queue_name;
	int partition;
	int to_node;
	int from_node;

public:
	PartitionAssignmentCommand(const std::string& queue_name, int partition, int to_node, int from_node = -1);

	PartitionAssignmentCommand(void* metadata);

	const std::string& get_queue_name();

	int get_partition();

	int get_to_node();

	int get_from_node();

	std::shared_ptr<char> get_metadata_bytes();
};

class PartitionLeaderAssignmentCommand {
private:
	std::string queue_name;
	int partition;
	int new_leader;
	int prev_leader;

public:
	PartitionLeaderAssignmentCommand(const std::string& queue_name, int partition, int new_leader, int prev_leader = -1);

	PartitionLeaderAssignmentCommand(void* metadata);

	const std::string& get_queue_name();

	int get_partition();

	int get_new_leader();

	int get_prev_leader();

	std::shared_ptr<char> get_metadata_bytes();
};