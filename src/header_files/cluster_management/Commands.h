#pragma once
#include <string>
#include <memory>
#include <vector>
#include "../util/Helper.h"
#include "../Enums.h"
#include "../Constants.h"
#include "../exceptions/CurruptionException.h"

#include "../__linux/memcpy_s.h"

class Command {
private:
	CommandType type;
	unsigned long long term;
	unsigned long long metadata_version;
	unsigned long long timestamp;

	std::shared_ptr<void> command_info;
public:
	Command();

	Command(CommandType type, unsigned long long term, unsigned long long timestamp, std::shared_ptr<void> command_info);

	Command(void* metadata);

	CommandType get_command_type();

	void* get_command_info();
	unsigned long long get_metadata_version();
	unsigned long long get_term();
	unsigned long long get_timestamp();

	void set_metadata_version(unsigned long long metadata_version);

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

	std::string get_command_key();

	friend class Command;
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

	std::string get_command_key();

	friend class Command;
};

class PartitionLeaderAssignmentCommand {
private:
	std::string queue_name;
	int partition;
	unsigned long long leader_id;
	int new_leader;
	int prev_leader;

public:
	PartitionLeaderAssignmentCommand(const std::string& queue_name, int partition, unsigned long long leader_id, int new_leader, int prev_leader = -1);

	PartitionLeaderAssignmentCommand(void* metadata);

	const std::string& get_queue_name();

	int get_partition();

	unsigned long long get_leader_id();

	int get_new_leader();

	int get_prev_leader();

	std::shared_ptr<char> get_metadata_bytes();

	std::string get_command_key();

	friend class Command;
};

class DeleteQueueCommand {
private:
	std::string queue_name;
public:
	DeleteQueueCommand(const std::string& queue_name);

	DeleteQueueCommand(void* metadata);

	const std::string& get_queue_name();

	std::shared_ptr<char> get_metadata_bytes();

	std::string get_command_key();

	friend class Command;
};

class RegisterDataNodeCommand {
private:
	int node_id;

	std::string address;
	int port;

	std::string external_address;
	int external_port;
public:
	RegisterDataNodeCommand(int node_id, const std::string& address, int port, const std::string& external_address, int external_port);

	RegisterDataNodeCommand(void* metadata);

	int get_node_id();

	const std::string& get_address();

	int get_port();

	const std::string& get_external_address();

	int get_external_port();

	std::shared_ptr<char> get_metadata_bytes();

	std::string get_command_key();

	friend class Command;
};

class UnregisterDataNodeCommand {
private:
	int node_id;
public:
	UnregisterDataNodeCommand(int node_id);

	UnregisterDataNodeCommand(void* metadata);

	int get_node_id();

	std::shared_ptr<char> get_metadata_bytes();

	std::string get_command_key();

	friend class Command;
};

class RegisterConsumerGroupCommand {
private:
	std::string queue_name;
	int partition_id;

	std::string group_id;
	unsigned long long consumer_id;
	unsigned long long stole_from_consumer;

	bool consume_from_beginning;
public:
	RegisterConsumerGroupCommand(const std::string& queue_name, int partition_id, const std::string& group_id, unsigned long long consumer_id, unsigned long long stole_from_consumer, bool consume_from_beginning);

	RegisterConsumerGroupCommand(void* metadata);

	const std::string& get_queue_name();

	int get_partition_id();

	const std::string& get_group_id();

	unsigned long long get_consumer_id();

	unsigned long long get_stole_from_consumer();

	bool get_consume_from_beginning();

	std::shared_ptr<char> get_metadata_bytes();

	std::string get_command_key();

	friend class Command;
};

class UnregisterConsumerGroupCommand {
private:
	std::string queue_name;
	int partition_id;

	std::string group_id;
	unsigned long long consumer_id;
public:
	UnregisterConsumerGroupCommand(const std::string& queue_name, int partition_id, const std::string& group_id, unsigned long long consumer_id);

	UnregisterConsumerGroupCommand(void* metadata);

	const std::string& get_queue_name();

	int get_partition_id();

	const std::string& get_group_id();

	unsigned long long get_consumer_id();

	std::shared_ptr<char> get_metadata_bytes();

	std::string get_command_key();

	friend class Command;
};

class AddLaggingFollowerCommand {
private:
	std::string queue_name;
	int partition_id;
	int node_id;
public:
	AddLaggingFollowerCommand(const std::string& queue_name, int partition_id, int node_id);

	AddLaggingFollowerCommand(void* metadata);

	const std::string& get_queue_name();

	int get_partition_id();

	int get_node_id();

	std::shared_ptr<char> get_metadata_bytes();

	std::string get_command_key();

	friend class Command;
};

class RemoveLaggingFollowerCommand {
private:
	std::string queue_name;
	int partition_id;
	int node_id;
public:
	RemoveLaggingFollowerCommand(const std::string& queue_name, int partition_id, int node_id);

	RemoveLaggingFollowerCommand(void* metadata);

	const std::string& get_queue_name();

	int get_partition_id();

	int get_node_id();

	std::shared_ptr<char> get_metadata_bytes();

	std::string get_command_key();

	friend class Command;
};

class RegisterTransactionGroupCommand {
private:
	int node_id;
	unsigned long long transaction_group_id;
	std::vector<std::string> registered_queues;

public:
	RegisterTransactionGroupCommand(int node_id, unsigned long long transaction_group_id, std::vector<std::string>* registered_queues);

	RegisterTransactionGroupCommand(void* metadata);

	int get_node_id();

	unsigned long long get_transaction_group_id();

	std::vector<std::string>* get_registered_queues();

	std::shared_ptr<char> get_metadata_bytes();

	std::string get_command_key();

	friend class Command;
};

class UnregisterTransactionGroupCommand {
private:
	int node_id;
	unsigned long long transaction_group_id;

public:
	UnregisterTransactionGroupCommand(int node_id, unsigned long long transaction_group_id);

	UnregisterTransactionGroupCommand(void* metadata);

	int get_node_id();

	unsigned long long get_transaction_group_id();

	std::shared_ptr<char> get_metadata_bytes();

	std::string get_command_key();

	friend class Command;
};