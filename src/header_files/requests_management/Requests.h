#pragma once
#include <vector>
#include <memory>

struct AppendEntriesRequest {
	int leader_id;
	unsigned long long term;
	unsigned long long prev_log_index;
	unsigned long long prev_log_term;
	unsigned long long leader_commit;
	int total_commands;
	long commands_total_bytes;
	void* commands_data;

	std::shared_ptr<char> commands_data_ptr;
};

struct CreateQueueRequest {
	// For authentication
	int username_length;
	char* username;
	int password_length;
	char* password;
	// ==================

	int queue_name_length;
	char* queue_name;
	int partitions;
	int replication_factor;
};

struct DataNodeHeartbeatRequest {
	int node_id;
	int address_length;
	const char* address;
	int port;
};

struct DeleteQueueRequest {
	// For authentication
	int username_length;
	char* username;
	int password_length;
	char* password;
	// ==================

	int queue_name_length;
	char* queue_name;
};

struct ProduceMessagesRequest {
	// For authentication
	int username_length;
	char* username;
	int password_length;
	char* password;
	// ==================

	int queue_name_length;
	char* queue_name;
	int transactional_id_length;
	char* transactional_id;
	long transaction_id;
	long producer_id;
	long epoch;
	int partition;
	std::shared_ptr<std::vector<char*>> messages;
	std::shared_ptr<std::vector<long>> messages_sizes;
};

struct ProducerConnectRequest {
	// For authentication
	int username_length;
	char* username;
	int password_length;
	char* password;
	// ==================

	int queue_name_length;
	char* queue_name;
	int transactional_id_length;
	char* transactional_id;
};

struct RequestVoteRequest {
	int candidate_id;
	long long term;
	long long last_log_index;
	long long last_log_term;
};