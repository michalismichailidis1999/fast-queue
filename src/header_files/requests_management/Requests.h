#pragma once
#include <vector>
#include <memory>

// Internal Requests

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

struct DataNodeHeartbeatRequest {
	int node_id;
	int address_length;
	const char* address;
	int port;
};

struct RequestVoteRequest {
	int candidate_id;
	long long term;
	long long last_log_index;
	long long last_log_term;
};

// ======================================================================

// External Requests

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
	int partition;
	std::shared_ptr<std::vector<char*>> messages;
	std::shared_ptr<std::vector<char*>> messages_keys;
	std::shared_ptr<std::vector<int>> messages_sizes;
	std::shared_ptr<std::vector<int>> messages_keys_sizes;
};

// ======================================================================