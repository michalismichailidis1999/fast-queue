#pragma once
#include <vector>
#include <memory>
#include <string>
#include <tuple>

#include "../__linux/memcpy_s.h"

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

	// for internal communication
	int address_length;
	const char* address;
	int port;

	// for external communication
	int external_address_length;
	const char* external_address;
	int external_port;

	bool register_node;
};

struct RequestVoteRequest {
	int candidate_id;
	unsigned long long term;
	unsigned long long last_log_index;
	unsigned long long last_log_term;
};

struct GetClusterMetadataUpdateRequest {
	int node_id;
	bool prev_req_index_matched;
	unsigned long long prev_log_index;
	unsigned long long prev_log_term;
	bool is_first_request;
};

struct ExpireConsumersRequest {
	std::shared_ptr<std::vector<std::tuple<std::string, std::string, unsigned long long>>> expired_consumers;
};

struct AddLaggingFollowerRequest {
	int queue_name_length;
	char* queue_name;
	int partition;
	int node_id;
};

struct RemoveLaggingFollowerRequest {
	int queue_name_length;
	char* queue_name;
	int partition;
	int node_id;
};

struct FetchMessagesRequest {
	int queue_name_length;
	char* queue_name;
	int partition;
	int node_id;
	unsigned long long message_offset;
};

// ======================================================================

// External Requests

struct AuthRequest {
	int username_length;
	char* username;
	int password_length;
	char* password;
};

struct CreateQueueRequest : AuthRequest {
	int queue_name_length;
	char* queue_name;
	int partitions;
	int replication_factor;
};

struct DeleteQueueRequest : AuthRequest {
	int queue_name_length;
	char* queue_name;
};

struct ProduceMessagesRequest : AuthRequest {
	int queue_name_length;
	char* queue_name;
	int partition;
	std::shared_ptr<std::vector<char*>> messages;
	std::shared_ptr<std::vector<char*>> messages_keys;
	std::shared_ptr<std::vector<int>> messages_sizes;
	std::shared_ptr<std::vector<int>> messages_keys_sizes;
};

struct RegisterConsumerRequest : AuthRequest {
	int queue_name_length;
	char* queue_name;
	int consumer_group_id_length;
	char* consumer_group_id;
	bool consume_from_beginning;
};

struct GetQueuePartitionsInfoRequest {
	int queue_name_length;
	char* queue_name;
};

struct GetConsumerAssignedPartitionsRequest : AuthRequest {
	int queue_name_length;
	char* queue_name;
	int consumer_group_id_length;
	char* consumer_group_id;
	unsigned long long consumer_id;
};

struct ConsumeRequest : AuthRequest {
	int queue_name_length;
	char* queue_name;
	int consumer_group_id_length;
	char* consumer_group_id;
	int partition;
	unsigned long long consumer_id;
	unsigned long long message_offset;
	bool read_single_offset_only;
};

struct AckMessageOffsetRequest : AuthRequest {
	int queue_name_length;
	char* queue_name;
	int consumer_group_id_length;
	char* consumer_group_id;
	int partition;
	unsigned long long consumer_id;
	unsigned long long message_offset;
};

// ======================================================================