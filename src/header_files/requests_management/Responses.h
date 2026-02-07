#pragma once
#include <vector>
#include <tuple>
#include "../network_management/Connection.h"

#include "../__linux/memcpy_s.h"

struct ErrorResponse {
	int error_message_size;
	char* error_message;
};

// Internal Responses

struct AppendEntriesResponse {
	unsigned long long term;
	bool log_matched;
	bool success;
};

struct DataNodeHeartbeatResponse {
	bool ok;
	int leader_id;
};

struct RequestVoteResponse {
	unsigned long long term;
	bool vote_granted;
};

struct ExpireConsumersResponse {
	bool ok;
	int leader_id;
};

struct AddLaggingFollowerResponse {
	bool ok;
	int leader_id;
};

struct RemoveLaggingFollowerResponse {
	bool ok;
	int leader_id;
};

struct FetchMessagesResponse {
	int total_messages;
	int messages_total_bytes;
	void* messages_data;
	unsigned long long last_message_offset;
	unsigned long long commited_offset;
	unsigned long long prev_message_offset;
	unsigned long long prev_message_leader_epoch;
	int consumer_offsets_count;
	void* consumer_offsets_data;
};

struct UnregisterTransactionGroupResponse {
	bool ok;
	int leader_id;
};

struct TransactionStatusUpdateResponse {
	bool ok;
};

// =======================================================

// External Responses

struct CreateQueueResponse {
	bool ok;
	bool created;
};

struct DeleteQueueResponse {
	bool ok;
	bool deleted;
};

struct GetControllerConnectionInfoResponse {
	std::vector<std::tuple<int, ConnectionInfo*>> connection_infos;
	int leader_id;
};

struct GetLeaderIdResponse {
	int leader_id;
};

struct ProduceMessagesResponse {
	bool ok;
};

struct GetQueuePartitionsInfoResponse {
	int total_partitions;
	std::vector<std::tuple<int, int, ConnectionInfo*>> connection_infos;
};

struct RegisterConsumerResponse {
	bool ok;
	unsigned long long consumer_id;
};

struct GetConsumerAssignedPartitionsResponse {
	std::vector<int> partitions;
};

struct ConsumeResponse {
	int total_messages;
	int messages_total_bytes;
	void* messages_data;
};

struct AckMessageOffsetResponse {
	bool ok;
};

struct RegisterTransactionGroupResponse {
	int leader_id;
	unsigned long long transaction_group_id;
};

struct BeginTransactionResponse {
	unsigned long long new_tx_id;
};

struct FinalizeTransactionResponse {
	bool ok;
};

struct VerifyTransactionGroupCreationResponse {
	bool ok;
};

struct TransactionGroupHeartbeatResponse {
	bool ok;
};

// =======================================================