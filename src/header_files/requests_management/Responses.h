#pragma once
#include <vector>
#include <tuple>
#include "../network_management/Connection.h"

struct ErrorResponse {
	int error_message_size;
	char* error_message;
};

// Internal Responses

struct AppendEntriesResponse {
	unsigned long long term;
	unsigned long long lag_index;
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

// =======================================================

// External Responses

struct CreateQueueResponse {
	bool ok;
};

struct DeleteQueueResponse {
	bool ok;
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

// =======================================================