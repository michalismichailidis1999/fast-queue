#pragma once
#include <vector>
#include <tuple>
#include "../network_management/Connection.h"

struct AppendEntriesResponse {
	unsigned long long term;
	unsigned long long lag_index;
	bool success;
};

struct CreateQueueResponse {
	bool ok;
};

struct DeleteQueueResponse {
	bool ok;
};

struct DataNodeHeartbeatResponse {
	bool ok;
	int leader_id;
};

struct ErrorResponse {
	int error_message_size;
	char* error_message;
};

struct GetControllerConnectionInfoResponse {
	std::vector<std::tuple<int, ConnectionInfo*>> connection_infos;
	int leader_id;
};

struct GetLeaderIdResponse {
	int leader_id;
};

struct RequestVoteResponse {
	unsigned long long term;
	bool vote_granted;
};