#pragma once
#include <vector>
#include <tuple>
#include "../network_management/Connection.h"

struct AppendEntriesResponse {
	long long term;
	bool success;
};

struct CreateQueueResponse {
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
	long long term;
	bool vote_granted;
};