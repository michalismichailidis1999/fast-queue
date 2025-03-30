#pragma once
#include <memory>
#include "./Responses.h"
#include "../Enums.h"

class ResponseMapper {
public:
	ResponseMapper();

	std::unique_ptr<ErrorResponse> to_error_response(char* res_buf, long res_buf_len);

	std::unique_ptr<AppendEntriesResponse> to_append_entries_response(char* res_buf, long res_buf_len);

	std::unique_ptr<RequestVoteResponse> to_request_vote_response(char* res_buf, long res_buf_len);

	std::unique_ptr<DataNodeConnectionResponse> to_data_node_connection_response(char* res_buf, long res_buf_len);

	std::unique_ptr<DataNodeHeartbeatResponse> to_data_node_heartbeat_response(char* res_buf, long res_buf_len);
};