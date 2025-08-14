#pragma once
#include <memory>
#include "./Responses.h"
#include "../Enums.h"
#include "../logging/Logger.h"

#include "../__linux/memcpy_s.h"

class ResponseMapper {
private:
	Logger* logger;
public:
	ResponseMapper(Logger* logger);

	std::unique_ptr<ErrorResponse> to_error_response(char* res_buf, long res_buf_len);

	std::unique_ptr<AppendEntriesResponse> to_append_entries_response(char* res_buf, long res_buf_len);

	std::unique_ptr<RequestVoteResponse> to_request_vote_response(char* res_buf, long res_buf_len);

	std::unique_ptr<DataNodeHeartbeatResponse> to_data_node_heartbeat_response(char* res_buf, long res_buf_len);
};