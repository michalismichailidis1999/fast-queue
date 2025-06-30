#include "../../header_files/requests_management/ResponseMapper.h"

ResponseMapper::ResponseMapper(Logger* logger) {
	this->logger = logger;
}

std::unique_ptr<ErrorResponse> ResponseMapper::to_error_response(char* res_buf, long res_buf_len) {
	long offset = sizeof(ErrorCode); // skip error code

	std::unique_ptr<ErrorResponse> res = std::make_unique<ErrorResponse>();

	while (offset < res_buf_len) {
		ResponseValueKey* key = (ResponseValueKey*)(res_buf + offset);

		if (*key == ResponseValueKey::ERROR_MESSAGE) {
			res.get()->error_message_size = *(int*)(res_buf + offset + sizeof(ResponseValueKey));
			res.get()->error_message = (res_buf + offset + sizeof(ResponseValueKey) + sizeof(int));
			offset += sizeof(RequestValueKey) + sizeof(int) + res.get()->error_message_size;
		}
		else {
			this->logger->log_error("Invalid response value " + std::to_string((int)(*key)) + " on response type ErrorResponse");
			return nullptr;
		}
	}

	return res;
}

std::unique_ptr<AppendEntriesResponse> ResponseMapper::to_append_entries_response(char* res_buf, long res_buf_len) {
	long offset = sizeof(ErrorCode); // skip error code

	std::unique_ptr<AppendEntriesResponse> res = std::make_unique<AppendEntriesResponse>();

	while (offset < res_buf_len) {
		ResponseValueKey* key = (ResponseValueKey*)(res_buf + offset);

		if (*key == ResponseValueKey::TERM) {
			res.get()->term = *(unsigned long long*)(res_buf + offset + sizeof(ResponseValueKey));
			offset += sizeof(RequestValueKey) + sizeof(unsigned long long);
		} else if (*key == ResponseValueKey::LOG_MATCHED) {
			res.get()->log_matched = *(bool*)(res_buf + offset + sizeof(ResponseValueKey));
			offset += sizeof(RequestValueKey) + sizeof(bool);
		}
		else if (*key == ResponseValueKey::SUCCESS) {
			res.get()->success = *(bool*)(res_buf + offset + sizeof(ResponseValueKey));
			offset += sizeof(RequestValueKey) + sizeof(bool);
		}
		else {
			this->logger->log_error("Invalid response value " + std::to_string((int)(*key)) + " on response type AppendEntriesResponse");
			return nullptr;
		}
	}

	return res;
}

std::unique_ptr<RequestVoteResponse> ResponseMapper::to_request_vote_response(char* res_buf, long res_buf_len) {
	long offset = sizeof(ErrorCode); // skip error code

	std::unique_ptr<RequestVoteResponse> res = std::make_unique<RequestVoteResponse>();

	while (offset < res_buf_len) {
		ResponseValueKey* key = (ResponseValueKey*)(res_buf + offset);

		if (*key == ResponseValueKey::TERM) {
			res.get()->term = *(unsigned long long*)(res_buf + offset + sizeof(ResponseValueKey));
			offset += sizeof(RequestValueKey) + sizeof(unsigned long long);
		}
		else if (*key == ResponseValueKey::VOTE_GRANTED) {
			res.get()->vote_granted = *(bool*)(res_buf + offset + sizeof(ResponseValueKey));
			offset += sizeof(RequestValueKey) + sizeof(bool);
		}
		else {
			this->logger->log_error("Invalid response value " + std::to_string((int)(*key)) + " on response type RequestVoteResponse");
			return nullptr;
		}
	}

	return res;
}

std::unique_ptr<DataNodeHeartbeatResponse> ResponseMapper::to_data_node_heartbeat_response(char* res_buf, long res_buf_len) {
	long offset = sizeof(ErrorCode); // skip error code

	std::unique_ptr<DataNodeHeartbeatResponse> res = std::make_unique<DataNodeHeartbeatResponse>();

	while (offset < res_buf_len) {
		ResponseValueKey* key = (ResponseValueKey*)(res_buf + offset);

		if (*key == ResponseValueKey::OK) {
			res.get()->ok = *(bool*)(res_buf + offset + sizeof(ResponseValueKey));
			offset += sizeof(RequestValueKey) + sizeof(bool);
		} else if (*key == ResponseValueKey::LEADER_ID) {
			res.get()->leader_id = *(int*)(res_buf + offset + sizeof(ResponseValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		}
		else {
			this->logger->log_error("Invalid response value " + std::to_string((int)(*key)) + " on response type DataNodeHeartbeatResponse");
			return nullptr;
		}
	}

	return res;
}