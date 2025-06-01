#include "../../header_files/requests_management/ResponseMapper.h"

ResponseMapper::ResponseMapper() {}

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
			printf("Invalid response value %d on response type ErrorResponse\n", *key);
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
			res.get()->term = *(long long*)(res_buf + offset + sizeof(ResponseValueKey));
			offset += sizeof(RequestValueKey) + sizeof(long long);
		} else if (*key == ResponseValueKey::SUCCESS) {
			res.get()->success = *(bool*)(res_buf + offset + sizeof(ResponseValueKey));
			offset += sizeof(RequestValueKey) + sizeof(bool);
		}
		else {
			printf("Invalid response value %d on response type AppendEntriesResponse\n", *key);
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
			res.get()->term = *(long long*)(res_buf + offset + sizeof(ResponseValueKey));
			offset += sizeof(RequestValueKey) + sizeof(long long);
		}
		else if (*key == ResponseValueKey::VOTE_GRANTED) {
			res.get()->vote_granted = *(bool*)(res_buf + offset + sizeof(ResponseValueKey));
			offset += sizeof(RequestValueKey) + sizeof(bool);
		}
		else {
			printf("Invalid response value %d on response type RequestVoteResponse\n", *key);
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
			printf("Invalid response value %d on response type DataNodeHeartbeatResponse\n", *key);
			return nullptr;
		}
	}

	return res;
}