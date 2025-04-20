#include "../../header_files/requests_management/ClassToByteTransformer.h"

ClassToByteTransformer::ClassToByteTransformer() {}

std::tuple<long, std::shared_ptr<char>> ClassToByteTransformer::transform(AppendEntriesRequest* obj) {
	long buf_size = sizeof(long) + sizeof(RequestType) + sizeof(int) + 4 * sizeof(unsigned long long) + 8 * sizeof(RequestValueKey);
	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	RequestType req_type = RequestType::APPEND_ENTRIES;

	RequestValueKey leader_id_type = RequestValueKey::LEADER_ID;
	RequestValueKey term_type = RequestValueKey::TERM;
	RequestValueKey prev_log_index_type = RequestValueKey::PREV_LOG_INDEX;
	RequestValueKey prev_log_term_type = RequestValueKey::PREV_LOG_TERM;
	RequestValueKey leader_commit_type = RequestValueKey::LEADER_COMMIT;

	RequestValueKey total_commands_type = RequestValueKey::TOTAL_COMMANDS;
	RequestValueKey commands_total_bytes_type = RequestValueKey::COMMANDS_TOTAL_BYTES;
	RequestValueKey commands_data_type = RequestValueKey::COMMANDS_DATA;

	long offset = 0;

	memcpy_s(buf.get() + offset, sizeof(long), &buf_size, sizeof(long));
	offset += sizeof(long);

	memcpy_s(buf.get() + offset, sizeof(RequestType), &req_type, sizeof(RequestType));
	offset += sizeof(RequestType);

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &leader_id_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->leader_id, sizeof(int));
	offset += sizeof(int);

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &term_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(unsigned long long), &obj->term, sizeof(long long));
	offset += sizeof(unsigned long long);

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &prev_log_index_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(unsigned long long), &obj->prev_log_index, sizeof(long long));
	offset += sizeof(unsigned long long);

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &prev_log_term_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(unsigned long long), &obj->prev_log_term, sizeof(long long));
	offset += sizeof(unsigned long long);

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &leader_commit_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(unsigned long long), &obj->leader_commit, sizeof(long long));
	offset += sizeof(unsigned long long);

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &total_commands_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->total_commands, sizeof(int));
	offset += sizeof(int);

	memcpy_s(buf.get() + offset, sizeof(long), &obj->commands_total_bytes, sizeof(int));
	offset += sizeof(long);

	memcpy_s(buf.get() + offset, obj->commands_total_bytes, obj->commands_data, obj->commands_total_bytes);

	return std::tuple<long, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<long, std::shared_ptr<char>> ClassToByteTransformer::transform(RequestVoteRequest* obj) {
	long buf_size = sizeof(long) + sizeof(RequestType) + sizeof(int) + 3 * sizeof(long long) + 4 * sizeof(RequestValueKey);
	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	RequestType req_type = RequestType::REQUEST_VOTE;

	RequestValueKey candidate_id_type = RequestValueKey::CANDIDATE_ID;
	RequestValueKey term_type = RequestValueKey::TERM;
	RequestValueKey last_log_index_type = RequestValueKey::LAST_LOG_INDEX;
	RequestValueKey last_log_term_type = RequestValueKey::LAST_LOG_TERM;

	long offset = 0;

	memcpy_s(buf.get() + offset, sizeof(long), &buf_size, sizeof(long));
	offset += sizeof(long);

	memcpy_s(buf.get() + offset, sizeof(RequestType), &req_type, sizeof(RequestType));
	offset += sizeof(RequestType);

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &candidate_id_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->candidate_id, sizeof(int));
	offset += sizeof(int);

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &term_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(long long), &obj->term, sizeof(long long));
	offset += sizeof(long long);
	long term_offset = offset;

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &last_log_index_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(long long), &obj->last_log_index, sizeof(long long));
	offset += sizeof(long long);

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &last_log_term_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(long long), &obj->last_log_term, sizeof(long long));

	return std::tuple<long, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<long, std::shared_ptr<char>> ClassToByteTransformer::transform(DataNodeConnectionRequest* obj) {
	long buf_size = sizeof(long) + sizeof(RequestType) + 3 * sizeof(int) + obj->address_length + 3 * sizeof(RequestValueKey);

	RequestType req_type = RequestType::DATA_NODE_CONNECTION;

	RequestValueKey node_id_type = RequestValueKey::NODE_ID;
	RequestValueKey address_type = RequestValueKey::NODE_ADDRESS;
	RequestValueKey port_type = RequestValueKey::NODE_PORT;

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	long offset = 0;

	memcpy_s(buf.get() + offset, sizeof(long), &buf_size, sizeof(long));
	offset += sizeof(long);

	memcpy_s(buf.get() + offset, sizeof(RequestType), &req_type, sizeof(RequestType));
	offset += sizeof(RequestType);

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &node_id_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->node_id, sizeof(int));
	offset += sizeof(int);

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &address_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->address_length, sizeof(int));
	offset += sizeof(int);

	memcpy_s(buf.get() + offset, obj->address_length, obj->address, obj->address_length);
	offset += (long)obj->address_length;

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &port_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->port, sizeof(int));

	return std::tuple<long, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<long, std::shared_ptr<char>> ClassToByteTransformer::transform(DataNodeHeartbeatRequest* obj) {
	long buf_size = sizeof(long) + sizeof(RequestType) + sizeof(int) + sizeof(RequestValueKey);

	RequestType req_type = RequestType::DATA_NODE_HEARTBEAT;

	RequestValueKey node_id_type = RequestValueKey::NODE_ID;

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	long offset = 0;

	memcpy_s(buf.get() + offset, sizeof(long), &buf_size, sizeof(long));
	offset += sizeof(long);

	memcpy_s(buf.get() + offset, sizeof(RequestType), &req_type, sizeof(RequestType));
	offset += sizeof(RequestType);

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &node_id_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->node_id, sizeof(int));
	offset += sizeof(int);

	return std::tuple<long, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<long, std::shared_ptr<char>> ClassToByteTransformer::transform(AppendEntriesResponse* obj) {
	long buf_size = sizeof(long) + sizeof(ErrorCode) + sizeof(long long) + sizeof(bool) + 2 * sizeof(ResponseValueKey);

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	ErrorCode err_code = ErrorCode::NONE;
	long offset = 0;

	ResponseValueKey term_type = ResponseValueKey::TERM;
	ResponseValueKey success_type = ResponseValueKey::SUCCESS;

	memcpy_s(buf.get(), sizeof(long), &buf_size, sizeof(long));
	offset += sizeof(long);

	memcpy_s(buf.get() + offset, sizeof(ErrorCode), &err_code, sizeof(ErrorCode));
	offset += sizeof(ErrorCode);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &term_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(long long), &obj->term, sizeof(long long));
	offset += sizeof(long long);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &success_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(bool), &obj->success, sizeof(bool));

	return std::tuple<long, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<long, std::shared_ptr<char>> ClassToByteTransformer::transform(RequestVoteResponse* obj) {
	long buf_size = sizeof(long) + sizeof(ErrorCode) + sizeof(long long) + sizeof(bool) + 2 * sizeof(ResponseValueKey);

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	ErrorCode err_code = ErrorCode::NONE;
	long offset = 0;

	ResponseValueKey term_type = ResponseValueKey::TERM;
	ResponseValueKey vote_granted_type = ResponseValueKey::VOTE_GRANTED;

	memcpy_s(buf.get(), sizeof(long), &buf_size, sizeof(long));
	offset += sizeof(long);

	memcpy_s(buf.get() + offset, sizeof(ErrorCode), &err_code, sizeof(ErrorCode));
	offset += sizeof(ErrorCode);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &term_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(long long), &obj->term, sizeof(long long));
	offset += sizeof(long long);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &vote_granted_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(bool), &obj->vote_granted, sizeof(bool));

	return std::tuple<long, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<long, std::shared_ptr<char>> ClassToByteTransformer::transform(DataNodeConnectionResponse* obj) {
	long buf_size = sizeof(long) + sizeof(ErrorCode) + sizeof(bool) + sizeof(ResponseValueKey);

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	ErrorCode err_code = ErrorCode::NONE;
	long offset = 0;

	ResponseValueKey ok_type = ResponseValueKey::OK;

	memcpy_s(buf.get(), sizeof(long), &buf_size, sizeof(long));
	offset += sizeof(long);

	memcpy_s(buf.get() + offset, sizeof(ErrorCode), &err_code, sizeof(ErrorCode));
	offset += sizeof(ErrorCode);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &ok_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(bool), &obj->ok, sizeof(bool));

	return std::tuple<long, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<long, std::shared_ptr<char>> ClassToByteTransformer::transform(DataNodeHeartbeatResponse* obj) {
	long buf_size = sizeof(long) + sizeof(ErrorCode) + sizeof(int) + sizeof(bool) + 2 * sizeof(ResponseValueKey);

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	ErrorCode err_code = ErrorCode::NONE;
	long offset = 0;

	ResponseValueKey ok_type = ResponseValueKey::OK;
	ResponseValueKey leader_id_type = ResponseValueKey::LEADER_ID;

	memcpy_s(buf.get(), sizeof(long), &buf_size, sizeof(long));
	offset += sizeof(long);

	memcpy_s(buf.get() + offset, sizeof(ErrorCode), &err_code, sizeof(ErrorCode));
	offset += sizeof(ErrorCode);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &ok_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(bool), &obj->ok, sizeof(bool));
	offset += sizeof(bool);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &leader_id_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->leader_id, sizeof(int));

	return std::tuple<long, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<long, std::shared_ptr<char>> ClassToByteTransformer::transform(CreateQueueResponse* obj) {
	long buf_size = sizeof(long) + sizeof(ErrorCode) + sizeof(bool) + sizeof(ResponseValueKey);

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	ErrorCode err_code = ErrorCode::NONE;
	long offset = 0;

	ResponseValueKey ok_type = ResponseValueKey::OK;

	memcpy_s(buf.get(), sizeof(long), &buf_size, sizeof(long));
	offset += sizeof(long);

	memcpy_s(buf.get() + offset, sizeof(ErrorCode), &err_code, sizeof(ErrorCode));
	offset += sizeof(ErrorCode);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &ok_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(bool), &obj->ok, sizeof(bool));

	return std::tuple<long, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<long, std::shared_ptr<char>> ClassToByteTransformer::transform(GetControllerConnectionInfoResponse* obj) {
	int count = obj->connection_infos.size();

	long buf_size = sizeof(long) + sizeof(ErrorCode) + sizeof(int) + sizeof(ResponseValueKey) + count * (3 * sizeof(int) + sizeof(ResponseValueKey));

	for (auto& info : obj->connection_infos)
		buf_size += std::get<1>(info)->address.size();

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	ErrorCode err_code = ErrorCode::NONE;
	long offset = 0;

	ResponseValueKey leader_id_type = ResponseValueKey::LEADER_ID;
	ResponseValueKey controller_connection_info_type = ResponseValueKey::CONTROLLER_CONNECTION_INFO;

	memcpy_s(buf.get(), sizeof(long), &buf_size, sizeof(long));
	offset += sizeof(long);

	memcpy_s(buf.get() + offset, sizeof(ErrorCode), &err_code, sizeof(ErrorCode));
	offset += sizeof(ErrorCode);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &leader_id_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->leader_id, sizeof(int));
	offset += sizeof(int);

	for (auto& info : obj->connection_infos) {
		int node_id = std::get<0>(info);
		ConnectionInfo* conn_info = std::get<1>(info);
		int address_size = conn_info->address.size();

		memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &controller_connection_info_type, sizeof(ResponseValueKey));
		offset += sizeof(ResponseValueKey);

		memcpy_s(buf.get() + offset, sizeof(int), &node_id, sizeof(int));
		offset += sizeof(int);

		memcpy_s(buf.get() + offset, sizeof(int), &address_size, sizeof(int));
		offset += sizeof(int);

		memcpy_s(buf.get() + offset, address_size, conn_info->address.c_str(), address_size);
		offset += address_size;

		memcpy_s(buf.get() + offset, sizeof(int), &conn_info->port, sizeof(int));
		offset += sizeof(int);
	}

	return std::tuple<long, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<long, std::shared_ptr<char>> ClassToByteTransformer::transform(GetLeaderIdResponse* obj) {
	long buf_size = sizeof(long) + sizeof(ErrorCode) + sizeof(int) + sizeof(ResponseValueKey);

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	ErrorCode err_code = ErrorCode::NONE;
	long offset = 0;

	ResponseValueKey leader_id_type = ResponseValueKey::LEADER_ID;

	memcpy_s(buf.get(), sizeof(long), &buf_size, sizeof(long));
	offset += sizeof(long);

	memcpy_s(buf.get() + offset, sizeof(ErrorCode), &err_code, sizeof(ErrorCode));
	offset += sizeof(ErrorCode);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &leader_id_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->leader_id, sizeof(int));

	return std::tuple<long, std::shared_ptr<char>>(buf_size, buf);
}