#include "../../header_files/requests_management/ClassToByteTransformer.h"

ClassToByteTransformer::ClassToByteTransformer() {}

std::tuple<unsigned int, std::shared_ptr<char>> ClassToByteTransformer::transform(AppendEntriesRequest* obj, bool used_as_response) {
	unsigned int buf_size = sizeof(unsigned int) + sizeof(RequestType) + sizeof(long) + 2 * sizeof(int) + 4 * sizeof(unsigned long long) + 6 * sizeof(RequestValueKey) + obj->commands_total_bytes;
	
	if (used_as_response) buf_size += sizeof(ErrorCode);

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	RequestType req_type = RequestType::APPEND_ENTRIES;

	RequestValueKey leader_id_type = RequestValueKey::LEADER_ID;
	RequestValueKey term_type = RequestValueKey::TERM;
	RequestValueKey prev_log_index_type = RequestValueKey::PREV_LOG_INDEX;
	RequestValueKey prev_log_term_type = RequestValueKey::PREV_LOG_TERM;
	RequestValueKey leader_commit_type = RequestValueKey::LEADER_COMMIT;
	RequestValueKey commands_type = RequestValueKey::COMMANDS;

	ErrorCode error = ErrorCode::NONE;

	int offset = 0;

	memcpy_s(buf.get() + offset, sizeof(unsigned int), &buf_size, sizeof(unsigned int));
	offset += sizeof(unsigned int);

	if (used_as_response) {
		memcpy_s(buf.get() + offset, sizeof(ErrorCode), &error, sizeof(ErrorCode));
		offset += sizeof(ErrorCode);
	}

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

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &commands_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);
	
	memcpy_s(buf.get() + offset, sizeof(int), &obj->total_commands, sizeof(int));
	offset += sizeof(int);
	
	memcpy_s(buf.get() + offset, sizeof(long), &obj->commands_total_bytes, sizeof(int));
	offset += sizeof(long);
	
	if(obj->total_commands > 0)
		memcpy_s(buf.get() + offset, obj->commands_total_bytes, obj->commands_data, obj->commands_total_bytes);

	return std::tuple<unsigned int, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<unsigned int, std::shared_ptr<char>> ClassToByteTransformer::transform(RequestVoteRequest* obj) {
	unsigned int buf_size = sizeof(unsigned int) + sizeof(RequestType) + sizeof(int) + 3 * sizeof(long long) + 4 * sizeof(RequestValueKey);
	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	RequestType req_type = RequestType::REQUEST_VOTE;

	RequestValueKey candidate_id_type = RequestValueKey::CANDIDATE_ID;
	RequestValueKey term_type = RequestValueKey::TERM;
	RequestValueKey last_log_index_type = RequestValueKey::LAST_LOG_INDEX;
	RequestValueKey last_log_term_type = RequestValueKey::LAST_LOG_TERM;

	int offset = 0;

	memcpy_s(buf.get() + offset, sizeof(unsigned int), &buf_size, sizeof(unsigned int));
	offset += sizeof(unsigned int);

	memcpy_s(buf.get() + offset, sizeof(RequestType), &req_type, sizeof(RequestType));
	offset += sizeof(RequestType);

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &candidate_id_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->candidate_id, sizeof(int));
	offset += sizeof(int);

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &term_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(unsigned long long), &obj->term, sizeof(unsigned long long));
	offset += sizeof(unsigned long long);

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &last_log_index_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(unsigned long long), &obj->last_log_index, sizeof(unsigned long long));
	offset += sizeof(unsigned long long);

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &last_log_term_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(long long), &obj->last_log_term, sizeof(long long));

	return std::tuple<unsigned int, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<unsigned int, std::shared_ptr<char>> ClassToByteTransformer::transform(DataNodeHeartbeatRequest* obj) {
	unsigned int buf_size = sizeof(unsigned int) + sizeof(RequestType) + 5 * sizeof(int) + sizeof(bool) + 6 * sizeof(RequestValueKey) + obj->address_length + obj->external_address_length;

	RequestType req_type = RequestType::DATA_NODE_HEARTBEAT;

	RequestValueKey node_id_type = RequestValueKey::NODE_ID;
	RequestValueKey address_type = RequestValueKey::NODE_ADDRESS;
	RequestValueKey port_type = RequestValueKey::NODE_PORT;
	RequestValueKey external_address_type = RequestValueKey::NODE_EXTERNAL_ADDRESS;
	RequestValueKey external_port_type = RequestValueKey::NODE_EXTERNAL_PORT;
	RequestValueKey register_node_type = RequestValueKey::REGISTER_NODE;

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	int offset = 0;

	memcpy_s(buf.get() + offset, sizeof(unsigned int), &buf_size, sizeof(unsigned int));
	offset += sizeof(unsigned int);

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
	offset += obj->address_length;

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &port_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->port, sizeof(int));
	offset += sizeof(int);

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &external_address_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->external_address_length, sizeof(int));
	offset += sizeof(int);

	memcpy_s(buf.get() + offset, obj->external_address_length, obj->external_address, obj->external_address_length);
	offset += obj->address_length;

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &external_port_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->external_port, sizeof(int));
	offset += sizeof(int);

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &register_node_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(bool), &obj->register_node, sizeof(bool));
	offset += sizeof(bool);

	return std::tuple<unsigned int, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<unsigned int, std::shared_ptr<char>> ClassToByteTransformer::transform(GetClusterMetadataUpdateRequest* obj) {
	unsigned int buf_size = sizeof(unsigned int) + sizeof(RequestType) + sizeof(int) + 2 * sizeof(bool) + 2 * sizeof(unsigned long long) + 5 * sizeof(RequestValueKey);

	RequestType req_type = RequestType::GET_CLUSTER_METADATA_UPDATES;

	RequestValueKey node_id_type = RequestValueKey::NODE_ID;
	RequestValueKey index_matched_type = RequestValueKey::INDEX_MATCHED;
	RequestValueKey prev_log_index_type = RequestValueKey::PREV_LOG_INDEX;
	RequestValueKey prev_log_term_type = RequestValueKey::PREV_LOG_TERM;
	RequestValueKey is_first_request_type = RequestValueKey::IS_FIRST_REQUEST;

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	int offset = 0;

	memcpy_s(buf.get() + offset, sizeof(unsigned int), &buf_size, sizeof(unsigned int));
	offset += sizeof(unsigned int);

	memcpy_s(buf.get() + offset, sizeof(RequestType), &req_type, sizeof(RequestType));
	offset += sizeof(RequestType);

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &node_id_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->node_id, sizeof(int));
	offset += sizeof(int);

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &index_matched_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(bool), &obj->prev_req_index_matched, sizeof(bool));
	offset += sizeof(bool);

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &prev_log_index_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(unsigned long long), &obj->prev_log_index, sizeof(unsigned long long));
	offset += sizeof(unsigned long long);

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &prev_log_term_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(unsigned long long), &obj->prev_log_term, sizeof(unsigned long long));
	offset += sizeof(unsigned long long);

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &is_first_request_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(bool), &obj->is_first_request, sizeof(bool));

	return std::tuple<unsigned int, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<unsigned int, std::shared_ptr<char>> ClassToByteTransformer::transform(AppendEntriesResponse* obj) {
	unsigned int buf_size = sizeof(unsigned int) + sizeof(ErrorCode) + sizeof(unsigned long long) + 2 * sizeof(bool) + 3 * sizeof(ResponseValueKey);

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	ErrorCode err_code = ErrorCode::NONE;
	int offset = 0;

	ResponseValueKey term_type = ResponseValueKey::TERM;
	ResponseValueKey log_matched_type = ResponseValueKey::LOG_MATCHED;
	ResponseValueKey success_type = ResponseValueKey::SUCCESS;

	memcpy_s(buf.get(), sizeof(unsigned int), &buf_size, sizeof(unsigned int));
	offset += sizeof(unsigned int);

	memcpy_s(buf.get() + offset, sizeof(ErrorCode), &err_code, sizeof(ErrorCode));
	offset += sizeof(ErrorCode);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &term_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(unsigned long long), &obj->term, sizeof(unsigned long long));
	offset += sizeof(unsigned long long);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &log_matched_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(bool), &obj->log_matched, sizeof(bool));
	offset += sizeof(bool);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &success_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(bool), &obj->success, sizeof(bool));

	return std::tuple<unsigned int, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<unsigned int, std::shared_ptr<char>> ClassToByteTransformer::transform(RequestVoteResponse* obj) {
	unsigned int buf_size = sizeof(unsigned int) + sizeof(ErrorCode) + sizeof(unsigned long long) + sizeof(bool) + 2 * sizeof(ResponseValueKey);

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	ErrorCode err_code = ErrorCode::NONE;
	int offset = 0;

	ResponseValueKey term_type = ResponseValueKey::TERM;
	ResponseValueKey vote_granted_type = ResponseValueKey::VOTE_GRANTED;

	memcpy_s(buf.get(), sizeof(unsigned int), &buf_size, sizeof(unsigned int));
	offset += sizeof(unsigned int);

	memcpy_s(buf.get() + offset, sizeof(ErrorCode), &err_code, sizeof(ErrorCode));
	offset += sizeof(ErrorCode);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &term_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(unsigned long long), &obj->term, sizeof(unsigned long long));
	offset += sizeof(long long);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &vote_granted_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(bool), &obj->vote_granted, sizeof(bool));

	return std::tuple<unsigned int, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<unsigned int, std::shared_ptr<char>> ClassToByteTransformer::transform(DataNodeHeartbeatResponse* obj) {
	unsigned int buf_size = sizeof(unsigned int) + sizeof(ErrorCode) + sizeof(int) + sizeof(bool) + 2 * sizeof(ResponseValueKey);

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	ErrorCode err_code = ErrorCode::NONE;
	int offset = 0;

	ResponseValueKey ok_type = ResponseValueKey::OK;
	ResponseValueKey leader_id_type = ResponseValueKey::LEADER_ID;

	memcpy_s(buf.get(), sizeof(unsigned int), &buf_size, sizeof(unsigned int));
	offset += sizeof(unsigned int);

	memcpy_s(buf.get() + offset, sizeof(ErrorCode), &err_code, sizeof(ErrorCode));
	offset += sizeof(ErrorCode);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &ok_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(bool), &obj->ok, sizeof(bool));
	offset += sizeof(bool);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &leader_id_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->leader_id, sizeof(int));

	return std::tuple<unsigned int, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<unsigned int, std::shared_ptr<char>> ClassToByteTransformer::transform(ExpireConsumersResponse* obj) {
	unsigned int buf_size = sizeof(unsigned int) + sizeof(ErrorCode) + sizeof(int) + sizeof(bool) + 2 * sizeof(ResponseValueKey);

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	ErrorCode err_code = ErrorCode::NONE;
	int offset = 0;

	ResponseValueKey ok_type = ResponseValueKey::OK;
	ResponseValueKey leader_id_type = ResponseValueKey::LEADER_ID;

	memcpy_s(buf.get(), sizeof(unsigned int), &buf_size, sizeof(unsigned int));
	offset += sizeof(unsigned int);

	memcpy_s(buf.get() + offset, sizeof(ErrorCode), &err_code, sizeof(ErrorCode));
	offset += sizeof(ErrorCode);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &ok_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(bool), &obj->ok, sizeof(bool));
	offset += sizeof(bool);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &leader_id_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->leader_id, sizeof(int));

	return std::tuple<unsigned int, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<unsigned int, std::shared_ptr<char>> ClassToByteTransformer::transform(CreateQueueResponse* obj) {
	unsigned int buf_size = sizeof(unsigned int) + sizeof(ErrorCode) + 2 * sizeof(bool) + 2 * sizeof(ResponseValueKey);

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	ErrorCode err_code = ErrorCode::NONE;
	int offset = 0;

	ResponseValueKey ok_type = ResponseValueKey::OK;
	ResponseValueKey queue_created_type = ResponseValueKey::QUEUE_CREATED;

	memcpy_s(buf.get(), sizeof(unsigned int), &buf_size, sizeof(unsigned int));
	offset += sizeof(unsigned int);

	memcpy_s(buf.get() + offset, sizeof(ErrorCode), &err_code, sizeof(ErrorCode));
	offset += sizeof(ErrorCode);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &ok_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(bool), &obj->ok, sizeof(bool));
	offset += sizeof(bool);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &queue_created_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(bool), &obj->created, sizeof(bool));
	offset += sizeof(bool);

	return std::tuple<unsigned int, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<unsigned int, std::shared_ptr<char>> ClassToByteTransformer::transform(DeleteQueueResponse* obj) {
	unsigned int buf_size = sizeof(unsigned int) + sizeof(ErrorCode) + 2 * sizeof(bool) + 2 * sizeof(ResponseValueKey);

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	ErrorCode err_code = ErrorCode::NONE;
	int offset = 0;

	ResponseValueKey ok_type = ResponseValueKey::OK;
	ResponseValueKey queue_deleted_type = ResponseValueKey::QUEUE_DELETED;

	memcpy_s(buf.get(), sizeof(unsigned int), &buf_size, sizeof(unsigned int));
	offset += sizeof(unsigned int);

	memcpy_s(buf.get() + offset, sizeof(ErrorCode), &err_code, sizeof(ErrorCode));
	offset += sizeof(ErrorCode);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &ok_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(bool), &obj->ok, sizeof(bool));
	offset += sizeof(bool);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &queue_deleted_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(bool), &obj->deleted, sizeof(bool));
	offset += sizeof(bool);

	return std::tuple<unsigned int, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<unsigned int, std::shared_ptr<char>> ClassToByteTransformer::transform(GetControllerConnectionInfoResponse* obj) {
	int count = obj->connection_infos.size();

	unsigned int buf_size = sizeof(unsigned int) + sizeof(ErrorCode) + sizeof(int) + sizeof(ResponseValueKey) + count * (3 * sizeof(int) + sizeof(ResponseValueKey));

	for (auto& info : obj->connection_infos)
		buf_size += std::get<1>(info)->address.size();

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	ErrorCode err_code = ErrorCode::NONE;
	int offset = 0;

	ResponseValueKey leader_id_type = ResponseValueKey::LEADER_ID;
	ResponseValueKey connection_info_type = ResponseValueKey::CONTROLLER_CONNECTION_INFO;

	memcpy_s(buf.get(), sizeof(unsigned int), &buf_size, sizeof(unsigned int));
	offset += sizeof(unsigned int);

	memcpy_s(buf.get() + offset, sizeof(ErrorCode), &err_code, sizeof(ErrorCode));
	offset += sizeof(ErrorCode);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &leader_id_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->leader_id, sizeof(int));
	offset += sizeof(int);

	for (auto& info : obj->connection_infos) {
		int node_id = std::get<0>(info);
		ConnectionInfo* conn_info = std::get<1>(info);
		int address_size = conn_info->external_address.size();

		memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &connection_info_type, sizeof(ResponseValueKey));
		offset += sizeof(ResponseValueKey);

		memcpy_s(buf.get() + offset, sizeof(int), &node_id, sizeof(int));
		offset += sizeof(int);

		memcpy_s(buf.get() + offset, sizeof(int), &address_size, sizeof(int));
		offset += sizeof(int);

		memcpy_s(buf.get() + offset, address_size, conn_info->external_address.c_str(), address_size);
		offset += address_size;

		memcpy_s(buf.get() + offset, sizeof(int), &conn_info->external_port, sizeof(int));
		offset += sizeof(int);
	}

	return std::tuple<unsigned int, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<unsigned int, std::shared_ptr<char>> ClassToByteTransformer::transform(GetLeaderIdResponse* obj) {
	unsigned int buf_size = sizeof(unsigned int) + sizeof(ErrorCode) + sizeof(int) + sizeof(ResponseValueKey);

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	ErrorCode err_code = ErrorCode::NONE;
	int offset = 0;

	ResponseValueKey leader_id_type = ResponseValueKey::LEADER_ID;

	memcpy_s(buf.get(), sizeof(unsigned int), &buf_size, sizeof(unsigned int));
	offset += sizeof(unsigned int);

	memcpy_s(buf.get() + offset, sizeof(ErrorCode), &err_code, sizeof(ErrorCode));
	offset += sizeof(ErrorCode);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &leader_id_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->leader_id, sizeof(int));

	return std::tuple<unsigned int, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<unsigned int, std::shared_ptr<char>> ClassToByteTransformer::transform(ProduceMessagesResponse* obj) {
	unsigned int buf_size = sizeof(unsigned int) + sizeof(ErrorCode) + sizeof(bool) + sizeof(ResponseValueKey);

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	ErrorCode err_code = ErrorCode::NONE;
	int offset = 0;

	ResponseValueKey ok_type = ResponseValueKey::OK;

	memcpy_s(buf.get(), sizeof(unsigned int), &buf_size, sizeof(unsigned int));
	offset += sizeof(unsigned int);

	memcpy_s(buf.get() + offset, sizeof(ErrorCode), &err_code, sizeof(ErrorCode));
	offset += sizeof(ErrorCode);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &ok_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(bool), &obj->ok, sizeof(bool));

	return std::tuple<unsigned int, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<unsigned int, std::shared_ptr<char>> ClassToByteTransformer::transform(GetQueuePartitionsInfoResponse* obj) {
	int count = obj->connection_infos.size();

	unsigned int buf_size = sizeof(unsigned int) + sizeof(ErrorCode) + sizeof(int) + sizeof(ResponseValueKey) + count * (4 * sizeof(int) + sizeof(ResponseValueKey));

	for (auto& info : obj->connection_infos)
		buf_size += std::get<2>(info)->external_address.size();

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	ErrorCode err_code = ErrorCode::NONE;
	int offset = 0;

	ResponseValueKey total_partitions_type = ResponseValueKey::TOTAL_PARTITIONS;
	ResponseValueKey connection_info_type = ResponseValueKey::PARTITION_NODE_CONNECTION_INFO;

	memcpy_s(buf.get(), sizeof(unsigned int), &buf_size, sizeof(unsigned int));
	offset += sizeof(unsigned int);

	memcpy_s(buf.get() + offset, sizeof(ErrorCode), &err_code, sizeof(ErrorCode));
	offset += sizeof(ErrorCode);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &total_partitions_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->total_partitions, sizeof(int));
	offset += sizeof(int);

	for (auto& info : obj->connection_infos) {
		int partition_id = std::get<0>(info);
		int node_id = std::get<1>(info);
		ConnectionInfo* conn_info = std::get<2>(info);
		int address_size = conn_info->external_address.size();

		memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &connection_info_type, sizeof(ResponseValueKey));
		offset += sizeof(ResponseValueKey);

		memcpy_s(buf.get() + offset, sizeof(int), &partition_id, sizeof(int));
		offset += sizeof(int);

		memcpy_s(buf.get() + offset, sizeof(int), &node_id, sizeof(int));
		offset += sizeof(int);

		memcpy_s(buf.get() + offset, sizeof(int), &address_size, sizeof(int));
		offset += sizeof(int);

		memcpy_s(buf.get() + offset, address_size, conn_info->external_address.c_str(), address_size);
		offset += address_size;

		memcpy_s(buf.get() + offset, sizeof(int), &conn_info->external_port, sizeof(int));
		offset += sizeof(int);
	}

	return std::tuple<unsigned int, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<unsigned int, std::shared_ptr<char>> ClassToByteTransformer::transform(RegisterConsumerResponse* obj) {
	unsigned int buf_size = sizeof(unsigned int) + sizeof(ErrorCode) + sizeof(bool) + sizeof(unsigned long long) + 2 * sizeof(ResponseValueKey);

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	ErrorCode err_code = ErrorCode::NONE;
	int offset = 0;

	ResponseValueKey ok_type = ResponseValueKey::OK;
	ResponseValueKey consumer_id_type = ResponseValueKey::CONSUMER_ID;

	memcpy_s(buf.get(), sizeof(unsigned int), &buf_size, sizeof(unsigned int));
	offset += sizeof(unsigned int);

	memcpy_s(buf.get() + offset, sizeof(ErrorCode), &err_code, sizeof(ErrorCode));
	offset += sizeof(ErrorCode);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &ok_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(bool), &obj->ok, sizeof(bool));
	offset += sizeof(bool);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &consumer_id_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(unsigned long long), &obj->consumer_id, sizeof(unsigned long long));

	return std::tuple<unsigned int, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<unsigned int, std::shared_ptr<char>> ClassToByteTransformer::transform(GetConsumerAssignedPartitionsResponse* obj) {
	unsigned int buf_size = sizeof(unsigned int) + sizeof(ErrorCode) + sizeof(int) * (1 + obj->partitions.size()) + sizeof(ResponseValueKey);

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	ErrorCode err_code = ErrorCode::NONE;
	int offset = 0;

	ResponseValueKey assigned_partitions_type = ResponseValueKey::ASSIGNED_PARTITIONS;

	int total_assigned_partitions = obj->partitions.size();

	memcpy_s(buf.get(), sizeof(unsigned int), &buf_size, sizeof(unsigned int));
	offset += sizeof(unsigned int);

	memcpy_s(buf.get() + offset, sizeof(ErrorCode), &err_code, sizeof(ErrorCode));
	offset += sizeof(ErrorCode);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &assigned_partitions_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &total_assigned_partitions, sizeof(int));
	offset += sizeof(int);

	for (int partition : obj->partitions) {
		memcpy_s(buf.get() + offset, sizeof(int), &partition, sizeof(int));
		offset += sizeof(int);
	}

	return std::tuple<unsigned int, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<unsigned int, std::shared_ptr<char>> ClassToByteTransformer::transform(ConsumeResponse* obj) {
	unsigned int buf_size = sizeof(unsigned int) + sizeof(ErrorCode) + 2 * sizeof(int) + obj->messages_total_bytes + sizeof(ResponseValueKey);

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	ErrorCode err_code = ErrorCode::NONE;
	int offset = 0;

	ResponseValueKey messages_type = ResponseValueKey::MESSAGES;

	memcpy_s(buf.get(), sizeof(unsigned int), &buf_size, sizeof(unsigned int));
	offset += sizeof(unsigned int);

	memcpy_s(buf.get() + offset, sizeof(ErrorCode), &err_code, sizeof(ErrorCode));
	offset += sizeof(ErrorCode);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &messages_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->total_messages, sizeof(int));
	offset += sizeof(int);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->messages_total_bytes, sizeof(int));
	offset += sizeof(int);

	memcpy_s(buf.get() + offset, obj->messages_total_bytes, obj->messages_data, obj->messages_total_bytes);
	offset += obj->messages_total_bytes;

	return std::tuple<unsigned int, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<unsigned int, std::shared_ptr<char>> ClassToByteTransformer::transform(AckMessageOffsetResponse* obj) {
	unsigned int buf_size = sizeof(unsigned int) + sizeof(ErrorCode) + sizeof(bool) + sizeof(ResponseValueKey);

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	ErrorCode err_code = ErrorCode::NONE;
	int offset = 0;

	ResponseValueKey ok_type = ResponseValueKey::OK;

	memcpy_s(buf.get(), sizeof(unsigned int), &buf_size, sizeof(unsigned int));
	offset += sizeof(unsigned int);

	memcpy_s(buf.get() + offset, sizeof(ErrorCode), &err_code, sizeof(ErrorCode));
	offset += sizeof(ErrorCode);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &ok_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(bool), &obj->ok, sizeof(bool));
	offset += sizeof(bool);

	return std::tuple<unsigned int, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<unsigned int, std::shared_ptr<char>> ClassToByteTransformer::transform(ExpireConsumersRequest* obj) {
	int total_consumers = obj->expired_consumers->size();

	unsigned int buf_size = sizeof(unsigned int) + sizeof(RequestType) + sizeof(RequestValueKey) + sizeof(int) + total_consumers * (2 * sizeof(int) + sizeof(unsigned long long));

	for (auto& iter : *(obj->expired_consumers.get()))
		buf_size += std::get<0>(iter).size() + std::get<1>(iter).size();

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	RequestType req_type = RequestType::EXPIRE_CONSUMERS;

	RequestValueKey expired_consumers_type = RequestValueKey::EXPIRED_CONSUMERS;

	int offset = 0;

	memcpy_s(buf.get() + offset, sizeof(unsigned int), &buf_size, sizeof(unsigned int));
	offset += sizeof(unsigned int);

	memcpy_s(buf.get() + offset, sizeof(RequestType), &req_type, sizeof(RequestType));
	offset += sizeof(RequestType);

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &expired_consumers_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &total_consumers, sizeof(int));
	offset += sizeof(int);

	for (auto& iter : *(obj->expired_consumers.get())) {
		std::string queue_name = std::get<0>(iter);
		std::string group_id = std::get<1>(iter);
		unsigned long long consumer_id = std::get<2>(iter);

		int queue_name_size = queue_name.size();
		int group_id_size = group_id.size();

		memcpy_s(buf.get() + offset, sizeof(int), &queue_name_size, sizeof(int));
		offset += sizeof(int);

		memcpy_s(buf.get() + offset, queue_name_size, queue_name.c_str(), queue_name_size);
		offset += queue_name_size;

		memcpy_s(buf.get() + offset, sizeof(int), &group_id_size, sizeof(int));
		offset += sizeof(int);

		memcpy_s(buf.get() + offset, group_id_size, group_id.c_str(), group_id_size);
		offset += group_id_size;

		memcpy_s(buf.get() + offset, sizeof(unsigned long long), &consumer_id, sizeof(unsigned long long));
		offset += sizeof(unsigned long long);
	}

	return std::tuple<unsigned int, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<unsigned int, std::shared_ptr<char>> ClassToByteTransformer::transform(AddLaggingFollowerRequest* obj) {
	unsigned int buf_size = sizeof(unsigned int) + sizeof(RequestType) + 3 * sizeof(RequestValueKey) + 3 * sizeof(int) + obj->queue_name_length;

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	RequestType req_type = RequestType::ADD_LAGGING_FOLLOWER;

	RequestValueKey queue_name_type = RequestValueKey::QUEUE_NAME;
	RequestValueKey partition_type = RequestValueKey::PARTITION;
	RequestValueKey node_id_type = RequestValueKey::NODE_ID;

	int offset = 0;

	memcpy_s(buf.get() + offset, sizeof(unsigned int), &buf_size, sizeof(unsigned int));
	offset += sizeof(unsigned int);

	memcpy_s(buf.get() + offset, sizeof(RequestType), &req_type, sizeof(RequestType));
	offset += sizeof(RequestType);

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &queue_name_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->queue_name_length, sizeof(int));
	offset += sizeof(int);

	memcpy_s(buf.get() + offset, obj->queue_name_length, obj->queue_name, obj->queue_name_length);
	offset += obj->queue_name_length;

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &partition_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->partition, sizeof(int));
	offset += sizeof(int);

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &node_id_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->node_id, sizeof(int));
	offset += sizeof(int);

	return std::tuple<unsigned int, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<unsigned int, std::shared_ptr<char>> ClassToByteTransformer::transform(RemoveLaggingFollowerRequest* obj) {
	unsigned int buf_size = sizeof(unsigned int) + sizeof(RequestType) + 3 * sizeof(RequestValueKey) + 3 * sizeof(int) + obj->queue_name_length;

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	RequestType req_type = RequestType::ADD_LAGGING_FOLLOWER;

	RequestValueKey queue_name_type = RequestValueKey::QUEUE_NAME;
	RequestValueKey partition_type = RequestValueKey::PARTITION;
	RequestValueKey node_id_type = RequestValueKey::NODE_ID;

	int offset = 0;

	memcpy_s(buf.get() + offset, sizeof(unsigned int), &buf_size, sizeof(unsigned int));
	offset += sizeof(unsigned int);

	memcpy_s(buf.get() + offset, sizeof(RequestType), &req_type, sizeof(RequestType));
	offset += sizeof(RequestType);

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &queue_name_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->queue_name_length, sizeof(int));
	offset += sizeof(int);

	memcpy_s(buf.get() + offset, obj->queue_name_length, obj->queue_name, obj->queue_name_length);
	offset += obj->queue_name_length;

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &partition_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->partition, sizeof(int));
	offset += sizeof(int);

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &node_id_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->node_id, sizeof(int));
	offset += sizeof(int);

	return std::tuple<unsigned int, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<unsigned int, std::shared_ptr<char>> ClassToByteTransformer::transform(FetchMessagesRequest* obj) {
	unsigned int buf_size = sizeof(unsigned int) + sizeof(RequestType) + 4 * sizeof(RequestValueKey) + 3 * sizeof(int) + sizeof(unsigned long long) + obj->queue_name_length;

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	RequestType req_type = RequestType::FETCH_MESSAGES;

	RequestValueKey queue_name_type = RequestValueKey::QUEUE_NAME;
	RequestValueKey partition_type = RequestValueKey::PARTITION;
	RequestValueKey node_id_type = RequestValueKey::NODE_ID;
	RequestValueKey message_offset_type = RequestValueKey::MESSAGE_OFFSET;

	int offset = 0;

	memcpy_s(buf.get() + offset, sizeof(unsigned int), &buf_size, sizeof(unsigned int));
	offset += sizeof(unsigned int);

	memcpy_s(buf.get() + offset, sizeof(RequestType), &req_type, sizeof(RequestType));
	offset += sizeof(RequestType);

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &queue_name_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->queue_name_length, sizeof(int));
	offset += sizeof(int);

	memcpy_s(buf.get() + offset, obj->queue_name_length, obj->queue_name, obj->queue_name_length);
	offset += obj->queue_name_length;

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &partition_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->partition, sizeof(int));
	offset += sizeof(int);

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &node_id_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->node_id, sizeof(int));
	offset += sizeof(int);

	memcpy_s(buf.get() + offset, sizeof(RequestValueKey), &message_offset_type, sizeof(RequestValueKey));
	offset += sizeof(RequestValueKey);

	memcpy_s(buf.get() + offset, sizeof(unsigned long long), &obj->message_offset, sizeof(unsigned long long));
	offset += sizeof(unsigned long long);

	return std::tuple<unsigned int, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<unsigned int, std::shared_ptr<char>> ClassToByteTransformer::transform(FetchMessagesResponse* obj) {
	unsigned int buf_size = sizeof(unsigned int) + sizeof(ErrorCode) + 2 * sizeof(int) + 4 * sizeof(unsigned long long) + obj->messages_total_bytes + 5 * sizeof(ResponseValueKey);

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	ErrorCode err_code = ErrorCode::NONE;
	int offset = 0;

	ResponseValueKey last_message_offset_type = ResponseValueKey::LAST_MESSAGE_OFFSET;
	ResponseValueKey prev_message_offset_type = ResponseValueKey::PREV_MESSAGE_OFFSET;
	ResponseValueKey prev_message_leader_epoch_type = ResponseValueKey::PREV_MESSAGE_LEADER_EPOCH;
	ResponseValueKey commited_offset_type = ResponseValueKey::COMMITED_OFFSET;
	ResponseValueKey messages_type = ResponseValueKey::MESSAGES;

	memcpy_s(buf.get(), sizeof(unsigned int), &buf_size, sizeof(unsigned int));
	offset += sizeof(unsigned int);

	memcpy_s(buf.get() + offset, sizeof(ErrorCode), &err_code, sizeof(ErrorCode));
	offset += sizeof(ErrorCode);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &last_message_offset_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(unsigned long long), &obj->last_message_offset, sizeof(unsigned long long));
	offset += sizeof(unsigned long long);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &prev_message_offset_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(unsigned long long), &obj->prev_message_offset, sizeof(unsigned long long));
	offset += sizeof(unsigned long long);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &prev_message_leader_epoch_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(unsigned long long), &obj->prev_message_leader_epoch, sizeof(unsigned long long));
	offset += sizeof(unsigned long long);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &commited_offset_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(unsigned long long), &obj->commited_offset, sizeof(unsigned long long));
	offset += sizeof(unsigned long long);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &messages_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->total_messages, sizeof(int));
	offset += sizeof(int);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->messages_total_bytes, sizeof(int));
	offset += sizeof(int);

	if (obj->total_messages > 0) {
		memcpy_s(buf.get() + offset, obj->messages_total_bytes, obj->messages_data, obj->messages_total_bytes);
		offset += obj->messages_total_bytes;
	}

	return std::tuple<unsigned int, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<unsigned int, std::shared_ptr<char>> ClassToByteTransformer::transform(AddLaggingFollowerResponse* obj) {
	unsigned int buf_size = sizeof(unsigned int) + sizeof(ErrorCode) + sizeof(int) + sizeof(bool) + 2 * sizeof(ResponseValueKey);

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	ErrorCode err_code = ErrorCode::NONE;
	int offset = 0;

	ResponseValueKey ok_type = ResponseValueKey::OK;
	ResponseValueKey leader_id_type = ResponseValueKey::LEADER_ID;

	memcpy_s(buf.get(), sizeof(unsigned int), &buf_size, sizeof(unsigned int));
	offset += sizeof(unsigned int);

	memcpy_s(buf.get() + offset, sizeof(ErrorCode), &err_code, sizeof(ErrorCode));
	offset += sizeof(ErrorCode);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &ok_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(bool), &obj->ok, sizeof(bool));
	offset += sizeof(bool);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &leader_id_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->leader_id, sizeof(int));

	return std::tuple<unsigned int, std::shared_ptr<char>>(buf_size, buf);
}

std::tuple<unsigned int, std::shared_ptr<char>> ClassToByteTransformer::transform(RemoveLaggingFollowerResponse* obj) {
	unsigned int buf_size = sizeof(unsigned int) + sizeof(ErrorCode) + sizeof(int) + sizeof(bool) + 2 * sizeof(ResponseValueKey);

	std::shared_ptr<char> buf = std::shared_ptr<char>(new char[buf_size]);

	ErrorCode err_code = ErrorCode::NONE;
	int offset = 0;

	ResponseValueKey ok_type = ResponseValueKey::OK;
	ResponseValueKey leader_id_type = ResponseValueKey::LEADER_ID;

	memcpy_s(buf.get(), sizeof(unsigned int), &buf_size, sizeof(unsigned int));
	offset += sizeof(unsigned int);

	memcpy_s(buf.get() + offset, sizeof(ErrorCode), &err_code, sizeof(ErrorCode));
	offset += sizeof(ErrorCode);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &ok_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(bool), &obj->ok, sizeof(bool));
	offset += sizeof(bool);

	memcpy_s(buf.get() + offset, sizeof(ResponseValueKey), &leader_id_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(buf.get() + offset, sizeof(int), &obj->leader_id, sizeof(int));

	return std::tuple<unsigned int, std::shared_ptr<char>>(buf_size, buf);
}