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

std::unique_ptr<ExpireConsumersResponse> ResponseMapper::to_expire_consumers_response(char* res_buf, long res_buf_len) {
	long offset = sizeof(ErrorCode); // skip error code

	std::unique_ptr<ExpireConsumersResponse> res = std::make_unique<ExpireConsumersResponse>();

	while (offset < res_buf_len) {
		ResponseValueKey* key = (ResponseValueKey*)(res_buf + offset);

		if (*key == ResponseValueKey::OK) {
			res.get()->ok = *(bool*)(res_buf + offset + sizeof(ResponseValueKey));
			offset += sizeof(RequestValueKey) + sizeof(bool);
		}
		else if (*key == ResponseValueKey::LEADER_ID) {
			res.get()->leader_id = *(int*)(res_buf + offset + sizeof(ResponseValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		}
		else {
			this->logger->log_error("Invalid response value " + std::to_string((int)(*key)) + " on response type ExpireConsumersResponse");
			return nullptr;
		}
	}

	return res;
}

std::unique_ptr<AddLaggingFollowerResponse> ResponseMapper::to_add_lagging_follower_response(char* res_buf, long res_buf_len) {
	long offset = sizeof(ErrorCode); // skip error code

	std::unique_ptr<AddLaggingFollowerResponse> res = std::make_unique<AddLaggingFollowerResponse>();

	while (offset < res_buf_len) {
		ResponseValueKey* key = (ResponseValueKey*)(res_buf + offset);

		if (*key == ResponseValueKey::OK) {
			res.get()->ok = *(bool*)(res_buf + offset + sizeof(ResponseValueKey));
			offset += sizeof(RequestValueKey) + sizeof(bool);
		}
		else if (*key == ResponseValueKey::LEADER_ID) {
			res.get()->leader_id = *(int*)(res_buf + offset + sizeof(ResponseValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		}
		else {
			this->logger->log_error("Invalid response value " + std::to_string((int)(*key)) + " on response type AddLaggingFollowerResponse");
			return nullptr;
		}
	}

	return res;
}

std::unique_ptr<RemoveLaggingFollowerResponse> ResponseMapper::to_remove_lagging_follower_response(char* res_buf, long res_buf_len) {
	long offset = sizeof(ErrorCode); // skip error code

	std::unique_ptr<RemoveLaggingFollowerResponse> res = std::make_unique<RemoveLaggingFollowerResponse>();

	while (offset < res_buf_len) {
		ResponseValueKey* key = (ResponseValueKey*)(res_buf + offset);

		if (*key == ResponseValueKey::OK) {
			res.get()->ok = *(bool*)(res_buf + offset + sizeof(ResponseValueKey));
			offset += sizeof(RequestValueKey) + sizeof(bool);
		}
		else if (*key == ResponseValueKey::LEADER_ID) {
			res.get()->leader_id = *(int*)(res_buf + offset + sizeof(ResponseValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		}
		else {
			this->logger->log_error("Invalid response value " + std::to_string((int)(*key)) + " on response type RemoveLaggingFollowerResponse");
			return nullptr;
		}
	}

	return res;
}

std::unique_ptr<FetchMessagesResponse> ResponseMapper::to_fetch_messages_response(char* res_buf, long res_buf_len) {
	long offset = sizeof(ErrorCode); // skip error code

	std::unique_ptr<FetchMessagesResponse> res = std::make_unique<FetchMessagesResponse>();

	while (offset < res_buf_len) {
		ResponseValueKey* key = (ResponseValueKey*)(res_buf + offset);

		if (*key == ResponseValueKey::MESSAGES) {
			res.get()->total_messages = *(int*)(res_buf + offset + sizeof(ResponseValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
			res.get()->messages_total_bytes = *(int*)(res_buf + offset);
			offset += sizeof(int);
			
			if (res.get()->total_messages > 0) {
				res.get()->messages_data = res_buf + offset;
				offset += res.get()->messages_total_bytes;
			}
		}
		else if (*key == ResponseValueKey::LAST_MESSAGE_OFFSET) {
			res.get()->last_message_offset = *(unsigned long long*)(res_buf + offset + sizeof(ResponseValueKey));
			offset += sizeof(RequestValueKey) + sizeof(unsigned long long);
		}
		else if (*key == ResponseValueKey::COMMITED_OFFSET) {
			res.get()->commited_offset = *(unsigned long long*)(res_buf + offset + sizeof(ResponseValueKey));
			offset += sizeof(RequestValueKey) + sizeof(unsigned long long);
		}
		else if (*key == ResponseValueKey::PREV_MESSAGE_OFFSET) {
			res.get()->prev_message_offset = *(unsigned long long*)(res_buf + offset + sizeof(ResponseValueKey));
			offset += sizeof(RequestValueKey) + sizeof(unsigned long long);
		}
		else if (*key == ResponseValueKey::PREV_MESSAGE_LEADER_EPOCH) {
			res.get()->prev_message_leader_epoch = *(unsigned long long*)(res_buf + offset + sizeof(ResponseValueKey));
			offset += sizeof(RequestValueKey) + sizeof(unsigned long long);
		}
		else if (*key == ResponseValueKey::CONSUMERS_ACKS) {
			res.get()->consumer_offsets_count = *(int*)(res_buf + offset + sizeof(ResponseValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);

			if (res.get()->consumer_offsets_count > 0) {
				res.get()->consumer_offsets_data = res_buf + offset;
				offset += res.get()->consumer_offsets_count * CONSUMER_ACK_TOTAL_BYTES;
			}
		}
		else {
			this->logger->log_error("Invalid response value " + std::to_string((int)(*key)) + " on response type FetchMessagesResponse");
			return nullptr;
		}
	}

	return res;
}

std::unique_ptr<TransactionStatusUpdateResponse> ResponseMapper::to_transaction_status_update_response(char* res_buf, long res_buf_len) {
	long offset = sizeof(ErrorCode); // skip error code

	std::unique_ptr<TransactionStatusUpdateResponse> res = std::make_unique<TransactionStatusUpdateResponse>();

	while (offset < res_buf_len) {
		ResponseValueKey* key = (ResponseValueKey*)(res_buf + offset);

		if (*key == ResponseValueKey::OK) {
			res.get()->ok = *(bool*)(res_buf + offset + sizeof(ResponseValueKey));
			offset += sizeof(RequestValueKey) + sizeof(bool);
		}
		else {
			this->logger->log_error("Invalid response value " + std::to_string((int)(*key)) + " on response type TransactionStatusUpdateResponse");
			return nullptr;
		}
	}

	return res;
}

std::unique_ptr<UnregisterTransactionGroupResponse> ResponseMapper::to_unregister_transaction_group_response(char* res_buf, long res_buf_len) {
	long offset = sizeof(ErrorCode); // skip error code

	std::unique_ptr<UnregisterTransactionGroupResponse> res = std::make_unique<UnregisterTransactionGroupResponse>();

	while (offset < res_buf_len) {
		ResponseValueKey* key = (ResponseValueKey*)(res_buf + offset);

		if (*key == ResponseValueKey::OK) {
			res.get()->ok = *(bool*)(res_buf + offset + sizeof(ResponseValueKey));
			offset += sizeof(RequestValueKey) + sizeof(bool);
		}
		else if (*key == ResponseValueKey::OK) {
			res.get()->leader_id = *(int*)(res_buf + offset + sizeof(ResponseValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		}
		else {
			this->logger->log_error("Invalid response value " + std::to_string((int)(*key)) + " on response type UnregisterTransactionGroupResponse");
			return nullptr;
		}
	}

	return res;
}