#include "../../header_files/requests_management/RequestMapper.h"

RequestMapper::RequestMapper(Logger* logger) {
	this->logger = logger;
}

std::unique_ptr<CreateQueueRequest> RequestMapper::to_create_queue_request(char* recvbuf, long recvbuflen) {
	long offset = sizeof(RequestType); // skip request type 

	std::unique_ptr<CreateQueueRequest> req = std::make_unique<CreateQueueRequest>();

	while (offset < recvbuflen) {
		RequestValueKey* key = (RequestValueKey*)(recvbuf + offset);

		if (*key == RequestValueKey::QUEUE_NAME) {
			req.get()->queue_name_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->queue_name = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->queue_name_length;
		} else if (*key == RequestValueKey::PARTITIONS) {
			req.get()->partitions = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		} else if (*key == RequestValueKey::REPLICATION_FACTOR) {
			req.get()->replication_factor = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		} else if (*key == RequestValueKey::USERNAME) {
			req.get()->username_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->username = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->username_length;
		} else if (*key == RequestValueKey::PASSWORD) {
			req.get()->password_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->password = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->password_length;
		}
		else {
			this->logger->log_error("Invalid request value " + std::to_string((int)(*key)) + " on request type CreateQueueRequest");
			throw std::exception("Invalid request value");
		}
	}

	return req;
}

std::unique_ptr<DeleteQueueRequest> RequestMapper::to_delete_queue_request(char* recvbuf, long recvbuflen) {
	long offset = sizeof(RequestType); // skip request type 

	std::unique_ptr<DeleteQueueRequest> req = std::make_unique<DeleteQueueRequest>();

	while (offset < recvbuflen) {
		RequestValueKey* key = (RequestValueKey*)(recvbuf + offset);

		if (*key == RequestValueKey::QUEUE_NAME) {
			req.get()->queue_name_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->queue_name = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->queue_name_length;
		} else if (*key == RequestValueKey::USERNAME) {
			req.get()->username_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->username = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->username_length;
		} else if (*key == RequestValueKey::PASSWORD) {
			req.get()->password_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->password = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->password_length;
		}
		else {
			this->logger->log_error("Invalid request value " + std::to_string((int)(*key)) + " on request type DeleteQueueRequest");
			throw std::exception("Invalid request value");
		}
	}

	return req;
}

std::unique_ptr<ProduceMessagesRequest> RequestMapper::to_produce_messages_request(char* recvbuf, long recvbuflen) {
	long offset = sizeof(RequestType); // skip request type 

	std::unique_ptr<ProduceMessagesRequest> req = std::make_unique<ProduceMessagesRequest>();

	req.get()->messages = std::make_shared<std::vector<char*>>();
	req.get()->messages_sizes = std::make_shared<std::vector<int>>();

	while (offset < recvbuflen) {
		RequestValueKey* key = (RequestValueKey*)(recvbuf + offset);

		if (*key == RequestValueKey::QUEUE_NAME) {
			req.get()->queue_name_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->queue_name = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->queue_name_length;
		} else if (*key == RequestValueKey::PARTITION) {
			req.get()->partition = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		} else if (*key == RequestValueKey::MESSAGE) {
			int* message_key_size = (int*)(recvbuf + offset + sizeof(RequestValueKey));
			int* message_size = (int*)(recvbuf + offset + sizeof(RequestValueKey) + sizeof(int));
			char* message_key = (char*)(recvbuf + offset + sizeof(RequestValueKey) + 2 * sizeof(int));
			char* message = (char*)(recvbuf + offset + sizeof(RequestValueKey) + 2 * sizeof(int) + *message_key_size);

			req.get()->messages_keys_sizes.get()->push_back(*message_key_size);
			req.get()->messages_sizes.get()->push_back(*message_size);
			req.get()->messages_keys.get()->push_back(message_key);
			req.get()->messages.get()->push_back(message);

			offset += sizeof(RequestValueKey) + sizeof(long) + *(message_size);
		} else if (*key == RequestValueKey::USERNAME) {
			req.get()->username_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->username = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->username_length;
		} else if (*key == RequestValueKey::PASSWORD) {
			req.get()->password_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->password = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->password_length;
		}
		else {
			this->logger->log_error("Invalid request value " + std::to_string((int)(*key)) + " on request type ProduceMessagesRequest");
			throw std::exception("Invalid request value");
		}
	}

	return req;
}

std::unique_ptr<GetQueuePartitionsInfoRequest> RequestMapper::to_get_queue_partitions_info_request(char* recvbuf, long recvbuflen) {
	long offset = sizeof(RequestType); // skip request type 

	std::unique_ptr<GetQueuePartitionsInfoRequest> req = std::make_unique<GetQueuePartitionsInfoRequest>();

	while (offset < recvbuflen) {
		RequestValueKey* key = (RequestValueKey*)(recvbuf + offset);

		if (*key == RequestValueKey::QUEUE_NAME) {
			req.get()->queue_name_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->queue_name = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->queue_name_length;
		}
		else {
			this->logger->log_error("Invalid request value " + std::to_string((int)(*key)) + " on request type GetQueuePartitionsInfoRequest");
			throw std::exception("Invalid request value");
		}
	}

	return req;
}

std::unique_ptr<AppendEntriesRequest> RequestMapper::to_append_entries_request(char* recvbuf, long recvbuflen) {
	long offset = sizeof(RequestType); // skip request type 

	std::unique_ptr<AppendEntriesRequest> req = std::make_unique<AppendEntriesRequest>();

	req.get()->total_commands = 0;
	req.get()->commands_total_bytes = 0;
	req.get()->commands_data = NULL;

	while (offset < recvbuflen) {
		RequestValueKey* key = (RequestValueKey*)(recvbuf + offset);

		if (*key == RequestValueKey::LEADER_ID) {
			req.get()->leader_id = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		} else if (*key == RequestValueKey::TERM) {
			req.get()->term = *(unsigned long long*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(long long);
		}
		else if (*key == RequestValueKey::PREV_LOG_INDEX) {
			req.get()->prev_log_index = *(unsigned long long*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(long long);
		}
		else if (*key == RequestValueKey::PREV_LOG_TERM) {
			req.get()->prev_log_term = *(unsigned long long*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(long long);
		}
		else if (*key == RequestValueKey::LEADER_COMMIT) {
			req.get()->leader_commit = *(unsigned long long*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(long long);
		}
		else if (*key == RequestValueKey::COMMANDS) {
			req.get()->total_commands = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->commands_total_bytes = *(long*)(recvbuf + offset + sizeof(RequestValueKey) + sizeof(int));
			req.get()->commands_data = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int) + sizeof(long);
			offset += sizeof(RequestValueKey) + sizeof(int) + sizeof(long) + req.get()->commands_total_bytes;
		}
		else {
			this->logger->log_error("Invalid request value " + std::to_string((int)(*key)) + " on request type AppendEntriesRequest");
			throw std::exception("Invalid request value");
		}
	}

	return req;
}

std::unique_ptr<RequestVoteRequest> RequestMapper::to_request_vote_request(char* recvbuf, long recvbuflen) {
	long offset = sizeof(RequestType); // skip request type 

	std::unique_ptr<RequestVoteRequest> req = std::make_unique<RequestVoteRequest>();

	while (offset < recvbuflen) {
		RequestValueKey* key = (RequestValueKey*)(recvbuf + offset);

		if (*key == RequestValueKey::CANDIDATE_ID) {
			req.get()->candidate_id = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		}
		else if (*key == RequestValueKey::TERM) {
			req.get()->term = *(long long*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(long long);
		}
		else if (*key == RequestValueKey::LAST_LOG_INDEX) {
			req.get()->last_log_index = *(long long*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(long long);
		}
		else if (*key == RequestValueKey::LAST_LOG_TERM) {
			req.get()->last_log_term = *(long long*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(long long);
		}
		else {
			this->logger->log_error("Invalid request value " + std::to_string((int)(*key)) + " on request type RequestVoteRequest");
			throw std::exception("Invalid request value");
		}
	}

	return req;
}

std::unique_ptr<DataNodeHeartbeatRequest> RequestMapper::to_data_node_heartbeat_request(char* recvbuf, long recvbuflen) {
	long offset = sizeof(RequestType); // skip request type 

	std::unique_ptr<DataNodeHeartbeatRequest> req = std::make_unique<DataNodeHeartbeatRequest>();

	while (offset < recvbuflen) {
		RequestValueKey* key = (RequestValueKey*)(recvbuf + offset);

		if (*key == RequestValueKey::NODE_ID) {
			req.get()->node_id = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		}
		else if (*key == RequestValueKey::NODE_ADDRESS) {
			req.get()->address_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->address = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->address_length;
		}
		else if (*key == RequestValueKey::NODE_PORT) {
			req.get()->port = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		}
		else {
			this->logger->log_error("Invalid request value " + std::to_string((int)(*key)) + " on request type DataNodeHeartbeatRequest");
			throw std::exception("Invalid request value");
		}
	}

	return req;
}

std::unique_ptr<GetClusterMetadataUpdateRequest> RequestMapper::to_get_cluster_metadata_update_request(char* recvbuf, long recvbuflen) {
	long offset = sizeof(RequestType); // skip request type 

	std::unique_ptr<GetClusterMetadataUpdateRequest> req = std::make_unique<GetClusterMetadataUpdateRequest>();

	while (offset < recvbuflen) {
		RequestValueKey* key = (RequestValueKey*)(recvbuf + offset);

		if (*key == RequestValueKey::NODE_ID) {
			req.get()->node_id = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		}
		else if (*key == RequestValueKey::INDEX_MATCHED) {
			req.get()->prev_req_index_matched = *(bool*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(bool);
		}
		else {
			this->logger->log_error("Invalid request value " + std::to_string((int)(*key)) + " on request type DataNodeHeartbeatRequest");
			throw std::exception("Invalid request value");
		}
	}

	return req;
}