#include "../../header_files/requests_management/RequestMapper.h"

RequestMapper::RequestMapper(Logger* logger) {
	this->logger = logger;
}

std::unique_ptr<CreateQueueRequest> RequestMapper::to_create_queue_request(char* recvbuf, int recvbuflen) {
	int offset = sizeof(RequestType); // skip request type 

	std::unique_ptr<CreateQueueRequest> req = std::make_unique<CreateQueueRequest>();
	req.get()->queue_name_length = 0;
	req.get()->cleanup_policy = (int)CleanupPolicyType::DELETE_SEGMENTS;

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
		}
		else if (*key == RequestValueKey::CLEANUP_POLICY) {
			req.get()->cleanup_policy = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		}
		else if (*key == RequestValueKey::USERNAME) {
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
			throw std::runtime_error("Invalid request value");
		}
	}

	return req;
}

std::unique_ptr<DeleteQueueRequest> RequestMapper::to_delete_queue_request(char* recvbuf, int recvbuflen) {
	int offset = sizeof(RequestType); // skip request type 

	std::unique_ptr<DeleteQueueRequest> req = std::make_unique<DeleteQueueRequest>();

	req.get()->queue_name_length = 0;

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
			throw std::runtime_error("Invalid request value");
		}
	}

	return req;
}

std::unique_ptr<ProduceMessagesRequest> RequestMapper::to_produce_messages_request(char* recvbuf, int recvbuflen) {
	int offset = sizeof(RequestType); // skip request type 

	std::unique_ptr<ProduceMessagesRequest> req = std::make_unique<ProduceMessagesRequest>();

	req.get()->queue_name_length = 0;
	req.get()->partition = -1;
	req.get()->transaction_group_id = 0;

	while (offset < recvbuflen) {
		RequestValueKey* key = (RequestValueKey*)(recvbuf + offset);

		if (*key == RequestValueKey::QUEUE_NAME) {
			req.get()->queue_name_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->queue_name = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->queue_name_length;
		} else if (*key == RequestValueKey::PARTITION) {
			req.get()->partition = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		} else if (*key == RequestValueKey::TRANSACTION_GROUP_ID) {
			req.get()->transaction_group_id = *(unsigned long long*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(unsigned long long);
		} else if (*key == RequestValueKey::MESSAGES) {
			int messages_total_bytes = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);

			unsigned int end_offset = offset + messages_total_bytes;

			while (offset < end_offset) {
				unsigned long long transaction_id = *(unsigned long long*)(recvbuf + offset);
				int message_key_size = *(int*)(recvbuf + offset + sizeof(unsigned long long));
				int message_size = *(int*)(recvbuf + offset + sizeof(unsigned long long) + sizeof(int));
				char* message_key = (char*)(recvbuf + offset + sizeof(unsigned long long) + 2 * sizeof(int));
				char* message = (char*)(recvbuf + offset + sizeof(unsigned long long) + 2 * sizeof(int) + message_key_size);

				auto messages_group = req.get()->transaction_messages_group[transaction_id];

				if (messages_group == nullptr) {
					messages_group = std::make_shared<MessageGroup>();
					req.get()->transaction_messages_group[transaction_id] = messages_group;
				}

				messages_group.get()->messages_keys_sizes.push_back(message_key_size);
				messages_group.get()->messages_sizes.push_back(message_size);
				messages_group.get()->messages_keys.push_back(message_key);
				messages_group.get()->messages.push_back(message);

				offset += sizeof(unsigned long long) + 2 * sizeof(int) + message_size + message_key_size;
			}
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
			throw std::runtime_error("Invalid request value");
		}
	}

	return req;
}

std::unique_ptr<GetQueuePartitionsInfoRequest> RequestMapper::to_get_queue_partitions_info_request(char* recvbuf, int recvbuflen) {
	int offset = sizeof(RequestType); // skip request type 

	std::unique_ptr<GetQueuePartitionsInfoRequest> req = std::make_unique<GetQueuePartitionsInfoRequest>();

	req.get()->queue_name_length = 0;

	while (offset < recvbuflen) {
		RequestValueKey* key = (RequestValueKey*)(recvbuf + offset);

		if (*key == RequestValueKey::QUEUE_NAME) {
			req.get()->queue_name_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->queue_name = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->queue_name_length;
		}
		else {
			this->logger->log_error("Invalid request value " + std::to_string((int)(*key)) + " on request type GetQueuePartitionsInfoRequest");
			throw std::runtime_error("Invalid request value");
		}
	}

	return req;
}

std::unique_ptr<AppendEntriesRequest> RequestMapper::to_append_entries_request(char* recvbuf, int recvbuflen, bool used_as_response) {
	int offset = sizeof(RequestType) + (used_as_response ? sizeof(ErrorCode) : 0); // skip request type 

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
			throw std::runtime_error("Invalid request value");
		}
	}

	return req;
}

std::unique_ptr<RequestVoteRequest> RequestMapper::to_request_vote_request(char* recvbuf, int recvbuflen) {
	int offset = sizeof(RequestType); // skip request type 

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
			throw std::runtime_error("Invalid request value");
		}
	}

	return req;
}

std::unique_ptr<DataNodeHeartbeatRequest> RequestMapper::to_data_node_heartbeat_request(char* recvbuf, int recvbuflen) {
	int offset = sizeof(RequestType); // skip request type 

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
		else if (*key == RequestValueKey::NODE_EXTERNAL_ADDRESS) {
			req.get()->external_address_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->external_address = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->external_address_length;
		}
		else if (*key == RequestValueKey::NODE_EXTERNAL_PORT) {
			req.get()->external_port = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		}
		else if (*key == RequestValueKey::REGISTER_NODE) {
			req.get()->register_node = *(bool*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(bool);
		}
		else {
			this->logger->log_error("Invalid request value " + std::to_string((int)(*key)) + " on request type DataNodeHeartbeatRequest");
			throw std::runtime_error("Invalid request value");
		}
	}

	return req;
}

std::unique_ptr<GetClusterMetadataUpdateRequest> RequestMapper::to_get_cluster_metadata_update_request(char* recvbuf, int recvbuflen) {
	int offset = sizeof(RequestType); // skip request type 

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
		else if (*key == RequestValueKey::PREV_LOG_INDEX) {
			req.get()->prev_log_index = *(unsigned long long*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(unsigned long long);
		}
		else if (*key == RequestValueKey::PREV_LOG_TERM) {
			req.get()->prev_log_term = *(unsigned long long*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(unsigned long long);
		}
		else if (*key == RequestValueKey::IS_FIRST_REQUEST) {
			req.get()->is_first_request = *(bool*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(bool);
		}
		else {
			this->logger->log_error("Invalid request value " + std::to_string((int)(*key)) + " on request type DataNodeHeartbeatRequest");
			throw std::runtime_error("Invalid request value");
		}
	}

	return req;
}

std::unique_ptr<RegisterConsumerRequest> RequestMapper::to_register_consumer_request(char* recvbuf, int recvbuflen) {
	int offset = sizeof(RequestType); // skip request type 

	std::unique_ptr<RegisterConsumerRequest> req = std::make_unique<RegisterConsumerRequest>();

	req.get()->queue_name_length = 0;
	req.get()->consumer_group_id_length = 0;
	req.get()->consume_from_beginning = false;

	while (offset < recvbuflen) {
		RequestValueKey* key = (RequestValueKey*)(recvbuf + offset);

		if (*key == RequestValueKey::QUEUE_NAME) {
			req.get()->queue_name_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->queue_name = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->queue_name_length;
		}
		else if (*key == RequestValueKey::CONSUMER_GROUP_ID) {
			req.get()->consumer_group_id_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->consumer_group_id = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->consumer_group_id_length;
		}
		else if (*key == RequestValueKey::CONSUME_FROM) {
			req.get()->consume_from_beginning = *(bool*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(bool);
		}
		else if (*key == RequestValueKey::USERNAME) {
			req.get()->username_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->username = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->username_length;
		}
		else if (*key == RequestValueKey::PASSWORD) {
			req.get()->password_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->password = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->password_length;
		}
		else {
			this->logger->log_error("Invalid request value " + std::to_string((int)(*key)) + " on request type ProduceMessagesRequest");
			throw std::runtime_error("Invalid request value");
		}
	}

	return req;
}

std::unique_ptr<GetConsumerAssignedPartitionsRequest> RequestMapper::to_get_consumer_assigned_partitions_request(char* recvbuf, int recvbuflen) {
	int offset = sizeof(RequestType); // skip request type 

	std::unique_ptr<GetConsumerAssignedPartitionsRequest> req = std::make_unique<GetConsumerAssignedPartitionsRequest>();

	req.get()->queue_name_length = 0;
	req.get()->consumer_group_id_length = 0;

	while (offset < recvbuflen) {
		RequestValueKey* key = (RequestValueKey*)(recvbuf + offset);

		if (*key == RequestValueKey::QUEUE_NAME) {
			req.get()->queue_name_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->queue_name = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->queue_name_length;
		}
		else if (*key == RequestValueKey::CONSUMER_GROUP_ID) {
			req.get()->consumer_group_id_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->consumer_group_id = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->consumer_group_id_length;
		}
		else if (*key == RequestValueKey::CONSUMER_ID) {
			req.get()->consumer_id = *(unsigned long long*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(unsigned long long);
		}
		else if (*key == RequestValueKey::USERNAME) {
			req.get()->username_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->username = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->username_length;
		}
		else if (*key == RequestValueKey::PASSWORD) {
			req.get()->password_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->password = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->password_length;
		}
		else {
			this->logger->log_error("Invalid request value " + std::to_string((int)(*key)) + " on request type GetConsumerAssignedPartitionsRequest");
			throw std::runtime_error("Invalid request value");
		}
	}

	return req;
}

std::unique_ptr<ConsumeRequest> RequestMapper::to_consume_request(char* recvbuf, int recvbuflen) {
	int offset = sizeof(RequestType); // skip request type 

	std::unique_ptr<ConsumeRequest> req = std::make_unique<ConsumeRequest>();

	req.get()->queue_name_length = 0;
	req.get()->consumer_group_id_length = 0;
	req.get()->message_offset = 0;
	req.get()->read_single_offset_only = false;

	while (offset < recvbuflen) {
		RequestValueKey* key = (RequestValueKey*)(recvbuf + offset);

		if (*key == RequestValueKey::QUEUE_NAME) {
			req.get()->queue_name_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->queue_name = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->queue_name_length;
		}
		else if (*key == RequestValueKey::CONSUMER_GROUP_ID) {
			req.get()->consumer_group_id_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->consumer_group_id = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->consumer_group_id_length;
		}
		else if (*key == RequestValueKey::PARTITION) {
			req.get()->partition = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		}
		else if (*key == RequestValueKey::CONSUMER_ID) {
			req.get()->consumer_id = *(unsigned long long*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(unsigned long long);
		}
		else if (*key == RequestValueKey::MESSAGE_OFFSET) {
			req.get()->message_offset = *(unsigned long long*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(unsigned long long);
		}
		else if (*key == RequestValueKey::READ_SINGLE_OFFSET_ONLY) {
			req.get()->read_single_offset_only = *(bool*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(bool);
		}
		else if (*key == RequestValueKey::USERNAME) {
			req.get()->username_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->username = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->username_length;
		}
		else if (*key == RequestValueKey::PASSWORD) {
			req.get()->password_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->password = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->password_length;
		}
		else {
			this->logger->log_error("Invalid request value " + std::to_string((int)(*key)) + " on request type ConsumeRequest");
			throw std::runtime_error("Invalid request value");
		}
	}

	return req;
}

std::unique_ptr<AckMessageOffsetRequest> RequestMapper::to_ack_message_offset_request(char* recvbuf, int recvbuflen) {
	int offset = sizeof(RequestType); // skip request type 

	std::unique_ptr<AckMessageOffsetRequest> req = std::make_unique<AckMessageOffsetRequest>();

	req.get()->queue_name_length = 0;
	req.get()->consumer_group_id_length = 0;
	req.get()->message_offset = 0;

	while (offset < recvbuflen) {
		RequestValueKey* key = (RequestValueKey*)(recvbuf + offset);

		if (*key == RequestValueKey::QUEUE_NAME) {
			req.get()->queue_name_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->queue_name = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->queue_name_length;
		}
		else if (*key == RequestValueKey::CONSUMER_GROUP_ID) {
			req.get()->consumer_group_id_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->consumer_group_id = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->consumer_group_id_length;
		}
		else if (*key == RequestValueKey::PARTITION) {
			req.get()->partition = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		}
		else if (*key == RequestValueKey::CONSUMER_ID) {
			req.get()->consumer_id = *(unsigned long long*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(unsigned long long);
		}
		else if (*key == RequestValueKey::MESSAGE_OFFSET) {
			req.get()->message_offset = *(unsigned long long*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(unsigned long long);
		}
		else if (*key == RequestValueKey::USERNAME) {
			req.get()->username_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->username = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->username_length;
		}
		else if (*key == RequestValueKey::PASSWORD) {
			req.get()->password_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->password = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->password_length;
		}
		else {
			this->logger->log_error("Invalid request value " + std::to_string((int)(*key)) + " on request type AckMessageOffsetRequest");
			throw std::runtime_error("Invalid request value");
		}
	}

	return req;
}

std::unique_ptr<ExpireConsumersRequest> RequestMapper::to_expire_consumers_request(char* recvbuf, int recvbuflen) {
	int offset = sizeof(RequestType); // skip request type 

	std::unique_ptr<ExpireConsumersRequest> req = std::make_unique<ExpireConsumersRequest>();

	req.get()->expired_consumers = std::make_shared<std::vector<std::tuple<std::string, std::string, unsigned long long>>>();
	

	while (offset < recvbuflen) {
		RequestValueKey* key = (RequestValueKey*)(recvbuf + offset);

		if (*key == RequestValueKey::EXPIRED_CONSUMERS) {
			int total_consumers_to_expire = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(int) + sizeof(RequestValueKey);

			for (int i = 0; i < total_consumers_to_expire; i++) {
				int queue_name_size = *(int*)(recvbuf + offset);
				offset += sizeof(int);

				std::string queue_name = std::string(recvbuf + offset, queue_name_size);
				offset += queue_name_size;

				int group_id_size = *(int*)(recvbuf + offset);
				offset += sizeof(int);

				std::string group_id = std::string(recvbuf + offset, group_id_size);
				offset += group_id_size;

				unsigned long long consumer_id = *(unsigned long long*)(recvbuf + offset);
				offset += sizeof(unsigned long long);
			}
		}
		else {
			this->logger->log_error("Invalid request value " + std::to_string((int)(*key)) + " on request type AckMessageOffsetRequest");
			throw std::runtime_error("Invalid request value");
		}
	}

	return req;
}

std::unique_ptr<AddLaggingFollowerRequest> RequestMapper::to_add_lagging_request(char* recvbuf, int recvbuflen) {
	int offset = sizeof(RequestType); // skip request type 

	std::unique_ptr<AddLaggingFollowerRequest> req = std::make_unique<AddLaggingFollowerRequest>();

	req.get()->queue_name_length = 0;

	while (offset < recvbuflen) {
		RequestValueKey* key = (RequestValueKey*)(recvbuf + offset);

		if (*key == RequestValueKey::QUEUE_NAME) {
			req.get()->queue_name_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->queue_name = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->queue_name_length;
		}
		else if (*key == RequestValueKey::PARTITION) {
			req.get()->partition = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		}
		else if (*key == RequestValueKey::NODE_ID) {
			req.get()->node_id = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		}
		else {
			this->logger->log_error("Invalid request value " + std::to_string((int)(*key)) + " on request type AddLaggingFollowerRequest");
			throw std::runtime_error("Invalid request value");
		}
	}

	return req;
}

std::unique_ptr<RemoveLaggingFollowerRequest> RequestMapper::to_remove_lagging_request(char* recvbuf, int recvbuflen) {
	int offset = sizeof(RequestType); // skip request type 

	std::unique_ptr<RemoveLaggingFollowerRequest> req = std::make_unique<RemoveLaggingFollowerRequest>();

	req.get()->queue_name_length = 0;

	while (offset < recvbuflen) {
		RequestValueKey* key = (RequestValueKey*)(recvbuf + offset);

		if (*key == RequestValueKey::QUEUE_NAME) {
			req.get()->queue_name_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->queue_name = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->queue_name_length;
		}
		else if (*key == RequestValueKey::PARTITION) {
			req.get()->partition = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		}
		else if (*key == RequestValueKey::NODE_ID) {
			req.get()->node_id = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		}
		else {
			this->logger->log_error("Invalid request value " + std::to_string((int)(*key)) + " on request type RemoveLaggingFollowerRequest");
			throw std::runtime_error("Invalid request value");
		}
	}

	return req;
}

std::unique_ptr<FetchMessagesRequest> RequestMapper::to_fetch_messages_request(char* recvbuf, int recvbuflen) {
	int offset = sizeof(RequestType); // skip request type 

	std::unique_ptr<FetchMessagesRequest> req = std::make_unique<FetchMessagesRequest>();

	req.get()->queue_name_length = 0;

	while (offset < recvbuflen) {
		RequestValueKey* key = (RequestValueKey*)(recvbuf + offset);

		if (*key == RequestValueKey::QUEUE_NAME) {
			req.get()->queue_name_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->queue_name = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->queue_name_length;
		}
		else if (*key == RequestValueKey::PARTITION) {
			req.get()->partition = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		}
		else if (*key == RequestValueKey::NODE_ID) {
			req.get()->node_id = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		}
		else if (*key == RequestValueKey::MESSAGE_OFFSET) {
			req.get()->message_offset = *(unsigned long long*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(unsigned long long);
		}
		else {
			this->logger->log_error("Invalid request value " + std::to_string((int)(*key)) + " on request type FetchMessagesRequest");
			throw std::runtime_error("Invalid request value");
		}
	}

	return req;
}

std::unique_ptr<RegisterTransactionGroupRequest> RequestMapper::to_register_transaction_group_request(char* recvbuf, int recvbuflen) {
	int offset = sizeof(RequestType); // skip request type 

	std::unique_ptr<RegisterTransactionGroupRequest> req = std::make_unique<RegisterTransactionGroupRequest>();

	req.get()->registered_queues = std::make_shared<std::vector<char*>>();
	req.get()->registered_queues_lengths = std::make_shared<std::vector<int>>();

	while (offset < recvbuflen) {
		RequestValueKey* key = (RequestValueKey*)(recvbuf + offset);

		if (*key == RequestValueKey::REGISTERED_QUEUES) {
			int total_queues = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);

			for (int i = 0; i < total_queues; i++) {
				int queue_name_size = *(int*)(recvbuf + offset);
				char* queue_name = (char*)(recvbuf + offset + sizeof(int));

				req.get()->registered_queues.get()->emplace_back(queue_name);
				req.get()->registered_queues_lengths.get()->emplace_back(queue_name_size);

				offset += sizeof(int) + queue_name_size;
			}
		}
		else if (*key == RequestValueKey::USERNAME) {
			req.get()->username_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->username = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->username_length;
		}
		else if (*key == RequestValueKey::PASSWORD) {
			req.get()->password_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->password = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->password_length;
		}
		else {
			this->logger->log_error("Invalid request value " + std::to_string((int)(*key)) + " on request type RegisterTransactionGroupRequest");
			throw std::runtime_error("Invalid request value");
		}
	}

	return req;
}

std::unique_ptr<UnregisterTransactionGroupRequest> RequestMapper::to_unregister_transaction_group_request(char* recvbuf, int recvbuflen) {
	int offset = sizeof(RequestType); // skip request type 

	std::unique_ptr<UnregisterTransactionGroupRequest> req = std::make_unique<UnregisterTransactionGroupRequest>();

	req.get()->node_id = -1;
	req.get()->transaction_group_id = 0;

	while (offset < recvbuflen) {
		RequestValueKey* key = (RequestValueKey*)(recvbuf + offset);

		if (*key == RequestValueKey::NODE_ID) {
			req.get()->node_id = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		}
		else if (*key == RequestValueKey::TRANSACTION_GROUP_ID) {
			req.get()->transaction_group_id = *(unsigned long long*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(unsigned long long);
		}
		else {
			this->logger->log_error("Invalid request value " + std::to_string((int)(*key)) + " on request type UnregisterTransactionGroupRequest");
			throw std::runtime_error("Invalid request value");
		}
	}

	return req;
}

std::unique_ptr<TransactionStatusUpdateRequest> RequestMapper::to_transaction_status_update_request(char* recvbuf, int recvbuflen) {
	int offset = sizeof(RequestType); // skip request type 

	std::unique_ptr<TransactionStatusUpdateRequest> req = std::make_unique<TransactionStatusUpdateRequest>();

	req.get()->transaction_group_id = 0;
	req.get()->transaction_id = 0;
	req.get()->status = (int)TransactionStatus::NONE;

	while (offset < recvbuflen) {
		RequestValueKey* key = (RequestValueKey*)(recvbuf + offset);

		if (*key == RequestValueKey::TRANSACTION_ID) {
			req.get()->transaction_id = *(unsigned long long*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(unsigned long long);
		}
		else if (*key == RequestValueKey::TRANSACTION_GROUP_ID) {
			req.get()->transaction_group_id = *(unsigned long long*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(unsigned long long);
		}
		else if (*key == RequestValueKey::TRANSACTION_STATUS) {
			req.get()->status = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		}
		else {
			this->logger->log_error("Invalid request value " + std::to_string((int)(*key)) + " on request type TransactionStatusUpdateRequest");
			throw std::runtime_error("Invalid request value");
		}
	}

	return req;
}

std::unique_ptr<BeginTransactionRequest> RequestMapper::to_begin_transaction_request(char* recvbuf, int recvbuflen) {
	int offset = sizeof(RequestType); // skip request type 

	std::unique_ptr<BeginTransactionRequest> req = std::make_unique<BeginTransactionRequest>();

	req.get()->transaction_group_id = 0;

	while (offset < recvbuflen) {
		RequestValueKey* key = (RequestValueKey*)(recvbuf + offset);

		if (*key == RequestValueKey::TRANSACTION_GROUP_ID) {
			req.get()->transaction_group_id = *(unsigned long long*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(unsigned long long);
		}
		else if (*key == RequestValueKey::USERNAME) {
			req.get()->username_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->username = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->username_length;
		}
		else if (*key == RequestValueKey::PASSWORD) {
			req.get()->password_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->password = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->password_length;
		}
		else {
			this->logger->log_error("Invalid request value " + std::to_string((int)(*key)) + " on request type BeginTransactionRequest");
			throw std::runtime_error("Invalid request value");
		}
	}

	return req;
}

std::unique_ptr<FinalizeTransactionRequest> RequestMapper::to_finalize_transaction_request(char* recvbuf, int recvbuflen) {
	int offset = sizeof(RequestType); // skip request type 

	std::unique_ptr<FinalizeTransactionRequest> req = std::make_unique<FinalizeTransactionRequest>();

	req.get()->transaction_group_id = 0;
	req.get()->tx_id = 0;
	req.get()->commit = false;

	while (offset < recvbuflen) {
		RequestValueKey* key = (RequestValueKey*)(recvbuf + offset);

		if (*key == RequestValueKey::TRANSACTION_GROUP_ID) {
			req.get()->transaction_group_id = *(unsigned long long*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(unsigned long long);
		}
		else if (*key == RequestValueKey::TRANSACTION_ID) {
			req.get()->tx_id = *(unsigned long long*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(unsigned long long);
		}
		else if (*key == RequestValueKey::COMMIT_TRANSACTION) {
			req.get()->commit = *(bool*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(bool);
		}
		else if (*key == RequestValueKey::USERNAME) {
			req.get()->username_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->username = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->username_length;
		}
		else if (*key == RequestValueKey::PASSWORD) {
			req.get()->password_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->password = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->password_length;
		}
		else {
			this->logger->log_error("Invalid request value " + std::to_string((int)(*key)) + " on request type FinalizeTransactionRequest");
			throw std::runtime_error("Invalid request value");
		}
	}

	return req;
}

std::unique_ptr<VerifyTransactionGroupCreationRequest> RequestMapper::to_verify_transaction_group_creation_request(char* recvbuf, int recvbuflen) {
	int offset = sizeof(RequestType); // skip request type 

	std::unique_ptr<VerifyTransactionGroupCreationRequest> req = std::make_unique<VerifyTransactionGroupCreationRequest>();

	req.get()->transaction_group_id = 0;

	while (offset < recvbuflen) {
		RequestValueKey* key = (RequestValueKey*)(recvbuf + offset);

		if (*key == RequestValueKey::TRANSACTION_GROUP_ID) {
			req.get()->transaction_group_id = *(unsigned long long*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(unsigned long long);
		}
		else {
			this->logger->log_error("Invalid request value " + std::to_string((int)(*key)) + " on request type VerifyTransactionGroupCreationRequest");
			throw std::runtime_error("Invalid request value");
		}
	}

	return req;
}