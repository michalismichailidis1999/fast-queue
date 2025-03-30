#pragma once
#include "RequestMapper.h"
#include "CreateQueueRequest.cpp"
#include "DeleteQueueRequest.cpp"
#include "ProducerConnectRequest.cpp"
#include "ProduceMessagesRequest.cpp"
#include "AppendEntriesRequest.cpp"
#include "RequestVoteRequest.cpp"
#include "DataNodeConnectionRequest.cpp"
#include "DataNodeHeartbeatRequest.cpp"

RequestMapper::RequestMapper() {}

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
		} else throw std::exception("Invalid request value");
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
		} else throw std::exception("Invalid request value");
	}

	return req;
}

std::unique_ptr<ProducerConnectRequest> RequestMapper::to_producer_connect_request(char* recvbuf, long recvbuflen) {
	long offset = sizeof(RequestType); // skip request type 

	std::unique_ptr<ProducerConnectRequest> req = std::make_unique<ProducerConnectRequest>();

	while (offset < recvbuflen) {
		RequestValueKey* key = (RequestValueKey*)(recvbuf + offset);

		if (*key == RequestValueKey::QUEUE_NAME) {
			req.get()->queue_name_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->queue_name = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->queue_name_length;
		} else if (*key == RequestValueKey::TRANSACTIONAL_ID) {
			req.get()->transactional_id_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->transactional_id = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->transactional_id_length;
		} else if (*key == RequestValueKey::USERNAME) {
			req.get()->username_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->username = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->username_length;
		} else if (*key == RequestValueKey::PASSWORD) {
			req.get()->password_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->password = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->password_length;
		} else throw std::exception("Invalid request value");
	}

	return req;
}

std::unique_ptr<ProduceMessagesRequest> RequestMapper::to_produce_messages_request(char* recvbuf, long recvbuflen) {
	long offset = sizeof(RequestType); // skip request type 

	std::unique_ptr<ProduceMessagesRequest> req = std::make_unique<ProduceMessagesRequest>();

	req.get()->messages = std::make_shared<std::vector<char*>>();
	req.get()->messages_sizes = std::make_shared<std::vector<long>>();

	while (offset < recvbuflen) {
		RequestValueKey* key = (RequestValueKey*)(recvbuf + offset);

		if (*key == RequestValueKey::QUEUE_NAME) {
			req.get()->queue_name_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->queue_name = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->queue_name_length;
		} else if (*key == RequestValueKey::TRANSACTIONAL_ID) {
			req.get()->transactional_id_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->transactional_id = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->transactional_id_length;
		} else if (*key == RequestValueKey::TRANSACTION_ID) {
			req.get()->transaction_id = *(long*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(long);
		} else if (*key == RequestValueKey::PRODUCER_ID) {
			req.get()->producer_id = *(long*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(long);
		} else if (*key == RequestValueKey::PRODUCER_EPOCH) {
			req.get()->epoch = *(long*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(long);
		} else if (*key == RequestValueKey::PARTITION) {
			req.get()->partition = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		} else if (*key == RequestValueKey::MESSAGE) {
			long* message_size = (long*)(recvbuf + offset + sizeof(RequestValueKey));
			char* message = (char*)(recvbuf + offset + sizeof(RequestValueKey) + sizeof(long));

			req.get()->messages_sizes.get()->push_back(*message_size);
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
		} else throw std::exception("Invalid request value");
	}

	return req;
}

std::unique_ptr<AppendEntriesRequest> RequestMapper::to_append_entries_request(char* recvbuf, long recvbuflen) {
	long offset = sizeof(RequestType); // skip request type 

	std::unique_ptr<AppendEntriesRequest> req = std::make_unique<AppendEntriesRequest>();

	while (offset < recvbuflen) {
		RequestValueKey* key = (RequestValueKey*)(recvbuf + offset);

		if (*key == RequestValueKey::LEADER_ID) {
			req.get()->leader_id = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		} else if (*key == RequestValueKey::TERM) {
			req.get()->term = *(long long*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(long long);
		}
		else if (*key == RequestValueKey::PREV_LOG_INDEX) {
			req.get()->prev_log_index = *(long long*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(long long);
		}
		else if (*key == RequestValueKey::PREV_LOG_TERM) {
			req.get()->prev_log_term = *(long long*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(long long);
		}
		else if (*key == RequestValueKey::LEADER_COMMIT) {
			req.get()->leader_commit = *(long long*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(long long);
		}
		else throw std::exception("Invalid request value");
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
		else throw std::exception("Invalid request value");
	}

	return req;
}

std::unique_ptr<DataNodeConnectionRequest> RequestMapper::to_data_node_connection_request(char* recvbuf, long recvbuflen) {
	long offset = sizeof(RequestType); // skip request type 

	std::unique_ptr<DataNodeConnectionRequest> req = std::make_unique<DataNodeConnectionRequest>();

	while (offset < recvbuflen) {
		RequestValueKey* key = (RequestValueKey*)(recvbuf + offset);

		if (*key == RequestValueKey::NODE_ID) {
			req.get()->node_id = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		} else if (*key == RequestValueKey::NODE_ADDRESS) {
			req.get()->address_length = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			req.get()->address = recvbuf + offset + sizeof(RequestValueKey) + sizeof(int);
			offset += sizeof(RequestValueKey) + sizeof(int) + req.get()->address_length;
		} else if (*key == RequestValueKey::NODE_PORT) {
			req.get()->port = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		}
		else throw std::exception("Invalid request value");
	}

	return req;
}

std::unique_ptr<DataNodeHeartbeatRequest> RequestMapper::to_data_node_heartbeat_request(char* recvbuf, long recvbuflen) {
	long offset = sizeof(RequestType); // skip request type 

	std::unique_ptr<DataNodeHeartbeatRequest> req = std::make_unique<DataNodeHeartbeatRequest>();

	req.get()->depth_count = -1;

	while (offset < recvbuflen) {
		RequestValueKey* key = (RequestValueKey*)(recvbuf + offset);

		if (*key == RequestValueKey::NODE_ID) {
			req.get()->node_id = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		}
		else if (*key == RequestValueKey::DEPTH_COUNT) {
			req.get()->depth_count = *(int*)(recvbuf + offset + sizeof(RequestValueKey));
			offset += sizeof(RequestValueKey) + sizeof(int);
		}
		else throw std::exception("Invalid request value");
	}

	if (req.get()->depth_count == -1)
		req.get()->depth_count = 0;

	return req;
}