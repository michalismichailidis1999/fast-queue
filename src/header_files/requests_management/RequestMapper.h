#pragma once
#include <memory>
#include "Enums.h"
#include "Requests.h"

class RequestMapper {
public:
	RequestMapper();

	std::unique_ptr<CreateQueueRequest> to_create_queue_request(char* recvbuf, long recvbuflen);

	std::unique_ptr<DeleteQueueRequest> to_delete_queue_request(char* recvbuf, long recvbuflen);

	std::unique_ptr<ProducerConnectRequest> to_producer_connect_request(char* recvbuf, long recvbuflen);

	std::unique_ptr<ProduceMessagesRequest> to_produce_messages_request(char* recvbuf, long recvbuflen);

	std::unique_ptr<AppendEntriesRequest> to_append_entries_request(char* recvbuf, long recvbuflen);

	std::unique_ptr<RequestVoteRequest> to_request_vote_request(char* recvbuf, long recvbuflen);

	std::unique_ptr<DataNodeConnectionRequest> to_data_node_connection_request(char* recvbuf, long recvbuflen);

	std::unique_ptr<DataNodeHeartbeatRequest> to_data_node_heartbeat_request(char* recvbuf, long recvbuflen);
};