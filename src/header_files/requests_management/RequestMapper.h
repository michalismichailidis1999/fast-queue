#pragma once
#include <memory>
#include "../Enums.h"
#include "./Requests.h"
#include "../logging/Logger.h"

#include "../__linux/memcpy_s.h"

class RequestMapper {
private:
	Logger* logger;
public:
	RequestMapper(Logger* logger);

	// Internal Requests

	std::unique_ptr<AppendEntriesRequest> to_append_entries_request(char* recvbuf, int recvbuflen, bool used_as_response = false);

	std::unique_ptr<RequestVoteRequest> to_request_vote_request(char* recvbuf, int recvbuflen);

	std::unique_ptr<DataNodeHeartbeatRequest> to_data_node_heartbeat_request(char* recvbuf, int recvbuflen);

	std::unique_ptr<GetClusterMetadataUpdateRequest> to_get_cluster_metadata_update_request(char* recvbuf, int recvbuflen);

	std::unique_ptr<ExpireConsumersRequest> to_expire_consumers_request(char* recvbuf, int recvbuflen);

	std::unique_ptr<AddLaggingFollowerRequest> to_add_lagging_request(char* recvbuf, int recvbuflen);

	std::unique_ptr<RemoveLaggingFollowerRequest> to_remove_lagging_request(char* recvbuf, int recvbuflen);

	std::unique_ptr<FetchMessagesRequest> to_fetch_messages_request(char* recvbuf, int recvbuflen);

	// =======================================================

	// External Requests

	std::unique_ptr<CreateQueueRequest> to_create_queue_request(char* recvbuf, int recvbuflen);

	std::unique_ptr<DeleteQueueRequest> to_delete_queue_request(char* recvbuf, int recvbuflen);

	std::unique_ptr<ProduceMessagesRequest> to_produce_messages_request(char* recvbuf, int recvbuflen);

	std::unique_ptr<GetQueuePartitionsInfoRequest> to_get_queue_partitions_info_request(char* recvbuf, int recvbuflen);

	std::unique_ptr<RegisterConsumerRequest> to_register_consumer_request(char* recvbuf, int recvbuflen);

	std::unique_ptr<GetConsumerAssignedPartitionsRequest> to_get_consumer_assigned_partitions_request(char* recvbuf, int recvbuflen);

	std::unique_ptr<ConsumeRequest> to_consume_request(char* recvbuf, int recvbuflen);

	std::unique_ptr<AckMessageOffsetRequest> to_ack_message_offset_request(char* recvbuf, int recvbuflen);

	// =======================================================
};