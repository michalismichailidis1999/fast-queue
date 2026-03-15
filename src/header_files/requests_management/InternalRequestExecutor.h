#pragma once
#include <memory>
#include "../logging/Logger.h"
#include "../Settings.h"
#include "../Enums.h"
#include "../network_management/ConnectionsManager.h"
#include "../network_management/Connection.h"
#include "../file_management/FileHandler.h"
#include "../cluster_management/Controller.h"
#include "../cluster_management/DataNode.h"
#include "../queue_management/QueueManager.h"
#include "../queue_management/TransactionHandler.h"
#include "../queue_management/messages_management/MessagesHandler.h"
#include "./ClassToByteTransformer.h"
#include "./Requests.h"

#include "../__linux/memcpy_s.h"

class InternalRequestExecutor {
private:
	Settings* settings;
	Logger* logger;
	ConnectionsManager* cm;
	FileHandler* fh;
	Controller* controller;
	DataNode* data_node;
	QueueManager* qm;
	MessagesHandler* mh;
	ClassToByteTransformer* transformer;
	TransactionHandler* th;

public:
	InternalRequestExecutor(Settings* settings, Logger* logger, ConnectionsManager* cm, FileHandler* fh, Controller* controller, DataNode* data_node, QueueManager* qm, MessagesHandler* mh, ClassToByteTransformer* transformer, TransactionHandler* th);

	void handle_append_entries_request(SocketSession* socket_session, AppendEntriesRequest* request);

	void handle_request_vote_request(SocketSession* socket_session, RequestVoteRequest* request);

	void handle_data_node_heartbeat_request(SocketSession* socket_session, DataNodeHeartbeatRequest* request);

	void handle_get_cluster_metadata_update_request(SocketSession* socket_session, GetClusterMetadataUpdateRequest* request);

	void handle_expire_consumers_request(SocketSession* socket_session, ExpireConsumersRequest* request);

	void handle_fetch_messages_request(SocketSession* socket_session, FetchMessagesRequest* request);

	void handle_add_lagging_follower_request(SocketSession* socket_session, AddLaggingFollowerRequest* request);

	void handle_remove_lagging_follower_request(SocketSession* socket_session, RemoveLaggingFollowerRequest* request);

	void handle_unregister_transaction_group_request(SocketSession* socket_session, UnregisterTransactionGroupRequest* request);

	void handle_transaction_status_update_request(SocketSession* socket_session, TransactionStatusUpdateRequest* request);
};