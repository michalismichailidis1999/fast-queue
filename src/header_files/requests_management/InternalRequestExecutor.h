#pragma once
#include <memory>
#include "../logging/Logger.h"
#include "../Settings.h"
#include "../Enums.h"
#include "../network_management/ConnectionsManager.h"
#include "../network_management/Connection.h"
#include "../file_management/FileHandler.h"
#include "../cluster_management/Controller.h"
#include "../queue_management/QueueManager.h"
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
	QueueManager* qm;
	MessagesHandler* mh;
	ClassToByteTransformer* transformer;

public:
	InternalRequestExecutor(Settings* settings, Logger* logger, ConnectionsManager* cm, FileHandler* fh, Controller* controller, QueueManager* qm, MessagesHandler* mh, ClassToByteTransformer* transformer);

	void handle_append_entries_request(SOCKET_ID socket, SSL* ssl, AppendEntriesRequest* request);

	void handle_request_vote_request(SOCKET_ID socket, SSL* ssl, RequestVoteRequest* request);

	void handle_data_node_heartbeat_request(SOCKET_ID socket, SSL* ssl, DataNodeHeartbeatRequest* request);

	void handle_get_cluster_metadata_update_request(SOCKET_ID socket, SSL* ssl, GetClusterMetadataUpdateRequest* request);

	void handle_expire_consumers_request(SOCKET_ID socket, SSL* ssl, ExpireConsumersRequest* request);

	void handle_fetch_messages_request(SOCKET_ID socket, SSL* ssl, FetchMessagesRequest* request);
};