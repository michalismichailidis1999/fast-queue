#pragma once
#include "Logger.h"
#include "Settings.h"
#include "ConnectionsManager.h"
#include "FileHandler.h"
#include "Controller.h"
#include "ClassToByteTransformer.h"
#include "Requests.h"

class InternalRequestExecutor {
private:
	Settings* settings;
	Logger* logger;
	ConnectionsManager* cm;
	FileHandler* fh;
	Controller* controller;
	ClassToByteTransformer* transformer;

public:
	InternalRequestExecutor(Settings* settings, Logger* logger, ConnectionsManager* cm, FileHandler* fh, Controller* controller, ClassToByteTransformer* transformer);

	void handle_append_entries_request(SOCKET_ID socket, SSL* ssl, AppendEntriesRequest* request);

	void handle_request_vote_request(SOCKET_ID socket, SSL* ssl, RequestVoteRequest* request);

	void handle_data_node_connection_request(SOCKET_ID socket, SSL* ssl, DataNodeConnectionRequest* request);

	void handle_data_node_heartbeat_request(SOCKET_ID socket, SSL* ssl, DataNodeHeartbeatRequest* request);

	void handle_delete_queue_request(SOCKET_ID socket, SSL* ssl, DeleteQueueRequest* request);
};