#pragma once
#include "Logger.h"
#include "Settings.h"
#include "ConnectionsManager.h"
#include "QueueManager.h"
#include "Controller.h"
#include "Util.h"
#include "FileHandler.h"
#include "ClusterMetadata.h"
#include "ClassToByteTransformer.h"
#include "Requests.h"

class ClientRequestExecutor {
private:
	ConnectionsManager* cm;
	QueueManager* qm;
	Controller* controller;
	ClusterMetadata* cluster_metadata;
	ClassToByteTransformer* transformer;
	FileHandler* fh;
	Util* util;
	Settings* settings;
	Logger* logger;
public:
	ClientRequestExecutor(ConnectionsManager* cm, QueueManager* qm, Controller* controller, ClusterMetadata* cluster_metadata, ClassToByteTransformer* transformer, FileHandler* fh, Util* util, Settings* settings, Logger* logger);

	void handle_get_controllers_connection_info_request(SOCKET_ID socket, SSL* ssl);

	void handle_get_controller_leader_id_request(SOCKET_ID socket, SSL* ssl);

	void handle_create_queue_request(SOCKET_ID socket, SSL* ssl, CreateQueueRequest* request);

	void handle_list_queues_request(SOCKET_ID socket, SSL* ssl);

	void handle_producer_connect_request(SOCKET_ID socket, SSL* ssl, ProducerConnectRequest* request);

	void handle_produce_request(SOCKET_ID socket, SSL* ssl, ProduceMessagesRequest* request);
};