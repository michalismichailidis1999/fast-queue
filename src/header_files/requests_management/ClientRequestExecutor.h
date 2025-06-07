#pragma once
#include "../Settings.h"
#include "../Constants.h"
#include "../logging/Logger.h"
#include "../network_management/ConnectionsManager.h"
#include "../queue_management/QueueManager.h"
#include "../cluster_management/Controller.h"
#include "../cluster_management/ClusterMetadata.h"
#include "../util/Util.h"
#include "../file_management/FileHandler.h"
#include "./ClassToByteTransformer.h"
#include "./Requests.h"

class ClientRequestExecutor {
private:
	ConnectionsManager* cm;
	QueueManager* qm;
	Controller* controller;
	ClassToByteTransformer* transformer;
	FileHandler* fh;
	Util* util;
	Settings* settings;
	Logger* logger;
public:
	ClientRequestExecutor(ConnectionsManager* cm, QueueManager* qm, Controller* controller, ClassToByteTransformer* transformer, FileHandler* fh, Util* util, Settings* settings, Logger* logger);

	void handle_get_controllers_connection_info_request(SOCKET_ID socket, SSL* ssl);

	void handle_get_controller_leader_id_request(SOCKET_ID socket, SSL* ssl);

	void handle_create_queue_request(SOCKET_ID socket, SSL* ssl, CreateQueueRequest* request);

	void handle_list_queues_request(SOCKET_ID socket, SSL* ssl);

	void handle_producer_connect_request(SOCKET_ID socket, SSL* ssl, ProducerConnectRequest* request);

	void handle_produce_request(SOCKET_ID socket, SSL* ssl, ProduceMessagesRequest* request);

	void handle_delete_queue_request(SOCKET_ID socket, SSL* ssl, DeleteQueueRequest* request);
};