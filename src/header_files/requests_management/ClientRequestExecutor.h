#pragma once
#include "../Settings.h"
#include "../Constants.h"
#include "../logging/Logger.h"
#include "../network_management/ConnectionsManager.h"
#include "../queue_management/QueueManager.h"
#include "../queue_management/messages_management/MessagesHandler.h"
#include "../queue_management/messages_management/MessageOffsetAckHandler.h"
#include "../cluster_management/Controller.h"
#include "../cluster_management/ClusterMetadata.h"
#include "./ClassToByteTransformer.h"
#include "./Requests.h"

class ClientRequestExecutor {
private:
	MessagesHandler* mh;
	MessageOffsetAckHandler* oah;
	ConnectionsManager* cm;
	QueueManager* qm;
	Controller* controller;
	ClassToByteTransformer* transformer;
	Settings* settings;
	Logger* logger;
public:
	ClientRequestExecutor(MessagesHandler* mh, MessageOffsetAckHandler* oah, ConnectionsManager* cm, QueueManager* qm, Controller* controller, ClassToByteTransformer* transformer, Settings* settings, Logger* logger);

	void handle_get_controllers_connection_info_request(SOCKET_ID socket, SSL* ssl);

	void handle_get_controller_leader_id_request(SOCKET_ID socket, SSL* ssl);

	void handle_create_queue_request(SOCKET_ID socket, SSL* ssl, CreateQueueRequest* request);

	void handle_produce_request(SOCKET_ID socket, SSL* ssl, ProduceMessagesRequest* request);

	void handle_delete_queue_request(SOCKET_ID socket, SSL* ssl, DeleteQueueRequest* request);

	void handle_get_queue_partitions_info_request(SOCKET_ID socket, SSL* ssl, GetQueuePartitionsInfoRequest* request);

	void handle_register_consumer_request(SOCKET_ID socket, SSL* ssl, RegisterConsumerRequest* request);

	void handle_get_consumer_assigned_partitions_request(SOCKET_ID socket, SSL* ssl, GetConsumerAssignedPartitionsRequest* request);

	void handle_consume_request(SOCKET_ID socket, SSL* ssl, ConsumeRequest* request);

	void handle_ack_message_offset_request(SOCKET_ID socket, SSL* ssl, AckMessageOffsetRequest* request);
};