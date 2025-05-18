#include "../../header_files/requests_management/ClientRequestExecutor.h"

ClientRequestExecutor::ClientRequestExecutor(ConnectionsManager* cm, QueueManager* qm, Controller* controller, ClusterMetadata* cluster_metadata, ClassToByteTransformer* transformer, FileHandler* fh, Util* util, Settings* settings, Logger* logger) {
	this->cm = cm;
	this->qm = qm;
	this->controller = controller;
	this->fh = fh;
	this->util = util;
	this->settings = settings;
	this->cluster_metadata = cluster_metadata;
	this->transformer = transformer;
	this->logger = logger;
}

void ClientRequestExecutor::handle_get_controllers_connection_info_request(SOCKET_ID socket, SSL* ssl) {
	std::unique_ptr<GetControllerConnectionInfoResponse> res = std::make_unique<GetControllerConnectionInfoResponse>();

	for (auto& controller_info : *this->settings->get_controller_nodes())
		res->connection_infos.emplace_back(std::get<0>(controller_info), std::get<1>(controller_info).get());

	res->leader_id = this->cluster_metadata->get_leader_id();

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(res.get());

	this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));
}

void ClientRequestExecutor::handle_get_controller_leader_id_request(SOCKET_ID socket, SSL* ssl) {
	std::unique_ptr<GetLeaderIdResponse> res = std::make_unique<GetLeaderIdResponse>();

	res->leader_id = this->cluster_metadata->get_leader_id();

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(res.get());

	this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));
}

void ClientRequestExecutor::handle_create_queue_request(SOCKET_ID socket, SSL* ssl, CreateQueueRequest* request) {
	if (this->controller == NULL) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_ACTION, "Request to create queue must only be sent to controller nodes");
		return;
	}

	if (this->cluster_metadata->get_leader_id() != this->settings->get_node_id()) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_LEADER, "Non leader node. Cannot create queue.");
		return;
	}

	if (request->replication_factor > this->controller->get_active_nodes_count()) {
		this->cm->respond_to_socket_with_error(
			socket,
			ssl,
			ErrorCode::TOO_FEW_AVAILABLE_NODES,
			"There are not enough active nodes to handle replication factor of " + std::to_string(request->replication_factor)
		);
		return;
	}

	std::string queue_name = std::string(request->queue_name, request->queue_name_length);

	std::shared_ptr<QueueMetadata> queue_metadata = std::shared_ptr<QueueMetadata>(
		new QueueMetadata(queue_name, request->partitions, request->replication_factor)
	);

	queue_metadata.get()->set_status(Status::PENDING_CREATION);

	this->controller->assign_new_queue_partitions_to_nodes(queue_metadata);

	std::unique_ptr<CreateQueueResponse> res = std::make_unique<CreateQueueResponse>();
	res.get()->ok = true;

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(res.get());

	this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));
}

void ClientRequestExecutor::handle_list_queues_request(SOCKET_ID socket, SSL* ssl) {
	this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INTERNAL_SERVER_ERROR, "Not implemented functionality yet");
}

void ClientRequestExecutor::handle_producer_connect_request(SOCKET_ID socket, SSL* ssl, ProducerConnectRequest* request) {
	this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INTERNAL_SERVER_ERROR, "Not implemented functionality yet");
}

void ClientRequestExecutor::handle_produce_request(SOCKET_ID socket, SSL* ssl, ProduceMessagesRequest* request) {
	this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INTERNAL_SERVER_ERROR, "Not implemented functionality yet");
}