#pragma once
#include <memory>
#include "InternalRequestExecutor.h"
#include "QueueMetadata.h"
#include "Enums.h"

#include "AppendEntriesRequest.cpp"
#include "RequestVoteRequest.cpp"
#include "DataNodeConnectionRequest.cpp"
#include "DataNodeHeartbeatRequest.cpp"
#include "CreateQueueRequest.cpp"
#include "DeleteQueueRequest.cpp"

#include "AppendEntriesResponse.cpp"
#include "RequestVoteResponse.cpp"
#include "DataNodeConnectionResponse.cpp"
#include "DataNodeHeartbeatResponse.cpp"
#include "CreateQueueResponse.cpp"

#include "ConnectionInfo.cpp"
#include "Connection.cpp"


InternalRequestExecutor::InternalRequestExecutor(Settings* settings, Logger* logger, ConnectionsManager* cm, FileHandler* fh, Controller* controller, ClassToByteTransformer* transformer) {
	this->settings = settings;
	this->logger = logger;
	this->cm = cm;
	this->fh = fh;
	this->controller = controller;
	this->transformer = transformer;
}

void InternalRequestExecutor::handle_append_entries_request(SOCKET_ID socket, SSL* ssl, AppendEntriesRequest* request) {
	if (this->controller == NULL) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_ACTION, "Append entries request must only be sent to controller nodes");
		return;
	}

	std::shared_ptr<AppendEntriesResponse> res = this->controller->handle_leader_append_entries(request);

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(res.get());

	this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));
}

void InternalRequestExecutor::handle_request_vote_request(SOCKET_ID socket, SSL* ssl, RequestVoteRequest* request) {
	if (this->controller == NULL) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_ACTION, "Vote request must only be sent to controller nodes");
		return;
	}

	std::shared_ptr<RequestVoteResponse> res = this->controller->handle_candidate_request_vote(request);

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(res.get());

	this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));
}

void InternalRequestExecutor::handle_data_node_connection_request(SOCKET_ID socket, SSL* ssl, DataNodeConnectionRequest* request) {
	std::shared_ptr<ConnectionInfo> connection_info = std::make_shared<ConnectionInfo>();

	connection_info.get()->address = std::string(request->address, request->address_length);
	connection_info.get()->port = request->port;

	std::unique_ptr<DataNodeConnectionResponse> res = std::make_unique<DataNodeConnectionResponse>();

	res.get()->ok = this->cm->connect_to_data_node(request->node_id, connection_info, 500);

	if (res.get()->ok && this->controller != NULL) this->controller->update_data_node_heartbeat(request->node_id, true);

	if (res.get()->ok)
		this->logger->log_info("Connected successfully to data node " + std::to_string(request->node_id));
	else
		this->logger->log_error("Failed to connect to data node " + std::to_string(request->node_id));

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(res.get());

	this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));
}

void InternalRequestExecutor::handle_data_node_heartbeat_request(SOCKET_ID socket, SSL* ssl, DataNodeHeartbeatRequest* request) {
	if (this->controller == NULL) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_ACTION, "Heartbeat must only be sent to controller nodes");
		return;
	}

	request->depth_count++;

	bool is_data_node_alive = this->controller->is_data_node_alive(request->node_id);

	std::unique_ptr<DataNodeHeartbeatResponse> res = std::make_unique<DataNodeHeartbeatResponse>();

	res.get()->leader_id = this->controller->get_leader_id();
	res.get()->ok = this->settings->get_node_id() == res.get()->leader_id && is_data_node_alive;

	if (res.get()->ok) this->controller->update_data_node_heartbeat(request->node_id);
	else if (this->settings->get_node_id() != res.get()->leader_id && res.get()->leader_id > 0 && request->depth_count <= 3) {
		// In case heartbeat sent to wrong controller node, redirect request to the leader and immediatelly respond to data node
		// that the leader was incorrect
		std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(request);

		std::mutex* mut = this->cm->get_controller_node_connections_mut();

		std::lock_guard<std::mutex> lock(*mut);

		std::map<int, std::shared_ptr<ConnectionPool>>* connections = this->cm->get_controller_node_connections();

		std::shared_ptr<ConnectionPool> pool = (*connections)[res.get()->leader_id];

		std::shared_ptr<Connection> connection = pool.get()->get_connection();

		if (connection.get() != NULL) {
			this->cm->send_request_to_socket(
				connection.get()->socket,
				connection.get()->ssl,
				std::get<1>(buf_tup).get(),
				std::get<0>(buf_tup),
				"DataNodeHeartbeatRequest"
			);

			pool.get()->add_connection(connection, true);
		}
	}

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(res.get());

	this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));
}

void InternalRequestExecutor::handle_delete_queue_request(SOCKET_ID socket, SSL* ssl, DeleteQueueRequest* request) {
	this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INTERNAL_SERVER_ERROR, "Not implemented functionality yet");
}