#include "../../header_files/requests_management/InternalRequestExecutor.h"


InternalRequestExecutor::InternalRequestExecutor(Settings* settings, Logger* logger, ConnectionsManager* cm, FileHandler* fh, Controller* controller, ClassToByteTransformer* transformer) {
	this->settings = settings;
	this->logger = logger;
	this->cm = cm;
	this->fh = fh;
	this->controller = controller;
	this->transformer = transformer;
}

void InternalRequestExecutor::handle_append_entries_request(SOCKET_ID socket, SSL* ssl, AppendEntriesRequest* request) {
	if (!this->settings->get_is_controller_node()) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_ACTION, "Append entries request must only be sent to controller nodes");
		return;
	}

	std::shared_ptr<AppendEntriesResponse> res = this->controller->handle_leader_append_entries(request);

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(res.get());

	this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));
}

void InternalRequestExecutor::handle_request_vote_request(SOCKET_ID socket, SSL* ssl, RequestVoteRequest* request) {
	if (!this->settings->get_is_controller_node()) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_ACTION, "Vote request must only be sent to controller nodes");
		return;
	}

	std::shared_ptr<RequestVoteResponse> res = this->controller->handle_candidate_request_vote(request);

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(res.get());

	this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));
}

void InternalRequestExecutor::handle_data_node_heartbeat_request(SOCKET_ID socket, SSL* ssl, DataNodeHeartbeatRequest* request) {
	if (!this->settings->get_is_controller_node()) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_ACTION, "Heartbeat must only be sent to controller nodes");
		return;
	}

	std::unique_ptr<DataNodeHeartbeatResponse> res = std::make_unique<DataNodeHeartbeatResponse>();

	std::unique_ptr<ConnectionInfo> info = std::make_unique<ConnectionInfo>();

	info.get()->address = std::string(request->address, request->address_length);
	info.get()->port = request->port;

	info.get()->external_address = std::string(request->external_address, request->external_address_length);
	info.get()->port = request->external_port;

	this->controller->update_data_node_heartbeat(request->node_id, request->register_node ? info.get() : NULL);

	res.get()->leader_id = this->controller->get_leader_id();
	res.get()->ok = this->settings->get_node_id() == res.get()->leader_id;

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(res.get());

	this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));
}

void InternalRequestExecutor::handle_get_cluster_metadata_update_request(SOCKET_ID socket, SSL* ssl, GetClusterMetadataUpdateRequest* request) {
	if (!this->settings->get_is_controller_node()) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_ACTION, "Cluster metadata can only be retrieved from controller node");
		return;
	}

	std::shared_ptr<AppendEntriesRequest> res = this->controller->get_cluster_metadata_updates(request);

	if (res == nullptr) {
		// TODO: Fix this
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INTERNAL_SERVER_ERROR, "No data node registration yet");
		return;
	}

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(res.get(), true);

	this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));
}