#include "../../header_files/requests_management/RequestManager.h"

RequestManager::RequestManager(ConnectionsManager* cm, Settings* settings, ClientRequestExecutor* client_request_executor, InternalRequestExecutor* internal_request_executor, RequestMapper* mapper, Logger* logger) {
	this->settings = settings;
	this->cm = cm;
	this->client_request_executor = client_request_executor;
	this->internal_request_executor = internal_request_executor;
	this->mapper = mapper;
	this->logger = logger;
}

bool RequestManager::is_user_authorized_for_action(char* username, int username_length, char* password, int password_length) {
	return true;
}

void RequestManager::execute_request(SOCKET_ID socket, SSL* ssl, bool internal_communication) {
	bool lock_removed = false;

	try
	{
		std::unique_ptr<char> req_size_buf = std::unique_ptr<char>(new char[sizeof(int)]);

		bool success = this->cm->receive_socket_buffer(socket, ssl, req_size_buf.get(), sizeof(int));

		if (!success) {
			this->cm->remove_socket_lock(socket);
			lock_removed = true;
			return;
		}

		int res_buffer_length = *((int*)req_size_buf.get());

		res_buffer_length -= sizeof(int);

		if (res_buffer_length <= 0) {
			this->logger->log_error("Received invalid request body format");
			this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_REQUEST_BODY, "Invalid request body format");
			this->cm->remove_socket_lock(socket);
			return;
		}
		
		if (res_buffer_length > this->settings->get_max_message_size() && !internal_communication) {
			std::string err_msg = "Message bytes (" + std::to_string(res_buffer_length) + ") was larger than maximum allowed bytes (" + std::to_string(this->settings->get_max_message_size()) + "). Closing this socket connection";
			this->cm->close_socket_connection(socket, ssl);
			this->cm->remove_socket_lock(socket);
			this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::TOO_MANY_BYTES_RECEIVED, err_msg);
			this->logger->log_error(err_msg);
			return;
		}

		std::unique_ptr<char> recvbuf = std::unique_ptr<char>(new char[res_buffer_length]);

		success = this->cm->receive_socket_buffer(socket, ssl, recvbuf.get(), res_buffer_length);

		this->cm->remove_socket_lock(socket);
		lock_removed = true;

		if (!success) return;

		RequestType request_type = (RequestType)((int)recvbuf.get()[0]);

		switch (request_type) {
		case RequestType::CREATE_QUEUE:
		{
			this->logger->log_info("Received and executing request type of CREATE_QUEUE");

			std::unique_ptr<CreateQueueRequest> request = this->mapper->to_create_queue_request(recvbuf.get(), res_buffer_length);

			if (
				this->settings->get_external_user_authentication_enabled()
				&& !this->is_user_authorized_for_action(
					request.get()->username,
					request.get()->username_length,
					request.get()->password,
					request.get()->password_length
				)
			) {
				this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::UNAUTHORIZED, "Insufficient user permissions to execute this action");
				return;
			}

			this->client_request_executor->handle_create_queue_request(socket, ssl, request.get());

			break;
		}
		case RequestType::DELETE_QUEUE:
		{
			this->logger->log_info("Received and executing request type of DELETE_QUEUE");

			std::unique_ptr<DeleteQueueRequest> request = this->mapper->to_delete_queue_request(recvbuf.get(), res_buffer_length);

			if (
				this->settings->get_external_user_authentication_enabled()
				&& !this->is_user_authorized_for_action(
					request.get()->username,
					request.get()->username_length,
					request.get()->password,
					request.get()->password_length
				)
				) {
				this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::UNAUTHORIZED, "Insufficient user permissions to execute this action");
				return;
			}

			this->client_request_executor->handle_delete_queue_request(socket, ssl, request.get());

			break;
		}
		case RequestType::GET_CONTROLLERS_CONNECTION_INFO:
		{
			this->logger->log_info("Received and executing request type of GET_CONTROLLERS_CONNECTION_INFO");

			this->client_request_executor->handle_get_controllers_connection_info_request(socket, ssl);

			break;
		}
		case RequestType::GET_CONTROLLER_LEADER_ID:
		{
			this->logger->log_info("Received and executing request type of GET_CONTROLLER_LEADER_ID");

			this->client_request_executor->handle_get_controller_leader_id_request(socket, ssl);

			break;
		}
		case RequestType::PRODUCE:
		{
			this->logger->log_info("Received and executing request type of PRODUCE");

			std::unique_ptr<ProduceMessagesRequest> request = this->mapper->to_produce_messages_request(recvbuf.get(), res_buffer_length);

			if (
				this->settings->get_external_user_authentication_enabled()
				&& !this->is_user_authorized_for_action(
					request.get()->username,
					request.get()->username_length,
					request.get()->password,
					request.get()->password_length
				)
				) {
				this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::UNAUTHORIZED, "Insufficient user permissions to execute this action");
				return;
			}

			this->client_request_executor->handle_produce_request(socket, ssl, request.get());

			break;
		}
		case RequestType::GET_QUEUE_PARTITIONS_INFO:
		{
			this->logger->log_info("Received and executing request type of GET_QUEUE_PARTITIONS_INFO");

			std::unique_ptr<GetQueuePartitionsInfoRequest> request = this->mapper->to_get_queue_partitions_info_request(recvbuf.get(), res_buffer_length);

			this->client_request_executor->handle_get_queue_partitions_info_request(socket, ssl, request.get());

			break;
		}
		case RequestType::APPEND_ENTRIES:
		{
			this->logger->log_info("Received and executing request type of APPEND_ENTRIES");

			std::unique_ptr<AppendEntriesRequest> request = this->mapper->to_append_entries_request(recvbuf.get(), res_buffer_length);

			this->internal_request_executor->handle_append_entries_request(socket, ssl, request.get());

			break;
		}
		case RequestType::REQUEST_VOTE:
		{
			this->logger->log_info("Received and executing request type of REQUEST_VOTE");

			std::unique_ptr<RequestVoteRequest> request = this->mapper->to_request_vote_request(recvbuf.get(), res_buffer_length);

			this->internal_request_executor->handle_request_vote_request(socket, ssl, request.get());

			break;
		}
		case RequestType::DATA_NODE_HEARTBEAT:
		{
			this->logger->log_info("Received and executing request type of DATA_NODE_HEARTBEAT");

			std::unique_ptr<DataNodeHeartbeatRequest> request = this->mapper->to_data_node_heartbeat_request(recvbuf.get(), res_buffer_length);

			this->internal_request_executor->handle_data_node_heartbeat_request(socket, ssl, request.get());

			break;
		}
		case RequestType::GET_CLUSTER_METADATA_UPDATES:
		{
			this->logger->log_info("Received and executing request type of GET_CLUSTER_METADATA_UPDATES");

			std::unique_ptr<GetClusterMetadataUpdateRequest> request = this->mapper->to_get_cluster_metadata_update_request(recvbuf.get(), res_buffer_length);

			this->internal_request_executor->handle_get_cluster_metadata_update_request(socket, ssl, request.get());

			break;
		}
		default:
			this->logger->log_error("Received invalid request type " + std::to_string((int)recvbuf.get()[0]));

			this->cm->respond_to_socket_with_error(
				socket,
				ssl,
				ErrorCode::INCORRECT_ACTION,
				"Received invalid request type " + std::to_string((int)recvbuf.get()[0])
			);

			return;
		}

		this->logger->log_info("Execution of request completed");
	}
	catch (const std::exception& ex)
	{
		this->logger->log_error("Error occured while trying to serve the request. Reason: " + std::string(ex.what()));
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INTERNAL_SERVER_ERROR, "Error occured while trying to serve the request");
	}

	if (!lock_removed) this->cm->remove_socket_lock(socket);
}