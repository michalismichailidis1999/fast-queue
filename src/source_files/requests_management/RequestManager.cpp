#include "../../header_files/requests_management/RequestManager.h"

RequestManager::RequestManager(ConnectionsManager* cm, Settings* settings, ClientRequestExecutor* client_request_executor, InternalRequestExecutor* internal_request_executor, RequestMapper* mapper, Logger* logger) {
	this->settings = settings;
	this->cm = cm;
	this->client_request_executor = client_request_executor;
	this->internal_request_executor = internal_request_executor;
	this->mapper = mapper;
	this->logger = logger;

	this->ping_res_buff_size = sizeof(unsigned int) + sizeof(bool);
	this->ping_res = std::unique_ptr<char>(new char[this->ping_res_buff_size]);

	bool success = true;

	memcpy_s(this->ping_res.get(), sizeof(int), &this->ping_res_buff_size, sizeof(int));
	memcpy_s(this->ping_res.get() + sizeof(int), sizeof(bool), &success, sizeof(bool));
}

bool RequestManager::is_user_authorized_for_action(AuthRequest* request) {
	return true;
}

bool RequestManager::is_invalid_external_request(RequestType req_type) {
	switch (req_type) {
	case RequestType::APPEND_ENTRIES: return true;
	case RequestType::REQUEST_VOTE: return true;
	case RequestType::DATA_NODE_HEARTBEAT: return true;
	case RequestType::GET_CLUSTER_METADATA_UPDATES: return true;
	case RequestType::EXPIRE_CONSUMERS: return true;
	case RequestType::FETCH_MESSAGES: return true;
	case RequestType::ADD_LAGGING_FOLLOWER: return true;
	case RequestType::REMOVE_LAGGING_FOLLOWER: return true;
	default:
		return false;
	}
}

bool RequestManager::is_invalid_internal_request(RequestType req_type) {
	return this->is_invalid_external_request(req_type);
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

		if (request_type != RequestType::NONE && !internal_communication && this->is_invalid_external_request(request_type)) {
			this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_ACTION, "Invalid request type");
			return;
		}

		if (request_type != RequestType::NONE && internal_communication && this->is_invalid_internal_request(request_type)) {
			this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_ACTION, "Invalid request type");
			return;
		}

		switch (request_type) {
		case RequestType::NONE: {
			this->cm->respond_to_socket(socket, ssl, this->ping_res.get(), this->ping_res_buff_size);
			break;
		}
		case RequestType::CREATE_QUEUE:
		{
			this->logger->log_info("Received and executing request type of CREATE_QUEUE");

			std::unique_ptr<CreateQueueRequest> request = this->mapper->to_create_queue_request(recvbuf.get(), res_buffer_length);

			if (
				this->settings->get_external_user_authentication_enabled()
				&& !this->is_user_authorized_for_action(request.get())
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
				&& !this->is_user_authorized_for_action(request.get())
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
				&& !this->is_user_authorized_for_action(request.get())
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
		case RequestType::REGISTER_CONSUMER:
		{
			this->logger->log_info("Received and executing request type of REGISTER_CONSUMER_REQUEST");

			std::unique_ptr<RegisterConsumerRequest> request = this->mapper->to_register_consumer_request(recvbuf.get(), res_buffer_length);

			if (
				this->settings->get_external_user_authentication_enabled()
				&& !this->is_user_authorized_for_action(request.get())
			) {
				this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::UNAUTHORIZED, "Insufficient user permissions to execute this action");
				return;
			}

			this->client_request_executor->handle_register_consumer_request(socket, ssl, request.get());

			break;
		}
		case RequestType::GET_CONSUMER_ASSIGNED_PARTITIONS:
		{
			this->logger->log_info("Received and executing request type of GET_CONSUMER_ASSIGNED_PARTITIONS");

			std::unique_ptr<GetConsumerAssignedPartitionsRequest> request = this->mapper->to_get_consumer_assigned_partitions_request(recvbuf.get(), res_buffer_length);

			if (
				this->settings->get_external_user_authentication_enabled()
				&& !this->is_user_authorized_for_action(request.get())
			) {
				this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::UNAUTHORIZED, "Insufficient user permissions to execute this action");
				return;
			}

			this->client_request_executor->handle_get_consumer_assigned_partitions_request(socket, ssl, request.get());

			break;
		}
		case RequestType::CONSUME:
		{
			this->logger->log_info("Received and executing request type of CONSUME");

			std::unique_ptr<ConsumeRequest> request = this->mapper->to_consume_request(recvbuf.get(), res_buffer_length);

			if (
				this->settings->get_external_user_authentication_enabled()
				&& !this->is_user_authorized_for_action(request.get())
			) {
				this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::UNAUTHORIZED, "Insufficient user permissions to execute this action");
				return;
			}

			this->client_request_executor->handle_consume_request(socket, ssl, request.get());

			break;
		}
		case RequestType::ACK:
		{
			this->logger->log_info("Received and executing request type of ACK");

			std::unique_ptr<AckMessageOffsetRequest> request = this->mapper->to_ack_message_offset_request(recvbuf.get(), res_buffer_length);

			if (
				this->settings->get_external_user_authentication_enabled()
				&& !this->is_user_authorized_for_action(request.get())
				) {
				this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::UNAUTHORIZED, "Insufficient user permissions to execute this action");
				return;
			}

			this->client_request_executor->handle_ack_message_offset_request(socket, ssl, request.get());

			break;
		}
		case RequestType::EXPIRE_CONSUMERS:
		{
			this->logger->log_info("Received and executing request type of EXPIRE_CONSUMERS");

			std::unique_ptr<ExpireConsumersRequest> request = this->mapper->to_expire_consumers_request(recvbuf.get(), res_buffer_length);

			this->internal_request_executor->handle_expire_consumers_request(socket, ssl, request.get());

			break;
		}
		case RequestType::FETCH_MESSAGES:
		{
			this->logger->log_info("Received and executing request type of FETCH_MESSAGES");

			std::unique_ptr<FetchMessagesRequest> request = this->mapper->to_fetch_messages_request(recvbuf.get(), res_buffer_length);

			this->internal_request_executor->handle_fetch_messages_request(socket, ssl, request.get());

			break;
		}
		case RequestType::ADD_LAGGING_FOLLOWER:
		{
			this->logger->log_info("Received and executing request type of ADD_LAGGING_FOLLOWER");

			std::unique_ptr<AddLaggingFollowerRequest> request = this->mapper->to_add_lagging_request(recvbuf.get(), res_buffer_length);

			this->internal_request_executor->handle_add_lagging_follower_request(socket, ssl, request.get());

			break;
		}
		case RequestType::REMOVE_LAGGING_FOLLOWER:
		{
			this->logger->log_info("Received and executing request type of REMOVE_LAGGING_FOLLOWER");

			std::unique_ptr<RemoveLaggingFollowerRequest> request = this->mapper->to_remove_lagging_request(recvbuf.get(), res_buffer_length);

			this->internal_request_executor->handle_remove_lagging_follower_request(socket, ssl, request.get());

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