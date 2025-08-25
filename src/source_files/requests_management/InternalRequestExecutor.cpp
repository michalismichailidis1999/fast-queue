#include "../../header_files/requests_management/InternalRequestExecutor.h"


InternalRequestExecutor::InternalRequestExecutor(Settings* settings, Logger* logger, ConnectionsManager* cm, FileHandler* fh, Controller* controller, DataNode* data_node, QueueManager* qm, MessagesHandler* mh, ClassToByteTransformer* transformer) {
	this->settings = settings;
	this->logger = logger;
	this->cm = cm;
	this->fh = fh;
	this->controller = controller;
	this->data_node = data_node;
	this->qm = qm;
	this->mh = mh;
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
	info.get()->external_port = request->external_port;

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

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(res.get(), true);

	this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));
}

void InternalRequestExecutor::handle_expire_consumers_request(SOCKET_ID socket, SSL* ssl, ExpireConsumersRequest* request) {
	if (!this->settings->get_is_controller_node()) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_ACTION, "Consumer expiration can only be handled by a controller node");
		return;
	}

	if (this->controller->get_leader_id() == 0) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::UNASSIGNED_LEADERSHIP, "No controller node leader elected yet");
		return;
	}

	std::unique_ptr<ExpireConsumersResponse> res = std::make_unique<ExpireConsumersResponse>();
	res.get()->leader_id = this->controller->get_leader_id();
	res.get()->ok = res.get()->leader_id == this->settings->get_node_id();

	if (res.get()->ok) this->controller->handle_consumers_expiration(request);

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(res.get());

	this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));
}

void InternalRequestExecutor::handle_fetch_messages_request(SOCKET_ID socket, SSL* ssl, FetchMessagesRequest* request) {
	if (request->queue_name_length == 0) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_REQUEST_BODY, "Queue name is required");
		return;
	}

	std::string queue_name = std::string(request->queue_name, request->queue_name_length);

	if (Helper::is_internal_queue(queue_name)) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_ACTION, "Cannot fetch messages from internal queue");
		return;
	}

	std::shared_ptr<Queue> queue = this->qm->get_queue(queue_name);

	if (queue == nullptr || queue.get()->get_metadata()->get_status() != Status::ACTIVE) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::QUEUE_DOES_NOT_EXIST, "Queue " + queue_name + " not found");
		return;
	}

	if ((request->partition > 0 && queue.get()->get_metadata()->get_partitions() - 1 < request->partition) || request->partition < 0) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_REQUEST_BODY, "Incorrect partition number " + std::to_string(request->partition));
		return;
	}

	std::shared_ptr<Partition> partition = queue.get()->get_partition(request->partition);

	if (partition == nullptr
		|| this->controller->get_partition_leader(queue_name, request->partition) != this->settings->get_node_id()) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_LEADER, "Node is not partition's " + std::to_string(request->partition) + " leader");
		return;
	}

	if (!this->controller->is_node_partition_owner(queue_name, request->partition, request->node_id)) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_ACTION, "Only partition followers can fetch data from partition");
		return;
	}

	this->data_node->update_follower_heartbeat(queue_name, request->partition, request->node_id);

	bool is_lagging_follower = this->controller->get_cluster_metadata()->check_if_follower_is_lagging(
		queue_name, request->partition, request->node_id
	);

	std::unique_ptr<FetchMessagesResponse> res = std::make_unique<FetchMessagesResponse>();
	res.get()->total_messages = 0;
	res.get()->messages_total_bytes = 0;
	res.get()->messages_data = NULL;
	res.get()->last_message_offset = partition->get_message_offset();
	res.get()->commited_offset = partition->get_last_replicated_offset();
	res.get()->prev_message_offset = 0;
	res.get()->prev_message_leader_epoch = 0;

	unsigned int message_bytes = 0;
	unsigned long long last_message_offset = 0;

	if (request->message_offset < partition->get_message_offset()) {
		auto messages_res = this->mh->read_partition_messages(
			partition.get(),
			request->message_offset,
			0,
			false,
			true
		);

		res.get()->total_messages = std::get<4>(messages_res);
		res.get()->messages_total_bytes = std::get<3>(messages_res) - std::get<2>(messages_res);
		res.get()->messages_data = std::get<0>(messages_res).get() + std::get<2>(messages_res);

		unsigned int offset = 0;

		for (int i = 0; i < res.get()->total_messages; i++) {
			memcpy_s(&message_bytes, TOTAL_METADATA_BYTES, (char*)res.get()->messages_data + offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);
			memcpy_s(&last_message_offset, MESSAGE_ID_SIZE, (char*)res.get()->messages_data + offset + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);

			offset += message_bytes;
		}

		auto messages_res_2 = this->mh->read_partition_messages(
			partition.get(),
			request->message_offset - 1,
			1,
			true
		);

		if (std::get<4>(messages_res_2) == 1) {
			void* message_offset = std::get<0>(messages_res_2).get() + std::get<2>(messages_res_2);

			memcpy_s(&res.get()->prev_message_offset, MESSAGE_ID_SIZE, (char*)message_offset + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);
			memcpy_s(&res.get()->prev_message_leader_epoch, MESSAGE_LEADER_ID_SIZE, (char*)message_offset + MESSAGE_LEADER_ID_OFFSET, MESSAGE_LEADER_ID_SIZE);
		}
	}
	else if (request->message_offset == partition->get_message_offset() + 1) last_message_offset = partition->get_message_offset();

	// TODO: Store last_message_offset as last replicated message offset and check the most commited offset to update partition last replicated offset
	
	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(res.get());

	this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));

	if (last_message_offset != partition->get_message_offset() || !is_lagging_follower) return;

	std::unique_ptr<RemoveLaggingFollowerRequest> req = std::make_unique<RemoveLaggingFollowerRequest>();
	req.get()->queue_name = request->queue_name;
	req.get()->queue_name_length = request->queue_name_length;
	req.get()->partition = request->partition;
	req.get()->node_id = request->node_id;

	if (this->settings->get_node_id() == this->controller->get_leader_id()) {
		this->controller->remove_lagging_follower(req.get());
		return;
	}

	std::shared_ptr<ConnectionPool> pool = this->cm->get_controller_node_connection(this->controller->get_leader_id());

	if (pool == nullptr) return;

	buf_tup = this->transformer->transform(req.get());

	this->cm->send_request_to_socket(
		pool.get(),
		3,
		std::get<1>(buf_tup).get(),
		std::get<0>(buf_tup),
		"RemoveLaggingFollower"
	);
}

void InternalRequestExecutor::handle_add_lagging_follower_request(SOCKET_ID socket, SSL* ssl, AddLaggingFollowerRequest* request) {
	if (!this->settings->get_is_controller_node()) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_ACTION, "Lag follower addition can only be handled by a controller node");
		return;
	}

	std::unique_ptr<AddLaggingFollowerResponse> res = std::make_unique<AddLaggingFollowerResponse>();
	res.get()->leader_id = this->controller->get_leader_id();
	res.get()->ok = res.get()->leader_id == this->settings->get_node_id();

	if (res.get()->ok) this->controller->add_lagging_follower(request);

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(res.get());

	this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));
}

void InternalRequestExecutor::handle_remove_lagging_follower_request(SOCKET_ID socket, SSL* ssl, RemoveLaggingFollowerRequest* request) {
	if (!this->settings->get_is_controller_node()) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_ACTION, "Lag follower removal can only be handled by a controller node");
		return;
	}

	std::unique_ptr<RemoveLaggingFollowerResponse> res = std::make_unique<RemoveLaggingFollowerResponse>();
	res.get()->leader_id = this->controller->get_leader_id();
	res.get()->ok = res.get()->leader_id == this->settings->get_node_id();

	if (res.get()->ok) this->controller->remove_lagging_follower(request);

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(res.get());

	this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));
}