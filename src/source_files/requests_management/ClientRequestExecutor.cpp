#include "../../header_files/requests_management/ClientRequestExecutor.h"

ClientRequestExecutor::ClientRequestExecutor(MessagesHandler* mh, MessageOffsetAckHandler* oah, ConnectionsManager* cm, QueueManager* qm, Controller* controller, DataNode* data_node, ClassToByteTransformer* transformer, Settings* settings, Logger* logger) {
	this->mh = mh;
	this->oah = oah;
	this->cm = cm;
	this->qm = qm;
	this->controller = controller;
	this->data_node = data_node;
	this->settings = settings;
	this->transformer = transformer;
	this->logger = logger;
}

void ClientRequestExecutor::handle_get_controllers_connection_info_request(SOCKET_ID socket, SSL* ssl) {
	std::unique_ptr<GetControllerConnectionInfoResponse> res = std::make_unique<GetControllerConnectionInfoResponse>();

	std::vector<std::shared_ptr<ConnectionInfo>> infos;

	for (auto& controller_info : this->settings->get_controller_nodes()) {
		auto info = std::get<1>(controller_info);
		res->connection_infos.emplace_back(std::get<0>(controller_info), info.get());
		infos.emplace_back(info);
	}

	res->leader_id = this->controller->get_cluster_metadata()->get_leader_id();

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(res.get());

	this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));
}

void ClientRequestExecutor::handle_get_controller_leader_id_request(SOCKET_ID socket, SSL* ssl) {
	std::unique_ptr<GetLeaderIdResponse> res = std::make_unique<GetLeaderIdResponse>();

	res->leader_id = this->controller->get_cluster_metadata()->get_leader_id();

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(res.get());

	this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));
}

void ClientRequestExecutor::handle_create_queue_request(SOCKET_ID socket, SSL* ssl, CreateQueueRequest* request) {
	if (!this->settings->get_is_controller_node()) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_ACTION, "Request to create queue must only be sent to controller nodes");
		return;
	}

	if (this->controller->get_cluster_metadata()->get_leader_id() != this->settings->get_node_id()) {
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

	if (request->queue_name_length == 0)
	{
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_REQUEST_BODY, "Queue name is required");
		return;
	}

	std::string queue_name = std::string(request->queue_name, request->queue_name_length);

	std::shared_ptr<QueueMetadata> queue_metadata = std::shared_ptr<QueueMetadata>(
		new QueueMetadata(queue_name, request->partitions, request->replication_factor)
	);

	queue_metadata.get()->set_status(Status::PENDING_CREATION);

	ErrorCode err = this->controller->assign_new_queue_partitions_to_nodes(queue_metadata);

	bool queue_created = true;

	if (err != ErrorCode::NONE)
		switch (err)
		{
		case ErrorCode::TOO_FEW_AVAILABLE_NODES:
			this->cm->respond_to_socket_with_error(
				socket,
				ssl,
				ErrorCode::TOO_FEW_AVAILABLE_NODES,
				"There are not enough active nodes to handle replication factor of " + std::to_string(request->replication_factor)
			);
			return;
		case ErrorCode::QUEUE_ALREADY_EXISTS:
			queue_created = false;
			break;
		default:
			break;
		}

	std::unique_ptr<CreateQueueResponse> res = std::make_unique<CreateQueueResponse>();
	res.get()->ok = true;
	res.get()->created = queue_created;

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(res.get());

	this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));
}

void ClientRequestExecutor::handle_delete_queue_request(SOCKET_ID socket, SSL* ssl, DeleteQueueRequest* request) {
	if (request->queue_name_length == 0)
	{
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_REQUEST_BODY, "Queue name is required");
		return;
	}

	std::string queue_name = std::string(request->queue_name, request->queue_name_length);

	if (Helper::is_internal_queue(queue_name)) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_ACTION, "Cannot delete queue. This name is assigned to an internal queue");
		return;
	}

	std::shared_ptr<Queue> queue = this->qm->get_queue(queue_name);

	std::unique_ptr<DeleteQueueResponse> res = std::make_unique<DeleteQueueResponse>();
	res->ok = true;

	if (queue != nullptr && queue.get()->get_metadata()->get_status() != Status::PENDING_DELETION) {
		this->controller->assign_queue_for_deletion(queue_name);
		queue.get()->get_metadata()->set_status(Status::PENDING_DELETION);
		res.get()->deleted = true;
	}
	else res.get()->deleted = false;

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(res.get());

	this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));
}

void ClientRequestExecutor::handle_produce_request(SOCKET_ID socket, SSL* ssl, ProduceMessagesRequest* request) {
	if (request->queue_name_length == 0)
	{
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_REQUEST_BODY, "Queue name is required");
		return;
	}

	std::string queue_name = std::string(request->queue_name, request->queue_name_length);

	std::shared_ptr<Queue> queue = this->qm->get_queue(queue_name);

	if (Helper::is_internal_queue(queue_name)) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_ACTION, "Cannot produce messages to internal queue");
		return;
	}

	if (queue == nullptr || queue.get()->get_metadata()->get_status() != Status::ACTIVE) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::QUEUE_DOES_NOT_EXIST, "Queue not found");
		return;
	}

	if (request->partition < 0) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_PARTITION_NUMBER, "Partition cannot be less than 0");
		return;
	}

	if (queue.get()->get_metadata()->get_partitions() - 1 < request->partition) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_PARTITION_NUMBER, "Queue has only " + std::to_string(queue.get()->get_metadata()->get_partitions()) + " partitions");
		return;
	}

	std::shared_ptr<Partition> partition = queue.get()->get_partition(request->partition);

	if (partition == nullptr) {
		this->cm->respond_to_socket_with_error(
			socket,
			ssl,
			ErrorCode::INCORRECT_LEADER,
			"Partition is not assigned to node"
		);

		return;
	}

	int partition_leader = this->controller->get_partition_leader(queue_name, request->partition);

	if (partition_leader == -1) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_LEADER, "Not assigned partition leader found");
		return;
	}

	if (partition_leader != this->settings->get_node_id()) {
		this->cm->respond_to_socket_with_error(
			socket, 
			ssl, 
			ErrorCode::INCORRECT_LEADER, 
			"Node is not partition's leader"
		);

		return;
	}

	std::unique_ptr<ProduceMessagesResponse> res = std::make_unique<ProduceMessagesResponse>();
	res.get()->ok = true;

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(res.get());

	if (request->messages.get()->size() == 0) {
		this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));
		return;
	}

	unsigned long long partition_leader_id = this->controller->get_queue_partition_unique_leader_id(queue_name, request->partition);

	if (partition_leader_id == 0) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::UNASSIGNED_LEADERSHIP, "Leader unique id has not been assigned yet");
		return;
	}

	this->mh->save_messages(
		partition.get(), 
		request, 
		true, 
		queue.get()->get_metadata()->get_replication_factor() > 1,
		partition_leader_id
	);

	std::string leader_key = queue_name + "_" + std::to_string(request->partition) + "_" + std::to_string(this->settings->get_node_id());

	this->controller->add_replicated_message_offset(leader_key, this->settings->get_node_id(), partition.get()->get_message_offset());

	this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));
}

void ClientRequestExecutor::handle_get_queue_partitions_info_request(SOCKET_ID socket, SSL* ssl, GetQueuePartitionsInfoRequest* request) {
	if (request->queue_name_length == 0)
	{
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_REQUEST_BODY, "Queue name is required");
		return;
	}

	std::string queue_name = std::string(request->queue_name, request->queue_name_length);

	std::shared_ptr<Queue> queue = this->qm->get_queue(queue_name);

	if (Helper::is_internal_queue(queue_name)) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_ACTION, "Cannot get internal queue's partitions info");
		return;
	}

	if (queue == nullptr || queue.get()->get_metadata()->get_status() != Status::ACTIVE) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::QUEUE_DOES_NOT_EXIST, "Queue not found");
		return;
	}

	std::unique_ptr<GetQueuePartitionsInfoResponse> res = std::make_unique<GetQueuePartitionsInfoResponse>();
	res.get()->total_partitions = queue.get()->get_metadata()->get_partitions();

	ClusterMetadata* cluster_metadata = this->controller->get_cluster_metadata();

	std::shared_mutex* partitions_mut = cluster_metadata->get_partitions_mut();

	std::shared_lock<std::shared_mutex> lock(*partitions_mut);

	auto queue_partitions_leaders = cluster_metadata->get_queue_partition_leaders(queue_name);

	if (queue_partitions_leaders == nullptr) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::UNASSIGNED_LEADERSHIP, "Queue does not have assigned leaders yet for its partitions");
		return;
	}

	std::unique_ptr<ConnectionInfo> node_conn_info = std::make_unique<ConnectionInfo>();
	node_conn_info.get()->external_address = this->settings->get_external_ip();
	node_conn_info.get()->external_port = this->settings->get_external_port();

	std::vector<std::shared_ptr<ConnectionPool>> pools_ref;

	for (auto& iter : *(queue_partitions_leaders.get())) {
		if (iter.second == this->settings->get_node_id()) {
			res.get()->connection_infos.emplace_back(iter.first, iter.second, node_conn_info.get());
			continue;
		}

		std::shared_ptr<ConnectionPool> pool = this->cm->get_node_connection_pool(iter.second);

		if (pool != nullptr) {
			res.get()->connection_infos.emplace_back(iter.first, iter.second, pool.get()->get_connection_info());
			pools_ref.emplace_back(pool);
		}
	}
	
	lock.unlock();

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(res.get());

	this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));
}

void ClientRequestExecutor::handle_register_consumer_request(SOCKET_ID socket, SSL* ssl, RegisterConsumerRequest* request) {
	if (!this->settings->get_is_controller_node()) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_ACTION, "Request to register consumer must only be sent to controller nodes");
		return;
	}

	if (this->controller->get_cluster_metadata()->get_leader_id() != this->settings->get_node_id()) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_LEADER, "Non leader node. Cannot register consumer.");
		return;
	}

	if (request->queue_name_length == 0)
	{
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_REQUEST_BODY, "Queue name is required");
		return;
	}

	if (request->consumer_group_id_length == 0)
	{
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_REQUEST_BODY, "Group id is required");
		return;
	}

	if (request->consumer_group_id_length > MAX_CONSUMER_GROUP_ID_CHARS)
	{
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_REQUEST_BODY, "Group id cannot be larger than " + std::to_string(MAX_CONSUMER_GROUP_ID_CHARS) + " characters");
		return;
	}

	std::string queue_name = std::string(request->queue_name, request->queue_name_length);

	std::shared_ptr<Queue> queue = this->qm->get_queue(queue_name);

	if (Helper::is_internal_queue(queue_name)) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_ACTION, "Cannot register consumer for internal queue");
		return;
	}

	if (queue == nullptr || queue.get()->get_metadata()->get_status() != Status::ACTIVE) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::QUEUE_DOES_NOT_EXIST, "Queue not found");
		return;
	}

	std::string group_id = std::string(request->consumer_group_id, request->consumer_group_id_length);

	std::unique_ptr<RegisterConsumerResponse> res = std::make_unique<RegisterConsumerResponse>();
	res.get()->consumer_id = this->controller->assign_consumer_group_to_partitions(request, queue.get(), group_id);
	res.get()->ok = res.get()->consumer_id != 0;

	if(res.get()->ok)
		this->data_node->update_consumer_heartbeat(queue_name, group_id, res.get()->consumer_id);

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(res.get());

	this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));
}

void ClientRequestExecutor::handle_get_consumer_assigned_partitions_request(SOCKET_ID socket, SSL* ssl, GetConsumerAssignedPartitionsRequest* request) {
	if (!this->settings->get_is_controller_node()) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_ACTION, "Request to get consumer assigned partitions must only be sent to controller nodes");
		return;
	}
	
	if (request->queue_name_length == 0)
	{
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_REQUEST_BODY, "Queue name is required");
		return;
	}

	if (request->consumer_group_id_length == 0)
	{
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_REQUEST_BODY, "Group id is required");
		return;
	}

	std::string queue_name = std::string(request->queue_name, request->queue_name_length);

	std::shared_ptr<Queue> queue = this->qm->get_queue(queue_name);

	if (Helper::is_internal_queue(queue_name)) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_ACTION, "Action is not allowed for internal queue");
		return;
	}

	if (queue == nullptr || queue.get()->get_metadata()->get_status() != Status::ACTIVE) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::QUEUE_DOES_NOT_EXIST, "Queue not found");
		return;
	}

	std::string group_id = std::string(request->consumer_group_id, request->consumer_group_id_length);

	std::unique_ptr<GetConsumerAssignedPartitionsResponse> res = std::make_unique<GetConsumerAssignedPartitionsResponse>();

	this->controller->find_consumer_assigned_partitions(
		queue_name,
		group_id,
		request->consumer_id,
		&res.get()->partitions
	);

	if (res.get()->partitions.size() > 0)
		this->data_node->update_consumer_heartbeat(queue_name, group_id, request->consumer_id);

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(res.get());

	this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));
}

void ClientRequestExecutor::handle_consume_request(SOCKET_ID socket, SSL* ssl, ConsumeRequest* request) {
	if (request->queue_name_length == 0)
	{
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_REQUEST_BODY, "Queue name is required");
		return;
	}

	if (request->consumer_group_id_length == 0)
	{
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_REQUEST_BODY, "Group id is required");
		return;
	}

	std::string queue_name = std::string(request->queue_name, request->queue_name_length);

	std::shared_ptr<Queue> queue = this->qm->get_queue(queue_name);

	if (Helper::is_internal_queue(queue_name)) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_ACTION, "Cannot consume from internal queue");
		return;
	}

	if (queue == nullptr || queue.get()->get_metadata()->get_status() != Status::ACTIVE) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::QUEUE_DOES_NOT_EXIST, "Queue not found");
		return;
	}

	if (request->partition < 0) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_PARTITION_NUMBER, "Partition cannot be less than 0");
		return;
	}

	if (queue.get()->get_metadata()->get_partitions() - 1 < request->partition) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_PARTITION_NUMBER, "Queue has only " + std::to_string(queue.get()->get_metadata()->get_partitions()) + " partitions");
		return;
	}

	if (this->controller->get_partition_leader(queue_name, request->partition) != this->settings->get_node_id()) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_LEADER, "Node is not partition's " + std::to_string(request->partition) + " leader");
		return;
	}

	std::string group_id = std::string(request->consumer_group_id, request->consumer_group_id_length);

	std::shared_ptr<Partition> partition = queue.get()->get_partition(request->partition);

	if (partition == nullptr) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_LEADER, "Node is not partition's " + std::to_string(request->partition) + " leader");
		return;
	}

	std::shared_ptr<Consumer> consumer = partition.get()->get_consumer(request->consumer_id);

	bool consumer_expired = this->data_node->has_consumer_expired(request->consumer_id);

	if (consumer != nullptr && consumer_expired) {
		partition->remove_consumer(request->consumer_id);
		consumer = nullptr;
	}

	if (consumer == nullptr) {
		ErrorCode err_code = this->controller->get_last_registered_consumer_id() >= request->consumer_id
			? ErrorCode::CONSUMER_UNREGISTERED
			: ErrorCode::CONSUMER_NOT_FOUND;

		this->cm->respond_to_socket_with_error(socket, ssl, err_code, "Consumer not found");
		return;
	}

	if (consumer.get()->get_group_id() != group_id) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_CONSUMER_GROUP_ID, "Incorrect consumer group id " + group_id);
		return;
	}

	this->data_node->update_consumer_heartbeat(queue_name, group_id, request->consumer_id);

	std::unique_ptr<ConsumeResponse> res = std::make_unique<ConsumeResponse>();

	auto messages_res = this->mh->read_partition_messages(
		partition.get(), 
		request->message_offset == 0 ? consumer->get_offset() + 1 : request->message_offset, 
		request->read_single_offset_only ? 1 : 0,
		false, 
		true, 
		partition->get_last_replicated_offset()
	);
	
	res.get()->total_messages = std::get<4>(messages_res);
	res.get()->messages_total_bytes = std::get<3>(messages_res) - std::get<2>(messages_res);
	res.get()->messages_data = std::get<0>(messages_res).get() + std::get<2>(messages_res);

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(res.get());

	this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));
}

void ClientRequestExecutor::handle_ack_message_offset_request(SOCKET_ID socket, SSL* ssl, AckMessageOffsetRequest* request) {
	if (request->queue_name_length == 0)
	{
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_REQUEST_BODY, "Queue name is required");
		return;
	}

	if (request->consumer_group_id_length == 0)
	{
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_REQUEST_BODY, "Group id is required");
		return;
	}

	std::string queue_name = std::string(request->queue_name, request->queue_name_length);

	std::shared_ptr<Queue> queue = this->qm->get_queue(queue_name);

	if (Helper::is_internal_queue(queue_name)) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_ACTION, "Cannot consume from internal queue");
		return;
	}

	if (queue == nullptr || queue.get()->get_metadata()->get_status() != Status::ACTIVE) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::QUEUE_DOES_NOT_EXIST, "Queue not found");
		return;
	}

	if (request->partition < 0) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_PARTITION_NUMBER, "Partition cannot be less than 0");
		return;
	}

	if (queue.get()->get_metadata()->get_partitions() - 1 < request->partition) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_PARTITION_NUMBER, "Queue has only " + std::to_string(queue.get()->get_metadata()->get_partitions()) + " partitions");
		return;
	}

	if (this->controller->get_partition_leader(queue_name, request->partition) != this->settings->get_node_id()) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_LEADER, "Node is not partition's " + std::to_string(request->partition) + " leader");
		return;
	}

	std::string group_id = std::string(request->consumer_group_id, request->consumer_group_id_length);

	std::shared_ptr<Partition> partition = queue.get()->get_partition(request->partition);

	if (partition == nullptr) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_LEADER, "Node is not partition's " + std::to_string(request->partition) + " leader");
		return;
	}

	std::shared_ptr<Consumer> consumer = partition.get()->get_consumer(request->consumer_id);

	bool consumer_expired = this->data_node->has_consumer_expired(request->consumer_id);

	if (consumer != nullptr && consumer_expired) {
		partition->remove_consumer(request->consumer_id);
		consumer = nullptr;
	}

	if (consumer == nullptr) {
		ErrorCode err_code = this->controller->get_last_registered_consumer_id() >= request->consumer_id
			? ErrorCode::CONSUMER_UNREGISTERED
			: ErrorCode::CONSUMER_NOT_FOUND;

		this->cm->respond_to_socket_with_error(socket, ssl, err_code, "Consumer not found");
		return;
	}

	if (consumer.get()->get_group_id() != group_id) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_CONSUMER_GROUP_ID, "Incorrect consumer group id " + group_id);
		return;
	}

	this->data_node->update_consumer_heartbeat(queue_name, group_id, request->consumer_id);

	this->oah->flush_consumer_offset_ack(partition.get(), consumer.get(), request->message_offset);

	std::unique_ptr<AckMessageOffsetResponse> res = std::make_unique<AckMessageOffsetResponse>();
	res.get()->ok = true;

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(res.get());

	this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));
}

void ClientRequestExecutor::handle_register_transaction_group_request(SOCKET_ID socket, SSL* ssl, RegisterTransactionGroupRequest* request) {
	if (!this->settings->get_is_controller_node()) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_ACTION, "Request to register transaction group must only be sent to controller nodes");
		return;
	}

	if (this->controller->get_cluster_metadata()->get_leader_id() != this->settings->get_node_id()) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_LEADER, "Non leader node. Cannot register transaction group.");
		return;
	}

	if (request->registered_queues->size() == 0) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_REQUEST_BODY, "At least one queue name is required to register a transaction group");
		return;
	}

	request->registered_queue_names = std::make_shared<std::vector<std::string>>();

	for (int i = 0; i < request->registered_queues->size(); i++) {
		int queue_name_size = (*(request->registered_queues_lengths.get()))[i];
		char* queue_name = (*(request->registered_queues.get()))[i];

		if (queue_name_size == 0) {
			this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_REQUEST_BODY, "Qeueue name cannot be size of 0 bytes");
			return;
		}

		request->registered_queue_names.get()->emplace_back(std::string(queue_name, queue_name_size));
	}

	auto res = this->controller->register_transaction_group(request);

	if (res == nullptr) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INTERNAL_SERVER_ERROR, "Something went wrong while trying to register transaction group");
		return;
	}

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(res.get());

	this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));
}