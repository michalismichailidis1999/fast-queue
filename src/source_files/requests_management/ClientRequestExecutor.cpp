#include "../../header_files/requests_management/ClientRequestExecutor.h"

ClientRequestExecutor::ClientRequestExecutor(MessagesHandler* mh, ConnectionsManager* cm, QueueManager* qm, Controller* controller, ClassToByteTransformer* transformer, FileHandler* fh, Util* util, Settings* settings, Logger* logger) {
	this->mh = mh;
	this->cm = cm;
	this->qm = qm;
	this->controller = controller;
	this->fh = fh;
	this->util = util;
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

	std::string queue_name = std::string(request->queue_name, request->queue_name_length);

	std::shared_ptr<QueueMetadata> queue_metadata = std::shared_ptr<QueueMetadata>(
		new QueueMetadata(queue_name, request->partitions, request->replication_factor)
	);

	queue_metadata.get()->set_status(Status::PENDING_CREATION);

	ErrorCode err = this->controller->assign_new_queue_partitions_to_nodes(queue_metadata);

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
		default:
			break;
		}

	std::unique_ptr<CreateQueueResponse> res = std::make_unique<CreateQueueResponse>();
	res.get()->ok = true;

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(res.get());

	this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));
}

void ClientRequestExecutor::handle_delete_queue_request(SOCKET_ID socket, SSL* ssl, DeleteQueueRequest* request) {
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
	}

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(res.get());

	this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));
}

void ClientRequestExecutor::handle_produce_request(SOCKET_ID socket, SSL* ssl, ProduceMessagesRequest* request) {
	std::string queue_name = std::string(request->queue_name, request->queue_name_length);

	std::shared_ptr<Queue> queue = this->qm->get_queue(queue_name);

	if (Helper::is_internal_queue(queue_name)) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_ACTION, "Cannot produce messages to internal queue");
		return;
	}

	if (queue == nullptr) {
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

	Partition* partition = queue.get()->get_partition(request->partition);

	if (partition == NULL) {
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

	this->mh->save_messages(partition, request);

	this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));
}

void ClientRequestExecutor::handle_get_queue_partitions_info_request(SOCKET_ID socket, SSL* ssl, GetQueuePartitionsInfoRequest* request) {
	std::string queue_name = std::string(request->queue_name, request->queue_name_length);

	std::shared_ptr<Queue> queue = this->qm->get_queue(queue_name);

	if (Helper::is_internal_queue(queue_name)) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_ACTION, "Cannot get internal queue's partitions info");
		return;
	}

	if (queue == nullptr) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::QUEUE_DOES_NOT_EXIST, "Queue not found");
		return;
	}

	std::unique_ptr<GetQueuePartitionsInfoResponse> res = std::make_unique<GetQueuePartitionsInfoResponse>();
	res.get()->total_partitions = queue.get()->get_metadata()->get_partitions();

	ClusterMetadata* cluster_metadata = this->controller->get_cluster_metadata();

	std::mutex* partitions_mut = cluster_metadata->get_partitions_mut();

	std::unique_lock<std::mutex> lock(*partitions_mut);

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

	std::string queue_name = std::string(request->queue_name, request->queue_name_length);

	std::shared_ptr<Queue> queue = this->qm->get_queue(queue_name);

	if (Helper::is_internal_queue(queue_name)) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::INCORRECT_ACTION, "Cannot register consumer for internal queue");
		return;
	}

	if (queue == nullptr) {
		this->cm->respond_to_socket_with_error(socket, ssl, ErrorCode::QUEUE_DOES_NOT_EXIST, "Queue not found");
		return;
	}

	std::unique_ptr<RegisterConsumerResponse> res = std::make_unique<RegisterConsumerResponse>();
	res.get()->ok = true;
	res.get()->consumer_id = this->controller->assign_consumer_group_to_partitions(request, queue.get());

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(res.get());

	this->cm->respond_to_socket(socket, ssl, std::get<1>(buf_tup).get(), std::get<0>(buf_tup));
}