#include "../../header_files/cluster_management/DataNode.h"

DataNode::DataNode(Controller* controller, ConnectionsManager* cm, ResponseMapper* response_mapper, ClassToByteTransformer* transformer, Settings* settings, Logger* logger) {
	this->controller = controller;
	this->cm = cm;
	this->response_mapper = response_mapper;
	this->transformer = transformer;
	this->settings = settings;
	this->logger = logger;
}

bool DataNode::is_controller_node() {
	return this->controller == NULL;
}

void DataNode::notify_controllers_about_node_existance(std::atomic_bool* should_terminate) {
	// if its a controller node, controllers already have connected to this node
	if (this->settings->get_is_controller_node()) return;

	std::lock_guard<std::mutex> lock(*this->cm->get_controller_node_connections_mut());

	auto controller_node_connections = this->cm->get_controller_node_connections(false);

	std::queue<std::pair<int, ConnectionPool*>> failed;

	std::unique_ptr<DataNodeConnectionRequest> req = std::make_unique<DataNodeConnectionRequest>();
	req.get()->node_id = this->settings->get_node_id();
	req.get()->address = this->settings->get_internal_ip().c_str();
	req.get()->address_length = this->settings->get_internal_ip().size();
	req.get()->port = this->settings->get_internal_port();

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(req.get());

	std::shared_ptr<char> req_buf = std::get<1>(buf_tup);
	long req_buf_size = std::get<0>(buf_tup);

	for (auto iter : *controller_node_connections)
		this->notify_controller_about_node_existance(
			iter.first, 
			req_buf.get(), 
			req_buf_size, 
			iter.second.get(), 
			&failed
		);

	while (!failed.empty() && !(*should_terminate)) {
		auto pair = failed.front();
		failed.pop();

		this->notify_controller_about_node_existance(
			pair.first,
			req_buf.get(),
			req_buf_size,
			pair.second,
			&failed
		);

		std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}
}

void DataNode::notify_controller_about_node_existance(int node_id, char* req_buf, long req_buf_size, ConnectionPool* pool, std::queue<std::pair<int, ConnectionPool*>>* failed) {
	std::tuple<std::shared_ptr<char>, long, bool> res_tup = this->cm->send_request_to_socket(
		pool,
		3,
		req_buf,
		req_buf_size,
		"DataNodeConnection"
	);

	if (std::get<1>(res_tup) == -1) {
		this->logger->log_error("Network issue occured while communicating with node " + std::to_string(node_id));
		if (failed != NULL) failed->emplace(node_id, pool);
		return;
	}

	std::unique_ptr<DataNodeConnectionResponse> res = this->response_mapper->to_data_node_connection_response(std::get<0>(res_tup).get(), std::get<1>(res_tup));

	if (res.get() == NULL) {
		if (failed != NULL) failed->emplace(node_id, pool);
		this->logger->log_error("Invalid mapping value in DataNodeConnectionResponse type");
		return;
	}

	if (!res.get()->ok) {
		if (failed != NULL) failed->emplace(node_id, pool);
		this->logger->log_info("Controller node " + std::to_string(node_id) + " could not connect to this data node");
	}
	else {
		this->controller->get_cluster_metadata()->set_leader_id(node_id);
		this->logger->log_info("Controller node " + std::to_string(node_id) + " has connected to this data node");
	}
}

void DataNode::send_heartbeats_to_leader(std::atomic_bool* should_terminate) {
	// if is controller node, heartbeats would be handled through raft consensus for quorum
	if (this->settings->get_is_controller_node()) return;

	ConnectionPool* pool = NULL;

	std::unique_ptr<DataNodeHeartbeatRequest> req = std::make_unique<DataNodeHeartbeatRequest>();
	req.get()->node_id = this->settings->get_node_id();

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(req.get());

	long req_buf_size = std::get<0>(buf_tup);
	std::shared_ptr<char> req_buf = std::get<1>(buf_tup);

	int leader_id = this->controller->get_cluster_metadata()->get_leader_id();

	if (leader_id == 0)
		leader_id = std::get<0>((*(this->settings->get_controller_nodes()))[0]);

	while (!(*should_terminate)) {
		if (pool == NULL) {
			std::lock_guard<std::mutex> lock(*this->cm->get_controller_node_connections_mut());
		
			auto controller_node_connections = this->cm->get_controller_node_connections(false);
		
			pool = (*controller_node_connections)[leader_id].get();
		}
		
		if (!this->send_heartbeat_to_leader(&leader_id, req_buf.get(), req_buf_size, pool))
			pool = NULL;

		std::this_thread::sleep_for(std::chrono::milliseconds(1000)); // this will be added in settings
	}
}

bool DataNode::send_heartbeat_to_leader(int* leader_id, char* req_buf, long req_buf_size, ConnectionPool* pool) {
	std::tuple<std::shared_ptr<char>, long, bool> res_tup = this->cm->send_request_to_socket(
		pool,
		3,
		req_buf,
		req_buf_size,
		"DataNodeHeartbeat"
	);

	if (std::get<1>(res_tup) == -1) {
		this->logger->log_error("Could not send heartbeat to leader (id=" + std::to_string(*leader_id) + ").");
		*leader_id = this->get_next_leader_id(*leader_id);
		return false;
	}

	std::unique_ptr<DataNodeHeartbeatResponse> res = this->response_mapper->to_data_node_heartbeat_response(std::get<0>(res_tup).get(), std::get<1>(res_tup));

	if (res.get() == NULL) {
		this->logger->log_error("Invalid mapping value in DataNodeHeartbeatResponse type");
		return false;
	}

	int prev_leader_id = *leader_id;
	bool update_leader = *leader_id != res.get()->leader_id;

	if (update_leader) {
		this->controller->get_cluster_metadata()->set_leader_id(res.get()->leader_id);
		this->logger->log_info("Sent heartbeat to incorrect leader node " + std::to_string(*leader_id) + " instead of actual leader node " + std::to_string(res.get()->leader_id));
	}

	if (!res.get()->ok || update_leader)
		*leader_id = update_leader ? res.get()->leader_id : *leader_id;
	else
		this->logger->log_info("Heartbeat sent to leader (id=" + std::to_string(*leader_id) + ") successfully");

	if (update_leader && *leader_id <= 0) {
		this->logger->log_info("No leader elected yet");
		*leader_id = prev_leader_id;
	}

	return res.get()->ok && !update_leader;
}

// get next leader id using round robin
int DataNode::get_next_leader_id(int leader_id) {
	int total_controller_nodes = this->settings->get_controller_nodes()->size();

	if (total_controller_nodes == 1)
		return std::get<0>((*(this->settings->get_controller_nodes()))[0]);

	int leader_index = 0;

	for (auto contr : *this->settings->get_controller_nodes()) {
		if (leader_id == std::get<0>(contr)) break;

		if (leader_index == total_controller_nodes - 1) break;

		leader_index++;
	}

	if (leader_index == total_controller_nodes - 1)
		return std::get<0>((*(this->settings->get_controller_nodes()))[0]);

	return std::get<0>((*(this->settings->get_controller_nodes()))[leader_index + 1]);
}