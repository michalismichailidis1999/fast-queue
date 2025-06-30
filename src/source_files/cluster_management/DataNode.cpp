#include "../../header_files/cluster_management/DataNode.h"

DataNode::DataNode(Controller* controller, ConnectionsManager* cm, RequestMapper* request_mapper, ResponseMapper* response_mapper, ClassToByteTransformer* transformer, Settings* settings, Logger* logger) {
	this->controller = controller;
	this->cm = cm;
	this->response_mapper = response_mapper;
	this->transformer = transformer;
	this->settings = settings;
	this->logger = logger;
}

void DataNode::send_heartbeats_to_leader(std::atomic_bool* should_terminate) {
	ConnectionPool* pool = NULL;

	std::unique_ptr<DataNodeHeartbeatRequest> req = std::make_unique<DataNodeHeartbeatRequest>();
	req.get()->node_id = this->settings->get_node_id();
	req.get()->address = this->settings->get_internal_ip().c_str();
	req.get()->address_length = this->settings->get_internal_ip().size();
	req.get()->port = this->settings->get_internal_port();

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(req.get());

	while (!(*should_terminate)) {
		if (this->settings->get_is_controller_node()) {
			std::this_thread::sleep_for(std::chrono::milliseconds(CHECK_FOR_SETTINGS_UPDATE));
			continue;
		}

		int leader_id = this->controller->get_cluster_metadata()->get_leader_id();

		if (leader_id == 0)
			leader_id = std::get<0>((this->settings->get_controller_nodes())[0]);

		if (pool == NULL) {
			std::lock_guard<std::mutex> lock(*this->cm->get_controller_node_connections_mut());

			auto controller_node_connections = this->cm->get_controller_node_connections(false);

			pool = (*controller_node_connections)[leader_id].get();
		}

		if (!this->send_heartbeat_to_leader(&leader_id, std::get<1>(buf_tup).get(), std::get<0>(buf_tup), pool))
			pool = NULL;

		std::this_thread::sleep_for(std::chrono::milliseconds(this->settings->get_heartbeat_to_leader_ms()));
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

	if (res.get() == NULL) return false;

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
	int total_controller_nodes = this->settings->get_controller_nodes().size();

	if (total_controller_nodes == 1)
		return std::get<0>((this->settings->get_controller_nodes())[0]);

	int leader_index = 0;

	for (auto contr : this->settings->get_controller_nodes()) {
		if (leader_id == std::get<0>(contr)) break;

		if (leader_index == total_controller_nodes - 1) break;

		leader_index++;
	}

	if (leader_index == total_controller_nodes - 1)
		return std::get<0>((this->settings->get_controller_nodes())[0]);

	return std::get<0>((this->settings->get_controller_nodes())[leader_index + 1]);
}

void DataNode::retrieve_cluster_metadata_updates(std::atomic_bool* should_terminate) {
	int leader_id = 0;
	int prev_leader_id = 0;
	std::unique_ptr<GetClusterMetadataUpdateRequest> req = nullptr;
	std::shared_ptr<AppendEntriesRequest> append_entries_req = nullptr;
	std::tuple<long, std::shared_ptr<char>> buf_tup = std::tuple<long, std::shared_ptr<char>>(0, nullptr);
	std::tuple<std::shared_ptr<char>, long, bool> res = std::tuple<std::shared_ptr<char>, long, bool>(nullptr, 0, false);
	std::shared_ptr<AppendEntriesResponse> append_entries_res = nullptr;

	bool index_matched = true;

	while (!(*should_terminate)) {
		if (this->settings->get_is_controller_node()) {
			std::this_thread::sleep_for(std::chrono::milliseconds(CHECK_FOR_SETTINGS_UPDATE));
			continue;
		}

		prev_leader_id = leader_id;
		leader_id = this->controller->get_cluster_metadata()->get_leader_id();

		if (leader_id != prev_leader_id && prev_leader_id != 0) index_matched = true;

		if (leader_id == 0) {
			std::this_thread::sleep_for(std::chrono::milliseconds(this->settings->get_cluster_update_receive_ms()));
			continue;
		}

		{
			std::lock_guard<std::mutex> lock(*this->cm->get_controller_node_connections_mut());

			std::shared_ptr<ConnectionPool> pool = this->cm->get_controller_node_connection(leader_id, false);

			if (pool == nullptr) goto end;

			req = std::make_unique<GetClusterMetadataUpdateRequest>();
			req.get()->node_id = this->settings->get_node_id();
			req.get()->prev_req_index_matched = index_matched;

			buf_tup = this->transformer->transform(req.get());

			res = this->cm->send_request_to_socket(
				pool.get(),
				3,
				std::get<1>(buf_tup).get(),
				std::get<0>(buf_tup),
				"GetClusterMetadataUpdate"
			);

			if (std::get<1>(res) == -1) {
				this->logger->log_error("Network issue occured while trying to get cluster metadata updates from leader");
				goto end;
			}

			append_entries_req = this->request_mapper->to_append_entries_request(
				std::get<0>(res).get(),
				std::get<1>(res)
			);

			if (append_entries_req == nullptr) goto end;

			append_entries_res = this->controller->handle_leader_append_entries(append_entries_req.get(), true);

			index_matched = append_entries_res.get()->log_matched;

		end: {}
		}

		std::this_thread::sleep_for(std::chrono::milliseconds(this->settings->get_cluster_update_receive_ms()));
	}
}