#include "../../header_files/cluster_management/DataNode.h"

DataNode::DataNode(Controller* controller, ConnectionsManager* cm, RequestMapper* request_mapper, ResponseMapper* response_mapper, ClassToByteTransformer* transformer, Util* util, Settings* settings, Logger* logger) {
	this->controller = controller;
	this->cm = cm;
	this->response_mapper = response_mapper;
	this->transformer = transformer;
	this->util = util;
	this->settings = settings;
	this->logger = logger;
}

void DataNode::send_heartbeats_to_leader(std::atomic_bool* should_terminate) {
	if (this->settings->get_is_controller_node()) return;

	std::shared_ptr<ConnectionPool> pool = nullptr;
	int leader_id = 0;
	int prev_leader_id = leader_id;

	std::unique_ptr<DataNodeHeartbeatRequest> req = std::make_unique<DataNodeHeartbeatRequest>();
	req.get()->node_id = this->settings->get_node_id();

	req.get()->address = this->settings->get_internal_ip().c_str();
	req.get()->address_length = this->settings->get_internal_ip().size();
	req.get()->port = this->settings->get_internal_port();

	req.get()->external_address = this->settings->get_external_ip().c_str();
	req.get()->external_address_length = this->settings->get_external_ip().size();
	req.get()->external_port = this->settings->get_external_port();

	req.get()->register_node = true;

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(req.get());

	while (!(should_terminate->load())) {
		try
		{
			if (leader_id <= 0)
				leader_id = std::get<0>((this->settings->get_controller_nodes())[0]);

			pool = pool != nullptr ? pool : this->get_leader_connection_pool(leader_id);

			if (pool != nullptr && !this->send_heartbeat_to_leader(&leader_id, std::get<1>(buf_tup).get(), std::get<0>(buf_tup), pool.get()))
			{
				pool = nullptr;

				if (!req.get()->register_node) {
					req.get()->register_node = true;
					buf_tup = this->transformer->transform(req.get());
				}
			}
			else if (req.get()->register_node) {
				req.get()->register_node = false;
				buf_tup = this->transformer->transform(req.get());
			}
		}
		catch (const std::exception& ex)
		{
			std::string err_msg = "Error occured while sending heartbeat to leader. Reason: " + std::string(ex.what());
			this->logger->log_error(err_msg);
		}

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
	if (this->settings->get_is_controller_node()) return;

	int leader_id = std::get<0>((this->settings->get_controller_nodes())[0]);

	std::unique_ptr<GetClusterMetadataUpdateRequest> req = nullptr;
	std::shared_ptr<AppendEntriesRequest> append_entries_req = nullptr;
	std::tuple<long, std::shared_ptr<char>> buf_tup = std::tuple<long, std::shared_ptr<char>>(0, nullptr);
	std::tuple<std::shared_ptr<char>, long, bool> res = std::tuple<std::shared_ptr<char>, long, bool>(nullptr, 0, false);
	std::shared_ptr<AppendEntriesResponse> append_entries_res = nullptr;

	std::shared_ptr<ConnectionPool> pool = nullptr;

	bool index_matched = true;
	bool is_first_request = true;

	while (!(should_terminate->load())) {
		try
		{
			{
				pool = pool != nullptr ? pool : this->get_leader_connection_pool(leader_id);

				if (pool == nullptr) {
					this->logger->log_error("Something went wrong while trying to fetch cluster updates. Could not retrieve leader connection pool for cluster metadata updates fetching");
					goto end;
				}

				req = std::make_unique<GetClusterMetadataUpdateRequest>();
				req.get()->node_id = this->settings->get_node_id();
				req.get()->prev_req_index_matched = index_matched;
				req.get()->prev_log_index = this->controller->get_last_log_index();
				req.get()->prev_log_term = this->controller->get_last_log_term();
				req.get()->is_first_request = is_first_request;

				buf_tup = this->transformer->transform(req.get());

				res = this->cm->send_request_to_socket(
					pool.get(),
					3,
					std::get<1>(buf_tup).get(),
					std::get<0>(buf_tup),
					"GetClusterMetadataUpdate"
				);

				if (std::get<1>(res) == -1 && std::get<2>(res)) {
					this->logger->log_error("Network issue occured while trying to get cluster metadata updates from leader");
					goto end;
				}

				if (std::get<1>(res) == -1 && !std::get<2>(res)) {
					this->logger->log_error("Error occured while trying to get cluster metadata updates from leader");
					goto end;
				}

				append_entries_req = this->request_mapper->to_append_entries_request(
					std::get<0>(res).get(),
					std::get<1>(res),
					true
				);

				if (append_entries_req == nullptr) goto end;

				append_entries_res = this->controller->handle_leader_append_entries(append_entries_req.get(), true);

				index_matched = append_entries_res.get()->log_matched;
				is_first_request = false;

				if (leader_id != append_entries_req.get()->leader_id) {
					index_matched = true;
					leader_id = append_entries_req.get()->leader_id;
					pool = nullptr;
					is_first_request = true;
				}

			end: {}
			}
		}
		catch (const std::exception& ex)
		{
			std::string err_msg = "Error occured while receiving cluster updates from leader. Reason: " + std::string(ex.what());
			this->logger->log_error(err_msg);
		}

		std::this_thread::sleep_for(std::chrono::milliseconds(this->settings->get_cluster_update_receive_ms()));
	}
}

void DataNode::update_consumer_heartbeat(const std::string& queue_name, const std::string& group_id, unsigned long long consumer_id) {
	std::lock_guard<std::mutex> lock(this->consumers_mut);

	this->consumer_heartbeats[consumer_id] = std::tuple<std::chrono::milliseconds, std::string, std::string>(
		this->util->get_current_time_milli(),
		group_id,
		queue_name
	);
}

bool DataNode::has_consumer_expired(unsigned long long consumer_id) {
	std::lock_guard<std::mutex> lock(this->consumers_mut);

	bool expired = false;

	if (this->expired_consumers.find(consumer_id) != this->expired_consumers.end()) {
		expired = true;
		this->expired_consumers.erase(consumer_id);
	}

	return expired;
}

void DataNode::check_for_dead_consumer(std::atomic_bool* should_terminate) {
	std::unique_ptr<ExpireConsumersRequest> req = std::make_unique<ExpireConsumersRequest>();
	req.get()->expired_consumers = std::make_shared<std::vector<std::tuple<std::string, std::string, unsigned long long>>>();

	std::shared_ptr<ExpireConsumersResponse> expire_consumers_res = nullptr;

	std::shared_ptr<ConnectionPool> pool = nullptr;
	int leader_id = std::get<0>((this->settings->get_controller_nodes())[0]);

	while (!(should_terminate->load())) {
		try {
			pool = leader_id != this->settings->get_node_id() && pool != nullptr ? pool : this->get_leader_connection_pool(leader_id);

			if (leader_id != this->settings->get_node_id() && pool == nullptr) {
				this->logger->log_error("Something went wrong while checking for dead consumers. Could not retrieve leader connection pool for cluster metadata updates fetching");
				std::this_thread::sleep_for(std::chrono::milliseconds(this->settings->get_dead_consumer_check_ms()));
				continue;
			}

			if (expire_consumers_res != nullptr && expire_consumers_res.get()->leader_id > 0 && expire_consumers_res.get()->leader_id != leader_id) {
				pool = nullptr;
				leader_id = expire_consumers_res.get()->leader_id;
				expire_consumers_res = nullptr;
				continue;
			}
			else {
				req.get()->expired_consumers->clear();

				{
					std::lock_guard<std::mutex> lock(this->consumers_mut);

					for (auto& iter : this->consumer_heartbeats)
						if (this->util->has_timeframe_expired(std::get<0>(iter.second), this->settings->get_dead_consumer_expire_ms())) {
							this->expired_consumers.insert(iter.first);
							req.get()->expired_consumers->emplace_back(std::get<2>(iter.second), std::get<1>(iter.second), iter.first);
						}
				}
			}

			if (req.get()->expired_consumers->size() == 0) {
				std::this_thread::sleep_for(std::chrono::milliseconds(this->settings->get_dead_consumer_check_ms()));
				continue;
			}

			if (leader_id == this->settings->get_node_id()) {
				this->controller->handle_consumers_expiration(req.get());
				std::this_thread::sleep_for(std::chrono::milliseconds(this->settings->get_dead_consumer_check_ms()));
				continue;
			}

			std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(req.get());

			auto res = this->cm->send_request_to_socket(
				pool.get(),
				3,
				std::get<1>(buf_tup).get(),
				std::get<0>(buf_tup),
				"ExpireConsumers"
			);

			if (std::get<1>(res) == -1 && std::get<2>(res)) {
				this->logger->log_error("Network issue occured while trying to send expire consumers request to leader node");
			} else if (std::get<1>(res) == -1 && !std::get<2>(res)) {
				this->logger->log_error("Error occured while trying to send expire consumers request to leader node");
			}
			else expire_consumers_res = this->response_mapper->to_expire_consumers_response(
				std::get<0>(res).get(),
				std::get<1>(res)
			);
		}
		catch (const std::exception& ex)
		{
			std::string err_msg = "Error occured while checking for dead consumers. Reason: " + std::string(ex.what());
			this->logger->log_error(err_msg);
		}

		std::this_thread::sleep_for(std::chrono::milliseconds(this->settings->get_dead_consumer_check_ms()));
	}
}

std::shared_ptr<ConnectionPool> DataNode::get_leader_connection_pool(int leader_id) {
	std::shared_lock<std::shared_mutex> lock(*(this->cm->get_controller_node_connections_mut()));

	std::map<int, std::shared_ptr<ConnectionPool>>* controller_node_connections = this->cm->get_controller_node_connections();

	if (controller_node_connections->find(leader_id) == controller_node_connections->end()) return nullptr;

	return (*controller_node_connections)[leader_id];
}