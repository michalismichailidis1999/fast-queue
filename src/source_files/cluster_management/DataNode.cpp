#include "../../header_files/cluster_management/DataNode.h"

DataNode::DataNode(Controller* controller, ConnectionsManager* cm, QueueManager* qm, MessagesHandler* mh, RequestMapper* request_mapper, ResponseMapper* response_mapper, ClassToByteTransformer* transformer, Util* util, FileHandler* fh, Settings* settings, Logger* logger) {
	this->controller = controller;
	this->cm = cm;
	this->qm = qm;
	this->mh = mh;
	this->request_mapper = request_mapper;
	this->response_mapper = response_mapper;
	this->transformer = transformer;
	this->util = util;
	this->fh = fh;
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

				if (leader_id != append_entries_req.get()->leader_id) {
					index_matched = true;
					leader_id = append_entries_req.get()->leader_id;
					pool = nullptr;
					is_first_request = true;
					goto end;
				}

				append_entries_res = this->controller->handle_leader_append_entries(append_entries_req.get(), true);

				index_matched = append_entries_res.get()->log_matched;
				is_first_request = false;
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
				std::this_thread::sleep_for(std::chrono::milliseconds(this->settings->get_dead_consumer_check_ms()));
				continue;
			}
			
			req.get()->expired_consumers->clear();

			{
				std::lock_guard<std::mutex> lock(this->consumers_mut);

				for (auto& iter : this->consumer_heartbeats)
					if (this->util->has_timeframe_expired(std::get<0>(iter.second), this->settings->get_dead_consumer_expire_ms())) {
						this->expired_consumers.insert(iter.first);
						req.get()->expired_consumers->emplace_back(std::get<2>(iter.second), std::get<1>(iter.second), iter.first);
					}
			}

			if (req.get()->expired_consumers->size() == 0) {
				expire_consumers_res = nullptr;
				std::this_thread::sleep_for(std::chrono::milliseconds(this->settings->get_dead_consumer_check_ms()));
				continue;
			}

			if (leader_id == this->settings->get_node_id()) {
				this->controller->handle_consumers_expiration(req.get());

				{
					std::lock_guard<std::mutex> lock(this->consumers_mut);
					for (auto& iter : *(req.get()->expired_consumers.get()))
						this->consumer_heartbeats.erase(std::get<2>(iter));
				}

				int new_leader_id = this->controller->get_leader_id();

				if (new_leader_id != leader_id) {
					leader_id = new_leader_id;
					pool = nullptr;
					expire_consumers_res = nullptr;
				}

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
			else {
				expire_consumers_res = this->response_mapper->to_expire_consumers_response(
					std::get<0>(res).get(),
					std::get<1>(res)
				);

				{
					std::lock_guard<std::mutex> lock(this->consumers_mut);
					for (auto& iter : *(req.get()->expired_consumers.get()))
						this->consumer_heartbeats.erase(std::get<2>(iter));
				}
			}
		}
		catch (const std::exception& ex)
		{
			std::string err_msg = "Error occured while checking for dead consumers. Reason: " + std::string(ex.what());
			this->logger->log_error(err_msg);
		}

		std::this_thread::sleep_for(std::chrono::milliseconds(this->settings->get_dead_consumer_check_ms()));
	}
}

void DataNode::fetch_data_from_partition_leaders(std::atomic_bool* should_terminate) {
	//if (this->settings->get_node_id() == 2) return;

	std::vector<std::tuple<std::string, int, int>> queues_partitions_to_fetch_from;

	while (!should_terminate->load()) {
		try
		{
			queues_partitions_to_fetch_from.clear();
			
			{
				ClusterMetadata* cluster_metadata = this->controller->get_cluster_metadata();

				std::shared_lock<std::shared_mutex> lock(*(cluster_metadata->get_partitions_mut()));

				if (cluster_metadata->nodes_partitions.find(this->settings->get_node_id()) != cluster_metadata->nodes_partitions.end())
					for (auto& iter : *(cluster_metadata->nodes_partitions[this->settings->get_node_id()].get())) {
						QueueMetadata* queue_metadata = cluster_metadata->get_queue_metadata(iter.first);

						if (queue_metadata == NULL || queue_metadata->get_replication_factor() == 1) continue;

						if (cluster_metadata->partition_leader_nodes.find(iter.first) == cluster_metadata->partition_leader_nodes.end())
							continue;

						auto queue_leaders = cluster_metadata->partition_leader_nodes[iter.first];

						if (queue_leaders == nullptr) continue;

						for (auto& iter2 : *(queue_leaders.get()))
							if (iter2.second != this->settings->get_node_id())
								queues_partitions_to_fetch_from.emplace_back(iter.first, iter2.first, iter2.second);
					}
			}

			for (auto& fetch_from : queues_partitions_to_fetch_from) {
				if (std::get<2>(fetch_from) == this->settings->get_node_id()) continue;

				std::shared_ptr<Queue> queue = this->qm->get_queue(std::get<0>(fetch_from));

				if (queue == nullptr) continue;

				std::shared_ptr<Partition> partition = queue.get()->get_partition(std::get<1>(fetch_from));

				if (partition == nullptr) continue;

				std::shared_ptr<ConnectionPool> pool = this->cm->get_node_connection_pool(std::get<2>(fetch_from));

				if (pool == nullptr) continue;

				std::unique_ptr<FetchMessagesRequest> req = std::make_unique<FetchMessagesRequest>();
				req.get()->queue_name = (char*)std::get<0>(fetch_from).c_str();
				req.get()->queue_name_length = std::get<0>(fetch_from).size();
				req.get()->partition = std::get<1>(fetch_from);
				req.get()->node_id = this->settings->get_node_id();
				req.get()->message_offset = partition.get()->get_message_offset() + 1;

				std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(req.get());

				auto res = this->cm->send_request_to_socket(
					pool.get(),
					3,
					std::get<1>(buf_tup).get(),
					std::get<0>(buf_tup),
					"FetchMessages"
				);

				if (std::get<1>(res) == -1 && std::get<2>(res)) {
					this->logger->log_error("Network issue occured while trying to send fetch messages request to partition leader node");
				}
				else if (std::get<1>(res) == -1 && !std::get<2>(res)) {
					this->logger->log_error("Error occured while trying to send fetch messages request to partition leader node");
				}
				else this->handle_fetch_messages_res(
					partition.get(), 
					this->response_mapper->to_fetch_messages_response(
						std::get<0>(res).get(), std::get<1>(res)
					).get()
				);
			}
		}
		catch (const std::exception& ex)
		{
			std::string err_msg = "Error occured while fetching data from partition leaders. Reason: " + std::string(ex.what());
			this->logger->log_error(err_msg);
		}

		std::this_thread::sleep_for(std::chrono::milliseconds(this->settings->get_fetch_from_leader_ms()));
	}
}

void DataNode::check_for_lagging_followers(std::atomic_bool* should_terminate) {
	std::unordered_map<int, std::shared_ptr<std::vector<std::tuple<std::string, int>>>> follower_nodes_to_check;

	std::unique_ptr<AddLaggingFollowerResponse> add_lagging_follower_res = nullptr;

	std::shared_ptr<ConnectionPool> pool = nullptr;
	int leader_id = std::get<0>((this->settings->get_controller_nodes())[0]);

	while (!should_terminate->load()) {
		try
		{
			follower_nodes_to_check.clear();

			{
				ClusterMetadata* cluster_metadata = this->controller->get_cluster_metadata();

				std::shared_lock<std::shared_mutex> lock(*(cluster_metadata->get_partitions_mut()));

				if (cluster_metadata->nodes_partitions.find(this->settings->get_node_id()) != cluster_metadata->nodes_partitions.end())
					for (auto& iter : *(cluster_metadata->nodes_partitions[this->settings->get_node_id()].get())) {
						QueueMetadata* queue_metadata = cluster_metadata->get_queue_metadata(iter.first);

						if (queue_metadata == NULL || queue_metadata->get_replication_factor() == 1) continue;

						if (cluster_metadata->partition_leader_nodes.find(iter.first) == cluster_metadata->partition_leader_nodes.end())
							continue;

						if (cluster_metadata->owned_partitions.find(iter.first) == cluster_metadata->owned_partitions.end())
							continue;

						auto queue_leaders = cluster_metadata->partition_leader_nodes[iter.first];
						auto partition_owners = cluster_metadata->owned_partitions[iter.first];

						if (queue_leaders == nullptr || partition_owners == nullptr) continue;

						for (auto& iter2 : *(queue_leaders.get()))
							if (
								iter2.second == this->settings->get_node_id()
								&& partition_owners.get()->find(iter2.first) != partition_owners.get()->end()
								&& (*(partition_owners.get()))[iter2.first].get()->size() > 1
							) for (int follower_id : *((*(partition_owners.get()))[iter2.first].get())) {
								if (cluster_metadata->is_follower_lagging(iter.first, iter2.first, follower_id))
									continue;
								
								auto node_follow_partitions = follower_nodes_to_check[follower_id];

								if (node_follow_partitions == nullptr) {
									node_follow_partitions = std::make_shared<std::vector<std::tuple<std::string, int>>>();
									follower_nodes_to_check[follower_id] = node_follow_partitions;
								}

								node_follow_partitions.get()->emplace_back(iter.first, iter2.first);
							}
					}
			}

			if (follower_nodes_to_check.size() == 0) {
				add_lagging_follower_res = nullptr;
				std::this_thread::sleep_for(std::chrono::milliseconds(this->settings->get_lag_followers_check_ms()));
				continue;
			}

			leader_id == this->controller->get_leader_id();

			pool = leader_id != this->settings->get_node_id() && pool != nullptr ? pool : this->get_leader_connection_pool(leader_id);

			if (leader_id != this->settings->get_node_id() && pool == nullptr) {
				this->logger->log_error("Something went wrong while checking for lagging followers. Could not retrieve leader connection pool for cluster metadata updates fetching");
				std::this_thread::sleep_for(std::chrono::milliseconds(this->settings->get_lag_followers_check_ms()));
				continue;
			}

			std::shared_lock<std::shared_mutex> followers_lock(this->follower_heartbeats_mut);

			for (auto& iter : follower_nodes_to_check) {
				if (iter.second == nullptr) continue;

				if (leader_id != this->settings->get_node_id() && pool == nullptr) continue;

				for (auto& iter2 : *(iter.second.get())) {
					std::string queue_name = std::get<0>(iter2);
					int partition = std::get<1>(iter2);

					std::string key = this->get_follower_heartbeat_key(queue_name, partition, iter.first);

					if (this->follower_heartbeats.find(key) == this->follower_heartbeats.end()) continue;

					if (!this->util->has_timeframe_expired(this->follower_heartbeats[key].count(), this->settings->get_lag_time_ms()))
						continue;

					if (leader_id == this->settings->get_node_id()) {
						// TODO: Call controller method here

						add_lagging_follower_res = nullptr;

						continue;
					}

					std::unique_ptr<AddLaggingFollowerRequest> req = std::make_unique<AddLaggingFollowerRequest>();
					req.get()->queue_name = (char*)queue_name.c_str();
					req.get()->queue_name_length = queue_name.size();
					req.get()->partition = partition;
					req.get()->node_id = iter.first;

					std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(req.get());

					auto res = this->cm->send_request_to_socket(
						pool.get(),
						3,
						std::get<1>(buf_tup).get(),
						std::get<0>(buf_tup),
						"AddLaggingFollower"
					);

					if (std::get<1>(res) == -1 && std::get<2>(res)) {
						this->logger->log_error("Network issue occured while trying to send add lagging follower request to leader node");
					}
					else if (std::get<1>(res) == -1 && !std::get<2>(res)) {
						this->logger->log_error("Error occured while trying to send add lagging follower request to leader node");
					}
					else {
						add_lagging_follower_res = this->response_mapper->to_add_lagging_follower_response(
							std::get<0>(res).get(),
							std::get<1>(res)
						);

						if (add_lagging_follower_res.get()->leader_id != leader_id) {
							leader_id = add_lagging_follower_res.get()->leader_id;
							pool = nullptr;
							add_lagging_follower_res = nullptr;
							break;
						}
					}
				}
			}
		}
		catch (const std::exception& ex)
		{
			std::string err_msg = "Error occured while checking for lagging followers. Reason: " + std::string(ex.what());
			this->logger->log_error(err_msg);
		}

		std::this_thread::sleep_for(std::chrono::milliseconds(this->settings->get_lag_followers_check_ms()));
	}
}

std::shared_ptr<ConnectionPool> DataNode::get_leader_connection_pool(int leader_id) {
	if (leader_id == this->settings->get_node_id()) return nullptr;

	std::shared_lock<std::shared_mutex> lock(*(this->cm->get_controller_node_connections_mut()));

	std::map<int, std::shared_ptr<ConnectionPool>>* controller_node_connections = this->cm->get_controller_node_connections();

	if (controller_node_connections->find(leader_id) == controller_node_connections->end()) return nullptr;

	return (*controller_node_connections)[leader_id];
}

void DataNode::handle_fetch_messages_res(Partition* partition, FetchMessagesResponse* res) {
	unsigned long long first_message_offset = 0;
	unsigned long long first_message_leader_epoch = 0;
	unsigned long long last_message_offset = 0;
	unsigned long long last_message_leader_epoch = 0;

	unsigned int message_bytes = 0;
	unsigned int offset = 0;

	for (int i = 0; i < res->total_messages; i++) {
		memcpy_s(&message_bytes, TOTAL_METADATA_BYTES, (char*)res->messages_data + offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);
		memcpy_s(&last_message_offset, MESSAGE_ID_SIZE, (char*)res->messages_data + offset + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);
		memcpy_s(&last_message_leader_epoch, MESSAGE_LEADER_ID_SIZE, (char*)res->messages_data + offset + MESSAGE_LEADER_ID_OFFSET, MESSAGE_LEADER_ID_SIZE);

		if (i == 0) {
			first_message_offset = last_message_offset;
			first_message_leader_epoch = last_message_leader_epoch;
		}

		offset += message_bytes;
	}

	if ((res->total_messages > 0 && partition->get_message_offset() > first_message_offset) || partition->get_message_offset() > res->last_message_offset) {
		unsigned long long offset_to_remove_from = res->total_messages > 0
			? Helper::get_min(first_message_offset, res->last_message_offset)
			: res->last_message_offset;

		if (!this->mh->remove_messages_after_message_id(partition, offset_to_remove_from)) {
			this->logger->log_error("Could not remove previous leadership uncommited messages");
			return;
		}

		this->set_partition_offset_to_prev_loc(partition, offset_to_remove_from);
	}

	if (
		res->total_messages > 0
		&& partition->get_message_offset() > 0 
		&& (
			partition->get_message_offset() != res->prev_message_offset
			|| partition->get_last_message_leader_epoch() != res->prev_message_leader_epoch
		)
	) {
		this->mh->remove_messages_after_message_id(partition, partition->get_message_offset() - 1);
		this->set_partition_offset_to_prev_loc(partition, partition->get_message_offset() - 1);
		return;
	}

	if (res->total_messages > 0) {
		std::lock_guard<std::mutex> lock(partition->write_mut);
		this->mh->save_messages(partition, res->messages_data, res->messages_total_bytes, nullptr, true);
	}

	partition->set_last_replicated_offset(res->commited_offset);

	if (res->total_messages == 0) return;

	partition->set_last_message_offset(last_message_offset);
	partition->set_last_message_leader_epoch(last_message_leader_epoch);

	std::lock_guard<std::shared_mutex> lock(partition->consumers_mut);

	if (!this->fh->check_if_exists(partition->get_offsets_path())) return;

	this->fh->write_to_file(
		partition->get_offsets_key(),
		partition->get_offsets_path(),
		sizeof(unsigned long long),
		0,
		&res->commited_offset,
		true
	);
}

void DataNode::set_partition_offset_to_prev_loc(Partition* partition, unsigned long long prev_loc) {
	if (prev_loc == 0) {
		partition->set_last_message_offset(0);
		partition->set_last_message_leader_epoch(0);
		return;
	}

	auto messages_res = this->mh->read_partition_messages(partition, prev_loc, 1, true);

	if (std::get<4>(messages_res) != 1) {
		this->logger->log_error("Something went wrong while trying to fix follower messages offset to match the partition leader");
		std::this_thread::sleep_for(std::chrono::milliseconds(2000));
		exit(EXIT_FAILURE);
	}

	char* message_offset = std::get<0>(messages_res).get() + std::get<2>(messages_res);

	unsigned long long current_last_message_offset = 0;
	unsigned long long current_last_message_leader = 0;

	memcpy_s(&current_last_message_offset, MESSAGE_ID_SIZE, message_offset + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);
	memcpy_s(&current_last_message_leader, MESSAGE_LEADER_ID_SIZE, message_offset + MESSAGE_LEADER_ID_OFFSET, MESSAGE_LEADER_ID_SIZE);

	partition->set_last_message_offset(current_last_message_offset);
	partition->set_last_message_leader_epoch(current_last_message_leader);
}

void DataNode::update_follower_heartbeat(const std::string& queue_name, int partition, int node_id) {
	std::lock_guard<std::shared_mutex> lock(this->follower_heartbeats_mut);

	this->follower_heartbeats[this->get_follower_heartbeat_key(queue_name, partition, node_id)] = this->util->get_current_time_milli();
}

std::string DataNode::get_follower_heartbeat_key(const std::string& queue_name, int partition, int node_id) {
	return queue_name + "_" + std::to_string(partition) + "_" + std::to_string(node_id);
}