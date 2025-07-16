#include "../../header_files/cluster_management/Controller.h"

Controller::Controller(ConnectionsManager* cm, QueueManager* qm, MessagesHandler* mh, ClusterMetadataApplyHandler* cmah, ResponseMapper* response_mapper, ClassToByteTransformer* transformer, Util* util, Logger* logger, Settings* settings, std::atomic_bool* should_terminate)
	: generator(std::random_device{}()), distribution(HEARTBEAT_SIGNAL_MIN_BOUND, HEARTBEAT_SIGNAL_MAX_BOUND)
{
	this->cm = cm;
	this->qm = qm;
	this->mh = mh;
	this->cmah = cmah;
	this->response_mapper = response_mapper;
	this->transformer = transformer;
	this->util = util;
	this->logger = logger;
	this->settings = settings;
	this->should_terminate = should_terminate;

	this->cluster_metadata = std::unique_ptr<ClusterMetadata>(new ClusterMetadata());
	this->future_cluster_metadata = std::unique_ptr<ClusterMetadata>(new ClusterMetadata());
	this->compacetd_cluster_metadata = std::unique_ptr<ClusterMetadata>(new ClusterMetadata());

	unsigned int total_controllers = 0;

	for (auto& controller : this->settings->get_controller_nodes()) {
		this->controller_nodes_ids.insert(std::get<0>(controller));
		total_controllers++;
	}

	this->is_the_only_controller_node = this->settings->get_is_controller_node() && total_controllers == 1;

	this->vote_for = -1;

	this->state = !this->is_the_only_controller_node ? NodeState::FOLLOWER : NodeState::LEADER;
	this->received_heartbeat = false;

	if (this->state == NodeState::LEADER) {
		this->cluster_metadata->set_leader_id(this->settings->get_node_id());
		this->logger->log_info("Is single controller node. Initialized as leader.");
	}

	this->term = 0;
	this->last_log_index = 0;
	this->last_log_term = 0;

	this->half_quorum_nodes_count = total_controllers / 2 + 1;
}

void Controller::update_quorum_communication_values() {
	this->term = this->future_cluster_metadata.get()->get_current_term();
	this->last_log_term = this->future_cluster_metadata.get()->get_current_term();
	this->last_log_index = this->future_cluster_metadata.get()->get_current_version();

	for(auto& iter : this->settings->get_controller_nodes())
		if(std::get<0>(iter) != this->settings->get_node_id())
			this->follower_indexes[std::get<0>(iter)] = std::tuple<unsigned long long, unsigned long long>(
				this->last_log_term, this->last_log_index
			);
}

void Controller::init_commit_index_and_last_applied() {
	QueueMetadata* cluster_metadata_queue = this->qm->get_queue(CLUSTER_METADATA_QUEUE_NAME).get()->get_metadata();

	this->commit_index = cluster_metadata_queue->get_last_commit_index();
	this->last_applied = cluster_metadata_queue->get_last_applied_index();
}

void Controller::run_controller_quorum_communication() {
	while (!(*this->should_terminate)) {
		if (!this->settings->get_is_controller_node())
		{
			std::this_thread::sleep_for(std::chrono::milliseconds(CHECK_FOR_SETTINGS_UPDATE));
			continue;
		}

		// TODO: Remove this after quorum communication is perfect
		this->logger->log_info("Quorum communication here.....");

		try
		{
			switch (this->get_state())
			{
			case NodeState::FOLLOWER:
				this->wait_for_leader_heartbeat();
				break;
			case NodeState::CANDIDATE:
				this->start_election();
				break;
			case NodeState::LEADER:
				this->append_entries_to_followers();
				break;
			}
		}
		catch (const std::exception& ex)
		{
			std::string err_msg = "Error occured during quorum communication. Reason: " + std::string(ex.what());
			this->logger->log_error(err_msg);
		}
	}
}

void Controller::start_election() {
	//if (this->settings->get_node_id() == 2) {
	//	this->step_down_to_follower();
	//	return;
	//}

	int expected = -1;

	if (!this->vote_for.compare_exchange_weak(expected, this->settings->get_node_id())) {
		this->set_state(NodeState::FOLLOWER);
		this->logger->log_info("Already vote for another candidate. Returning back to follower state");
		return;
	}

	std::shared_lock<std::shared_mutex> lock(*this->cm->get_controller_node_connections_mut());

	auto controller_node_connections = this->cm->get_controller_node_connections();

	this->term++;

	this->logger->log_info("Starting new election with term " + std::to_string(this->term));

	int votes = 1;

	std::unique_ptr<RequestVoteRequest> req = std::make_unique<RequestVoteRequest>();
	req.get()->term = this->term;
	req.get()->candidate_id = this->settings->get_node_id();
	req.get()->last_log_index = this->last_log_index;
	req.get()->last_log_term = this->last_log_term;

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(req.get());

	for (auto iter : *controller_node_connections) {
		std::tuple<std::shared_ptr<char>, long, bool> res_tup = this->cm->send_request_to_socket(
			iter.second.get(),
			3,
			std::get<1>(buf_tup).get(),
			std::get<0>(buf_tup),
			"RequestVote"
		);

		if (std::get<1>(res_tup) == -1) {
			this->logger->log_error("Network issue occured while communicating with node " + std::to_string(iter.first));
			continue;
		}

		std::unique_ptr<RequestVoteResponse> res = this->response_mapper->to_request_vote_response(
			std::get<0>(res_tup).get(),
			std::get<1>(res_tup)
		);

		if (res.get() == NULL) continue;

		if (res.get()->vote_granted) {
			votes++;
			this->logger->log_info("Vote granted from node " + std::to_string(iter.first));
		}
		else {
			this->logger->log_info(
				"Vote not granted from node "
				+ std::to_string(iter.first)
			);

			if (this->term < res.get()->term) {
				this->term = res.get()->term;
				req.get()->term = res.get()->term;
				buf_tup = this->transformer->transform(req.get());

				this->logger->log_error("Updated term to " + std::to_string(res.get()->term));
			}
		}
	}

	this->vote_for = -1;

	if (votes < this->half_quorum_nodes_count) {
		this->step_down_to_follower();
		return;
	}

	this->future_cluster_metadata->copy_from(this->cluster_metadata.get());
	this->set_state(NodeState::LEADER);
	this->cluster_metadata->set_leader_id(this->settings->get_node_id());
	this->logger->log_info("Elected as leader");

	std::lock_guard<std::shared_mutex> lag_lock(this->follower_indexes_mut);

	for (auto& iter : this->follower_indexes) {
		this->follower_indexes[iter.first] = std::tuple<unsigned long long, unsigned long long>(this->last_log_term, this->last_log_index);
		this->update_data_node_heartbeat(iter.first, NULL, true);
	}
}

void Controller::append_entries_to_followers() {
	std::lock_guard<std::mutex> lock(this->append_enties_mut);

	std::shared_lock<std::shared_mutex> connections_lock(*this->cm->get_controller_node_connections_mut());

	auto controller_node_connections = this->cm->get_controller_node_connections();

	std::vector<unsigned long long> largest_versions_sent(controller_node_connections->size());
	unsigned int version_sent_index = 0;

	int replication_count = 1;

	for (auto iter : *controller_node_connections) {
		std::shared_ptr<AppendEntriesRequest> req = this->prepare_append_entries_request(iter.first);

		std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(req.get());

		std::tuple<std::shared_ptr<char>, long, bool> res_tup = this->cm->send_request_to_socket(
			iter.second.get(),
			3,
			std::get<1>(buf_tup).get(),
			std::get<0>(buf_tup),
			"AppendEntries"
		);

		if (std::get<1>(res_tup) == -1) {
			this->logger->log_error("Network issue occured while communicating with node " + std::to_string(iter.first));
			largest_versions_sent[version_sent_index++] = 0;
			continue;
		}

		std::unique_ptr<AppendEntriesResponse> res = this->response_mapper->to_append_entries_response(
			std::get<0>(res_tup).get(),
			std::get<1>(res_tup)
		);

		this->update_data_node_heartbeat(iter.first, NULL, true);

		if (res.get() == NULL) {
			this->logger->log_error("Invalid mapping value in AppendEntriesResponse type");
			largest_versions_sent[version_sent_index++] = 0;
			continue;
		}

		if (!res.get()->success) {
			this->logger->log_error("Node " + std::to_string(iter.first) + " rejected AppendEntries request");

			if (!res.get()->log_matched && req.get()->prev_log_index > 0) {
				if (req.get()->prev_log_index - 1 > 0) {
					auto messages_res = this->mh->read_partition_messages(
						this->qm->get_queue(CLUSTER_METADATA_QUEUE_NAME).get()->get_partition(0),
						req.get()->prev_log_index - 1,
						1
					);

					if (std::get<4>(messages_res) == 1) {
						unsigned long long prev_log_index = 0;
						unsigned long long prev_log_term = 0;

						char* message_offset = std::get<0>(messages_res).get() + std::get<2>(messages_res);

						memcpy_s(&prev_log_index, MESSAGE_ID_SIZE, message_offset + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);
						memcpy_s(&prev_log_term, COMMAND_TERM_SIZE, message_offset + COMMAND_TERM_OFFSET, COMMAND_TERM_SIZE);

						std::lock_guard<std::shared_mutex> lock(this->follower_indexes_mut);
						this->follower_indexes[iter.first] = std::tuple<unsigned long long, unsigned long long>(prev_log_term, prev_log_index);
					}
					else throw std::exception("Something went wrong while trying to append entries to follower");
				}
				else {
					std::lock_guard<std::shared_mutex> lock(this->follower_indexes_mut);
					this->follower_indexes[iter.first] = std::tuple<unsigned long long, unsigned long long>(0, 0);
				}
			}

			largest_versions_sent[version_sent_index++] = 0;

			continue;
		}
		else {
			replication_count++;

			if (req.get()->total_commands > 0) {
				unsigned long long last_log_index = 0;
				unsigned long long last_log_term = 0;

				unsigned int command_bytes = 0;

				unsigned int offset = 0;

				while (offset < req.get()->commands_total_bytes) {
					memcpy_s(&command_bytes, TOTAL_METADATA_BYTES, (char*)req.get()->commands_data + offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);
					memcpy_s(&last_log_index, MESSAGE_ID_SIZE, (char*)req.get()->commands_data + offset + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);
					memcpy_s(&last_log_term, COMMAND_TERM_SIZE, (char*)req.get()->commands_data + offset + COMMAND_TERM_OFFSET, COMMAND_TERM_SIZE);

					offset += command_bytes;
				}

				std::lock_guard<std::shared_mutex> lock(this->follower_indexes_mut);
				this->follower_indexes[iter.first] = std::tuple<unsigned long long, unsigned long long>(last_log_term, last_log_index);
			}
		}

		unsigned long long last_message_id = 0;

		if (req.get()->total_commands > 0) {
			unsigned int offset = 0;
			unsigned int command_bytes = 0;

			while (offset < req.get()->commands_total_bytes) {
				memcpy_s(&command_bytes, TOTAL_METADATA_BYTES, (char*)req.get()->commands_data + offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);
				
				memcpy_s(&last_message_id, MESSAGE_ID_SIZE, (char*)req.get()->commands_data + offset + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);

				offset += command_bytes;
			}
		}
		else last_message_id = req.get()->prev_log_index;

		largest_versions_sent[version_sent_index++] = last_message_id;
	}

	if (replication_count >= this->half_quorum_nodes_count) {
		unsigned long long largest_replicated_index = !this->is_the_only_controller_node 
			? this->get_largest_replicated_index(&largest_versions_sent)
			: this->last_log_index;

		if (largest_replicated_index > 0 && largest_replicated_index > this->commit_index) {
			this->commit_index = largest_replicated_index;
			this->mh->update_cluster_metadata_commit_index(largest_replicated_index);
		}
	}

	connections_lock.unlock();

	std::this_thread::sleep_for(std::chrono::milliseconds(LEADER_TIMEOUT));
}

void Controller::step_down_to_follower() {
	this->set_state(NodeState::FOLLOWER);

	this->logger->log_info("Went back to follower");
}

void Controller::wait_for_leader_heartbeat() {
	std::unique_lock<std::mutex> lock(this->heartbeat_mut);

	int milli = this->distribution(this->generator);

	this->heartbeat_condition.wait_for(
		lock, 
		std::chrono::milliseconds(milli),
		[&] { return received_heartbeat; }
	);

	if (received_heartbeat) received_heartbeat = false;
	else {
		this->vote_for = -1;
		this->set_state(NodeState::CANDIDATE);
		this->logger->log_info("Became a candidate after waiting for " + std::to_string(milli) + " milliseconds");
	}
}

std::shared_ptr<AppendEntriesResponse> Controller::handle_leader_append_entries(AppendEntriesRequest* request, bool from_data_node) {
	std::lock_guard<std::mutex> lock(this->append_enties_mut);

	std::shared_ptr<AppendEntriesResponse> res = std::make_shared<AppendEntriesResponse>();
	res.get()->success = false;
	res.get()->term = this->term;
	res.get()->log_matched = true;

	if (!from_data_node) {
		if (this->get_state() == NodeState::LEADER) this->step_down_to_follower();

		this->set_received_heartbeat(true);
		this->logger->log_info("Received heartbeat from leader");

		this->cluster_metadata->set_leader_id(request->leader_id);

		if (request->term > this->term) {
			this->term = request->term;
			res.get()->term = request->term;
		}
	}

	if (
		request->prev_log_index != this->last_log_index
		|| request->prev_log_term != this->last_log_term
	) {
		res.get()->log_matched = false;

		if (
			request->prev_log_index < this->last_log_index
			|| (request->prev_log_index == this->last_log_index
				&& request->prev_log_term > this->last_log_term)
		) {
			Partition* partition = this->qm->get_queue(CLUSTER_METADATA_QUEUE_NAME).get()->get_partition(0);

			unsigned long long message_to_delete_from = request->prev_log_index < this->last_log_index
				? request->prev_log_index
				: request->prev_log_index - 1;

			if (this->commit_index > message_to_delete_from)
				throw std::exception("Node has commit index larger than current leader");

			if (!this->mh->remove_messages_after_message_id(partition, request->prev_log_index))
				throw std::exception("Failed to remove uncommited logs");

			if (message_to_delete_from > 1) {
				auto messages_res = this->mh->read_partition_messages(partition, message_to_delete_from);

				if (std::get<4>(messages_res) != 1) {
					this->logger->log_error("Something went wrong while trying to fix log offset to match the leader");
					std::this_thread::sleep_for(std::chrono::milliseconds(2000));
					exit(EXIT_FAILURE);
				}

				char* message_offset = std::get<0>(messages_res).get() + std::get<2>(messages_res);

				memcpy_s(&this->last_log_index, MESSAGE_ID_SIZE, message_offset + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);
				memcpy_s(&this->last_log_term, COMMAND_TERM_SIZE, message_offset + COMMAND_TERM_OFFSET, COMMAND_TERM_SIZE);
			}
			else {
				this->last_log_index = 0;
				this->last_log_term = 0;
			}

			if (request->prev_log_index == this->last_log_index && request->prev_log_term == this->last_log_term);
				res.get()->log_matched = true;
		}

		if(!res.get()->log_matched) return res;
	}

	if (this->store_commands(request->commands_data, request->total_commands, request->commands_total_bytes)) {
		if (request->leader_commit > this->commit_index) {
			this->mh->update_cluster_metadata_commit_index(request->leader_commit);
			this->commit_index = request->leader_commit;
		}

		res.get()->success = true;
	}

	return res;
}

std::shared_ptr<RequestVoteResponse> Controller::handle_candidate_request_vote(RequestVoteRequest* request) {
	std::shared_ptr<RequestVoteResponse> res = std::make_shared<RequestVoteResponse>();

	res.get()->term = this->term;

	int expected = -1;

	res.get()->vote_granted = request->term >= this->term
		&& request->last_log_term >= this->last_log_term
		&& request->last_log_index >= this->last_log_index
		&& (this->vote_for.compare_exchange_weak(expected, request->candidate_id)
			|| this->vote_for == request->candidate_id);

	if(this->settings->get_node_id() == 1)
		res.get()->vote_granted = (this->vote_for.compare_exchange_weak(expected, request->candidate_id)
			|| this->vote_for == request->candidate_id);

	if (request->term > this->term)
		this->term = request->term;

	if (!res.get()->vote_granted)
		this->logger->log_info("Vote was not granted for candidate " + std::to_string(request->candidate_id));
	else
		this->logger->log_info("Vote was granted for candidate " + std::to_string(request->candidate_id));
		
	if (res.get()->vote_granted && this->get_state() == NodeState::LEADER) {
		this->logger->log_info("Vote for other candidate. Stepping down as Leader and becoming Follower");
		this->set_state(NodeState::FOLLOWER);
	}

	return res;
}

void Controller::set_state(NodeState state) {
	std::lock_guard<std::mutex> lock(this->state_mut);
	this->state = state;
}

NodeState Controller::get_state() {
	std::lock_guard<std::mutex> lock(this->state_mut);
	return this->state;
}

void Controller::set_received_heartbeat(bool received_heartbeat) {
	std::lock_guard<std::mutex> lock(this->heartbeat_mut);
	this->received_heartbeat = received_heartbeat;
}

int Controller::get_leader_id() {
	return this->cluster_metadata->get_leader_id();
}

void Controller::update_data_node_heartbeat(int node_id, ConnectionInfo* info, bool is_controller_node) {
	{
		std::lock_guard<std::mutex> lock(this->heartbeats_mut);

		this->data_nodes_heartbeats[node_id] = this->util->get_current_time_milli();

		this->logger->log_info("Node " + std::to_string(node_id) + " heartbeat updated");
	}

	if (!is_controller_node) {
		std::lock_guard<std::shared_mutex> lock(this->follower_indexes_mut);

		if(this->follower_indexes.find(node_id) == this->follower_indexes.end())
			this->follower_indexes[node_id] = std::tuple<unsigned long long, unsigned long long>(
				this->last_log_term, 
				this->last_log_index
			);
	}

	if (!is_controller_node && this->get_state() == NodeState::LEADER && info != NULL && !this->future_cluster_metadata.get()->has_node_partitions(node_id)) {
		Command command = Command(
			CommandType::REGISTER_DATA_NODE,
			this->term,
			this->util->get_current_time_milli().count(),
			std::shared_ptr<RegisterDataNodeCommand>(new RegisterDataNodeCommand(node_id, info->address, info->port))
		);

		std::vector<Command> commands(1);
		commands[0] = command;

		this->store_commands(&commands);

		this->future_cluster_metadata->init_node_partitions(node_id);
	}
}

void Controller::assign_partition_to_node(const std::string& queue_name, int partition, std::vector<std::tuple<CommandType, std::shared_ptr<void>>>* cluster_changes, int owner_node) {
	std::vector<std::tuple<int, int>> to_insert_back;

	std::tuple<int, int> min_partitions_tup = this->future_cluster_metadata->nodes_partition_counts->extractTopElement();
	int node_id = std::get<1>(min_partitions_tup);

	if (node_id <= 0) node_id = this->settings->get_node_id();

	auto node_queues = this->future_cluster_metadata->nodes_partitions[node_id];
	auto owned_partitions = this->future_cluster_metadata->owned_partitions[queue_name];

	if (node_queues == nullptr) {
		node_queues = std::make_shared<std::unordered_map<std::string, std::shared_ptr<std::unordered_set<int>>>>();
		this->future_cluster_metadata->nodes_partitions[node_id] = node_queues;
	}

	if (owned_partitions == nullptr) {
		owned_partitions = std::make_shared<std::unordered_map<int, std::shared_ptr<std::unordered_set<int>>>>();
		this->future_cluster_metadata->owned_partitions[queue_name] = owned_partitions;
	}

	auto partition_owners = (*(owned_partitions.get()))[partition];

	if (partition_owners == nullptr) {
		partition_owners = std::make_shared<std::unordered_set<int>>();
		(*(owned_partitions.get()))[partition] = partition_owners;
	}

	bool continue_looping = !this->future_cluster_metadata->nodes_partition_counts->is_null_index(node_id);

	while (continue_looping) {
		bool node_has_queue = node_queues->find(queue_name) != node_queues->end();

		if (!node_has_queue) {
			auto queue_partitions = std::make_shared<std::unordered_set<int>>();
			queue_partitions.get()->insert(partition);

			(*node_queues)[queue_name] = queue_partitions;
			partition_owners.get()->insert(node_id);

			std::get<0>(min_partitions_tup)++;
			to_insert_back.emplace_back(min_partitions_tup);
			break;
		}

		auto node_partitions = (*node_queues)[queue_name].get();

		bool node_has_partition = node_has_queue && node_partitions->find(partition) != node_partitions->end();

		if (!node_has_partition) {
			node_partitions->insert(partition);
			partition_owners.get()->insert(node_id);
			std::get<0>(min_partitions_tup)++;
			to_insert_back.emplace_back(min_partitions_tup);
			break;
		}

		to_insert_back.emplace_back(min_partitions_tup);

		min_partitions_tup = this->future_cluster_metadata->nodes_partition_counts->extractTopElement();
		node_id = std::get<1>(min_partitions_tup);

		continue_looping = !this->future_cluster_metadata->nodes_partition_counts->is_null_index(node_id);

		if (!continue_looping) break;

		node_queues = this->future_cluster_metadata->nodes_partitions[node_id];

		if (node_queues == NULL) {
			node_queues = std::make_shared<std::unordered_map<std::string, std::shared_ptr<std::unordered_set<int>>>>();
			this->future_cluster_metadata->nodes_partitions[node_id] = node_queues;
		}
	}

	for (auto& tup : to_insert_back)
		this->future_cluster_metadata->nodes_partition_counts->insert(std::get<1>(tup), std::get<0>(tup));

	std::shared_ptr<PartitionAssignmentCommand> change = std::shared_ptr<PartitionAssignmentCommand>(
		new PartitionAssignmentCommand(
			queue_name,
			partition,
			node_id,
			owner_node
		)
	);

	cluster_changes->emplace_back(CommandType::ALTER_PARTITION_ASSIGNMENT, change);

	if (owner_node == -1) return;

	auto owner_node_queues = this->future_cluster_metadata->nodes_partitions[node_id].get();
	auto owner_node_queue_partitions = (*owner_node_queues)[queue_name].get();
	owner_node_queue_partitions->erase(partition);

	if (owner_node_queue_partitions->size() == 0) owner_node_queues->erase(queue_name);

	int current_partitions_count = this->future_cluster_metadata->nodes_partition_counts->remove(owner_node);
	this->future_cluster_metadata->nodes_partition_counts->insert(owner_node, current_partitions_count - 1);
	partition_owners.get()->erase(owner_node);
}

void Controller::assign_partition_leader_to_node(const std::string& queue_name, int partition, std::vector<std::tuple<CommandType, std::shared_ptr<void>>>* cluster_changes, int leader_node) {
	auto owned_partitions = this->future_cluster_metadata->owned_partitions[queue_name];
	auto partition_owners = (*(owned_partitions.get()))[partition];

	auto partitions_leaders = this->future_cluster_metadata->partition_leader_nodes[queue_name];

	if (partitions_leaders == nullptr) {
		partitions_leaders = std::make_shared<std::unordered_map<int, int>>();
		this->future_cluster_metadata->partition_leader_nodes[queue_name] = partitions_leaders;
	}

	int node_id = -1;
	int min_leader_count = MAX_QUEUE_PARTITIONS + 1;

	int this_node_id = this->settings->get_node_id(); // server's assigned id

	for (auto owner_node : *(partition_owners.get())) {
		if (leader_node == owner_node) continue;

		int leader_count = this->future_cluster_metadata->nodes_leader_partition_counts->get(owner_node);

		if (leader_count < min_leader_count) {
			min_leader_count = leader_count;
			node_id = owner_node;
		}
	}

	if (node_id == -1) return;

	(*(partitions_leaders.get()))[partition] = node_id;
	this->future_cluster_metadata->nodes_leader_partition_counts->update(node_id, min_leader_count + 1);

	std::shared_ptr<PartitionLeaderAssignmentCommand> change = std::shared_ptr<PartitionLeaderAssignmentCommand>(
		new PartitionLeaderAssignmentCommand(
			queue_name,
			partition,
			node_id,
			leader_node
		)
	);

	cluster_changes->emplace_back(CommandType::ALTER_PARTITION_LEADER_ASSIGNMENT, change);

	if (leader_node == -1) return;

	int current_leader_partitions = this->future_cluster_metadata->nodes_leader_partition_counts->remove(leader_node);
	this->future_cluster_metadata->nodes_leader_partition_counts->insert(leader_node, current_leader_partitions - 1);
}

void Controller::repartition_node_data(int node_id) {
	if (this->get_state() != NodeState::LEADER) {
		this->logger->log_warning("Stopping node's " + std::to_string(node_id) + " repartitions. Current node is not leader anymore.");
		return;
	}

	std::lock_guard<std::mutex> partition_assignment_lock(this->partition_assignment_mut);
	std::lock_guard<std::mutex> heartbeats_lock(this->heartbeats_mut);

	int partitions_count = this->future_cluster_metadata->nodes_partition_counts->remove(node_id);

	if (partitions_count > 0) {
		this->logger->log_info("Reassigning node's " + std::to_string(node_id) + " partitions...");

		std::lock_guard<std::mutex> node_partitions_lock(this->future_cluster_metadata->nodes_partitions_mut);

		std::vector<std::tuple<CommandType, std::shared_ptr<void>>> cluster_changes;

		for (auto& queue_partitions_pair : *(this->future_cluster_metadata->nodes_partitions[node_id].get())) {
			QueueMetadata* metadata = this->future_cluster_metadata->get_queue_metadata(queue_partitions_pair.first);
			bool skip_partitions_reassignment = metadata->get_replication_factor() > this->data_nodes_heartbeats.size() + 1;

			for (int partition : *(queue_partitions_pair.second.get())) {
				if (this->get_state() != NodeState::LEADER) {
					this->logger->log_warning(
						"Stopping node's " + std::to_string(node_id) + " repartitions. Node " + std::to_string(this->settings->get_node_id()) + " is not leader anymore."
					);
					return;
				}

				this->assign_partition_leader_to_node(queue_partitions_pair.first, partition, &cluster_changes, node_id);

				if (!skip_partitions_reassignment)
					this->assign_partition_to_node(queue_partitions_pair.first, partition, &cluster_changes, node_id);
			}
		}

		unsigned long long timestamp = this->util->get_current_time_milli().count();

		std::vector<Command> commands;

		for (auto& change : cluster_changes)
			commands.emplace_back(Command(
				std::get<0>(change),
				this->term,
				timestamp,
				std::get<1>(change)
			));

		this->store_commands(&commands);

		this->logger->log_info("Reassignment of node's " + std::to_string(node_id) + " partitions completed");
	}
}

ErrorCode Controller::assign_new_queue_partitions_to_nodes(std::shared_ptr<QueueMetadata> queue_metadata) {
	std::lock_guard<std::mutex> partition_assignment_lock(this->partition_assignment_mut);
	std::lock_guard<std::mutex> heatbeats_lock(this->heartbeats_mut);
	std::lock_guard<std::mutex> partitions_lock(this->future_cluster_metadata->nodes_partitions_mut);

	if (this->future_cluster_metadata->get_queue_metadata(queue_metadata.get()->get_name()) != NULL)
		return ErrorCode::NONE;

	if (queue_metadata.get()->get_replication_factor() > this->data_nodes_heartbeats.size() + 1)
		return ErrorCode::TOO_FEW_AVAILABLE_NODES;

	unsigned long long timestamp = this->util->get_current_time_milli().count();

	std::vector<std::tuple<CommandType, std::shared_ptr<void>>> cluster_changes;

	this->future_cluster_metadata->add_queue_metadata(queue_metadata);

	for (int i = 1; i <= queue_metadata->get_replication_factor(); i++)
		for (int j = 0; j < queue_metadata->get_partitions(); j++)
			this->assign_partition_to_node(queue_metadata->get_name(), j, &cluster_changes);

	for (int j = 0; j < queue_metadata->get_partitions(); j++)
		this->assign_partition_leader_to_node(queue_metadata->get_name(), j, &cluster_changes);

	std::vector<Command> commands;

	std::shared_ptr<CreateQueueCommand> command_info = std::shared_ptr<CreateQueueCommand>(
		new CreateQueueCommand(
			queue_metadata.get()->get_name(), 
			queue_metadata.get()->get_partitions(), 
			queue_metadata.get()->get_replication_factor()
		)
	);

	commands.emplace_back(Command(
		CommandType::CREATE_QUEUE,
		this->term,
		timestamp,
		command_info
	));

	for (auto& change : cluster_changes)
		commands.emplace_back(Command(
			std::get<0>(change),
			this->term,
			timestamp,
			std::get<1>(change)
		));

	this->store_commands(&commands);

	queue_metadata.get()->set_status(Status::ACTIVE);

	return ErrorCode::NONE;
}

void Controller::assign_queue_for_deletion(std::string& queue_name) {
	if (this->future_cluster_metadata->get_queue_metadata(queue_name) == NULL) return;

	this->future_cluster_metadata->remove_queue_metadata(queue_name);

	QueueMetadata* metadata = this->cluster_metadata->get_queue_metadata(queue_name);

	metadata->set_status(Status::PENDING_DELETION);

	std::vector<Command> commands = std::vector<Command>(1);
	commands[0] = Command(
		CommandType::DELETE_QUEUE,
		this->term,
		this->util->get_current_time_milli().count(),
		std::shared_ptr<DeleteQueueCommand>(new DeleteQueueCommand(queue_name))
	);

	this->store_commands(&commands);
}

void Controller::check_for_dead_data_nodes() {
	std::vector<int> expired_nodes;
	NodeState state = NodeState::LEADER;

	while (!(*this->should_terminate)) {
		expired_nodes.clear();

		try
		{
			{
				std::lock_guard<std::mutex> lock(this->heartbeats_mut);

				for (auto iter : this->data_nodes_heartbeats)
					if (this->get_state() == NodeState::LEADER) {
						if (iter.second.count() != 0
							&& this->util->has_timeframe_expired(iter.second, this->settings->get_data_node_expire_ms())
							) {
							this->data_nodes_heartbeats[iter.first] = std::chrono::milliseconds(0);
							expired_nodes.emplace_back(iter.first);
						}
					}
					else this->data_nodes_heartbeats[iter.first] = this->util->get_current_time_milli();
			}

			if (!this->settings->get_is_controller_node()) {
				std::this_thread::sleep_for(std::chrono::milliseconds(CHECK_FOR_SETTINGS_UPDATE));
				continue;
			}

			if (this->get_state() != NodeState::LEADER) {
				std::this_thread::sleep_for(std::chrono::milliseconds(this->settings->get_dead_data_node_check_ms()));
				continue;
			}

			if (expired_nodes.size() > 0)
				for (int node_id : expired_nodes) {
					if (this->get_state() != NodeState::LEADER)
						break;

					this->future_cluster_metadata->remove_node_partitions(node_id);

					if (!this->is_controller_node(node_id)) {
						Command command = Command(
							CommandType::UNREGISTER_DATA_NODE,
							this->term,
							this->util->get_current_time_milli().count(),
							std::shared_ptr<UnregisterDataNodeCommand>(new UnregisterDataNodeCommand(node_id))
						);

						std::vector<Command> commands(1);
						commands[0] = command;

						this->store_commands(&commands);
					}

					this->logger->log_info("Data node " + std::to_string(node_id) + " heartbeat expired");
				}
		}
		catch (const std::exception& ex)
		{
			std::string err_msg = "Error occured while checking for unresponsive nodes. Reason: " + std::string(ex.what());
			this->logger->log_error(err_msg);
		}

		std::this_thread::sleep_for(std::chrono::milliseconds(this->settings->get_dead_data_node_check_ms()));
	}
}

int Controller::get_active_nodes_count() {
	std::lock_guard<std::mutex> lock(this->heartbeats_mut);
	return this->data_nodes_heartbeats.size() + 1; // +1 is for current node running
}

void Controller::store_commands(std::vector<Command>* commands) {
	if (commands->size() == 0) return;

	long total_bytes = 0;

	for (auto& command : *commands) {
		command.set_metadata_version(++this->future_cluster_metadata->metadata_version);
		auto bytes_tup = command.get_metadata_bytes();
		total_bytes += std::get<0>(bytes_tup);
	}

	std::unique_ptr<char> messages_bytes = std::unique_ptr<char>(new char[total_bytes]);
	long offset = 0;

	for (auto& command : *commands) {
		auto bytes_tup = command.get_metadata_bytes();
		unsigned int command_bytes = std::get<0>(bytes_tup);
		memcpy_s(messages_bytes.get() + offset, command_bytes, std::get<1>(bytes_tup).get(), command_bytes);
		offset += command_bytes;
	}

	try
	{
		this->mh->save_messages(
			this->qm->get_queue(CLUSTER_METADATA_QUEUE_NAME).get()->get_partition(0),
			messages_bytes.get(),
			total_bytes
		);
	}
	catch (const std::exception& ex)
	{
		// TODO: Log error
		return;
	}

	auto& last_command = commands->back();

	this->last_log_index = last_command.get_metadata_version();
	this->last_log_term = last_command.get_term();
}

bool Controller::store_commands(void* commands, int total_commands, long commands_total_bytes) {
	if (total_commands <= 0 || commands_total_bytes <= 0) return true;

	unsigned long offset = 0;

	unsigned long long last_command_index = 0;
	unsigned long long last_command_term = 0;

	unsigned int messgae_bytes = 0;

	for (int i = 0; i < total_commands; i++) {
		memcpy_s(&messgae_bytes, TOTAL_METADATA_BYTES, (char*)commands + offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);

		memcpy_s(&last_command_index, MESSAGE_ID_SIZE, (char*)commands + offset + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);
		memcpy_s(&last_command_term, COMMAND_TERM_SIZE, (char*)commands + offset + COMMAND_TERM_OFFSET, COMMAND_TERM_SIZE);

		offset += messgae_bytes;
	}

	try
	{
		this->mh->save_messages(
			this->qm->get_queue(CLUSTER_METADATA_QUEUE_NAME).get()->get_partition(0),
			commands,
			commands_total_bytes
		);
	}
	catch (const std::exception& ex)
	{
		// TODO: Log error
		return false;
	}

	this->last_log_index = last_command_index;
	this->last_log_term = last_command_term;

	return true;
}

void Controller::execute_command(void* command_metadata) {
	Command command = Command(command_metadata);

	this->cmah->apply_command(this->cluster_metadata.get(), &command);

	this->last_applied = command.get_metadata_version();

	if (command.get_command_type() == CommandType::REGISTER_DATA_NODE) {
		std::lock_guard<std::mutex> lock(this->heartbeats_mut);
		std::lock_guard<std::shared_mutex> followers_lock(this->follower_indexes_mut);

		RegisterDataNodeCommand* command_info = (RegisterDataNodeCommand*)command.get_command_info();

		if (this->settings->get_node_id() == command_info->get_node_id()) return;

		this->data_nodes_heartbeats[command_info->get_node_id()] = this->util->get_current_time_milli();

		if (this->follower_indexes.find(command_info->get_node_id()) == this->follower_indexes.end())
			this->follower_indexes[command_info->get_node_id()] = std::tuple<unsigned long long, unsigned long long>(
				this->last_log_term,
				this->last_log_index
			);
	}
	else if (command.get_command_type() == CommandType::UNREGISTER_DATA_NODE) {
		std::lock_guard<std::mutex> lock(this->heartbeats_mut);
		std::lock_guard<std::shared_mutex> followers_lock(this->follower_indexes_mut);

		UnregisterDataNodeCommand* command_info = (UnregisterDataNodeCommand*)command.get_command_info();

		this->data_nodes_heartbeats.erase(command_info->get_node_id());
		this->follower_indexes.erase(command_info->get_node_id());
	}
}

void Controller::check_for_commit_and_last_applied_diff() {
	std::shared_ptr<Queue> queue = this->qm->get_queue(CLUSTER_METADATA_QUEUE_NAME);
	Partition* partition = queue.get()->get_partition(0);
	queue.reset();

	while (!(*this->should_terminate)) {
		unsigned long long commit_index = this->commit_index;

		if (commit_index <= this->last_applied) {
			std::this_thread::sleep_for(std::chrono::milliseconds(CHECK_FOR_UNAPPLIED_COMMANDS));
			continue;
		}

		try
		{
			auto& res = this->mh->read_partition_messages(partition, this->last_applied + 1);

			unsigned int total_commands = std::get<4>(res);

			if (total_commands == 0) {
				std::this_thread::sleep_for(std::chrono::milliseconds(CHECK_FOR_UNAPPLIED_COMMANDS));
				continue;
			}

			std::shared_ptr<char> commands_batch = std::get<0>(res);
			unsigned int batch_size = std::get<1>(res);
			unsigned int read_start = std::get<2>(res);
			unsigned int read_end = std::get<3>(res);

			unsigned long long metadata_version = 0;
			unsigned long long prev_metadata_version = 0;
			unsigned long long command_bytes = 0;

			unsigned int offset = read_start;

			try
			{
				while (offset < read_end) {
					memcpy_s(&metadata_version, MESSAGE_ID_SIZE, commands_batch.get() + offset + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);

					if (metadata_version > commit_index) break;

					this->execute_command(commands_batch.get() + offset);
					prev_metadata_version = metadata_version;

					memcpy_s(&command_bytes, TOTAL_METADATA_BYTES, commands_batch.get() + offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);

					offset += command_bytes;
				}
			}
			catch (const std::exception& ex)
			{
				if (prev_metadata_version > 0)
					commit_index = prev_metadata_version;

				std::string err_msg = "Error occured while executing commited commands. Reason: " + std::string(ex.what());
				this->logger->log_error(err_msg);
			}

			this->mh->update_cluster_metadata_last_applied(prev_metadata_version);
			this->last_applied = prev_metadata_version;
		}
		catch (const std::exception& ex)
		{
			std::string err_msg = "Error occured while executing commited commands. Reason: " + std::string(ex.what());
			this->logger->log_error(err_msg);
		}

		std::this_thread::sleep_for(std::chrono::milliseconds(CHECK_FOR_UNAPPLIED_COMMANDS));
	}
}

ClusterMetadata* Controller::get_compacted_cluster_metadata() {
	return this->compacetd_cluster_metadata.get();
}

ClusterMetadata* Controller::get_cluster_metadata() {
	return this->cluster_metadata.get();
}

ClusterMetadata* Controller::get_future_cluster_metadata() {
	return this->future_cluster_metadata.get();
}

std::shared_ptr<AppendEntriesRequest> Controller::prepare_append_entries_request(int follower_id) {
	std::shared_lock<std::shared_mutex> lock(this->follower_indexes_mut);

	bool follower_is_registered = this->follower_indexes.find(follower_id) != this->follower_indexes.end();

	std::shared_ptr<AppendEntriesRequest> req = std::make_shared<AppendEntriesRequest>();
	req.get()->leader_id = this->settings->get_node_id();
	req.get()->term = this->term;
	req.get()->leader_commit = this->commit_index;
	req.get()->prev_log_term = follower_is_registered ? std::get<0>(this->follower_indexes[follower_id]) : this->last_log_term;
	req.get()->prev_log_index = follower_is_registered ? std::get<1>(this->follower_indexes[follower_id]) : this->last_log_index;

	unsigned long long index_to_send = req.get()->prev_log_index + 1;

	if (index_to_send > this->last_log_index) {
		req.get()->total_commands = 0;
		req.get()->commands_total_bytes = 0;
		req.get()->commands_data = NULL;
		return req;
	}

	std::shared_ptr<Queue> queue = this->qm->get_queue(CLUSTER_METADATA_QUEUE_NAME);

	auto messages_res = this->mh->read_partition_messages(queue.get()->get_partition(0), index_to_send);

	req.get()->total_commands = std::get<4>(messages_res);
	req.get()->commands_total_bytes = std::get<3>(messages_res) - std::get<2>(messages_res);
	req.get()->commands_data = std::get<4>(messages_res) == 0 ? NULL : std::get<0>(messages_res).get() + std::get<2>(messages_res);
	// the below line keeps commands to memory until we go to next follower to prepare request
	req.get()->commands_data_ptr = std::get<4>(messages_res) == 0 ? nullptr : std::get<0>(messages_res);

	return req;
}

unsigned long long Controller::get_largest_replicated_index(std::vector<unsigned long long>* largest_indexes_sent) {
	if (largest_indexes_sent->size() == 0) return 0;

	std::sort(largest_indexes_sent->begin(), largest_indexes_sent->end());

	return (*largest_indexes_sent)[this->half_quorum_nodes_count - 2];
}

int Controller::get_partition_leader(const std::string& queue, int partition) {
	return this->cluster_metadata->get_partition_leader(queue, partition);
}

std::shared_ptr<AppendEntriesRequest> Controller::get_cluster_metadata_updates(GetClusterMetadataUpdateRequest* request) {
	{
		std::lock_guard<std::shared_mutex> lock(this->follower_indexes_mut);

		if (this->follower_indexes.find(request->node_id) == this->follower_indexes.end()) return nullptr;

		unsigned long long prev_log_index = std::get<1>(this->follower_indexes[request->node_id]);

		if (!request->is_first_request && !request->prev_req_index_matched) {
			if (prev_log_index - 1 > 0) {
				auto messages_res = this->mh->read_partition_messages(
					this->qm->get_queue(CLUSTER_METADATA_QUEUE_NAME).get()->get_partition(0),
					prev_log_index - 1,
					1
				);

				if (std::get<4>(messages_res) == 1) {
					unsigned long long prev_log_index = 0;
					unsigned long long prev_log_term = 0;

					char* message_offset = std::get<0>(messages_res).get() + std::get<2>(messages_res);

					memcpy_s(&prev_log_index, MESSAGE_ID_SIZE, message_offset + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);
					memcpy_s(&prev_log_term, COMMAND_TERM_SIZE, message_offset + COMMAND_TERM_OFFSET, COMMAND_TERM_SIZE);

					std::lock_guard<std::shared_mutex> lock(this->follower_indexes_mut);
					this->follower_indexes[request->node_id] = std::tuple<unsigned long long, unsigned long long>(
						prev_log_term, prev_log_index
					);
				}
				else throw std::exception("Something went wrong while trying to send cluster metadata updates to data node");
			}
			else this->follower_indexes[request->node_id] = std::tuple<unsigned long long, unsigned long long>(0, 0);
		}
		else if(!request->is_first_request && request->prev_log_index > 0)
			this->follower_indexes[request->node_id] = std::tuple<unsigned long long, unsigned long long>(
				request->prev_log_term, request->prev_log_index
			);
	}

	auto res = this->prepare_append_entries_request(request->node_id);
	res.get()->leader_id = this->cluster_metadata->get_leader_id();

	return res;
}

unsigned long long Controller::get_last_command_applied() {
	return this->last_applied;
}

bool Controller::is_controller_node(int node_id) {
	return this->controller_nodes_ids.find(node_id) != this->controller_nodes_ids.end();
}

unsigned long long Controller::get_last_log_index() {
	return this->last_log_index;
}

unsigned long long Controller::get_last_log_term() {
	return this->last_log_term;
}