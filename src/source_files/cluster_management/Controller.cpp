#include "../../header_files/cluster_management/Controller.h"

Controller::Controller(ConnectionsManager* cm, QueueManager* qm, MessagesHandler* mh, ResponseMapper* response_mapper, ClassToByteTransformer* transformer, Util* util, Logger* logger, Settings* settings, ClusterMetadata* cluster_metadata, ClusterMetadata* future_cluster_metadata, std::atomic_bool* should_terminate)
	: generator(std::random_device{}()), distribution(HEARTBEAT_SIGNAL_MIN_BOUND, HEARTBEAT_SIGNAL_MAX_BOUND)
{
	this->cm = cm;
	this->qm = qm;
	this->mh = mh;
	this->response_mapper = response_mapper;
	this->transformer = transformer;
	this->util = util;
	this->logger = logger;
	this->cluster_metadata = cluster_metadata;
	this->future_cluster_metadata = future_cluster_metadata;
	this->settings = settings;
	this->should_terminate = should_terminate;

	this->is_the_only_controller_node = this->settings->get_controller_nodes()->size() == 1;

	this->vote_for = -1;

	this->state = !this->is_the_only_controller_node ? NodeState::FOLLOWER : NodeState::LEADER;
	this->received_heartbeat = false;

	if (this->state == NodeState::LEADER) {
		this->cluster_metadata->set_leader_id(this->settings->get_node_id());
		this->logger->log_info("Is single controller node. Initialized as leader.");
	}

	this->term = 0;
	this->commit_index = 0;
	this->last_applied = 0;
	this->last_log_index = 0;
	this->last_log_term = 0;

	this->first_cached_log_index = 0;

	this->half_quorum_nodes_count = this->settings->get_controller_nodes()->size() / 2 + 1;

	this->dummy_node_id = -1;
}

void Controller::run_controller_quorum_communication() {
	while (!(*this->should_terminate)) {
		if (this->is_the_only_controller_node) {
			for(auto& command : this->log)
				this->execute_command(command.get());

			this->log.clear();

			std::this_thread::sleep_for(std::chrono::milliseconds(3000));
			continue;
		}

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
}

void Controller::start_election() {
	int expected = -1;

	if (!this->vote_for.compare_exchange_weak(expected, this->settings->get_node_id())) {
		this->set_state(NodeState::FOLLOWER);
		this->logger->log_info("Already vote for another candidate. Returning back to follower state");
		return;
	}

	std::lock_guard<std::mutex> lock(*this->cm->get_controller_node_connections_mut());

	auto controller_node_connections = this->cm->get_controller_node_connections(false);

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
		std::shared_ptr<Connection> connection = iter.second.get()->get_connection();

		if (connection.get() == NULL) {
			this->logger->log_error(
				"Could not request vote from node " + std::to_string(iter.first) + ". No connections left in connection pool."
			);
			continue;
		}

		std::tuple<std::shared_ptr<char>, long, bool> res_tup = this->cm->send_request_to_socket(
			connection.get()->socket,
			connection.get()->ssl,
			std::get<1>(buf_tup).get(),
			std::get<0>(buf_tup),
			"RequestVote"
		);

		if (std::get<1>(res_tup) == -1) {
			iter.second.get()->add_connection(std::get<2>(res_tup) ? nullptr : connection, true);

			if (std::get<2>(res_tup)) {
				this->cm->remove_socket_connection_heartbeat(connection.get()->socket);
				this->logger->log_error("Network issue occured while communicating with node " + std::to_string(iter.first));
			}

			continue;
		}

		std::unique_ptr<RequestVoteResponse> res = this->response_mapper->to_request_vote_response(
			std::get<0>(res_tup).get(),
			std::get<1>(res_tup)
		);

		iter.second.get()->add_connection(connection, true);

		if (res.get() == NULL) {
			this->logger->log_error("Invalid mapping value in RequestVoteResponse type");
			continue;
		}

		if (res.get()->vote_granted) {
			votes++;
			this->logger->log_info("Vote granted from node " + std::to_string(iter.first));
		}
		else {
			long long prev_term = this->term;
			this->term = res.get()->term;
			req.get()->term = res.get()->term;
			buf_tup = this->transformer->transform(req.get());
			this->logger->log_info(
				"Vote not granted from node " 
				+ std::to_string(iter.first) 
				+ ". Updating term from " 
				+ std::to_string(prev_term) 
				+ " to " 
				+ std::to_string(res.get()->term)
			);
		}
	}

	this->vote_for = -1;

	if (votes < this->half_quorum_nodes_count) {
		this->set_state(NodeState::FOLLOWER);
		this->logger->log_info("Went back to follower");
		return;
	}

	this->future_cluster_metadata->copy_from(this->cluster_metadata);
	this->set_state(NodeState::LEADER);
	this->cluster_metadata->set_leader_id(this->settings->get_node_id());
	this->logger->log_info("Elected as leader");
}

void Controller::append_entries_to_followers() {
	std::unique_lock<std::mutex> connections_lock(*this->cm->get_controller_node_connections_mut());
	std::unique_lock<std::mutex> log_lock(this->log_mut);

	auto controller_node_connections = this->cm->get_controller_node_connections(false);

	std::unique_ptr<AppendEntriesRequest> req = std::make_unique<AppendEntriesRequest>();
	req.get()->leader_id = this->settings->get_node_id();
	req.get()->term = this->term;
	req.get()->leader_commit = this->commit_index;
	req.get()->prev_log_index = 0;
	req.get()->prev_log_term = 0;

	long commands_total_bytes = 0;
	int total_commands = 0;

	long remaining_messsage_bytes = this->settings->get_max_message_size();

	remaining_messsage_bytes -= 2 * sizeof(long) - sizeof(RequestType) - sizeof(int) * 2 - sizeof(unsigned long long) * 4 - sizeof(RequestValueKey) * 8;

	for (auto& command : this->log) {
		long command_bytes = 0;

		memcpy_s(&command_bytes, sizeof(long), command.get() + TOTAL_METADATA_BYTES_OFFSET, sizeof(long));

		remaining_messsage_bytes -= command_bytes;

		if (remaining_messsage_bytes < 0) break;

		commands_total_bytes += command_bytes;

		total_commands++;
	}

	std::unique_ptr<char> commands = std::unique_ptr<char>(new char[commands_total_bytes]);
	long offset = 0;

	for (int i = 0; i < total_commands; i++) {
		long command_bytes = 0;
		memcpy_s(&command_bytes, sizeof(long), this->log[i].get() + TOTAL_METADATA_BYTES_OFFSET, sizeof(long));
		memcpy_s(commands.get() + offset, command_bytes, this->log[i].get(), command_bytes);
		offset += command_bytes;
	}

	req.get()->total_commands = total_commands;
	req.get()->commands_total_bytes = commands_total_bytes;
	req.get()->commands_data = commands.get();

	std::tuple<long, std::shared_ptr<char>> buf_tup = this->transformer->transform(req.get());

	int replication_count = 1;

	for (auto iter : *controller_node_connections) {
		std::shared_ptr<Connection> connection = iter.second.get()->get_connection();

		if (connection.get() == NULL) {
			this->logger->log_error(
				"Could not append entries to node " + std::to_string(iter.first) + ". No connections left in connection pool."
			);
			continue;
		}

		std::tuple<std::shared_ptr<char>, long, bool> res_tup = this->cm->send_request_to_socket(
			connection.get()->socket,
			connection.get()->ssl,
			std::get<1>(buf_tup).get(),
			std::get<0>(buf_tup),
			"AppendEntries"
		);

		if (std::get<1>(res_tup) == -1) {
			iter.second.get()->add_connection(std::get<2>(res_tup) ? nullptr : connection, true);

			if (std::get<2>(res_tup)) {
				this->cm->remove_socket_connection_heartbeat(connection.get()->socket);
				this->logger->log_error("Network issue occured while communicating with node " + std::to_string(iter.first));
			}

			continue;
		}

		std::unique_ptr<AppendEntriesResponse> res = this->response_mapper->to_append_entries_response(
			std::get<0>(res_tup).get(),
			std::get<1>(res_tup)
		);

		iter.second.get()->add_connection(connection, true);

		if (res.get() == NULL) {
			this->logger->log_error("Invalid mapping value in AppendEntriesResponse type");
			continue;
		}

		if (!res.get()->success) {
			this->logger->log_error("Node " + std::to_string(iter.first) + " rejected AppendEntries request. Setting term to " + std::to_string(res.get()->term) + " and going back to being a Follower.");
			this->term = res.get()->term;
			this->set_state(NodeState::FOLLOWER);
			return;
		}
		else replication_count++;
	}

	if (replication_count >= this->half_quorum_nodes_count) {
		for (int i = 0; i < total_commands; i++)
			this->execute_command(this->log[i].get());

		this->log.erase(this->log.begin(), this->log.begin() + total_commands);
	}

	log_lock.unlock();
	connections_lock.unlock();

	std::this_thread::sleep_for(std::chrono::milliseconds(LEADER_TIMEOUT));
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

std::shared_ptr<AppendEntriesResponse> Controller::handle_leader_append_entries(AppendEntriesRequest* request) {
	std::shared_ptr<AppendEntriesResponse> res = std::make_shared<AppendEntriesResponse>();
	res.get()->success = false;
	res.get()->term = -1;

	if (request->term >= this->term && request->prev_log_term >= this->last_log_term && request->prev_log_index >= this->last_log_index) {
		this->set_received_heartbeat(true);

		res.get()->success = true;

		if (this->get_state() == NodeState::LEADER) {
			this->logger->log_info("Received heartbeat from another leader with higher term and log entries. Stepping down to follower");
			this->set_state(NodeState::FOLLOWER);
			this->cluster_metadata->set_leader_id(request->leader_id);
		} else this->logger->log_info("Received heartbeat from leader");

		// TODO: Append log entries

		this->term = request->term;
		this->commit_index = request->leader_commit;
	} res.get()->term = this->term;

	return res;
}

std::shared_ptr<RequestVoteResponse> Controller::handle_candidate_request_vote(RequestVoteRequest* request) {
	std::shared_ptr<RequestVoteResponse> res = std::make_shared<RequestVoteResponse>();

	res.get()->term = this->term;

	int expected = -1;

	res.get()->vote_granted = request->term >= this->term
		&& (request->last_log_term > this->last_log_term
			|| request->last_log_index >= this->last_log_index)
		&& (this->vote_for.compare_exchange_weak(expected, request->candidate_id)
			|| this->vote_for == request->candidate_id);

	if (!res.get()->vote_granted)
		this->logger->log_info("Vote was not granted for candidate " + std::to_string(request->candidate_id));
	else
		this->logger->log_info("Vote was granted for candidate " + std::to_string(request->candidate_id));
		
		
	if (res.get()->vote_granted && this->get_state() == NodeState::LEADER) {
		this->logger->log_info("Vote for other candidate. Stepping down as Leader and becoming Follower");
		this->set_received_heartbeat(true);
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

void Controller::update_data_node_heartbeat(int node_id, bool is_first_connection) {
	std::lock_guard<std::mutex> lock(this->heartbeats_mut);

	this->data_nodes_heartbeats[node_id] = this->util->get_current_time_milli();

	if (is_first_connection) {
		this->cluster_metadata->init_node_partitions(node_id);
		this->future_cluster_metadata->init_node_partitions(node_id);
	}

	this->logger->log_info("Node " + std::to_string(node_id) + " heartbeat updated");
}

void Controller::assign_partition_to_node(const std::string& queue_name, int partition, std::vector<std::tuple<CommandType, std::shared_ptr<void>>>* cluster_changes, int owner_node) {
	std::vector<std::tuple<int, int>> to_insert_back;

	std::tuple<int, int> min_partitions_tup = this->future_cluster_metadata->nodes_partition_counts->extractTopElement();
	int node_id = std::get<1>(min_partitions_tup);

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

		if (owner_node != this_node_id && this->data_nodes_heartbeats.find(owner_node) == this->data_nodes_heartbeats.end())
			continue;

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
		this->logger->log_warning("Stopping node's " + std::to_string(node_id) + " repartitions. Current not is not leader anymore.");
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
			unsigned int replication_factor = metadata->get_replication_factor();
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

		this->insert_commands_to_log(&commands);

		this->logger->log_info("Reassignment of node's " + std::to_string(node_id) + " partitions completed");
	}

	this->data_nodes_heartbeats.erase(node_id);
}

void Controller::assign_new_queue_partitions_to_nodes(std::shared_ptr<QueueMetadata> queue_metadata) {
	if (this->get_state() != NodeState::LEADER) return;

	std::lock_guard<std::mutex> partition_assignment_lock(this->partition_assignment_mut);
	std::lock_guard<std::mutex> heatbeats_lock(this->heartbeats_mut);
	std::lock_guard<std::mutex> partitions_lock(this->future_cluster_metadata->nodes_partitions_mut);

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

	this->insert_commands_to_log(&commands);
}

void Controller::check_for_dead_data_nodes() {
	std::vector<int> nodes_data_to_repartition;
	NodeState state = NodeState::LEADER;

	while (!(*this->should_terminate)) {
		state = this->get_state();

		nodes_data_to_repartition.clear();

		{
			std::lock_guard<std::mutex> lock(this->heartbeats_mut);

			for (auto iter : this->data_nodes_heartbeats)
				if (state == NodeState::LEADER) {
					if (this->util->has_timeframe_expired(iter.second, 10000))
						nodes_data_to_repartition.emplace_back(iter.first);
				}
				else this->data_nodes_heartbeats[iter.first] = this->util->get_current_time_milli();
		}

		if (state != NodeState::LEADER) {
			std::this_thread::sleep_for(std::chrono::milliseconds(5000));
			continue;
		}

		if (nodes_data_to_repartition.size() > 0)
			for (int node_id : nodes_data_to_repartition) {
				this->repartition_node_data(node_id);
				this->logger->log_info("Data node " + std::to_string(node_id) + " heartbeat expired");
			}

		std::this_thread::sleep_for(std::chrono::milliseconds(5000));
	}
}

bool Controller::is_data_node_alive(int node_id) {
	std::lock_guard<std::mutex> lock(this->heartbeats_mut);

	if (this->data_nodes_heartbeats.find(node_id) == this->data_nodes_heartbeats.end()) return false;

	return !this->util->has_timeframe_expired(this->data_nodes_heartbeats[node_id], 10000);
}

int Controller::get_active_nodes_count() {
	std::lock_guard<std::mutex> lock(this->heartbeats_mut);
	return this->data_nodes_heartbeats.size() + 1; // +1 is for current node running
}

void Controller::insert_commands_to_log(std::vector<Command>* commands) {
	std::lock_guard<std::mutex> lock(this->log_mut);

	if (commands->size() == 0) return;

	for (auto& command : *commands) {
		command.set_metadata_version(++this->future_cluster_metadata->metadata_version);
		this->log.emplace_back(std::get<1>(command.get_metadata_bytes()));
	}
}

void Controller::execute_command(void* command_metadata) {
	Command command = Command(command_metadata);

	this->cluster_metadata->apply_command(&command);

	switch (command.get_command_type()) {
	case CommandType::CREATE_QUEUE:
		this->execute_create_queue_command((CreateQueueCommand*)command.get_command_info());
		break;
	default:
		break;
	}

	this->commit_index = command.get_metadata_version();
}

void Controller::execute_create_queue_command(CreateQueueCommand* command) {
	QueueMetadata* metadata = this->cluster_metadata->get_queue_metadata(command->get_queue_name());

	if (metadata == NULL) {
		std::shared_ptr<QueueMetadata> new_metadata = std::shared_ptr<QueueMetadata>(
			new QueueMetadata(command->get_queue_name(), command->get_partitions(), command->get_replication_factor())
		);

		new_metadata.get()->set_status(Status::PENDING_CREATION);

		metadata = new_metadata.get();
	}

	this->qm->create_queue(metadata);

	metadata->set_status(Status::ACTIVE);
}