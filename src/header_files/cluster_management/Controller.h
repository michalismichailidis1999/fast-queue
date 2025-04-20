#pragma once
#include <atomic>
#include <random>
#include <mutex>
#include <memory>
#include <tuple>
#include <chrono>
#include "../logging/Logger.h"
#include "../Settings.h"
#include "../network_management/ConnectionsManager.h"
#include "../requests_management/ResponseMapper.h"
#include "../queue_management/messages_management/MessagesHandler.h"
#include "../queue_management/QueueManager.h"
#include "../queue_management/QueueMetadata.h"
#include "../requests_management/ClassToByteTransformer.h"
#include "./ClusterMetadata.h"
#include "./Commands.h"
#include "../Enums.h"
#include "../Constants.h"
#include "../util/Util.h"

struct AppendEntriesRequest;
struct AppendEntriesResponse;
struct RequestVoteRequest;
struct RequestVoteResponse;
struct Connection;

class Controller {
private:
	ConnectionsManager* cm;
	ResponseMapper* response_mapper;
	QueueManager* qm;
	MessagesHandler* mh;
	Util* util;
	Logger* logger;
	Settings* settings;
	ClassToByteTransformer* transformer;

	ClusterMetadata* cluster_metadata;
	ClusterMetadata* future_cluster_metadata;

	bool is_the_only_controller_node;
	int half_quorum_nodes_count;

	std::mt19937 generator;
	std::uniform_int_distribution<int> distribution;

	std::atomic_bool* should_terminate;

	NodeState state;
	std::mutex state_mut;

	bool received_heartbeat;
	std::mutex heartbeat_mut;
	std::condition_variable heartbeat_condition;

	std::atomic_int vote_for;

	std::atomic<unsigned long long> term;
	std::atomic<unsigned long long> commit_index;
	std::atomic<unsigned long long> last_applied;
	std::atomic<unsigned long long> last_log_index;
	std::atomic<unsigned long long> last_log_term;

	std::vector<std::shared_ptr<char>> log;
	std::mutex log_mut;

	long long first_cached_log_index;

	std::map<int, std::chrono::milliseconds> data_nodes_heartbeats;
	std::mutex heartbeats_mut;

	std::mutex partition_assignment_mut;
	int dummy_node_id;

	void start_election();
	void append_entries_to_followers();
	void wait_for_leader_heartbeat();

	void set_state(NodeState state);
	NodeState get_state();

	void set_received_heartbeat(bool received_heartbeat);

	void assign_partition_to_node(const std::string& queue_name, int partition, std::vector<std::tuple<CommandType, std::shared_ptr<void>>>* cluster_changes, int owner_node = -1);
	void assign_partition_leader_to_node(const std::string& queue_name, int partition, std::vector<std::tuple<CommandType, std::shared_ptr<void>>>* cluster_changes, int leader_node = -1);

	void repartition_node_data(int node_id);

	void rollback_cluster_metadata_changes(std::vector<std::tuple<CommandType, std::shared_ptr<void>>>* cluster_changes);

	void insert_commands_to_log(std::vector<Command>* commands);

	void apply_command(void* command_metadata, bool execute_command = true);

	void execute_create_queue_command(CreateQueueCommand* command);
public:
	Controller(ConnectionsManager* cm, QueueManager* qm, MessagesHandler* mh, ResponseMapper* response_mapper, ClassToByteTransformer* transformer, Util* util, Logger* logger, Settings* settings, ClusterMetadata* cluster_metadata, ClusterMetadata* future_cluster_metadata, std::atomic_bool* should_terminate);

	std::shared_ptr<AppendEntriesResponse> handle_leader_append_entries(AppendEntriesRequest* request);
	std::shared_ptr<RequestVoteResponse> handle_candidate_request_vote(RequestVoteRequest* request);

	void run_controller_quorum_communication();

	void check_for_dead_data_nodes();
	bool is_data_node_alive(int node_id);

	int get_leader_id();
	void update_data_node_heartbeat(int node_id, bool is_first_connection = false);

	void assign_new_queue_partitions_to_nodes(std::shared_ptr<QueueMetadata> queue_metadata);

	int get_active_nodes_count();
};