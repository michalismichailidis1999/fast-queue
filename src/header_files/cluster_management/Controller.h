#pragma once
#include <atomic>
#include <random>
#include <mutex>
#include <memory>
#include <tuple>
#include <chrono>
#include <thread>
#include "../logging/Logger.h"
#include "../Settings.h"
#include "../network_management/ConnectionsManager.h"
#include "../requests_management/ResponseMapper.h"
#include "../queue_management/messages_management/MessagesHandler.h"
#include "../queue_management/QueueManager.h"
#include "../queue_management/QueueMetadata.h"
#include "../queue_management/TransactionHandler.h"
#include "../requests_management/ClassToByteTransformer.h"
#include "./ClusterMetadata.h"
#include "./ClusterMetadataApplyHandler.h"
#include "./Commands.h"
#include "../Enums.h"
#include "../Constants.h"
#include "../util/Util.h"

#include "../__linux/memcpy_s.h"

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
	ClusterMetadataApplyHandler* cmah;
	TransactionHandler* th;
	Util* util;
	Logger* logger;
	Settings* settings;
	ClassToByteTransformer* transformer;

	std::unique_ptr<ClusterMetadata> cluster_metadata;
	std::unique_ptr<ClusterMetadata> future_cluster_metadata;

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

	std::unordered_map<int, std::tuple<unsigned long long, unsigned long long>> follower_indexes;
	std::shared_mutex follower_indexes_mut;

	std::map<int, std::chrono::milliseconds> data_nodes_heartbeats;
	std::mutex heartbeats_mut;

	std::mutex partition_assignment_mut;

	std::mutex append_enties_mut;

	std::unordered_set<int> controller_nodes_ids;

	void start_election();
	void append_entries_to_followers();
	void wait_for_leader_heartbeat();

	void step_down_to_follower();

	void set_state(NodeState state);
	NodeState get_state();

	void set_received_heartbeat(bool received_heartbeat);

	bool assign_partition_to_node(const std::string& queue_name, int partition, std::vector<std::tuple<CommandType, std::shared_ptr<void>>>* cluster_changes, int owner_node = -1);
	bool assign_partition_leader_to_node(const std::string& queue_name, int partition, std::vector<std::tuple<CommandType, std::shared_ptr<void>>>* cluster_changes, int leader_node = -1);

	bool repartition_node_data(int node_id);
	void set_new_followers_to_lagging_after_repartition(std::vector<std::tuple<CommandType, std::shared_ptr<void>>>* cluster_changes);

	void store_commands(std::vector<Command>* commands);
	bool store_commands(void* commands, int total_commands, long commands_total_bytes);

	void execute_command(void* command_metadata);

	std::shared_ptr<AppendEntriesRequest> prepare_append_entries_request(int follower_id);

	bool is_controller_node(int node_id);
public:
	Controller(ConnectionsManager* cm, QueueManager* qm, MessagesHandler* mh, ClusterMetadataApplyHandler* cmah, ResponseMapper* response_mapper, ClassToByteTransformer* transformer, Util* util, Logger* logger, Settings* settings, std::atomic_bool* should_terminate);

	void set_transaction_handler(TransactionHandler* th);

	void update_quorum_communication_values();

	void init_commit_index_and_last_applied();
	
	std::shared_ptr<AppendEntriesResponse> handle_leader_append_entries(AppendEntriesRequest* request, bool from_data_node = false);
	std::shared_ptr<RequestVoteResponse> handle_candidate_request_vote(RequestVoteRequest* request);

	void run_controller_quorum_communication();

	void check_for_dead_data_nodes();

	int get_leader_id();
	void update_data_node_heartbeat(int node_id, ConnectionInfo* info, bool is_controller_node = false);

	ErrorCode assign_new_queue_partitions_to_nodes(std::shared_ptr<QueueMetadata> queue_metadata);

	void assign_queue_for_deletion(std::string& queue_name);

	int get_active_nodes_count();

	void check_for_commit_and_last_applied_diff();

	int get_partition_leader(const std::string& queue, int partition);

	bool is_node_partition_owner(const std::string& queue, int partition, int node_id);

	unsigned long long get_queue_partition_unique_leader_id(const std::string& queue, int partition);

	ClusterMetadata* get_cluster_metadata();

	ClusterMetadata* get_future_cluster_metadata();

	std::shared_ptr<AppendEntriesRequest> get_cluster_metadata_updates(GetClusterMetadataUpdateRequest* request);

	unsigned long long get_last_command_applied();

	unsigned long long get_last_log_index();

	unsigned long long get_last_log_term();

	unsigned long long assign_consumer_group_to_partitions(RegisterConsumerRequest* request, Queue* queue, const std::string& group_id);

	void find_consumer_assigned_partitions(const std::string& queue_name, const std::string& group_id, unsigned long long consumer_id, std::vector<int>* partitions_list);

	unsigned long long get_last_registered_consumer_id();

	void handle_consumers_expiration(ExpireConsumersRequest* request);

	void add_lagging_follower(AddLaggingFollowerRequest* request);

	void remove_lagging_follower(RemoveLaggingFollowerRequest* request);

	unsigned long long get_largest_replicated_index(std::vector<unsigned long long>* largest_indexes_sent, int half_nodes_count);

	void add_replicated_message_offset(const std::string& leader_key, int node_id, unsigned long long message_offset);

	void get_replicated_message_offsets(const std::string& queue_name, const std::string& leader_key, std::vector<unsigned long long>* offsets);

	std::shared_ptr<RegisterTransactionGroupResponse> register_transaction_group(RegisterTransactionGroupRequest* request);

	void unregister_transaction_group(int node_id, unsigned long long transaction_group_id);
};