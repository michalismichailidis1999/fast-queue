#pragma once
#include <memory>
#include <atomic>
#include <chrono>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <tuple>
#include <mutex>
#include <string>
#include "./Controller.h"
#include "../network_management/ConnectionsManager.h"
#include "../queue_management/QueueMetadata.h"
#include "../queue_management/QueueManager.h"
#include "../queue_management/Queue.h"
#include "../queue_management/Partition.h"
#include "../queue_management/messages_management/MessagesHandler.h"
#include "../queue_management/messages_management/MessageOffsetAckHandler.h"
#include "./ClusterMetadata.h"
#include "../util/ConnectionPool.h"
#include "../util/Util.h"
#include "../requests_management/Responses.h"
#include "../requests_management/Requests.h"
#include "../requests_management/ResponseMapper.h"
#include "../requests_management/RequestMapper.h"
#include "../requests_management/ClassToByteTransformer.h"
#include "../file_management/FileHandler.h"
#include "../Settings.h"
#include "../Enums.h"
#include "../logging/Logger.h"

#include "../__linux/memcpy_s.h"

struct Connection;

class DataNode {
private:
	Controller* controller;
	ConnectionsManager* cm;
	QueueManager* qm;
	MessagesHandler* mh;
	MessageOffsetAckHandler* moah;
	RequestMapper* request_mapper;
	ResponseMapper* response_mapper;
	ClassToByteTransformer* transformer;
	Util* util;
	FileHandler* fh;
	Settings* settings;
	Logger* logger;

	bool send_heartbeat_to_leader(int* leader_id, char* req_buf, long req_buf_size, ConnectionPool* pool);

	int get_next_leader_id(int leader_id);

	std::shared_ptr<ConnectionPool> get_leader_connection_pool(int leader_id);

	std::unordered_map<unsigned long long, std::tuple<std::chrono::milliseconds, std::string, std::string>> consumer_heartbeats;
	std::unordered_set<unsigned long long> expired_consumers;
	std::mutex consumers_mut;

	std::unordered_map<std::string, std::chrono::milliseconds> follower_heartbeats;
	std::shared_mutex follower_heartbeats_mut;

	void handle_fetch_messages_res(Partition* partition, FetchMessagesResponse* res);

	void set_partition_offset_to_prev_loc(Partition* partition, unsigned long long prev_loc);

	std::string get_follower_heartbeat_key(const std::string& queue_name, int partition, int node_id);
public:
	DataNode(Controller* controller, ConnectionsManager* cm, QueueManager* qm, MessagesHandler* mh, MessageOffsetAckHandler* moah, RequestMapper* request_mapper, ResponseMapper* response_mapper, ClassToByteTransformer* transformer, Util* util, FileHandler* fh, Settings* settings, Logger* logger);

	void send_heartbeats_to_leader(std::atomic_bool* should_terminate);

	void retrieve_cluster_metadata_updates(std::atomic_bool* should_terminate);

	void update_consumer_heartbeat(const std::string& queue_name, const std::string& group_id, unsigned long long consumer_id);

	bool has_consumer_expired(unsigned long long consumer_id);

	void check_for_dead_consumer(std::atomic_bool* should_terminate);

	void fetch_data_from_partition_leaders(std::atomic_bool* should_terminate);

	void check_for_lagging_followers(std::atomic_bool* should_terminate);

	void update_follower_heartbeat(const std::string& queue_name, int partition, int node_id);
};