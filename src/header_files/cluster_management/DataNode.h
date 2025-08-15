#pragma once
#include <memory>
#include <chrono>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <tuple>
#include <mutex>
#include "./Controller.h"
#include "../network_management/ConnectionsManager.h"
#include "./ClusterMetadata.h"
#include "../util/ConnectionPool.h"
#include "../util/Util.h"
#include "../requests_management/ResponseMapper.h"
#include "../requests_management/RequestMapper.h"
#include "../requests_management/ClassToByteTransformer.h"
#include "../Settings.h"
#include "../Enums.h"
#include "../logging/Logger.h"

#include "../__linux/memcpy_s.h"

struct Connection;

class DataNode {
private:
	Controller* controller;
	ConnectionsManager* cm;
	RequestMapper* request_mapper;
	ResponseMapper* response_mapper;
	ClassToByteTransformer* transformer;
	Util* util;
	Settings* settings;
	Logger* logger;

	bool send_heartbeat_to_leader(int* leader_id, char* req_buf, long req_buf_size, ConnectionPool* pool);

	int get_next_leader_id(int leader_id);

	std::unordered_map<unsigned long long, std::tuple<std::chrono::milliseconds, std::shared_ptr<Consumer>>> consumer_heartbeats;
	std::unordered_set<unsigned long long> expired_consumers;
	std::mutex consumers_mut;
public:
	DataNode(Controller* controller, ConnectionsManager* cm, RequestMapper* request_mapper, ResponseMapper* response_mapper, ClassToByteTransformer* transformer, Util* util, Settings* settings, Logger* logger);

	void send_heartbeats_to_leader(std::atomic_bool* should_terminate);

	void retrieve_cluster_metadata_updates(std::atomic_bool* should_terminate);

	void update_consumer_heartbeat(std::shared_ptr<Consumer> consumer);

	bool has_consumer_expired(unsigned long long consumer_id);

	void check_for_dead_consumer(std::atomic_bool* should_terminate);
};