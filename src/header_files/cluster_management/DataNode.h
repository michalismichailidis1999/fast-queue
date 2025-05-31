#pragma once
#include <memory>
#include "./Controller.h"
#include "../network_management/ConnectionsManager.h"
#include "./ClusterMetadata.h"
#include "../util/ConnectionPool.h"
#include "../requests_management/ResponseMapper.h"
#include "../requests_management/ClassToByteTransformer.h"
#include "../Settings.h"
#include "../Enums.h"
#include "../logging/Logger.h"

struct Connection;

class DataNode {
private:
	Controller* controller;
	ConnectionsManager* cm;
	ResponseMapper* response_mapper;
	ClassToByteTransformer* transformer;
	Settings* settings;
	Logger* logger;

	bool is_controller_node();

	void notify_controller_about_node_existance(int node_id, char* req_buf, long req_buf_size, ConnectionPool* pool, std::queue<std::pair<int, ConnectionPool*>>* failed);

	bool send_heartbeat_to_leader(int* leader_id, char* req_buf, long req_buf_size, ConnectionPool* pool);

	int get_next_leader_id(int leader_id);
public:
	DataNode(Controller* controller, ConnectionsManager* cm, ResponseMapper* response_mapper, ClassToByteTransformer* transformer, Settings* settings, Logger* logger);

	void notify_controllers_about_node_existance(std::atomic_bool* should_terminate);

	void send_heartbeats_to_leader(std::atomic_bool* should_terminate);
};