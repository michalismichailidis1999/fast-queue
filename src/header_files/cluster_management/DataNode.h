#pragma once
#include <memory>
#include "./Controller.h"
#include "../network_management/ConnectionsManager.h"
#include "./ClusterMetadata.h"
#include "../util/ConnectionPool.h"
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
	Settings* settings;
	Logger* logger;

	bool send_heartbeat_to_leader(int* leader_id, char* req_buf, long req_buf_size, ConnectionPool* pool);

	int get_next_leader_id(int leader_id);
public:
	DataNode(Controller* controller, ConnectionsManager* cm, RequestMapper* request_mapper, ResponseMapper* response_mapper, ClassToByteTransformer* transformer, Settings* settings, Logger* logger);

	void send_heartbeats_to_leader(std::atomic_bool* should_terminate);

	void retrieve_cluster_metadata_updates(std::atomic_bool* should_terminate);
};