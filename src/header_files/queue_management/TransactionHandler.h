#pragma once
#include <unordered_map>
#include <string>
#include <memory>
#include <chrono>
#include <shared_mutex>
#include <queue>
#include "../Settings.h"
#include "../logging/Logger.h"
#include "../cluster_management/ClusterMetadata.h"
#include "../network_management/ConnectionsManager.h"
#include "../file_management/FileHandler.h"
#include "./file_management/QueueSegmentFilePathMapper.h"
#include "./util/Util.h"

typedef struct {
	std::string file_path;
	std::string file_key;
} TransactionFileSegment;

class TransactionHandler {
private:
	ConnectionsManager* cm;
	FileHandler* fh;
	QueueSegmentFilePathMapper* pm;
	ClusterMetadata* cluster_metadata;
	Util* util;
	Settings* settings;
	Logger* logger;

	std::unordered_map<int, std::shared_ptr<TransactionFileSegment>> transaction_segment_files;

	std::unordered_map<unsigned long long, std::chrono::milliseconds> heartbeats;
	std::mutex heartbeats_mut;

	std::unordered_map<unsigned long long, std::shared_ptr<std::queue<unsigned long long>>> open_transactions;
	std::shared_mutex transactions_mut;

public:
	TransactionHandler(ConnectionsManager* cm, FileHandler* fh, QueueSegmentFilePathMapper* pm, ClusterMetadata* cluster_metadata, Util* util, Settings* settings, Logger* logger);

	void init_transaction_segment(int segment_id);

	void update_transaction_group_heartbeat(unsigned long long transaction_group_id);

	void add_transaction_group(unsigned long long transaction_group_id);

	void remove_transaction_group(unsigned long long transaction_group_id);
};