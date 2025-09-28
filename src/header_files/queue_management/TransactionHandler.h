#pragma once
#include <unordered_map>
#include <string>
#include <memory>
#include "../Settings.h"
#include "../logging/Logger.h"
#include "../cluster_management/ClusterMetadata.h"
#include "../network_management/ConnectionsManager.h"
#include "../file_management/FileHandler.h"
#include "./file_management/QueueSegmentFilePathMapper.h"

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
	Settings* settings;
	Logger* logger;

	std::unordered_map<int, std::shared_ptr<TransactionFileSegment>> transaction_segment_files;

public:
	TransactionHandler(ConnectionsManager* cm, FileHandler* fh, QueueSegmentFilePathMapper* pm, ClusterMetadata* cluster_metadata, Settings* settings, Logger* logger);

	void init_transaction_segment(int segment_id);
};