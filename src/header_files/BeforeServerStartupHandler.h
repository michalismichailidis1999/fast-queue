#pragma once
#include <regex>
#include <string>
#include <memory>
#include "./queue_management/QueueManager.h"
#include "./queue_management/QueueMetadata.h"
#include "./queue_management/Partition.h"
#include "./queue_management/PartitionSegment.h"
#include "./queue_management/messages_management/SegmentAllocator.h"
#include "./queue_management/messages_management/index_management/BTreeNode.h"
#include "./cluster_management/ClusterMetadata.h"
#include "./file_management/FileHandler.h"
#include "./file_management/QueueSegmentFilePathMapper.h"
#include "./util/Util.h"
#include "./logging/Logger.h"
#include "./Settings.h"
#include "./Constants.h"
#include "./Enums.h"

class BeforeServerStartupHandler {
private:
	QueueManager* qm;
	SegmentAllocator* sa;
	ClusterMetadata* cluster_metadata;
	ClusterMetadata* future_cluster_metadata;
	FileHandler* fh;
	QueueSegmentFilePathMapper* pm;
	Util* util;
	Logger* logger;
	Settings* settings;

	void clear_unnecessary_files_and_initialize_queues();

	std::shared_ptr<QueueMetadata> get_queue_metadata(const std::string& queue_metadata_file_path, const std::string& queue_name, bool must_exist = false);

	void set_partition_segment_message_map(Partition* partition, bool is_cluster_metadata_queue);

	void set_partition_active_segment(Partition* partition, bool is_cluster_metadata_queue);

	void set_segment_index(const std::string& queue_name, PartitionSegment* segment, int partition = -1);

public:
	BeforeServerStartupHandler(QueueManager* qm, SegmentAllocator* sa, ClusterMetadata* cluster_metadata, ClusterMetadata* future_cluster_metadata, FileHandler* fh, QueueSegmentFilePathMapper* pm, Util* util, Logger* logger, Settings* settings);

	void initialize_required_folders_and_queues();

	void rebuild_cluster_metadata();
};