#pragma once
#include <regex>
#include <string>
#include <memory>
#include <future>
#include <vector>
#include "./queue_management/TransactionHandler.h"
#include "./queue_management/QueueManager.h"
#include "./queue_management/QueueMetadata.h"
#include "./queue_management/Partition.h"
#include "./queue_management/PartitionSegment.h"
#include "./queue_management/messages_management/SegmentAllocator.h"
#include "./queue_management/messages_management/MessageOffsetAckHandler.h"
#include "./queue_management/messages_management/index_management/BTreeNode.h"
#include "./queue_management/messages_management/index_management/SegmentMessageMap.h"
#include "./cluster_management/ClusterMetadata.h"
#include "./cluster_management/Controller.h"
#include "./cluster_management/DataNode.h"
#include "./cluster_management/ClusterMetadataApplyHandler.h"
#include "./file_management/FileHandler.h"
#include "./file_management/QueueSegmentFilePathMapper.h"
#include "./util/Util.h"
#include "./logging/Logger.h"
#include "./Settings.h"
#include "./Constants.h"
#include "./Enums.h"

#include "./__linux/memcpy_s.h"

class BeforeServerStartupHandler {
private:
	Controller* controller;
	DataNode* data_node;
	ClusterMetadataApplyHandler* cmah;
	TransactionHandler* th;
	QueueManager* qm;
	MessageOffsetAckHandler* oah;
	SegmentAllocator* sa;
	SegmentMessageMap* smm;
	FileHandler* fh;
	QueueSegmentFilePathMapper* pm;
	Util* util;
	Logger* logger;
	Settings* settings;

	void clear_unnecessary_files_and_initialize_queues();

	std::shared_ptr<QueueMetadata> get_queue_metadata(const std::string& queue_metadata_file_path, const std::string& queue_name, bool must_exist = false);

	void set_partition_segment_message_map(Partition* partition, bool is_cluster_metadata_queue);

	void set_partition_active_segment(Partition* partition, bool is_cluster_metadata_queue);

	void set_partition_replicated_offset(Partition* partition);

	void set_partition_transaction_changes(Partition* partition);

	void set_segment_index(const std::string& queue_name, PartitionSegment* segment, int partition = -1);

	void set_segment_last_message_offset_and_timestamp(Partition* partition, PartitionSegment* segment);

	void handle_compacted_segment(const std::string& queue_name, int partition_id, unsigned long long segment_id, bool is_internal_queue);

	void handle_transaction_segments();
public:
	BeforeServerStartupHandler(Controller* controller, DataNode* data_node, ClusterMetadataApplyHandler* cmah, TransactionHandler* th, QueueManager* qm, MessageOffsetAckHandler* oah, SegmentAllocator* sa, SegmentMessageMap* smm, FileHandler* fh, QueueSegmentFilePathMapper* pm, Util* util, Logger* logger, Settings* settings);

	void initialize_required_folders_and_queues();

	void rebuild_cluster_metadata();
};