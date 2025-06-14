#pragma once
#include <chrono>
#include <unordered_map>
#include "../Enums.h"
#include "../Settings.h"
#include "../logging/Logger.h"
#include "../exceptions/CurruptionException.h"
#include "./QueueManager.h"
#include "./Partition.h"
#include "./PartitionSegment.h"
#include "./SegmentLockManager.h"
#include "./messages_management/MessagesHandler.h"
#include "./messages_management/index_management/BTreeNode.h"
#include "../file_management/FileHandler.h"
#include "../file_management/QueueSegmentFilePathMapper.h"
#include "../cluster_management/ClusterMetadata.h"
#include "../cluster_management/ClusterMetadataApplyHandler.h"
#include "../cluster_management/Controller.h"

class CompactionHandler {
private:
	Controller* controller;
	QueueManager* qm;
	MessagesHandler* mh;
	SegmentLockManager* lock_manager;
	ClusterMetadataApplyHandler* cmah;
	FileHandler* fh;
	QueueSegmentFilePathMapper* pm;
	Logger* logger;
	Settings* settings;

	std::unordered_set<std::string> existing_keys;

	void handle_queue_partitions_segment_compaction(const std::string& queue_name, std::atomic_bool* should_terminate);

	bool handle_partition_oldest_segment_compaction(Partition* partition);

	std::shared_ptr<PartitionSegment> compact_segment(Partition* partition, PartitionSegment* segment, std::shared_ptr<PartitionSegment> write_segment = nullptr, std::shared_ptr<BTreeNode> write_node = nullptr);

	void compact_internal_segment(PartitionSegment* segment);

	bool continue_compaction(Queue* queue);

	std::tuple<std::shared_ptr<PartitionSegment>, std::shared_ptr<BTreeNode>> initialize_compacted_segment_write_locations(Partition* partition, PartitionSegment* segment);

	std::shared_ptr<PartitionSegment> get_prev_compacted_segment(Partition* partition, unsigned long long prev_segment_id);

	std::shared_ptr<BTreeNode> find_index_node_with_last_message(PartitionSegment* segment, void* page_data);

	std::shared_ptr<BTreeNode> find_index_node_with_last_message(PartitionSegment* segment, void* page_data, BTreeNode* current_last_node);

	void parse_index_node_rows(Partition* partition, PartitionSegment* read_segment, std::shared_ptr<PartitionSegment> write_segment, BTreeNode* node, bool reverse_order);
public:
	CompactionHandler(Controller* controller, QueueManager* qm, MessagesHandler* mh, SegmentLockManager* lock_manager, ClusterMetadataApplyHandler* cmah, FileHandler* fh, QueueSegmentFilePathMapper* pm, Logger* logger, Settings* settings);

	void compact_closed_segments(std::atomic_bool* should_terminate);
};