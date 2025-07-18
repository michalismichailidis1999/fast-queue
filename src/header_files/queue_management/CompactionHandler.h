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

	std::unordered_map<std::string, unsigned int> existing_keys;

	void handle_queue_partitions_segment_compaction(const std::string& queue_name, std::atomic_bool* should_terminate);

	bool handle_partition_oldest_segment_compaction(Partition* partition);

	std::shared_ptr<PartitionSegment> compact_segment(Partition* partition, PartitionSegment* segment);

	void compact_segment(Partition* partition, PartitionSegment* segment, std::shared_ptr<PartitionSegment> write_segment);

	bool continue_compaction(Queue* queue);

	std::shared_ptr<PartitionSegment> initialize_compacted_segment_write_locations(Partition* partition, PartitionSegment* segment);

	std::shared_ptr<PartitionSegment> get_prev_compacted_segment(Partition* partition, unsigned long long prev_segment_id);
public:
	CompactionHandler(Controller* controller, QueueManager* qm, MessagesHandler* mh, SegmentLockManager* lock_manager, ClusterMetadataApplyHandler* cmah, FileHandler* fh, QueueSegmentFilePathMapper* pm, Logger* logger, Settings* settings);

	void compact_closed_segments(std::atomic_bool* should_terminate);
};