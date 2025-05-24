#pragma once
#include <chrono>
#include <unordered_map>
#include "./QueueManager.h"
#include "./Partition.h"
#include "./PartitionSegment.h"
#include "./SegmentLockManager.h"
#include "../logging/Logger.h"
#include "../Settings.h"
#include "../file_management/FileHandler.h"
#include "../file_management/QueueSegmentFilePathMapper.h"
#include "../util/BloomFilter.h"
#include "../exceptions/CurruptionException.h"
#include "../queue_management/messages_management/index_management/BTreeNode.h"

class CompactionHandler {
private:
	QueueManager* qm;
	SegmentLockManager* lock_manager;
	FileHandler* fh;
	QueueSegmentFilePathMapper* pm;
	Logger* logger;
	Settings* settings;

	std::unique_ptr<BloomFilter> bf;

	void handle_queue_partitions_segment_compaction(const std::string& queue_name, std::atomic_bool* should_terminate);

	bool handle_partition_oldest_segment_compaction(Partition* partition);

	void compact_segment(PartitionSegment* segment);

	void compact_internal_segment(PartitionSegment* segment);

	bool continue_compaction(Queue* queue);

	bool ignore_message(void* message);
public:
	CompactionHandler(QueueManager* qm, SegmentLockManager* lock_manager, FileHandler* fh, QueueSegmentFilePathMapper* pm, Logger* logger, Settings* settings);

	void compact_closed_segments(std::atomic_bool* should_terminate);
};