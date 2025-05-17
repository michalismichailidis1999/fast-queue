#pragma once
#include <chrono>
#include "./QueueManager.h"
#include "./SegmentLockManager.h"
#include "../file_management/FileHandler.h"
#include "../file_management/QueueSegmentFilePathMapper.h"
#include "../Settings.h"
#include "../logging/Logger.h"
#include "../util/Helper.h"
#include "../util/Util.h"

class RetentionHandler {
private:
	QueueManager* qm;
	SegmentLockManager* lock_manager;
	FileHandler* fh;
	QueueSegmentFilePathMapper* pm;
	Util* util;
	Logger* logger;
	Settings* settings;

	void handle_queue_partitions_segment_retention(const std::string& queue_name, std::atomic_bool* should_terminate);

	bool handle_partition_oldest_segment_retention(Partition* partition);

	bool continue_retention(Queue* queue);
public:
	RetentionHandler(QueueManager* qm, SegmentLockManager* lock_manager, FileHandler* fh, QueueSegmentFilePathMapper* pm, Util* util, Logger* logger, Settings* settings);

	void remove_expired_segments(std::atomic_bool* should_terminate);
};