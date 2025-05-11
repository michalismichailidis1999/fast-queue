#pragma once
#include <chrono>
#include "./QueueManager.h"
#include "../file_management/FileHandler.h"
#include "../file_management/QueueSegmentFilePathMapper.h"
#include "../Settings.h"
#include "../logging/Logger.h"
#include "../util/Helper.h"

class RetentionHandler {
private:
	QueueManager* qm;
	FileHandler* fh;
	QueueSegmentFilePathMapper* pm;
	Logger* logger;
	Settings* settings;

	void handle_queue_partitions_segment_retention(const std::string& queue_name, std::atomic_bool* should_terminate);

	bool handle_partition_oldest_segment_retention(Partition* partition);

	bool continue_retention(Queue* queue);
public:
	RetentionHandler(QueueManager* qm, FileHandler* fh, QueueSegmentFilePathMapper* pm, Logger* logger, Settings* settings);

	void remove_expired_segments(std::atomic_bool* should_terminate);
};