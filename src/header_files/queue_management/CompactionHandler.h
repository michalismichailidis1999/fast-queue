#pragma once
#include <chrono>
#include <unordered_map>
#include "./QueueManager.h"
#include "./Partition.h"
#include "./PartitionSegment.h"
#include "../logging/Logger.h"
#include "../Settings.h"

class CompactionHandler {
private:
	QueueManager* qm;
	Logger* logger;
	Settings* settings;

	std::unordered_map<std::string, unsigned long long> segments_to_compact_from;
public:
	CompactionHandler(QueueManager* qm, Logger* logger, Settings* settings);

	void compact_closed_segments(std::atomic_bool* should_terminate);
};