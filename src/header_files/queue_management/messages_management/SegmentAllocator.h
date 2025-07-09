#pragma once
#include "../Partition.h"
#include "../SegmentLockManager.h"
#include "../../file_management/DiskFlusher.h"
#include "../../file_management/QueueSegmentFilePathMapper.h"
#include "../../queue_management/messages_management/index_management/SegmentMessageMap.h"
#include "../../logging/Logger.h"

class SegmentAllocator {
private:
	SegmentMessageMap* smm;
	SegmentLockManager* lock_manager;
	QueueSegmentFilePathMapper* pm;
	DiskFlusher* df;
	Logger* logger;
public:
	SegmentAllocator(SegmentMessageMap* smm, SegmentLockManager* lock_manager, QueueSegmentFilePathMapper* pm, DiskFlusher* df, Logger* logger);

	bool allocate_new_segment(Partition* partition);
};