#pragma once
#include "../Partition.h"
#include "../../file_management/DiskFlusher.h"
#include "../../file_management/QueueSegmentFilePathMapper.h"
#include "../../queue_management/messages_management/index_management/SegmentMessageMap.h"

class SegmentAllocator {
private:
	SegmentMessageMap* smm;
	QueueSegmentFilePathMapper* pm;
	DiskFlusher* df;
public:
	SegmentAllocator(SegmentMessageMap* smm, QueueSegmentFilePathMapper* pm, DiskFlusher* df);

	void allocate_new_segment(Partition* partition);
};