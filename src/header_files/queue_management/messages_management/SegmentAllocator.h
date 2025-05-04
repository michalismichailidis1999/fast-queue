#pragma once
#include "../Partition.h"
#include "../../file_management/DiskFlusher.h"
#include "../../queue_management/messages_management/index_management/SegmentMessageMap.h"

class SegmentAllocator {
private:
	SegmentMessageMap* smm;
	DiskFlusher* df;
public:
	SegmentAllocator(SegmentMessageMap* smm, DiskFlusher* df);

	void allocate_new_segment(Partition* partition);
};