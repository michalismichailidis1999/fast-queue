#pragma once
#include "../../../file_management/DiskFlusher.h"
#include "../../../file_management/DiskReader.h"
#include "../../PartitionSegment.h"

class SegmentMessageMap {
private:
	DiskFlusher* df;
	DiskReader* dr;
public:
	SegmentMessageMap(DiskFlusher* df, DiskReader* dr);

	void add_last_message_info_to_segment_map(PartitionSegment* segment, bool is_internal_queue);
};