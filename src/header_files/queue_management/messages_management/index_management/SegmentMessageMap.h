#pragma once
#include "../../../file_management/DiskFlusher.h"
#include "../../../file_management/DiskReader.h"
#include "../../../file_management/QueueSegmentFilePathMapper.h"
#include "../../Partition.h"
#include "../../PartitionSegment.h"

class SegmentMessageMap {
private:
	DiskFlusher* df;
	DiskReader* dr;
	QueueSegmentFilePathMapper* pm;
public:
	SegmentMessageMap(DiskFlusher* df, DiskReader* dr, QueueSegmentFilePathMapper* pm);

	void add_last_message_info_to_segment_map(Partition* partition, PartitionSegment* segment, bool is_internal_queue);

	std::shared_ptr<PartitionSegment> find_message_segment(unsigned long long message_id);
};