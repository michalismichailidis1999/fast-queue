#include "../../../../header_files/queue_management/messages_management/index_management/SegmentMessageMap.h"

SegmentMessageMap::SegmentMessageMap(DiskFlusher* df, DiskReader* dr) {
	this->df = df;
	this->dr = dr;
}

void SegmentMessageMap::add_last_message_info_to_segment_map(PartitionSegment* segment, bool is_internal_queue) {

}