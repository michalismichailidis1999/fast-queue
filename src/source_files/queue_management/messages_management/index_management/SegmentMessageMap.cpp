#include "../../../../header_files/queue_management/messages_management/index_management/SegmentMessageMap.h"

SegmentMessageMap::SegmentMessageMap(DiskFlusher* df, DiskReader* dr) {
	this->df = df;
	this->dr = dr;
}