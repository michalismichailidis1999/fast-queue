#include "../../../header_files/queue_management/messages_management/SegmentAllocator.h"

SegmentAllocator::SegmentAllocator(SegmentMessageMap* smm, DiskFlusher* df) {
	this->smm = smm;
	this->df = df;
}

void SegmentAllocator::allocate_new_segment(Partition* partition) {

}