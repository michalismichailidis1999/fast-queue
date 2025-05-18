#pragma once
#include "../../../file_management/DiskFlusher.h"
#include "../../../file_management/DiskReader.h"
#include "../../../file_management/QueueSegmentFilePathMapper.h"
#include "../../Partition.h"
#include "../../PartitionSegment.h"
#include "../../../exceptions/CurruptionException.h"

class SegmentMessageMap {
private:
	DiskFlusher* df;
	DiskReader* dr;
	QueueSegmentFilePathMapper* pm;

	int get_page_segment_offset(void* map_page, unsigned long long message_id);
public:
	SegmentMessageMap(DiskFlusher* df, DiskReader* dr, QueueSegmentFilePathMapper* pm);

	void add_last_message_info_to_segment_map(Partition* partition, PartitionSegment* segment);

	void fill_new_page_with_values(void* page, unsigned long long smallest_segment_id);

	std::shared_ptr<PartitionSegment> find_message_segment(Partition* partition, unsigned long long message_id, bool* success);
};