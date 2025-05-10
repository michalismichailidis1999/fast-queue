#include "../../../../header_files/queue_management/messages_management/index_management/SegmentMessageMap.h"

SegmentMessageMap::SegmentMessageMap(DiskFlusher* df, DiskReader* dr, QueueSegmentFilePathMapper* pm) {
	this->df = df;
	this->dr = dr;
	this->pm = pm;
}

void SegmentMessageMap::add_last_message_info_to_segment_map(Partition* partition, PartitionSegment* segment, bool is_internal_queue) {
	std::string file_key = pm->get_segment_message_map_key(
		partition->get_queue_name(), 
		is_internal_queue ? -1 : partition->get_partition_id()
	);

	std::string file_path = pm->get_segment_message_map_path(
		partition->get_queue_name(),
		is_internal_queue ? -1 : partition->get_partition_id()
	);

	unsigned int page_id = segment->get_id() / MAPPED_SEGMENTS_PER_PAGE;

	unsigned int page_offset = page_id * MESSAGES_LOC_MAP_PAGE_SIZE;

	unsigned int message_offset = (segment->get_id() % MAPPED_SEGMENTS_PER_PAGE) * sizeof(unsigned long long);

	std::unique_ptr<char> map_page = std::unique_ptr<char>(new char[MESSAGES_LOC_MAP_PAGE_SIZE]);

	this->dr->read_data_from_disk(
		file_key,
		file_path,
		map_page.get(),
		MESSAGES_LOC_MAP_PAGE_SIZE,
		page_offset,
		is_internal_queue
	);

	unsigned long long last_message_offset = segment->get_last_message_offset();

	memcpy_s(map_page.get() + message_offset, sizeof(unsigned long long), &last_message_offset, sizeof(unsigned long long));

	this->df->write_data_to_specific_file_location(
		file_key,
		file_path,
		map_page.get(),
		MESSAGES_LOC_MAP_PAGE_SIZE,
		page_offset,
		is_internal_queue,
		is_internal_queue
	);
}

std::shared_ptr<PartitionSegment> SegmentMessageMap::find_message_segment(unsigned long long message_id) {
	return nullptr;
}