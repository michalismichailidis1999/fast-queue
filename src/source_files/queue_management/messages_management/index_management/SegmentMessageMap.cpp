#include "../../../../header_files/queue_management/messages_management/index_management/SegmentMessageMap.h"

SegmentMessageMap::SegmentMessageMap(DiskFlusher* df, DiskReader* dr, QueueSegmentFilePathMapper* pm) {
	this->df = df;
	this->dr = dr;
	this->pm = pm;
}

void SegmentMessageMap::add_last_message_info_to_segment_map(Partition* partition, PartitionSegment* segment) {
	unsigned int page_id = segment->get_id() / MAPPED_SEGMENTS_PER_PAGE;

	unsigned int page_offset = page_id * MESSAGES_LOC_MAP_PAGE_SIZE;

	unsigned int message_offset = (segment->get_id() % MAPPED_SEGMENTS_PER_PAGE) * sizeof(unsigned long long);

	std::unique_ptr<char> map_page = std::unique_ptr<char>(new char[MESSAGES_LOC_MAP_PAGE_SIZE]);

	this->dr->read_data_from_disk(
		partition->get_message_map_key(),
		partition->get_message_map_path(),
		map_page.get(),
		MESSAGES_LOC_MAP_PAGE_SIZE,
		page_offset
	);

	unsigned long long last_message_offset = segment->get_last_message_offset();

	memcpy_s(map_page.get() + message_offset, sizeof(unsigned long long), &last_message_offset, sizeof(unsigned long long));

	this->df->write_data_to_specific_file_location(
		partition->get_message_map_key(),
		partition->get_message_map_path(),
		map_page.get(),
		MESSAGES_LOC_MAP_PAGE_SIZE,
		page_offset
	);
}

std::shared_ptr<PartitionSegment> SegmentMessageMap::find_message_segment(Partition* partition, unsigned long long message_id) {
	bool is_internal_queue = Helper::is_internal_queue(partition->get_queue_name());

	unsigned long long starting_page_segment = 0;
	unsigned long long segment_id = 0;

	unsigned int page_id = 0;

	std::unique_ptr<char> map_page = std::unique_ptr<char>(new char[MESSAGES_LOC_MAP_PAGE_SIZE]);

	while (true) {
		bool success = this->dr->read_data_from_disk(
			partition->get_message_map_key(),
			partition->get_message_map_path(),
			map_page.get(),
			MESSAGES_LOC_MAP_PAGE_SIZE,
			page_id * MESSAGES_LOC_MAP_PAGE_SIZE
		);

		if (!success) return nullptr;

		memcpy_s(&starting_page_segment, sizeof(unsigned long long), map_page.get(), sizeof(unsigned long long));

		int segment_offset = this->get_page_segment_offset(map_page.get(), message_id);

		if (segment_offset == -1 || segment_offset == 0) return nullptr;

		if(segment_offset == -2) {
			page_id++;
			continue;
		}

		segment_id = starting_page_segment + segment_offset;
	}

	if (segment_id == 0) return nullptr;

	std::string segment_key = this->pm->get_file_key(
		partition->get_queue_name(),
		segment_id
	);

	std::string segment_path = this->pm->get_file_path(
		partition->get_queue_name(),
		segment_id,
		is_internal_queue ? -1 : partition->get_partition_id()
	);

	std::string index_key = this->pm->get_file_key(
		partition->get_queue_name(),
		segment_id,
		true
	);

	std::string index_path = this->pm->get_file_path(
		partition->get_queue_name(),
		segment_id,
		is_internal_queue ? -1 : partition->get_partition_id(),
		true
	);

	std::shared_ptr<PartitionSegment> segment = std::shared_ptr<PartitionSegment>(
		new PartitionSegment(segment_id, segment_key, segment_path)
	);

	segment.get()->set_index(index_key, index_path);
	segment.get()->set_to_read_only();

	return segment;
}

// -1 -> message is probably located in current segment
// -2 -> go to next map page to search
int SegmentMessageMap::get_page_segment_offset(void* map_page, unsigned long long message_id) {
	unsigned long long first_message_id = 0;
	unsigned long long last_message_id = 0;

	memcpy_s(&first_message_id, sizeof(unsigned long long), (char*)map_page + sizeof(unsigned long long), sizeof(unsigned long long));
	memcpy_s(&last_message_id, sizeof(unsigned long long), (char*)map_page + MESSAGES_LOC_MAP_PAGE_SIZE - sizeof(unsigned long long), sizeof(unsigned long long));

	if (first_message_id == 0) return -1;

	if (first_message_id >= message_id) return 1;

	if (last_message_id != 0 && message_id > last_message_id) return -2;

	unsigned int start_pos = 1;
	unsigned int end_pos = MAPPED_SEGMENTS_PER_PAGE - 1;

	if (last_message_id == 0)
	{
		unsigned int offset = MESSAGES_LOC_MAP_PAGE_SIZE - sizeof(unsigned long long);

		while (last_message_id == 0) {
			offset -= sizeof(unsigned long long);
			end_pos--;

			memcpy_s(&last_message_id, sizeof(unsigned long long), (char*)map_page + offset, sizeof(unsigned long long));
		}
	}

	if (end_pos == 0 || message_id > last_message_id) return -1;

	unsigned long long middle_message_id = 0;

	unsigned int pos = start_pos + (end_pos - start_pos) / 2;

	while (start_pos <= end_pos) {
		memcpy_s(&first_message_id, sizeof(unsigned long long), (char*)map_page + start_pos * sizeof(unsigned long long), sizeof(unsigned long long));
		memcpy_s(&middle_message_id, sizeof(unsigned long long), (char*)map_page + pos * sizeof(unsigned long long), sizeof(unsigned long long));
		memcpy_s(&last_message_id, sizeof(unsigned long long), (char*)map_page + end_pos * sizeof(unsigned long long), sizeof(unsigned long long));

		if (last_message_id == message_id) return end_pos;
		if (middle_message_id == message_id) return pos;
		if (first_message_id == message_id) return start_pos;

		if (middle_message_id > message_id) start_pos = pos + 1;
		else end_pos = pos - 1;
	}

	return pos;
}