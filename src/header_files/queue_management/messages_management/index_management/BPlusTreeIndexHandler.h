#pragma once
#include "../../../file_management/DiskFlusher.h"
#include "../../../file_management/DiskReader.h"
#include "../../PartitionSegment.h"

class BPlusTreeIndexHandler {
private:
	DiskFlusher* disk_flusher;
	DiskReader* disk_reader;

public:
	BPlusTreeIndexHandler(DiskFlusher* disk_flusher, DiskReader* disk_reader);

	void add_message_to_index(PartitionSegment* segment, unsigned long long message_id, unsigned int message_pos);
};