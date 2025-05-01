#pragma once
#include <memory>
#include "../../../file_management/DiskFlusher.h"
#include "../../../file_management/DiskReader.h"
#include "../../Partition.h"
#include "./BTreeNode.h"
#include "../../../util/Helper.h"
#include "../../../Constants.h"

class BPlusTreeIndexHandler {
private:
	DiskFlusher* disk_flusher;
	DiskReader* disk_reader;

public:
	BPlusTreeIndexHandler(DiskFlusher* disk_flusher, DiskReader* disk_reader);

	void add_message_to_index(Partition* partition, unsigned long long message_id, unsigned int message_pos);
};