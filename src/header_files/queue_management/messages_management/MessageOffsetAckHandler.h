#pragma once
#include "../../file_management/FileHandler.h"
#include "../../file_management/QueueSegmentFilePathMapper.h"
#include "../Partition.h"

#include "../../__linux/memcpy_s.h"

class MessageOffsetAckHandler {
private:
	FileHandler* fh;
	QueueSegmentFilePathMapper* pm;
public:
	MessageOffsetAckHandler(FileHandler* fh, QueueSegmentFilePathMapper* pm);

	void flush_partition_consumer_offsets(Partition* partition, bool from_server_startup = false);
};