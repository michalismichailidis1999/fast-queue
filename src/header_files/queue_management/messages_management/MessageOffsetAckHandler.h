#pragma once
#include <shared_mutex>
#include <memory>
#include <unordered_map>
#include "../../file_management/FileHandler.h"
#include "../../file_management/QueueSegmentFilePathMapper.h"
#include "../Partition.h"
#include "./Consumer.h"
#include "../../Constants.h"

#include "../../__linux/memcpy_s.h"

class MessageOffsetAckHandler {
private:
	FileHandler* fh;
	QueueSegmentFilePathMapper* pm;

	void compact_partition_offsets(Partition* partition);

	void assign_latest_offset_to_partition_consumers(Partition* partition);
public:
	MessageOffsetAckHandler(FileHandler* fh, QueueSegmentFilePathMapper* pm);

	void flush_consumer_offset_ack(Partition* partition, Consumer* consumer, unsigned long long offset);

	friend class BeforeServerStartupHandler;
};