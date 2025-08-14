#pragma once
#include "../Settings.h"
#include "./FileHandler.h"
#include "../queue_management/Partition.h"
#include "../queue_management/PartitionSegment.h"
#include "./CacheHandler.h"
#include "../logging/Logger.h"

#include "../__linux/memcpy_s.h"

class DiskReader {
private:
	FileHandler* fh;
	CacheHandler* ch;
	Logger* logger;
	Settings* settings;

public:
	DiskReader(FileHandler* fh, CacheHandler* ch, Logger* logger, Settings* settings);

	std::shared_ptr<char> read_message_from_cache(Partition* partition, PartitionSegment* segment, unsigned long long message_id);

	std::shared_ptr<char> read_index_page_from_cache(Partition* partition, PartitionSegment* segment, unsigned long long page_offset);

	unsigned long read_data_from_disk(const std::string& key, const std::string& path, void* data, unsigned long total_bytes, long long pos = -1);
};