#pragma once
#include "../Settings.h"
#include "./FileHandler.h"
#include "../logging/Logger.h"

class DiskReader {
private:
	FileHandler* fh;
	Logger* logger;
	Settings* settings;

public:
	DiskReader(FileHandler* fh, Logger* logger, Settings* settings);

	void read_data_from_disk(const std::string& key, const std::string& path, void* data, unsigned long total_bytes, long long pos = -1);
};