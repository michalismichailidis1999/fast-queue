#include "../../header_files/file_management/DiskReader.h"

// TODO: Implement caching for index page retrievals

DiskReader::DiskReader(FileHandler* fh, Logger* logger, Settings* settings) {
	this->fh = fh;
	this->logger = logger;
	this->settings = settings;
}

bool DiskReader::read_data_from_disk(const std::string& key, const std::string& path, void* data, unsigned long total_bytes, long long pos) {
	return this->fh->read_from_file(
		key,
		path,
		total_bytes,
		pos,
		data
	);
}