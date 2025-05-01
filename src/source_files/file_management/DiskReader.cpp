#include "../../header_files/file_management/DiskReader.h"

// TODO: Implement caching & add locking mechanism for index page retrievals

DiskReader::DiskReader(FileHandler* fh, Logger* logger, Settings* settings) {
	this->fh = fh;
	this->logger = logger;
	this->settings = settings;
}

void DiskReader::read_data_from_disk(const std::string& key, const std::string& path, void* data, unsigned long total_bytes, long long pos, bool is_static_file) {
	this->fh->read_from_file(
		key,
		path,
		total_bytes,
		pos,
		data,
		is_static_file
	);
}