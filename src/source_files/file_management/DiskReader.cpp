#include "DiskReader.h"

DiskReader::DiskReader(FileHandler* fh, Logger* logger, Settings* settings) {
	this->fh = fh;
	this->logger = logger;
	this->settings = settings;
}