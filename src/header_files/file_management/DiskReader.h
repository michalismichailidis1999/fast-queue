#pragma once
#include "Settings.h"
#include "FileHandler.h"
#include "Logger.h"

class DiskReader {
private:
	FileHandler* fh;
	Logger* logger;
	Settings* settings;

public:
	DiskReader(FileHandler* fh, Logger* logger, Settings* settings);
};