#pragma once
#include "../../../file_management/DiskFlusher.h"
#include "../../../file_management/DiskReader.h"

class SegmentMessageMap {
private:
	DiskFlusher* df;
	DiskReader* dr;
public:
	SegmentMessageMap(DiskFlusher* df, DiskReader* dr);
};