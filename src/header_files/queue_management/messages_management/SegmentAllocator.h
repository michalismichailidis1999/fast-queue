#pragma once
#include "../Partition.h"
#include "../../file_management/FileHandler.h"

class SegmentAllocator {
private:
	FileHandler* fh;
public:
	SegmentAllocator(FileHandler* fh);

	void allocate_new_segment(Partition* partition);
};