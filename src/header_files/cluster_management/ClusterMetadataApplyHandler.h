#pragma once
#include "./ClusterMetadata.h"
#include "../file_management/FileHandler.h"
#include "../queue_management/PartitionSegment.h"

class ClusterMetadataApplyHandler {
private:
	FileHandler* fh;
public:
	ClusterMetadataApplyHandler(FileHandler* fh);

	void apply_commands_from_segment(ClusterMetadata* cluster_metadata, unsigned long long segment_id);
};