#pragma once
#include "./ClusterMetadata.h"
#include "./Commands.h"
#include "../file_management/FileHandler.h"
#include "../file_management/QueueSegmentFilePathMapper.h"
#include "../queue_management/QueueManager.h"
#include "../Settings.h"

class ClusterMetadataApplyHandler {
private:
	QueueManager* qm;
	FileHandler* fh;
	QueueSegmentFilePathMapper* pm;
	Settings* settings;

	void apply_create_queue_command(ClusterMetadata* cluster_metadata, CreateQueueCommand* command);
	void apply_partition_assignment_command(PartitionAssignmentCommand* command);
	void apply_partition_leader_assignment_command(PartitionLeaderAssignmentCommand* command);
	void apply_delete_queue_command(ClusterMetadata* cluster_metadata, DeleteQueueCommand* command);
public:
	ClusterMetadataApplyHandler(QueueManager* qm, FileHandler* fh, QueueSegmentFilePathMapper* pm, Settings* settings);

	void apply_commands_from_segment(ClusterMetadata* cluster_metadata, unsigned long long segment_id);

	void apply_command(ClusterMetadata* cluster_metadata, Command* command, bool is_from_initialization = false);
};