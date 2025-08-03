#pragma once
#include <unordered_map>
#include "./ClusterMetadata.h"
#include "./Commands.h"
#include "../network_management/ConnectionsManager.h"
#include "../file_management/FileHandler.h"
#include "../file_management/QueueSegmentFilePathMapper.h"
#include "../queue_management/QueueManager.h"
#include "../queue_management/messages_management/Consumer.h"
#include "../Settings.h"
#include "../logging/Logger.h"

class ClusterMetadataApplyHandler {
private:
	QueueManager* qm;
	ConnectionsManager* cm;
	FileHandler* fh;
	QueueSegmentFilePathMapper* pm;
	Settings* settings;
	Logger* logger;

	void apply_create_queue_command(ClusterMetadata* cluster_metadata, CreateQueueCommand* command);
	void apply_partition_assignment_command(PartitionAssignmentCommand* command);
	void apply_partition_leader_assignment_command(PartitionLeaderAssignmentCommand* command);
	void apply_delete_queue_command(ClusterMetadata* cluster_metadata, DeleteQueueCommand* command);
	void apply_register_data_node_command(ClusterMetadata* cluster_metadata, RegisterDataNodeCommand* command);
	void apply_unregister_data_node_command(ClusterMetadata* cluster_metadata, UnregisterDataNodeCommand* command);
	void apply_register_consumer_group_command(RegisterConsumerGroupCommand* command);
	void apply_unregister_consumer_group_command(UnregisterConsumerGroupCommand* command);
public:
	ClusterMetadataApplyHandler(QueueManager* qm, ConnectionsManager* cm, FileHandler* fh, QueueSegmentFilePathMapper* pm, Settings* settings, Logger* logger);

	void apply_commands_from_segment(ClusterMetadata* cluster_metadata, unsigned long long segment_id, unsigned long long last_applied, bool from_compaction = false, std::unordered_map<int, Command>* registered_nodes = NULL, ClusterMetadata* future_cluster_metadata = NULL);

	void apply_command(ClusterMetadata* cluster_metadata, Command* command);
};