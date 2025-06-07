#include "../../header_files/cluster_management/ClusterMetadataApplyHandler.h"

ClusterMetadataApplyHandler::ClusterMetadataApplyHandler(QueueManager* qm, FileHandler* fh, Settings* settings) {
	this->qm = qm;
	this->fh = fh;
	this->settings = settings;
}

void ClusterMetadataApplyHandler::apply_commands_from_segment(ClusterMetadata* cluster_metadata, unsigned long long segment_id) {

}

void ClusterMetadataApplyHandler::apply_command(ClusterMetadata* cluster_metadata, Command* command, bool is_from_initialization) {
	if (command->get_metadata_version() >= cluster_metadata->get_current_version()) {
		if(!is_from_initialization) cluster_metadata->apply_command(command);
		return;
	}

	switch (command->get_command_type()) {
	case CommandType::CREATE_QUEUE:
		this->apply_create_queue_command(cluster_metadata, (CreateQueueCommand*)command->get_command_info());
		break;
	case CommandType::ALTER_PARTITION_ASSIGNMENT:
		this->apply_partition_assignment_command((PartitionAssignmentCommand*)command->get_command_info());
		break;
	case CommandType::ALTER_PARTITION_LEADER_ASSIGNMENT:
		this->apply_partition_leader_assignment_command((PartitionLeaderAssignmentCommand*)command->get_command_info());
		break;
	case CommandType::DELETE_QUEUE:
		this->apply_delete_queue_command(cluster_metadata, (DeleteQueueCommand*)command->get_command_info());
		break;
	default:
		break;
	}

	cluster_metadata->apply_command(command);
}

void ClusterMetadataApplyHandler::apply_create_queue_command(ClusterMetadata* cluster_metadata, CreateQueueCommand* command) {
	QueueMetadata* metadata = cluster_metadata->get_queue_metadata(command->get_queue_name());

	if (metadata == NULL) {
		std::shared_ptr<QueueMetadata> new_metadata = std::shared_ptr<QueueMetadata>(
			new QueueMetadata(command->get_queue_name(), command->get_partitions(), command->get_replication_factor())
		);

		new_metadata.get()->set_status(Status::PENDING_CREATION);

		metadata = new_metadata.get();

		cluster_metadata->add_queue_metadata(new_metadata);
	}
	else metadata->set_status(Status::PENDING_CREATION);

	this->qm->create_queue(metadata);

	metadata->set_status(Status::ACTIVE);
}

void ClusterMetadataApplyHandler::apply_partition_assignment_command(PartitionAssignmentCommand* command) {
	if (command->get_from_node() != this->settings->get_node_id() && command->get_to_node() != this->settings->get_node_id())
		return;

	if (command->get_from_node() == this->settings->get_node_id())
		this->qm->remove_assigned_partition_from_queue(command->get_queue_name(), command->get_partition());
	else
		this->qm->add_assigned_partition_to_queue(command->get_queue_name(), command->get_partition());
}

void ClusterMetadataApplyHandler::apply_partition_leader_assignment_command(PartitionLeaderAssignmentCommand* command) {
	// TODO: Add logic here
}

void ClusterMetadataApplyHandler::apply_delete_queue_command(ClusterMetadata* cluster_metadata, DeleteQueueCommand* command) {
	cluster_metadata->remove_queue_metadata(command->get_queue_name());

	this->qm->delete_queue(command->get_queue_name());
}