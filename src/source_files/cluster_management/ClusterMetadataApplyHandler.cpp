#include "../../header_files/cluster_management/ClusterMetadataApplyHandler.h"

ClusterMetadataApplyHandler::ClusterMetadataApplyHandler(QueueManager* qm, ConnectionsManager* cm, FileHandler* fh, QueueSegmentFilePathMapper* pm, Settings* settings) {
	this->qm = qm;
	this->cm = cm;
	this->fh = fh;
	this->pm = pm;
	this->settings = settings;
}

void ClusterMetadataApplyHandler::apply_commands_from_segment(ClusterMetadata* cluster_metadata, unsigned long long segment_id, unsigned long long last_applied, bool from_compaction, std::unordered_map<int, Command>* registered_nodes, ClusterMetadata* future_cluster_metadata) {
	std::unique_ptr<char> batch_size = std::unique_ptr<char>(new char[READ_MESSAGES_BATCH_SIZE]);

	std::string segment_key = this->pm->get_file_key(CLUSTER_METADATA_QUEUE_NAME, segment_id, -1);
	std::string segment_path = this->pm->get_file_path(CLUSTER_METADATA_QUEUE_NAME, segment_id, -1);

	unsigned long long pos = SEGMENT_METADATA_TOTAL_BYTES;

	unsigned long bytes_read = this->fh->read_from_file(segment_key, segment_path, READ_MESSAGES_BATCH_SIZE, pos, batch_size.get());

	unsigned int command_total_bytes = 0;
	bool is_command_active = true;

	while (bytes_read > 0) {
		unsigned long offset = 0;

		while (offset < bytes_read) {
			memcpy_s(&command_total_bytes, TOTAL_METADATA_BYTES, batch_size.get() + TOTAL_METADATA_BYTES_OFFSET + offset, TOTAL_METADATA_BYTES);
			memcpy_s(&is_command_active, MESSAGE_IS_ACTIVE_SIZE, batch_size.get() + MESSAGE_IS_ACTIVE_OFFSET + offset, MESSAGE_IS_ACTIVE_SIZE);

			if (offset + command_total_bytes > bytes_read) break;

			if (!is_command_active) {
				offset += command_total_bytes;
				continue;
			}

			Command command = Command(batch_size.get() + offset);

			if(future_cluster_metadata != NULL)
				future_cluster_metadata->apply_command(&command);

			if (!from_compaction && command.get_metadata_version() > last_applied) {
				offset += command_total_bytes;
				continue;
			}

			cluster_metadata->apply_command(&command);

			if (registered_nodes != NULL) {
				RegisterDataNodeCommand* register_info = NULL;
				UnregisterDataNodeCommand* unregister_info = NULL;

				switch (command.get_command_type())
				{
				case CommandType::REGISTER_DATA_NODE:
					register_info = (RegisterDataNodeCommand*)command.get_command_info();
					(*registered_nodes)[register_info->get_node_id()] = command;
					break;
				case CommandType::UNREGISTER_DATA_NODE:
					unregister_info = (UnregisterDataNodeCommand*)command.get_command_info();
					registered_nodes->erase(unregister_info->get_node_id());
					break;
				default:
					break;
				}
			}

			offset += command_total_bytes;
		}

		if (bytes_read < READ_MESSAGES_BATCH_SIZE) break;

		pos += READ_MESSAGES_BATCH_SIZE - (bytes_read == offset ? 0 : bytes_read - offset);

		bytes_read = this->fh->read_from_file(segment_key, segment_path, READ_MESSAGES_BATCH_SIZE, pos, batch_size.get());
	}

	this->fh->close_file(segment_key);
}

void ClusterMetadataApplyHandler::apply_command(ClusterMetadata* cluster_metadata, Command* command) {
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
	case CommandType::REGISTER_DATA_NODE:
		this->apply_register_data_node_command(cluster_metadata, (RegisterDataNodeCommand*)command->get_command_info());
		break;
	case CommandType::UNREGISTER_DATA_NODE:
		this->apply_unregister_data_node_command(cluster_metadata, (UnregisterDataNodeCommand*)command->get_command_info());
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

void ClusterMetadataApplyHandler::apply_register_data_node_command(ClusterMetadata* cluster_metadata, RegisterDataNodeCommand* command) {
	cluster_metadata->init_node_partitions(command->get_node_id());

	std::shared_ptr<ConnectionInfo> info = std::make_shared<ConnectionInfo>();
	info.get()->address = command->get_address();
	info.get()->port = command->get_port();

	this->cm->initialize_data_node_connection_pool(command->get_node_id(), info);
}

void ClusterMetadataApplyHandler::apply_unregister_data_node_command(ClusterMetadata* cluster_metadata, UnregisterDataNodeCommand* command) {
	cluster_metadata->remove_node_partitions(command->get_node_id());
	this->cm->remove_data_node_connections(command->get_node_id());
}