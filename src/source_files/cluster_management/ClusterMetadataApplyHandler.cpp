#include "../../header_files/cluster_management/ClusterMetadataApplyHandler.h"

ClusterMetadataApplyHandler::ClusterMetadataApplyHandler(FileHandler* fh) {
	this->fh = fh;
}

void ClusterMetadataApplyHandler::apply_commands_from_segment(ClusterMetadata* cluster_metadata, unsigned long long segment_id) {

}