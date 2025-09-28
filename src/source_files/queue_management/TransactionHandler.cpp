#include "../../header_files/queue_management/TransactionHandler.h"

TransactionHandler::TransactionHandler(ConnectionsManager* cm, FileHandler* fh, QueueSegmentFilePathMapper* pm, ClusterMetadata* cluster_metadata, Settings* settings, Logger* logger) {
	this->cm = cm;
	this->fh = fh;
	this->pm = pm;
	this->cluster_metadata = cluster_metadata;
	this->settings = settings;
	this->logger = logger;
}

void TransactionHandler::init_transaction_segment(int segment_id) {
	std::string transaction_segment_key = this->pm->get_transactions_segment_key(segment_id);
	std::string transaction_segment_path = this->pm->get_transactions_segment_path(segment_id);

	std::shared_ptr<TransactionFileSegment> segment_file_info = std::make_shared<TransactionFileSegment>();
	segment_file_info.get()->file_key = transaction_segment_key;
	segment_file_info.get()->file_path = transaction_segment_path;

	this->transaction_segment_files[segment_id] = segment_file_info;

	if (this->fh->check_if_exists(transaction_segment_path)) return;

	this->fh->create_new_file(transaction_segment_path, 0, NULL, transaction_segment_key);
}