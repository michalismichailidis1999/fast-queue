#include "../../header_files/queue_management/TransactionHandler.h"

TransactionHandler::TransactionHandler(ConnectionsManager* cm, FileHandler* fh, QueueSegmentFilePathMapper* pm, ClusterMetadata* cluster_metadata, Util* util, Settings* settings, Logger* logger) {
	this->cm = cm;
	this->fh = fh;
	this->pm = pm;
	this->cluster_metadata = cluster_metadata;
	this->util = util;
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

void TransactionHandler::update_transaction_group_heartbeat(unsigned long long transaction_group_id) {
	std::lock_guard<std::mutex> lock(this->heartbeats_mut);
	this->heartbeats[transaction_group_id] = this->util->get_current_time_milli();
}

void TransactionHandler::add_transaction_group(unsigned long long transaction_group_id) {
	{
		std::lock_guard<std::shared_mutex> lock(this->transactions_mut);

		if (this->open_transactions.find(transaction_group_id) == this->open_transactions.end())
			this->open_transactions[transaction_group_id] = std::make_shared<std::queue<unsigned long long>>();
	}

	this->update_transaction_group_heartbeat(transaction_group_id);
}

void TransactionHandler::remove_transaction_group(unsigned long long transaction_group_id) {
	{
		std::shared_lock<std::shared_mutex> slock(this->transactions_mut);
		// TODO: Close all open transactions before remove the group
	}

	std::lock_guard<std::shared_mutex> lock(this->transactions_mut);
	this->open_transactions.erase(transaction_group_id);
}