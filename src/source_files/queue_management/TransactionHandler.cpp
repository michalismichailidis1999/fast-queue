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
	std::lock_guard<std::shared_mutex> lock(this->ts_groups_heartbeats_mut);
	this->ts_groups_heartbeats[transaction_group_id] = this->util->get_current_time_milli();
}

void TransactionHandler::update_transaction_heartbeat(unsigned long long tx_id) {
	std::lock_guard<std::shared_mutex> lock(this->transactions_heartbeats_mut);
	this->transactions_heartbeats[tx_id] = this->util->get_current_time_milli();
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

	{
		std::lock_guard<std::shared_mutex> lock(this->ts_groups_heartbeats_mut);
		this->ts_groups_heartbeats.erase(transaction_group_id);
	}

	std::lock_guard<std::shared_mutex> lock(this->transactions_mut);
	this->open_transactions.erase(transaction_group_id);
}

unsigned long long TransactionHandler::init_transaction(unsigned long long transaction_group_id) {
	unsigned long long new_tx_id = this->get_new_transaction_id(transaction_group_id);

	int segment_id = this->get_transaction_segment(new_tx_id);

	this->write_transaction_change_to_segment(transaction_group_id, new_tx_id, segment_id, TransactionStatus::BEGIN);

	this->update_transaction_group_heartbeat(transaction_group_id);

	{
		std::lock_guard<std::shared_mutex> lock(this->transactions_mut);

		auto ts_group_open_txs = this->open_transactions[transaction_group_id];

		if (ts_group_open_txs == nullptr) {
			ts_group_open_txs = std::make_shared<std::queue<unsigned long long>>();
			this->open_transactions[transaction_group_id] = ts_group_open_txs;
		}

		ts_group_open_txs.get()->push(new_tx_id);
	}

	this->update_transaction_heartbeat(new_tx_id);

	return new_tx_id;
}

int TransactionHandler::get_transaction_segment(unsigned long long transaction_id) {
	return transaction_id % this->settings->get_transactions_partition_count();
}

unsigned long long TransactionHandler::get_new_transaction_id(unsigned long long transaction_group_id) {
	std::lock_guard<std::mutex> lock(this->cluster_metadata->transaction_ids_mut);
	return ++this->cluster_metadata->transaction_ids[transaction_group_id];
}

void TransactionHandler::write_transaction_change_to_segment(unsigned long long transaction_group_id, unsigned long long tx_id, int segment_id, TransactionStatus status_change) {
	std::shared_ptr<TransactionFileSegment> ts_segment = this->transaction_segment_files[segment_id];

	if (ts_segment == nullptr) {
		std::string err_msg = "Transaction segment " + std::to_string(segment_id) + " not found";
		throw std::runtime_error(err_msg);
	}

	std::shared_lock<std::shared_mutex> slock(ts_segment.get()->mut);
	
	std::unique_ptr<char> tx_change_bytes = std::unique_ptr<char>(new char[TX_CHANGE_TOTAL_BYTES]);

	memcpy_s(tx_change_bytes.get() + TX_CHANGE_GROUP_ID_OFFSET, TX_CHANGE_GROUP_ID_SIZE, &transaction_group_id, TX_CHANGE_GROUP_ID_SIZE);
	memcpy_s(tx_change_bytes.get() + TX_CHANGE_ID_OFFSET, TX_CHANGE_ID_SIZE, &tx_id, TX_CHANGE_ID_SIZE);
	memcpy_s(tx_change_bytes.get() + TX_CHANGE_STATUS_OFFSET, TX_CHANGE_STATUS_SIZE, &status_change, TX_CHANGE_STATUS_SIZE);

	long long write_pos = this->fh->write_to_file(
		ts_segment.get()->file_key,
		ts_segment.get()->file_path,
		TX_CHANGE_TOTAL_BYTES,
		-1,
		tx_change_bytes.get(),
		true
	);

	slock.unlock();

	// TODO: Check if write pos + TX_CHANGE_TOTAL_BYTES > threshold and if yes compact the segment
	if (write_pos + TX_CHANGE_TOTAL_BYTES >= MAX_TRANSACTION_SEGMENT_SIZE) {
		std::lock_guard<std::shared_mutex> lock(ts_segment.get()->mut);
		// TODO: Compact transaction segment
	}
}