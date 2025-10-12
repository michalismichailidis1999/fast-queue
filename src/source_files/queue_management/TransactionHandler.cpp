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
	std::string transaction_temp_segment_key = this->pm->get_transactions_segment_key(segment_id, true);
	std::string transaction_temp_segment_path = this->pm->get_transactions_segment_path(segment_id, true);

	std::shared_ptr<TransactionFileSegment> segment_file_info = std::make_shared<TransactionFileSegment>();
	segment_file_info.get()->file_key = transaction_segment_key;
	segment_file_info.get()->file_path = transaction_segment_path;
	segment_file_info.get()->temp_file_key = transaction_temp_segment_key;
	segment_file_info.get()->temp_file_path = transaction_temp_segment_path;
	segment_file_info.get()->written_bytes = 0;
	segment_file_info.get()->segment_id = segment_id;

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
			this->open_transactions[transaction_group_id] = std::make_shared<std::set<unsigned long long>>();
	}

	this->update_transaction_group_heartbeat(transaction_group_id);
}

void TransactionHandler::remove_transaction_group(unsigned long long transaction_group_id) {
	{
		std::shared_lock<std::shared_mutex> slock(this->transactions_mut);
		std::lock_guard<std::mutex> tx_to_close_mut(this->transactions_to_close_mut);

		auto open_txs = this->open_transactions[transaction_group_id];

		if (open_txs != nullptr && open_txs.get()->size() > 0)
			for (unsigned long long open_tx_id : *(open_txs.get()))
				this->transactions_to_close.insert(open_tx_id);
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

	this->write_transaction_change_to_segment(new_tx_id, segment_id, TransactionStatus::BEGIN);

	this->update_transaction_group_heartbeat(transaction_group_id);

	{
		std::lock_guard<std::shared_mutex> lock(this->transactions_mut);

		auto ts_group_open_txs = this->open_transactions[transaction_group_id];

		if (ts_group_open_txs == nullptr) {
			ts_group_open_txs = std::make_shared<std::set<unsigned long long>>();
			this->open_transactions[transaction_group_id] = ts_group_open_txs;
		}

		ts_group_open_txs.get()->insert(new_tx_id);
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

void TransactionHandler::write_transaction_change_to_segment(unsigned long long tx_id, int segment_id, TransactionStatus status_change) {
	std::shared_ptr<TransactionFileSegment> ts_segment = this->transaction_segment_files[segment_id];

	if (ts_segment == nullptr) {
		std::string err_msg = "Transaction segment " + std::to_string(segment_id) + " not found";
		throw std::runtime_error(err_msg);
	}

	std::shared_lock<std::shared_mutex> slock(ts_segment.get()->mut);
	
	std::unique_ptr<char> tx_change_bytes = std::unique_ptr<char>(new char[TX_CHANGE_TOTAL_BYTES]);

	memcpy_s(tx_change_bytes.get() + TX_CHANGE_ID_OFFSET, TX_CHANGE_ID_SIZE, &tx_id, TX_CHANGE_ID_SIZE);
	memcpy_s(tx_change_bytes.get() + TX_CHANGE_STATUS_OFFSET, TX_CHANGE_STATUS_SIZE, &status_change, TX_CHANGE_STATUS_SIZE);

	long long write_pos = -1;

	try {
		write_pos = this->fh->write_to_file(
			ts_segment.get()->file_key,
			ts_segment.get()->file_path,
			TX_CHANGE_TOTAL_BYTES,
			-1,
			tx_change_bytes.get(),
			true
		);
	}
	catch (const std::exception& ex) {
		this->logger->log_error(ex.what());

		if (this->fh->check_if_exists(ts_segment->temp_file_path)) {
			this->fh->rename_file(ts_segment->temp_file_key, ts_segment->temp_file_key, ts_segment->file_path);

			write_pos = this->fh->write_to_file(
				ts_segment.get()->file_key,
				ts_segment.get()->file_path,
				TX_CHANGE_TOTAL_BYTES,
				-1,
				tx_change_bytes.get(),
				true
			);
		}
		else throw ex;
	}

	slock.unlock();

	std::lock_guard<std::shared_mutex> xlock(ts_segment.get()->mut);
	ts_segment.get()->written_bytes += TX_CHANGE_TOTAL_BYTES;

	if (ts_segment.get()->written_bytes >= MAX_TRANSACTION_SEGMENT_SIZE) {
		this->compact_transaction_segment(ts_segment.get());
		ts_segment.get()->written_bytes = 0;
	}
}

void TransactionHandler::compact_transaction_segment(TransactionFileSegment* ts_segment) {
	if (this->fh->check_if_exists(ts_segment->temp_file_path) || !this->fh->check_if_exists(ts_segment->file_path)) {
		this->logger->log_error("Cannot compact transaction segment " + std::to_string(ts_segment->segment_id) + ". Something went wrong with their files");
		return;
	}

	std::unordered_map<unsigned long long, std::shared_ptr<std::vector<std::shared_ptr<char>>>> transactions;

	unsigned int read_batch_size = (READ_MESSAGES_BATCH_SIZE / TX_CHANGE_TOTAL_BYTES) * TX_CHANGE_TOTAL_BYTES;

	std::unique_ptr<char> batch = std::unique_ptr<char>(new char [read_batch_size]);

	unsigned int read_pos = 0;

	unsigned int bytes_read = this->fh->read_from_file(
		ts_segment->file_key,
		ts_segment->file_path,
		read_batch_size,
		read_pos,
		batch.get()
	);

	unsigned long long transaction_id = 0;
	TransactionStatus status = TransactionStatus::BEGIN;

	while (bytes_read > 0) {

		unsigned int offset = 0;

		while (offset < bytes_read) {
			memcpy_s(&transaction_id, TX_CHANGE_ID_SIZE, batch.get() + offset + TX_CHANGE_ID_OFFSET, TX_CHANGE_ID_SIZE);
			memcpy_s(&status, TX_CHANGE_STATUS_SIZE, batch.get() + offset + TX_CHANGE_STATUS_OFFSET, TX_CHANGE_STATUS_SIZE);

			if (status == TransactionStatus::END) {
				transactions.erase(transaction_id);
				offset += TX_CHANGE_TOTAL_BYTES;
				continue;
			}

			if (status == TransactionStatus::BEGIN)
				transactions[transaction_id] = std::make_shared<std::vector<std::shared_ptr<char>>>();

			std::shared_ptr<char> tx_change_info = std::shared_ptr<char>(new char[TX_CHANGE_TOTAL_BYTES]);
			transactions[transaction_id].get()->emplace_back(tx_change_info);

			offset += TX_CHANGE_TOTAL_BYTES;
		}

		if (bytes_read < read_batch_size) break;

		read_pos += read_batch_size;

		bytes_read = this->fh->read_from_file(
			ts_segment->file_key,
			ts_segment->file_path,
			read_batch_size,
			read_pos,
			batch.get()
		);
	}

	this->fh->create_new_file(
		ts_segment->temp_file_path,
		0,
		NULL,
		ts_segment->temp_file_key,
		false
	);

	for (auto& iter : transactions)
		for (auto& tx_info : *(iter.second.get()))
			this->fh->write_to_file(
				ts_segment->temp_file_key,
				ts_segment->temp_file_path,
				TX_CHANGE_TOTAL_BYTES,
				-1,
				tx_info.get(),
				true
			);

	this->fh->delete_dir_or_file(ts_segment->file_path, ts_segment->file_key);
	this->fh->rename_file(ts_segment->temp_file_key, ts_segment->temp_file_key, ts_segment->file_path);
}