#include "../../header_files/queue_management/TransactionHandler.h"

TransactionHandler::TransactionHandler(QueueManager* qm, ConnectionsManager* cm, FileHandler* fh, CacheHandler* ch, QueueSegmentFilePathMapper* pm, ClusterMetadata* cluster_metadata, ResponseMapper* response_mapper, ClassToByteTransformer* transformer, Util* util, Settings* settings, Logger* logger) {
	this->qm = qm;
	this->cm = cm;
	this->fh = fh;
	this->ch = ch;
	this->pm = pm;
	this->cluster_metadata = cluster_metadata;
	this->response_mapper = response_mapper;
	this->transformer = transformer;
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

// This will run only in transaction group insertion, transaction initialization or finalization
void TransactionHandler::update_transaction_group_heartbeat(unsigned long long transaction_group_id) {
	std::lock_guard<std::shared_mutex> lock(this->ts_groups_heartbeats_mut);
	this->ts_groups_heartbeats[transaction_group_id] = this->util->get_current_time_milli();
}

void TransactionHandler::update_transaction_heartbeat(unsigned long long transaction_group_id, unsigned long long tx_id) {
	std::lock_guard<std::shared_mutex> lock(this->transactions_heartbeats_mut);
	this->transactions_heartbeats[this->get_transaction_key(transaction_group_id, tx_id)] = this->util->get_current_time_milli();
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
				this->transactions_to_close.insert(this->get_transaction_key(transaction_group_id, open_tx_id));
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

	if (new_tx_id == 0) return new_tx_id;

	int segment_id = this->get_transaction_segment(new_tx_id);

	this->write_transaction_change_to_segment(transaction_group_id, new_tx_id, segment_id, TransactionStatus::BEGIN);

	this->update_transaction_group_heartbeat(transaction_group_id);

	{
		std::lock_guard<std::shared_mutex> lock(this->transactions_mut);

		auto ts_group_open_txs = this->open_transactions[transaction_group_id];

		if (ts_group_open_txs == nullptr) {
			ts_group_open_txs = std::make_shared<std::set<unsigned long long>>();
			this->open_transactions[transaction_group_id] = ts_group_open_txs;
		}

		ts_group_open_txs.get()->insert(new_tx_id);

		this->open_transactions_statuses[this->get_transaction_key(transaction_group_id, new_tx_id)] = TransactionStatus::BEGIN;
	}

	this->update_transaction_heartbeat(transaction_group_id, new_tx_id);

	return new_tx_id;
}

// Distributed transactions steps (If any failure occurs the transaction closure will run in the background)
// 1) init_transaction method will initialize the transaction in transaction group assignee node
// 2) Client will call either commit() or abort() to finalize the transaction and request manager will redirect the request to this method
// 3) Finalize status will be sent with fan-out approach to all necessary nodes (nodes that at least contains 1 partition leader from all transaction group queues)
// 4) Finalize status will be written to transaction segment and update status in memory
// 5) End transaction
void TransactionHandler::finalize_transaction(unsigned long long transaction_group_id, unsigned long long tx_id, bool commit) {
	try
	{
		this->update_transaction_group_heartbeat(transaction_group_id);

		std::shared_ptr<std::unordered_set<int>> tx_nodes = this->find_all_transaction_nodes(transaction_group_id);

		TransactionStatus finalize_status = commit ? TransactionStatus::COMMIT : TransactionStatus::ABORT;

		std::vector<std::future<std::tuple<bool, int>>> node_notifications;
		std::vector<TransactionStatus> statuses = std::vector<TransactionStatus>(2);
		statuses[0] = finalize_status;
		statuses[1] = TransactionStatus::END;

		for (auto status : statuses)
			this->notify_group_nodes_node_about_transaction_status_change(
				tx_nodes, transaction_group_id, tx_id, status
			);
	}
	catch (const std::exception& ex)
	{
		std::unique_lock<std::shared_mutex> transactions_lock(this->transactions_mut);

		std::string tx_key = this->get_transaction_key(transaction_group_id, tx_id);

		{
			std::lock_guard<std::shared_mutex> heartbeats_lock(this->transactions_heartbeats_mut);
			std::lock_guard<std::mutex> tx_to_close_lock(this->transactions_to_close_mut);
			this->transactions_heartbeats.erase(tx_key);
			this->transactions_to_close.insert(tx_key);
		}

		std::string err_msg = "Finalization of transaction "
			+ std::to_string(tx_id)
			+ " from transaction group "
			+ std::to_string(transaction_group_id)
			+ " failed. Reason: "
			+ std::string(ex.what());

		this->logger->log_error(err_msg);

		throw ex;
	}
}

int TransactionHandler::get_transaction_segment(unsigned long long transaction_id) {
	return transaction_id % this->settings->get_transactions_partition_count();
}

unsigned long long TransactionHandler::get_new_transaction_id(unsigned long long transaction_group_id) {
	std::lock_guard<std::mutex> lock(this->cluster_metadata->transaction_ids_mut);

	if (this->cluster_metadata->transaction_ids.find(transaction_group_id) == this->cluster_metadata->transaction_ids.end())
		return 0;

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

	long long current_timestamp = this->util->get_current_time_milli().count();

	memcpy_s(tx_change_bytes.get() + TX_CHANGE_GROUP_ID_OFFSET, TX_CHANGE_GROUP_ID_SIZE, &transaction_group_id, TX_CHANGE_GROUP_ID_SIZE);
	memcpy_s(tx_change_bytes.get() + TX_CHANGE_ID_OFFSET, TX_CHANGE_ID_SIZE, &tx_id, TX_CHANGE_ID_SIZE);
	memcpy_s(tx_change_bytes.get() + TX_CHANGE_STATUS_OFFSET, TX_CHANGE_STATUS_SIZE, &status_change, TX_CHANGE_STATUS_SIZE);
	memcpy_s(tx_change_bytes.get() + TX_CHANGE_TIMESTAMP_OFFSET, TX_CHANGE_TIMESTAMP_SIZE, &current_timestamp, TX_CHANGE_TIMESTAMP_SIZE);

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

	std::unique_lock<std::shared_mutex> xlock(ts_segment.get()->mut);
	ts_segment.get()->written_bytes += TX_CHANGE_TOTAL_BYTES;

	if (ts_segment.get()->written_bytes >= MAX_TRANSACTION_SEGMENT_SIZE) {
		ts_segment.get()->written_bytes = 0;

		std::thread([this, ts_segment, lock = std::move(xlock)]() mutable {
			this->compact_transaction_segment(ts_segment.get());
		}).detach();
	}
}

void TransactionHandler::compact_transaction_segment(TransactionFileSegment* ts_segment) {
	if (this->fh->check_if_exists(ts_segment->temp_file_path) || !this->fh->check_if_exists(ts_segment->file_path)) {
		this->logger->log_error("Cannot compact transaction segment " + std::to_string(ts_segment->segment_id) + ". Something went wrong with their files");
		return;
	}

	std::unordered_map<std::string, std::shared_ptr<std::vector<std::shared_ptr<char>>>> transactions;

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

	unsigned long long transaction_group_id = 0;
	unsigned long long transaction_id = 0;
	TransactionStatus status = TransactionStatus::NONE;

	std::string tx_key = "";

	while (bytes_read > 0) {

		unsigned int offset = 0;

		while (offset < bytes_read) {
			memcpy_s(&transaction_group_id, TX_CHANGE_GROUP_ID_SIZE, batch.get() + offset + TX_CHANGE_GROUP_ID_OFFSET, TX_CHANGE_GROUP_ID_SIZE);
			memcpy_s(&transaction_id, TX_CHANGE_ID_SIZE, batch.get() + offset + TX_CHANGE_ID_OFFSET, TX_CHANGE_ID_SIZE);
			memcpy_s(&status, TX_CHANGE_STATUS_SIZE, batch.get() + offset + TX_CHANGE_STATUS_OFFSET, TX_CHANGE_STATUS_SIZE);

			tx_key = this->get_transaction_key(transaction_group_id, transaction_id);

			if (status == TransactionStatus::END) {
				transactions.erase(tx_key);
				offset += TX_CHANGE_TOTAL_BYTES;
				continue;
			}

			if (status == TransactionStatus::BEGIN)
				transactions[tx_key] = std::make_shared<std::vector<std::shared_ptr<char>>>();

			std::shared_ptr<char> tx_change_info = std::shared_ptr<char>(new char[TX_CHANGE_TOTAL_BYTES]);
			memcpy_s(tx_change_info.get(), TX_CHANGE_TOTAL_BYTES, batch.get() + offset, TX_CHANGE_TOTAL_BYTES);
			transactions[tx_key].get()->emplace_back(tx_change_info);

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

void TransactionHandler::capture_transaction_changes(Partition* partition, TransactionChangeCapture& change_capture, TransactionStatus status) {
	if (status == TransactionStatus::NONE)
		this->capture_transaction_change_to_memory(change_capture);

	std::shared_lock<std::shared_mutex> slock(this->transaction_changes_mut);

	std::unique_ptr<char> tx_change_bytes = std::unique_ptr<char>(new char[TX_MSG_CAPTURE_TOTAL_BYTES]);

	long long current_timestamp = this->util->get_current_time_milli().count();
	unsigned int queue_name_length = change_capture.queue.size();

	memcpy_s(tx_change_bytes.get() + TX_MSG_CAPTURE_TX_GROUP_ID_OFFSET, TX_MSG_CAPTURE_TX_GROUP_ID_SIZE, &change_capture.transaction_group_id, TX_MSG_CAPTURE_TX_GROUP_ID_SIZE);
	memcpy_s(tx_change_bytes.get() + TX_MSG_CAPTURE_TX_ID_OFFSET, TX_MSG_CAPTURE_TX_ID_SIZE, &change_capture.transaction_id, TX_MSG_CAPTURE_TX_ID_SIZE);
	memcpy_s(tx_change_bytes.get() + TX_MSG_CAPTURE_STATUS_OFFSET, TX_MSG_CAPTURE_STATUS_SIZE, &status, TX_MSG_CAPTURE_STATUS_SIZE);
	memcpy_s(tx_change_bytes.get() + TX_MSG_CAPTURE_TIMESTAMP_OFFSET, TX_MSG_CAPTURE_TIMESTAMP_SIZE, &current_timestamp, TX_MSG_CAPTURE_TIMESTAMP_SIZE);
	memcpy_s(tx_change_bytes.get() + TX_MSG_CAPTURE_FILE_START_POS_OFFSET, TX_MSG_CAPTURE_FILE_START_POS_SIZE, &change_capture.file_start_offset, TX_MSG_CAPTURE_FILE_START_POS_SIZE);
	memcpy_s(tx_change_bytes.get() + TX_MSG_CAPTURE_FILE_END_POS_OFFSET, TX_MSG_CAPTURE_FILE_END_POS_SIZE, &change_capture.file_end_offset, TX_MSG_CAPTURE_FILE_END_POS_SIZE);
	memcpy_s(tx_change_bytes.get() + TX_MSG_CAPTURE_MSG_ID_OFFSET, TX_MSG_CAPTURE_MSG_ID_SIZE, &change_capture.first_message_id, TX_MSG_CAPTURE_MSG_ID_SIZE);
	memcpy_s(tx_change_bytes.get() + TX_MSG_CAPTURE_SEG_ID_OFFSET, TX_MSG_CAPTURE_SEG_ID_SIZE, &change_capture.segment_id, TX_MSG_CAPTURE_SEG_ID_SIZE);
	memcpy_s(tx_change_bytes.get() + TX_MSG_CAPTURE_QUEUE_NAME_LENGTH_OFFSET, TX_MSG_CAPTURE_QUEUE_NAME_LENGTH_SIZE, &queue_name_length, TX_MSG_CAPTURE_QUEUE_NAME_LENGTH_SIZE);
	memcpy_s(tx_change_bytes.get() + TX_MSG_CAPTURE_QUEUE_NAME_OFFSET, queue_name_length, change_capture.queue.c_str(), queue_name_length);
	memcpy_s(tx_change_bytes.get() + TX_MSG_CAPTURE_PARTITION_ID_OFFSET, TX_MSG_CAPTURE_PARTITION_ID_SIZE, &change_capture.partition_id, TX_MSG_CAPTURE_PARTITION_ID_SIZE);

	long long write_pos = -1;

	try {
		write_pos = this->fh->write_to_file(
			partition->get_transaction_changes_key(),
			partition->get_transaction_changes_path(),
			TX_MSG_CAPTURE_TOTAL_BYTES,
			-1,
			tx_change_bytes.get(),
			true
		);
	}
	catch (const std::exception& ex) {
		this->logger->log_error(ex.what());

		std::string temp_tx_changes_key = this->pm->get_partition_tx_changes_key(
			partition->get_queue_name(), partition->get_partition_id(), true
		);

		std::string temp_tx_changes_path = this->pm->get_partition_tx_changes_path(
			partition->get_queue_name(), partition->get_partition_id(), true
		);

		if (this->fh->check_if_exists(temp_tx_changes_path)) {
			this->fh->rename_file(temp_tx_changes_key, temp_tx_changes_path, partition->get_transaction_changes_path());

			write_pos = this->fh->write_to_file(
				partition->get_transaction_changes_key(),
				partition->get_transaction_changes_path(),
				TX_MSG_CAPTURE_TOTAL_BYTES,
				-1,
				tx_change_bytes.get(),
				true
			);
		}
		else throw ex;
	}

	slock.unlock();

	if (write_pos >= MAX_TRANSACTION_SEGMENT_SIZE)
		std::thread([this, partition]() {
			std::unique_lock<std::shared_mutex> xlock(this->transaction_changes_mut);
			this->compact_transaction_change_captures(partition);
		}).detach();
}

void TransactionHandler::capture_transaction_changes_end(Partition* partition, TransactionChangeCapture* change_capture) {
	TransactionChangeCapture dummy_change_capture = TransactionChangeCapture{
		0,
		0,
		0,
		change_capture->transaction_id,
		change_capture->transaction_group_id,
		change_capture->queue,
		change_capture->partition_id,
		partition->get_active_segment()->get_id()
	};

	std::lock_guard<std::mutex> lock(partition->transaction_chages_mut);

	this->capture_transaction_changes(partition, dummy_change_capture, TransactionStatus::END);
}

void TransactionHandler::capture_transaction_change_to_memory(TransactionChangeCapture& change_capture) {
	std::lock_guard<std::shared_mutex> lock(this->transactions_mut);

	std::shared_ptr<TransactionChangeCapture> tx_capture_copy = std::make_shared<TransactionChangeCapture>();
	tx_capture_copy.get()->transaction_id = change_capture.transaction_id;
	tx_capture_copy.get()->transaction_group_id = change_capture.transaction_group_id;
	tx_capture_copy.get()->first_message_id = change_capture.first_message_id;
	tx_capture_copy.get()->file_start_offset = change_capture.file_start_offset;
	tx_capture_copy.get()->file_end_offset = change_capture.file_end_offset;

	std::string key = this->get_transaction_key(change_capture.transaction_group_id, change_capture.transaction_id);

	auto tx_captured_changes = this->transaction_changes[key];

	if (tx_captured_changes == nullptr) {
		tx_captured_changes = std::make_shared<std::vector<std::shared_ptr<TransactionChangeCapture>>>();
		this->transaction_changes[key] = tx_captured_changes;
	}

	tx_captured_changes.get()->emplace_back(tx_capture_copy);
}

void TransactionHandler::compact_transaction_change_captures(Partition* partition) {
	std::string temp_tx_captures_key = this->pm->get_partition_tx_changes_key(partition->get_queue_name(), partition->get_partition_id(), true);
	std::string temp_tx_captures_path = this->pm->get_partition_tx_changes_path(partition->get_queue_name(), partition->get_partition_id(), true);

	if (this->fh->check_if_exists(temp_tx_captures_path) || !this->fh->check_if_exists(partition->get_transaction_changes_path())) {
		this->logger->log_error("Cannot compact queue's " + partition->get_queue_name() + " partition's " + std::to_string(partition->get_partition_id()) + " transaction changes. Something went wrong with their files");
		return;
	}

	std::unordered_map<std::string, std::shared_ptr<std::vector<std::shared_ptr<char>>>> transaction_change_captures;

	unsigned int read_batch_size = (READ_MESSAGES_BATCH_SIZE / TX_MSG_CAPTURE_TOTAL_BYTES) * TX_MSG_CAPTURE_TOTAL_BYTES;

	std::unique_ptr<char> batch = std::unique_ptr<char>(new char[read_batch_size]);

	unsigned int read_pos = 0;

	unsigned int bytes_read = this->fh->read_from_file(
		partition->get_transaction_changes_key(),
		partition->get_transaction_changes_path(),
		read_batch_size,
		read_pos,
		batch.get()
	);

	unsigned long long transaction_group_id = 0;
	unsigned long long transaction_id = 0;
	TransactionStatus status = TransactionStatus::NONE;

	std::string tx_key = "";

	while (bytes_read > 0) {

		unsigned int offset = 0;

		while (offset < bytes_read) {
			memcpy_s(&transaction_group_id, TX_MSG_CAPTURE_TX_GROUP_ID_SIZE, batch.get() + offset + TX_MSG_CAPTURE_TX_GROUP_ID_OFFSET, TX_MSG_CAPTURE_TX_GROUP_ID_SIZE);
			memcpy_s(&transaction_id, TX_MSG_CAPTURE_TX_ID_SIZE, batch.get() + offset + TX_MSG_CAPTURE_TX_ID_OFFSET, TX_MSG_CAPTURE_TX_ID_SIZE);
			memcpy_s(&status, TX_MSG_CAPTURE_STATUS_SIZE, batch.get() + offset + TX_MSG_CAPTURE_STATUS_OFFSET, TX_MSG_CAPTURE_STATUS_SIZE);

			tx_key = this->get_transaction_key(transaction_group_id, transaction_id);

			if (status == TransactionStatus::END) {
				transaction_change_captures.erase(tx_key);
				offset += TX_MSG_CAPTURE_TOTAL_BYTES;
				continue;
			}

			if (status == TransactionStatus::BEGIN)
				transaction_change_captures[tx_key] = std::make_shared<std::vector<std::shared_ptr<char>>>();

			std::shared_ptr<char> tx_change_info = std::shared_ptr<char>(new char[TX_MSG_CAPTURE_TOTAL_BYTES]);
			memcpy_s(tx_change_info.get(), TX_MSG_CAPTURE_TOTAL_BYTES, batch.get() + offset, TX_MSG_CAPTURE_TOTAL_BYTES);

			transaction_change_captures[tx_key].get()->emplace_back(tx_change_info);

			offset += TX_MSG_CAPTURE_TOTAL_BYTES;
		}

		if (bytes_read < read_batch_size) break;

		read_pos += read_batch_size;

		this->fh->read_from_file(
			partition->get_transaction_changes_key(),
			partition->get_transaction_changes_path(),
			read_batch_size,
			read_pos,
			batch.get()
		);
	}

	this->fh->create_new_file(
		temp_tx_captures_path,
		0,
		NULL,
		temp_tx_captures_key,
		false
	);

	for (auto& iter : transaction_change_captures)
		for (auto& tx_info : *(iter.second.get()))
			this->fh->write_to_file(
				temp_tx_captures_key,
				temp_tx_captures_path,
				TX_CHANGE_TOTAL_BYTES,
				-1,
				tx_info.get(),
				true
			);

	this->fh->delete_dir_or_file(partition->get_transaction_changes_path(), partition->get_transaction_changes_key());
	this->fh->rename_file(temp_tx_captures_key, temp_tx_captures_path, partition->get_transaction_changes_path());
}

std::string TransactionHandler::get_transaction_key(unsigned long long transaction_group_id, unsigned long long transaction_id) {
	return std::to_string(transaction_group_id) + "_" + std::to_string(transaction_id);
}

std::shared_ptr<std::unordered_set<int>> TransactionHandler::find_all_transaction_nodes(unsigned long long transaction_group_id) {
	int node_id = 0;

	{
		std::shared_lock<std::shared_mutex> slock1(this->cluster_metadata->transaction_groups_mut);
		node_id = this->cluster_metadata->transaction_group_nodes[transaction_group_id];
	}

	if (node_id <= 0)
		throw std::runtime_error("Transaction's group " + std::to_string(transaction_group_id) + " assigned node not found");

	std::shared_ptr<std::unordered_set<std::string>> tx_group_assigned_queues = nullptr;

	{
		std::shared_lock<std::shared_mutex> slock2(this->cluster_metadata->transaction_groups_mut);

		auto node_tx_groups = this->cluster_metadata->nodes_transaction_groups[transaction_group_id];

		if (node_tx_groups == nullptr || node_tx_groups.get()->find(transaction_group_id) == node_tx_groups.get()->end())
			throw std::runtime_error("Transaction group " + std::to_string(transaction_group_id) + " not found");

		tx_group_assigned_queues = (*(node_tx_groups.get()))[transaction_group_id];

		if (tx_group_assigned_queues == nullptr)
			throw std::runtime_error("No assigned queues found for transaction group " + std::to_string(transaction_group_id));
	}

	std::shared_ptr<std::unordered_set<int>> transaction_nodes = std::make_shared<std::unordered_set<int>>();

	std::shared_lock<std::shared_mutex> slock2(this->cluster_metadata->nodes_partitions_mut);

	for (const std::string& tx_assigned_queue : *(tx_group_assigned_queues.get()))
	{
		if (this->cluster_metadata->partition_leader_nodes.find(tx_assigned_queue) == this->cluster_metadata->partition_leader_nodes.end()
			|| this->cluster_metadata->partition_leader_nodes[tx_assigned_queue] == nullptr)
			throw std::runtime_error("Queue partition leaders not found for assigned transaction group's " + std::to_string(transaction_group_id) + " queue " + tx_assigned_queue);

		QueueMetadata* metadata = this->cluster_metadata->get_queue_metadata(tx_assigned_queue);

		if (metadata == NULL)
			throw std::runtime_error("Queue metadata not found for assigned transaction group's " + std::to_string(transaction_group_id) + " queue " + tx_assigned_queue);

		if (metadata->get_partitions() != this->cluster_metadata->partition_leader_nodes[tx_assigned_queue].get()->size())
			throw std::runtime_error("Not all queue partitions have assigned leaders on assigned transaction group's " + std::to_string(transaction_group_id) + " queue " + tx_assigned_queue);

		for (auto& iter : *(this->cluster_metadata->partition_leader_nodes[tx_assigned_queue].get()))
			transaction_nodes.get()->insert(iter.second);
	}

	return transaction_nodes;
}

std::tuple<bool, int> TransactionHandler::notify_node_about_transaction_status_change(int node_id, unsigned long long transaction_group_id, unsigned long long tx_id, TransactionStatus status_change) {
	if (node_id == this->settings->get_node_id()) {
		this->handle_transaction_status_change_notification(transaction_group_id, tx_id, status_change);
		return std::tuple<bool, int>(true, node_id);
	}

	std::shared_ptr<ConnectionPool> pool = this->cm->get_node_connection_pool(node_id);

	if (pool == nullptr) {
		this->logger->log_error("Could not notify node " + std::to_string(node_id) + " about transaction status change. No connection pool found.");
		return std::tuple<bool, int>(false, node_id);
	}

	std::unique_ptr<TransactionStatusUpdateRequest> req = std::make_unique<TransactionStatusUpdateRequest>();
	req.get()->transaction_group_id = transaction_group_id;
	req.get()->transaction_id = tx_id;
	req.get()->status = status_change;

	auto req_buf = this->transformer->transform(req.get());

	auto res_buf = this->cm->send_request_to_socket(
		pool.get(),
		1,
		std::get<1>(req_buf).get(),
		std::get<0>(req_buf),
		"TransactionStatusUpdate"
	);

	if (std::get<1>(res_buf) == -1) return std::tuple<bool, int>(false, node_id);

	std::unique_ptr<TransactionStatusUpdateResponse> res = this->response_mapper->to_transaction_status_update_response(
		std::get<1>(req_buf).get(), std::get<0>(req_buf)
	);

	return std::tuple<bool, int>(res != nullptr && res.get()->ok, node_id);
}

void TransactionHandler::handle_transaction_status_change_notification(unsigned long long transaction_group_id, unsigned long long tx_id, TransactionStatus status_change) {
	std::shared_lock<std::shared_mutex> slock(this->transaction_changes_mut);

	std::string tx_key = this->get_transaction_key(transaction_group_id, tx_id);
	std::string queue_partition_key = "";
	std::unordered_set<std::string> captured_changes_ends;

	if (this->transaction_changes.find(tx_key) == this->transaction_changes.end()) return;

	auto changes = this->transaction_changes[tx_key];

	if (changes == nullptr) return;

	slock.unlock();

	for (auto change : *(changes.get())) {
		if (change.get() == nullptr) continue;

		std::shared_ptr<Queue> queue = this->qm->get_queue(change.get()->queue);

		if (queue == nullptr) continue;

		std::shared_ptr<Partition> partition = queue.get()->get_partition(change.get()->partition_id);

		if (partition == nullptr) continue;

		queue_partition_key = partition->get_queue_name() + "_" + std::to_string(partition->get_partition_id());

		if (status_change == TransactionStatus::END && captured_changes_ends.find(queue_partition_key) == captured_changes_ends.end())
		{
			this->capture_transaction_changes_end(partition.get(), change.get());
			captured_changes_ends.insert(queue_partition_key);
			continue;
		}

		std::string segment_key = this->pm->get_file_key(change.get()->queue, change.get()->segment_id, change.get()->partition_id);
		std::string segment_path = this->pm->get_file_path(change.get()->queue, change.get()->segment_id, change.get()->partition_id);

		unsigned int messages_batch_size = change.get()->file_end_offset - change.get()->file_start_offset;

		std::unique_ptr<char> batch = std::unique_ptr<char>(new char[messages_batch_size]);

		unsigned int batch_size = this->fh->read_from_file(
			segment_key,
			segment_path,
			messages_batch_size,
			change.get()->file_start_offset,
			batch.get()
		);

		if (messages_batch_size != batch_size)
			throw std::runtime_error("Error occured while trying to finalize uncommited messages.");

		unsigned int offset = 0;
		unsigned int message_bytes = 0;
		unsigned long long message_id = 0;

		while (offset < batch_size) {
			memcpy_s(&message_bytes, TOTAL_METADATA_BYTES, batch.get() + offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);
			memcpy_s(&message_id, MESSAGE_ID_SIZE, batch.get() + offset + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);

			memcpy_s(batch.get() + offset + MESSAGE_COMMIT_STATUS_OFFSET, MESSAGE_COMMIT_STATUS_SIZE, &status_change, MESSAGE_COMMIT_STATUS_OFFSET);

			Helper::update_checksum(batch.get() + offset, message_bytes);

			this->ch->update_message_commit_status(
				change.get()->queue, change.get()->partition_id, change.get()->segment_id, message_id, status_change
			);

			offset += message_bytes;
		}

		this->fh->write_to_file(
			segment_key,
			segment_path,
			batch_size,
			change.get()->file_start_offset,
			batch.get(),
			true
		);
	}

	if (status_change != TransactionStatus::END) return;

	this->remove_transaction(transaction_group_id, tx_id);

	// TODO: Increase consume pointer in all partitions
}

void TransactionHandler::notify_group_nodes_node_about_transaction_status_change(std::shared_ptr<std::unordered_set<int>> tx_nodes, unsigned long long transaction_group_id, unsigned long long tx_id, TransactionStatus status_change) {
	std::vector<std::future<std::tuple<bool, int>>> node_notifications;

	std::string tx_key = this->get_transaction_key(transaction_group_id, tx_id);
	unsigned int segment_id = this->get_transaction_segment(tx_id);

	{
		std::lock_guard<std::shared_mutex> xlock(this->transactions_heartbeats_mut);
		if (this->transactions_heartbeats.find(tx_key) == this->transactions_heartbeats.end())
			throw std::runtime_error("The transaction is about to close or is closed already due to failure");
	}
	
	for (int node_id : *(tx_nodes.get()))
		node_notifications.emplace_back(
			std::async(
				std::launch::async,
				[this, node_id, transaction_group_id, tx_id, status_change]() {
					return this->notify_node_about_transaction_status_change(
						node_id, transaction_group_id, tx_id, status_change
					);
				}
			)
		);

	for (auto& res : node_notifications) {
		auto tup = res.get();

		if (!std::get<0>(tup))
			throw std::runtime_error(
				"Communication with node "
				+ std::to_string(std::get<1>(tup))
				+ " failed due to network issue or internal server error"
			);
	}

	this->write_transaction_change_to_segment(transaction_group_id, tx_id, segment_id, status_change);

	if (status_change != TransactionStatus::END) {
		{
			std::lock_guard<std::shared_mutex> xlock(this->transactions_heartbeats_mut);
			if (this->transactions_heartbeats.find(tx_key) == this->transactions_heartbeats.end())
				throw std::runtime_error("The transaction is about to close or is closed already due to failure");
		}

		{
			std::lock_guard<std::shared_mutex> lock(this->transactions_mut);
			this->open_transactions_statuses[tx_key] = status_change;
		}

		this->update_transaction_heartbeat(transaction_group_id, tx_id);

		return;
	}

	this->remove_transaction(transaction_group_id, tx_id);
}

void TransactionHandler::remove_transaction(unsigned long long transaction_group_id, unsigned long long tx_id) {
	std::string tx_key = this->get_transaction_key(transaction_group_id, tx_id);

	{
		std::lock_guard<std::shared_mutex> xlock2(this->transactions_heartbeats_mut);
		this->transactions_heartbeats.erase(tx_key);
	}

	{
		std::lock_guard<std::shared_mutex> xlock3(this->transaction_changes_mut);
		this->transaction_changes.erase(tx_key);
	}

	{
		std::lock_guard<std::shared_mutex> xlock3(this->transactions_mut);
		this->open_transactions_statuses.erase(tx_key);

		if (this->open_transactions.find(transaction_group_id) != this->open_transactions.end()
			&& this->open_transactions[transaction_group_id] != nullptr)
			this->open_transactions[transaction_group_id].get()->erase(tx_id);
	}
}

void TransactionHandler::close_uncommited_open_transactions_when_leader_change(const std::string& queue_name) {
	std::lock_guard<std::shared_mutex> lock1(this->transactions_mut);
	std::lock_guard<std::mutex> lock2(this->transactions_to_close_mut);
	std::lock_guard<std::shared_mutex> lock3(this->transactions_heartbeats_mut);

	for (auto& iter : this->open_transactions)
		if (this->cluster_metadata->transaction_group_contains_queue(iter.first, queue_name))
		{
			for (auto& tx_id : *(iter.second.get()))
			{
				std::string tx_key = this->get_transaction_key(iter.first, tx_id);
				this->transactions_to_close.insert(tx_key);
				this->transactions_heartbeats.erase(tx_key);
			}

			iter.second.get()->clear();
		}
}

void TransactionHandler::check_for_expired_transaction_groups(std::atomic_bool* should_terminate) {
	if (!this->settings->get_is_controller_node()) return;

	while (!should_terminate->load()) {
		try {
			// TODO: Complete this method
		}
		catch (const std::exception& ex)
		{
			std::string err_msg = "Error occured while trying to check for expired transaction groups. Reason: " + std::string(ex.what());
			this->logger->log_error(err_msg);
		}

		std::this_thread::sleep_for(std::chrono::milliseconds(this->settings->get_check_for_expired_transaction_groups_ms()));
	}
}

void TransactionHandler::check_for_expired_transactions(std::atomic_bool* should_terminate) {
	while (!should_terminate->load()) {
		try {
			// TODO: Complete this method
		}
		catch (const std::exception& ex)
		{
			std::string err_msg = "Error occured while trying to check for expired transactions. Reason: " + std::string(ex.what());
			this->logger->log_error(err_msg);
		}

		std::this_thread::sleep_for(std::chrono::milliseconds(this->settings->get_check_for_expired_transactions_ms()));
	}
}

void TransactionHandler::close_failed_transactions_in_background(std::atomic_bool* should_terminate) {
	while (!should_terminate->load()) {
		try {
			// TODO: Complete this method
		} catch (const std::exception& ex)
		{
			std::string err_msg = "Error occured while trying to close failed transactions in background. Reason: " + std::string(ex.what());
			this->logger->log_error(err_msg);
		}

		std::this_thread::sleep_for(std::chrono::milliseconds(CHECK_FOR_TRANSACTIONS_TO_CLOSE));
	}
}