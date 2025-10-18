#pragma once
#include <unordered_map>
#include <set>
#include <queue>
#include <string>
#include <memory>
#include <chrono>
#include <shared_mutex>
#include "../Settings.h"
#include "../logging/Logger.h"
#include "../cluster_management/ClusterMetadata.h"
#include "../network_management/ConnectionsManager.h"
#include "../file_management/FileHandler.h"
#include "../file_management/QueueSegmentFilePathMapper.h"
#include "../queue_management/Partition.h"
#include "../util/Util.h"
#include "../Constants.h"
#include "../Enums.h"

#include "../__linux/memcpy_s.h"

typedef struct {
	std::string file_path;
	std::string file_key;
	std::string temp_file_key;
	std::string temp_file_path;
	std::shared_mutex mut;
	unsigned long long written_bytes;
	int segment_id;
} TransactionFileSegment;

typedef struct {
	long long file_start_offset;
	long long file_end_offset;
	unsigned long long first_message_id;
	unsigned long long transaction_id;
	unsigned long long transaction_group_id;
} TransactionChangeCapture;

class TransactionHandler {
private:
	ConnectionsManager* cm;
	FileHandler* fh;
	QueueSegmentFilePathMapper* pm;
	ClusterMetadata* cluster_metadata;
	Util* util;
	Settings* settings;
	Logger* logger;

	std::unordered_map<int, std::shared_ptr<TransactionFileSegment>> transaction_segment_files;

	std::unordered_map<unsigned long long, std::chrono::milliseconds> ts_groups_heartbeats;
	std::shared_mutex ts_groups_heartbeats_mut;

	std::unordered_map<unsigned long long, std::chrono::milliseconds> transactions_heartbeats;
	std::shared_mutex transactions_heartbeats_mut;

	std::unordered_map<unsigned long long, std::shared_ptr<std::set<unsigned long long>>> open_transactions;
	std::unordered_map<unsigned long long, unsigned long long> transactions_groups_map;
	std::unordered_map<unsigned long long, std::shared_ptr<std::queue<std::shared_ptr<TransactionChangeCapture>>>> transaction_changes;
	std::shared_mutex transactions_mut;

	// Will handle them in the backgroun (only in case when transaction group is unregistered due to timeout)
	std::set<unsigned long long> transactions_to_close;
	std::mutex transactions_to_close_mut;

	int get_transaction_segment(unsigned long long transaction_id);

	unsigned long long get_new_transaction_id(unsigned long long transaction_group_id);

	void write_transaction_change_to_segment(unsigned long long transaction_group_id, unsigned long long tx_id, int segment_id, TransactionStatus status_change);

	void compact_transaction_segment(TransactionFileSegment* ts_segment);

	void capture_transaction_change_to_memory(TransactionChangeCapture& change_capture);
public:
	TransactionHandler(ConnectionsManager* cm, FileHandler* fh, QueueSegmentFilePathMapper* pm, ClusterMetadata* cluster_metadata, Util* util, Settings* settings, Logger* logger);

	void init_transaction_segment(int segment_id);

	void update_transaction_group_heartbeat(unsigned long long transaction_group_id);

	void update_transaction_heartbeat(unsigned long long tx_id);

	void add_transaction_group(unsigned long long transaction_group_id);

	void remove_transaction_group(unsigned long long transaction_group_id);

	void capture_transaction_changes(Partition* partition, TransactionChangeCapture& change_capture);

	unsigned long long init_transaction(unsigned long long transaction_group_id);
};