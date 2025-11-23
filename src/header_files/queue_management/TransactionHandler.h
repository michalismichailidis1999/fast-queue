#pragma once
#include <unordered_map>
#include <set>
#include <unordered_set>
#include <string>
#include <memory>
#include <atomic>
#include <chrono>
#include <shared_mutex>
#include <future>
#include <vector>
#include <tuple>
#include "../Settings.h"
#include "../logging/Logger.h"
#include "../cluster_management/ClusterMetadata.h"
#include "../network_management/ConnectionsManager.h"
#include "../file_management/FileHandler.h"
#include "../file_management/CacheHandler.h"
#include "../file_management/QueueSegmentFilePathMapper.h"
#include "../queue_management/Partition.h"
#include "../queue_management/QueueManager.h"
#include "../requests_management/Requests.h"
#include "../requests_management/Responses.h"
#include "../requests_management/ResponseMapper.h"
#include "../requests_management/ClassToByteTransformer.h"
#include "../util/Util.h"
#include "../util/ConnectionPool.h"
#include "../util/Helper.h"
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
	std::string queue;
	unsigned int partition_id;
	unsigned long long segment_id;
} TransactionChangeCapture;

class TransactionHandler {
private:
	QueueManager* qm;
	ConnectionsManager* cm;
	FileHandler* fh;
	CacheHandler* ch;
	QueueSegmentFilePathMapper* pm;
	ClusterMetadata* cluster_metadata;
	ResponseMapper* response_mapper;
	ClassToByteTransformer* transformer;
	Util* util;
	Settings* settings;
	Logger* logger;

	std::unordered_map<int, std::shared_ptr<TransactionFileSegment>> transaction_segment_files;

	std::unordered_map<unsigned long long, std::chrono::milliseconds> ts_groups_heartbeats;
	std::shared_mutex ts_groups_heartbeats_mut;

	std::unordered_map<std::string, std::chrono::milliseconds> transactions_heartbeats;
	std::shared_mutex transactions_heartbeats_mut;

	std::unordered_map<unsigned long long, std::shared_ptr<std::set<unsigned long long>>> open_transactions;
	std::unordered_map<std::string, std::shared_ptr<std::vector<std::shared_ptr<TransactionChangeCapture>>>> transaction_changes;
	std::unordered_map<std::string, TransactionStatus> open_transactions_statuses;
	std::shared_mutex transactions_mut;
	std::shared_mutex transaction_changes_mut;

	// Will handle them in the background
	std::set<std::string> transactions_to_close;
	std::mutex transactions_to_close_mut;

	std::function<bool(UnregisterTransactionGroupRequest*)> controller_unregister_transaction_group_cb;

	int get_transaction_segment(unsigned long long transaction_id);

	unsigned long long get_new_transaction_id(unsigned long long transaction_group_id);

	void write_transaction_change_to_segment(unsigned long long transaction_group_id, unsigned long long tx_id, int segment_id, TransactionStatus status_change);

	void compact_transaction_segment(TransactionFileSegment* ts_segment);

	void compact_transaction_change_captures(Partition* partition);

	void capture_transaction_change_to_memory(TransactionChangeCapture& change_capture);

	std::string get_transaction_key(unsigned long long transaction_group_id, unsigned long long transaction_id);

	std::shared_ptr<std::unordered_set<int>> find_all_transaction_nodes(unsigned long long transaction_group_id);

	std::tuple<bool, int> notify_node_about_transaction_status_change(int node_id, unsigned long long transaction_group_id, unsigned long long tx_id, TransactionStatus status_change);

	void notify_group_nodes_node_about_transaction_status_change(std::shared_ptr<std::unordered_set<int>> tx_nodes, unsigned long long transaction_group_id, unsigned long long tx_id, TransactionStatus status_change, bool closing_in_background = false);

	void remove_transaction_group(unsigned long long transaction_group_id);

	void remove_transaction(unsigned long long transaction_group_id, unsigned long long tx_id, bool closing_in_background = false);

	void get_transaction_key_parts(const std::string& tx_key, unsigned long long* transaction_group_id, unsigned long long* tx_id);
public:
	TransactionHandler(QueueManager* qm, ConnectionsManager* cm, FileHandler* fh, CacheHandler* ch, QueueSegmentFilePathMapper* pm, ClusterMetadata* cluster_metadata, ResponseMapper* response_mapper, ClassToByteTransformer* transformer, Util* util, Settings* settings, Logger* logger);

	void init_transaction_segment(int segment_id);

	void update_transaction_group_heartbeat(unsigned long long transaction_group_id);

	void update_transaction_heartbeat(unsigned long long transaction_group_id, unsigned long long tx_id);

	void add_transaction_group(unsigned long long transaction_group_id);

	void capture_transaction_changes(Partition* partition, TransactionChangeCapture& change_capture, TransactionStatus status = TransactionStatus::NONE);

	void capture_transaction_changes_end(Partition* partition, TransactionChangeCapture* change_capture);

	unsigned long long init_transaction(unsigned long long transaction_group_id);

	void finalize_transaction(unsigned long long transaction_group_id, unsigned long long tx_id, bool commit);

	void handle_transaction_status_change_notification(unsigned long long transaction_group_id, unsigned long long tx_id, TransactionStatus status_change);

	void close_uncommited_open_transactions_when_leader_change(const std::string& queue_name);

	void check_for_expired_transaction_groups(std::atomic_bool* should_terminate);

	void check_for_expired_transactions(std::atomic_bool* should_terminate);

	void close_failed_transactions_in_background(std::atomic_bool* should_terminate);

	bool unregister_transaction_group(UnregisterTransactionGroupRequest* request, unsigned long long transaction_group_id);

	void set_controller_unregister_transaction_group_cb(std::function<bool(UnregisterTransactionGroupRequest*)> cb);
};