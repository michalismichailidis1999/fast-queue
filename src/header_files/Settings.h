#pragma once
#include <string>
#include <vector>
#include <tuple>
#include <memory>
#include <atomic>
#include <shared_mutex>
#include <functional>

typedef struct {
	bool controller_nodes;
} SettingsUpdates;

struct ConnectionInfo;

class Settings {
private:
	// general properties
	unsigned int node_id;

	unsigned int max_message_size;
	unsigned int segment_size;
	unsigned int max_cached_memory;
	unsigned int flush_to_disk_after_ms;

	unsigned int request_parallelism;
	unsigned int request_polling_interval_ms;
	unsigned int maximum_connections;
	unsigned int request_timeout_ms;

	unsigned int retention_ms;
	unsigned int retention_worker_wait_ms;

	std::string log_path;
	std::string trace_log_path;
	// -------------------------------------------

	// node type properties
	bool is_controller_node;
	std::vector<std::tuple<int, std::shared_ptr<ConnectionInfo>>> controller_nodes;
	// -------------------------------------------

	// internal communication properties
	std::string internal_ip;
	unsigned int internal_port;

	bool internal_ssl_enabled;
	std::string internal_ssl_cert_path;
	std::string internal_ssl_cert_key_path;
	std::string internal_ssl_cert_ca_path;
	// -------------------------------------------

	// external communication properties
	std::string external_ip;
	unsigned int external_port;

	bool external_ssl_enabled;
	std::string external_ssl_cert_path;
	std::string external_ssl_cert_key_path;
	std::string external_ssl_cert_ca_path;
	// -------------------------------------------

	std::shared_mutex mut;

	void set_settings_variable(char* conf, int var_start_pos, int var_end_pos, int equal_pos);
public:
	Settings(char* conf, long total_conf_chars);

	// general properties getters
	unsigned int get_node_id();

	unsigned int get_max_message_size();
	unsigned int get_segment_size();
	unsigned int get_max_cached_memory();
	unsigned int get_flush_to_disk_after_ms();

	unsigned int get_request_parallelism();
	unsigned int get_request_polling_interval_ms();
	unsigned int get_maximum_connections();
	unsigned int get_request_timeout_ms();

	unsigned int get_retention_ms();
	unsigned int get_retention_worker_wait_ms();

	const std::string& get_log_path();
	const std::string& get_trace_log_path();
	// -------------------------------------------
	
	// node type properties getters
	bool get_is_controller_node();
	std::vector<std::tuple<int, std::shared_ptr<ConnectionInfo>>>* get_controller_nodes();
	// -------------------------------------------

	// internal communication properties getters
	const std::string& get_internal_ip();
	unsigned int get_internal_port();

	bool get_internal_ssl_enabled();
	const std::string& get_internal_ssl_cert_path();
	const std::string& get_internal_ssl_cert_key_path();
	const std::string& get_internal_ssl_cert_ca_path();
	// -------------------------------------------

	// external communication properties getters
	const std::string& get_external_ip();
	unsigned int get_external_port();

	bool get_external_ssl_enabled();
	const std::string& get_external_ssl_cert_path();
	const std::string& get_external_ssl_cert_key_path();
	const std::string& get_external_ssl_cert_ca_path();
	// -------------------------------------------

	void update_values_with_new_settings(Settings* new_settings);

	bool should_stop_server(Settings* new_settings);
};