#pragma once
#include <string>
#include <vector>
#include <tuple>
#include <memory>

struct ConnectionInfo;

class Settings {
private:
	// general properties
	int node_id;

	long max_message_size;
	long segment_size;
	long max_cached_memory;
	long flush_to_disk_after_ms;

	int request_parallelism;
	int request_polling_interval_ms;
	int maximum_connections;
	long request_timeout_ms;

	std::string log_path;
	std::string trace_log_path;
	// -------------------------------------------

	// node type properties
	bool is_controller_node;
	std::vector<std::tuple<int, std::shared_ptr<ConnectionInfo>>> controller_nodes;
	std::vector<std::tuple<int, std::shared_ptr<ConnectionInfo>>> external_controller_nodes;
	// -------------------------------------------

	// internal communication properties
	std::string internal_ip;
	int internal_port;

	bool internal_ssl_enabled;
	std::string internal_ssl_cert_path;
	std::string internal_ssl_cert_key_path;
	std::string internal_ssl_cert_ca_path;
	// -------------------------------------------

	// external communication properties
	std::string external_ip;
	int external_port;

	bool external_ssl_enabled;
	std::string external_ssl_cert_path;
	std::string external_ssl_cert_key_path;
	std::string external_ssl_cert_ca_path;
	// -------------------------------------------

	void set_settings_variable(char* conf, int var_start_pos, int var_end_pos, int equal_pos);
public:
	Settings(char* conf, long total_conf_chars);

	// general properties getters
	int get_node_id();

	long get_max_message_size();
	long get_segment_size();
	long get_max_cached_memory();
	long get_flush_to_disk_after_ms();

	int get_request_parallelism();
	int get_request_polling_interval_ms();
	int get_maximum_connections();
	long get_request_timeout_ms();

	const std::string& get_log_path();
	const std::string& get_trace_log_path();
	// -------------------------------------------
	
	// node type properties getters
	bool get_is_controller_node();
	std::vector<std::tuple<int, std::shared_ptr<ConnectionInfo>>>* get_controller_nodes();
	std::vector<std::tuple<int, std::shared_ptr<ConnectionInfo>>>* get_external_controller_nodes();
	// -------------------------------------------

	// internal communication properties getters
	const std::string& get_internal_ip();
	int get_internal_port();

	bool get_internal_ssl_enabled();
	const std::string& get_internal_ssl_cert_path();
	const std::string& get_internal_ssl_cert_key_path();
	const std::string& get_internal_ssl_cert_ca_path();
	// -------------------------------------------

	// external communication properties getters
	const std::string& get_external_ip();
	int get_external_port();

	bool get_external_ssl_enabled();
	const std::string& get_external_ssl_cert_path();
	const std::string& get_external_ssl_cert_key_path();
	const std::string& get_external_ssl_cert_ca_path();
	// -------------------------------------------
};