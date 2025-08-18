#include "../header_files/Settings.h"
#include "../header_files/network_management/Connection.h"

Settings::Settings(char* conf, long total_conf_chars) {
	int equals_pos = -1;
	int var_start_pos = 0;

	for (int i = 0; i < total_conf_chars; i++) {
		if (conf[i] == '=') equals_pos = i;
		else if (conf[i] == '\n' && equals_pos == -1) {
			var_start_pos = i + 1;
			continue;
		}
		else if ((conf[i] == '\n' || conf[i] == ' ' || conf[i] == '#') && equals_pos > 0) {
			int step_increase = 1;

			if (conf[i] == '#' || conf[i] == ' ')
				for (int j = i; j < total_conf_chars; j++)
					if (conf[j] == '\n') break;
					else step_increase++;

			this->set_settings_variable(conf, var_start_pos, conf[i - 1] == '\r' ? i - 2 : i - 1, equals_pos);
			var_start_pos = i + step_increase;
			equals_pos = -1;
		}

		if(i == total_conf_chars - 1 && equals_pos > 0)
			this->set_settings_variable(conf, var_start_pos, i, equals_pos);
	}

	if (this->segment_size > MAX_SEGMENT_SIZE) {
		std::string err_msg = "Segment cannot be larger than " + std::to_string(MAX_SEGMENT_SIZE) + " bytes";
		throw std::runtime_error(err_msg.c_str());
	}

	this->is_controller_node = false;

	for(auto& controller_tup : this->controller_nodes)
		if (std::get<0>(controller_tup) == this->node_id) {
			this->is_controller_node = true;
			break;
		}
}

void Settings::set_settings_variable(char* conf, int var_start_pos, int var_end_pos, int equal_pos) {
	int lhs_size = equal_pos - var_start_pos;
	int rhs_size = var_end_pos - equal_pos;

	if (lhs_size == 0)
		throw std::runtime_error("Incorrect syntax near = in configuration file");

	std::string lhs = lhs_size > 0 ? std::string(conf + var_start_pos, lhs_size) : "";
	std::string rhs = rhs_size > 0 ? std::string(conf + equal_pos + 1, rhs_size) : "";

	try
	{
		if (
			lhs == "node_id"
			|| lhs == "internal_port"
			|| lhs == "external_port"
			|| lhs == "request_parallelism"
			|| lhs == "request_polling_interval_ms"
			|| lhs == "maximum_connections"
			|| lhs == "max_message_size"
			|| lhs == "segment_size"
			|| lhs == "max_cached_memory"
			|| lhs == "request_timeout_ms"
			|| lhs == "flush_to_disk_after_ms"
			|| lhs == "retention_ms"
			|| lhs == "retention_worker_wait_ms"
			|| lhs == "dead_data_node_check_ms"
			|| lhs == "data_node_expire_ms"
			|| lhs == "heartbeat_to_leader_ms"
			|| lhs == "cluster_update_receive_ms"
			|| lhs == "dead_consumer_check_ms"
			|| lhs == "dead_consumer_expire_ms"
		) {
			unsigned int* val = lhs == "node_id" ? &this->node_id
				: lhs == "internal_port" ? &this->internal_port
				: lhs == "external_port" ? &this->external_port
				: lhs == "request_parallelism" ? &this->request_parallelism
				: lhs == "request_polling_interval_ms" ? &this->request_polling_interval_ms
				: lhs == "maximum_connections" ? &this->maximum_connections
				: lhs == "max_message_size" ? &this->max_message_size
				: lhs == "segment_size" ? &this->segment_size
				: lhs == "max_cached_memory" ? &this->max_cached_memory
				: lhs == "request_timeout_ms" ? &this->request_timeout_ms
				: lhs == "flush_to_disk_after_ms" ? &this->flush_to_disk_after_ms
				: lhs == "retention_ms" ? &this->retention_ms
				: lhs == "retention_worker_wait_ms" ? &this->retention_worker_wait_ms
				: lhs == "dead_data_node_check_ms" ? &this->dead_data_node_check_ms
				: lhs == "data_node_expire_ms" ? &this->data_node_expire_ms
				: lhs == "heartbeat_to_leader_ms" ? &this->heartbeat_to_leader_ms
				: lhs == "cluster_update_receive_ms" ? &this->cluster_update_receive_ms
				: lhs == "dead_consumer_check_ms" ? &this->dead_consumer_check_ms
				: &this->dead_consumer_expire_ms;

			*(val) = rhs_size > 0 ? std::atoi(rhs.c_str()) : 0;
		}
		else if (
			lhs == "log_path"
			|| lhs == "trace_log_path"
			|| lhs == "external_ip"
			|| lhs == "external_ssl_cert_path"
			|| lhs == "external_ssl_cert_key_path"
			|| lhs == "external_ssl_cert_ca_path"
			|| lhs == "external_ssl_cert_pass"
			|| lhs == "internal_ip"
			|| lhs == "internal_ssl_cert_path"
			|| lhs == "internal_ssl_cert_key_path"
			|| lhs == "internal_ssl_cert_ca_path"
			|| lhs == "internal_ssl_cert_pass"
		) {
			std::string* val = lhs == "log_path" ? &this->log_path
				: lhs == "trace_log_path" ? &this->trace_log_path
				: lhs == "external_ip" ? &this->external_ip
				: lhs == "external_ssl_cert_path" ? &this->external_ssl_cert_path
				: lhs == "external_ssl_cert_key_path" ? &this->external_ssl_cert_key_path
				: lhs == "external_ssl_cert_ca_path" ? &this->external_ssl_cert_ca_path
				: lhs == "external_ssl_cert_pass" ? &this->external_ssl_cert_pass
				: lhs == "internal_ip" ? &this->internal_ip
				: lhs == "internal_ssl_cert_path" ? &this->internal_ssl_cert_path
				: lhs == "internal_ssl_cert_key_path" ? &this->internal_ssl_cert_key_path
				: lhs == "internal_ssl_cert_ca_path" ? &this->internal_ssl_cert_ca_path
				: &this->internal_ssl_cert_pass;

			for (int i = 0; i < rhs.size(); i++)
				if (rhs[i] == '\\')
					rhs[i] = '/';

			*val = rhs;
		}
		else if ((lhs == "controller_nodes") && rhs_size > 0) {
			int id_server_seperator_pos = -1;
			int starting_pos = 0;

			std::vector<std::tuple<int, std::shared_ptr<ConnectionInfo>>>* nodes = &this->controller_nodes;

			int node_id = 0;
			bool parsing_external_part = true;

			int external_url_ending_pos = -1;

			std::shared_ptr<ConnectionInfo> info = nullptr;

			for (int i = 0; i < rhs_size; i++) {
				if (rhs[i] == ',' || rhs[i] == '&' || i == rhs_size - 1) {

					if(parsing_external_part)
						node_id = std::atoi(std::string(rhs.c_str() + starting_pos, id_server_seperator_pos - starting_pos).c_str());

					std::string server_url = parsing_external_part
						? std::string(rhs.c_str() + id_server_seperator_pos + 1, i - id_server_seperator_pos)
						: std::string(rhs.c_str() + external_url_ending_pos + 1, i - external_url_ending_pos);

					int j = 0;
					for (j = 0; j < server_url.size(); j++)
						if (server_url[j] == ':')
							break;

					if (j == server_url.size())
						throw std::runtime_error("No controller url found");

					std::string address = server_url.substr(0, j);
					int port = std::atoi(server_url.substr(j + 1, server_url.size() - address.size() - 1 - (rhs[i] == ',' || rhs[i] == '&' ? 1 : 0)).c_str());

					if (parsing_external_part) {
						info = std::make_shared<ConnectionInfo>();
						info.get()->external_port = port;
						info.get()->external_address = address == "localhost" ? "127.0.0.1" : address;

						nodes->emplace_back(std::tuple<int, std::shared_ptr<ConnectionInfo>>(node_id, info));
					}
					else {
						info.get()->port = port;
						info.get()->address = address == "localhost" ? "127.0.0.1" : address;
					}

					if (!parsing_external_part) {
						id_server_seperator_pos = -1;
						starting_pos = i + 1;
					}
					else {
						external_url_ending_pos = i;
						parsing_external_part = false;
					}
				}
				else if (rhs[i] == '@') {
					id_server_seperator_pos = i;
					parsing_external_part = true;
				}
			}

			if(nodes->size() == 0)
				throw std::runtime_error("No controller node found");
		}
		else if (
			lhs == "internal_ssl_enabled"
			|| lhs == "internal_mutual_tls_enabled"
			|| lhs == "external_ssl_enabled"
			|| lhs == "external_mutual_tls_enabled"
			|| lhs == "external_user_authentication_enabled"
		) {
			bool* val = lhs == "internal_ssl_enabled" ? &this->internal_ssl_enabled
				: lhs == "internal_mutual_tls_enabled" ? &this->internal_mutual_tls_enabled
				: lhs == "external_ssl_enabled" ? &this->external_ssl_enabled
				: lhs == "external_mutual_tls_enabled" ? &this->external_mutual_tls_enabled
				: &this->external_user_authentication_enabled;

			*val = rhs == "true";
		}
	}
	catch (const std::exception&)
	{
		throw std::runtime_error(("Invalid " + lhs + " value in configuration file").c_str());
	}
}

// general properties getters

unsigned int Settings::get_node_id() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->node_id;
}

unsigned int Settings::get_max_message_size() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->max_message_size;
}

unsigned int Settings::get_segment_size() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->segment_size;
}

unsigned int Settings::get_index_message_gap_size() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->index_message_gap_size;
}

unsigned int Settings::get_max_cached_memory() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->max_cached_memory;
}

unsigned int Settings::get_flush_to_disk_after_ms() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->flush_to_disk_after_ms;
}

unsigned int Settings::get_request_parallelism() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->request_parallelism;
}

unsigned int Settings::get_request_polling_interval_ms() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->request_polling_interval_ms;
}

unsigned int Settings::get_maximum_connections() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->maximum_connections;
}

unsigned int Settings::get_idle_connection_check_ms() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->idle_connection_check_ms;
}

unsigned int Settings::get_idle_connection_timeout_ms() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->idle_connection_timeout_ms;
}

unsigned int Settings::get_request_timeout_ms() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->request_timeout_ms;
}

unsigned int Settings::get_retention_ms() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->retention_ms;
}

unsigned int Settings::get_retention_worker_wait_ms() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->retention_worker_wait_ms;
}

unsigned int Settings::get_dead_data_node_check_ms() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->dead_data_node_check_ms;
}

unsigned int Settings::get_data_node_expire_ms() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->data_node_expire_ms;
	//return 100000000;
}

unsigned int Settings::get_heartbeat_to_leader_ms() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->heartbeat_to_leader_ms;
}

unsigned int Settings::get_cluster_update_receive_ms() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->cluster_update_receive_ms;
}

unsigned int Settings::get_dead_consumer_check_ms() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->dead_consumer_check_ms;
}

unsigned int Settings::get_dead_consumer_expire_ms() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->dead_consumer_expire_ms;
}

const std::string& Settings::get_log_path() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->log_path;
}

const std::string& Settings::get_trace_log_path() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->trace_log_path;
}

// ------------------------------------------------------------

// node type properties getters

bool Settings::get_is_controller_node() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->is_controller_node;
}

std::vector<std::tuple<int, std::shared_ptr<ConnectionInfo>>> Settings::get_controller_nodes() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->controller_nodes;
}

// ------------------------------------------------------------

// internal communication properties getters

const std::string& Settings::get_internal_ip() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->internal_ip;
}

unsigned int Settings::get_internal_port() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->internal_port;
}

bool Settings::get_internal_ssl_enabled() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->internal_ssl_enabled;
}

const std::string& Settings::get_internal_ssl_cert_path() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->internal_ssl_cert_path;
}

const std::string& Settings::get_internal_ssl_cert_key_path() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->internal_ssl_cert_key_path;
}

const std::string& Settings::get_internal_ssl_cert_ca_path() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->internal_ssl_cert_ca_path;
}

const std::string& Settings::get_internal_ssl_cert_pass() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->internal_ssl_cert_pass;
}

bool Settings::get_internal_mutual_tls_enabled() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->internal_mutual_tls_enabled;
}

// ------------------------------------------------------------

// external communication properties getters

const std::string& Settings::get_external_ip() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->external_ip;
}

unsigned int Settings::get_external_port() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->external_port;
}

bool Settings::get_external_ssl_enabled() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->external_ssl_enabled;
}

const std::string& Settings::get_external_ssl_cert_path() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->external_ssl_cert_path;
}

const std::string& Settings::get_external_ssl_cert_key_path() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->external_ssl_cert_key_path;
}

const std::string& Settings::get_external_ssl_cert_ca_path() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->external_ssl_cert_ca_path;
}

const std::string& Settings::get_external_ssl_cert_pass() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->external_ssl_cert_pass;
}

bool Settings::get_external_mutual_tls_enabled() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->external_mutual_tls_enabled;
}

bool Settings::get_external_user_authentication_enabled() {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->external_user_authentication_enabled;
}

// ------------------------------------------------------------