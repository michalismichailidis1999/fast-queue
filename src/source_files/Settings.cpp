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
}

void Settings::set_settings_variable(char* conf, int var_start_pos, int var_end_pos, int equal_pos) {
	int lhs_size = equal_pos - var_start_pos;
	int rhs_size = var_end_pos - equal_pos;

	if (lhs_size == 0)
		throw std::exception("Incorrect syntax near = in configuration file");

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
		) {
			int* val = lhs == "node_id" ? &this->node_id
				: lhs == "internal_port" ? &this->internal_port
				: lhs == "external_port" ? &this->external_port
				: lhs == "request_parallelism" ? &this->request_parallelism
				: lhs == "request_polling_interval_ms" ? &this->request_polling_interval_ms
				: &this->maximum_connections;

			*(val) = rhs_size > 0 ? std::atoi(rhs.c_str()) : 0;
		}
		else if (
			lhs == "max_message_size"
			|| lhs == "segment_size"
			|| lhs == "max_cached_memory"
			|| lhs == "flush_to_disk_after_ms"
			|| lhs == "request_timeout_ms"
		) {
			long* val = lhs == "max_message_size" ? &this->max_message_size
				: lhs == "segment_size" ? &this->segment_size
				: lhs == "max_cached_memory" ? &this->max_cached_memory
				: lhs == "request_timeout_ms" ? &this->request_timeout_ms
				: &this->flush_to_disk_after_ms;
			*(val) = rhs_size > 0 ? std::atol(rhs.c_str()) : 0;
		}
		else if (
			lhs == "log_path"
			|| lhs == "trace_log_path"
			|| lhs == "external_ip"
			|| lhs == "external_ssl_cert_path"
			|| lhs == "external_ssl_cert_key_path"
			|| lhs == "external_ssl_cert_ca_path"
			|| lhs == "internal_ip"
			|| lhs == "internal_ssl_cert_path"
			|| lhs == "internal_ssl_cert_key_path"
			|| lhs == "internal_ssl_cert_ca_path"
		) {
			std::string* val = lhs == "log_path" ? &this->log_path
				: lhs == "trace_log_path" ? &this->trace_log_path
				: lhs == "external_ip" ? &this->external_ip
				: lhs == "external_ssl_cert_path" ? &this->external_ssl_cert_path
				: lhs == "external_ssl_cert_key_path" ? &this->external_ssl_cert_key_path
				: lhs == "external_ssl_cert_ca_path" ? &this->external_ssl_cert_ca_path
				: lhs == "internal_ip" ? &this->internal_ip
				: lhs == "internal_ssl_cert_path" ? &this->internal_ssl_cert_path
				: lhs == "internal_ssl_cert_key_path" ? &this->internal_ssl_cert_key_path
				: &this->internal_ssl_cert_ca_path;

			*val = rhs;
		}
		else if ((lhs == "controller_nodes" || lhs == "external_controller_nodes") && rhs_size > 0) {
			int id_server_seperator_pos = -1;
			int starting_pos = 0;

			std::vector<std::tuple<int, std::shared_ptr<ConnectionInfo>>>* nodes = lhs == "controller_nodes"
				? &this->controller_nodes
				: &this->external_controller_nodes;

			for (int i = 0; i < rhs_size; i++) {
				if (rhs[i] == ',' || i == rhs_size - 1) {
					int node_id = std::atoi(std::string(rhs.c_str() + starting_pos, id_server_seperator_pos - starting_pos).c_str());
					std::string server_url = std::string(rhs.c_str() + id_server_seperator_pos + 1, i - id_server_seperator_pos);

					int j = 0;
					for (j = 0; j < server_url.size(); j++)
						if (server_url[j] == ':')
							break;

					if (j == server_url.size())
						throw std::exception();

					std::string address = server_url.substr(0, j);
					int port = std::atoi(server_url.substr(j + 1, server_url.size() - address.size() - 1 - (rhs[i] == ',' ? 1 : 0)).c_str());

					std::shared_ptr<ConnectionInfo> info = std::make_shared<ConnectionInfo>();
					info.get()->port = port;
					info.get()->address = address == "localhost" ? "127.0.0.1" : address;

					nodes->emplace_back(std::tuple<int, std::shared_ptr<ConnectionInfo>>(node_id, info));

					id_server_seperator_pos = -1;
					starting_pos = i + 1;
				}
				else if (rhs[i] == '@') id_server_seperator_pos = i;
			}

			if(nodes->size() == 0)
				throw std::exception();
		}
		else if (
			lhs == "is_controller_node" 
			|| lhs == "internal_ssl_enabled"
			|| lhs == "external_ssl_enabled"
		) {
			bool* val = lhs == "is_controller_node" ? &this->is_controller_node
				: lhs == "internal_ssl_enabled" ? &this->internal_ssl_enabled
				: &this->external_ssl_enabled;

			*val = rhs == "true";
		}
	}
	catch (const std::exception&)
	{
		throw std::exception(("Invalid " + lhs + " value in configuration file").c_str());
	}
}

// general properties getters

int Settings::get_node_id() {
	return this->node_id;
}

long Settings::get_max_message_size() {
	return this->max_message_size;
}

long Settings::get_segment_size() {
	return this->segment_size;
}

long Settings::get_max_cached_memory() {
	return this->max_cached_memory;
}

long Settings::get_flush_to_disk_after_ms() {
	return this->flush_to_disk_after_ms;
}

int Settings::get_request_parallelism() {
	return this->request_parallelism;
}

int Settings::get_request_polling_interval_ms() {
	return this->request_polling_interval_ms;
}

int Settings::get_maximum_connections() {
	return this->maximum_connections;
}

long Settings::get_request_timeout_ms() {
	return this->request_timeout_ms;
}

const std::string& Settings::get_log_path() {
	return this->log_path;
}

const std::string& Settings::get_trace_log_path() {
	return this->trace_log_path;
}

// ------------------------------------------------------------

// node type properties getters

bool Settings::get_is_controller_node() {
	return this->is_controller_node;
}

std::vector<std::tuple<int, std::shared_ptr<ConnectionInfo>>>* Settings::get_controller_nodes() {
	return &this->controller_nodes;
}

std::vector<std::tuple<int, std::shared_ptr<ConnectionInfo>>>* Settings::get_external_controller_nodes() {
	return &this->external_controller_nodes;
}

// ------------------------------------------------------------

// internal communication properties getters

const std::string& Settings::get_internal_ip() {
	return this->internal_ip;
}

int Settings::get_internal_port() {
	return this->internal_port;
}

bool Settings::get_internal_ssl_enabled() {
	return this->internal_ssl_enabled;
}

const std::string& Settings::get_internal_ssl_cert_path() {
	return this->internal_ssl_cert_path;
}

const std::string& Settings::get_internal_ssl_cert_key_path() {
	return this->internal_ssl_cert_key_path;
}

const std::string& Settings::get_internal_ssl_cert_ca_path() {
	return this->internal_ssl_cert_ca_path;
}

// ------------------------------------------------------------

// external communication properties getters

const std::string& Settings::get_external_ip() {
	return this->external_ip;
}

int Settings::get_external_port() {
	return this->external_port;
}

bool Settings::get_external_ssl_enabled() {
	return this->external_ssl_enabled;
}

const std::string& Settings::get_external_ssl_cert_path() {
	return this->external_ssl_cert_path;
}

const std::string& Settings::get_external_ssl_cert_key_path() {
	return this->external_ssl_cert_key_path;
}

const std::string& Settings::get_external_ssl_cert_ca_path() {
	return this->external_ssl_cert_ca_path;
}

// ------------------------------------------------------------