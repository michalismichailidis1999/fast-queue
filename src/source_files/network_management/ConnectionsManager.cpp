#include "../../header_files/network_management/ConnectionsManager.h"

ConnectionsManager::ConnectionsManager(SocketHandler* socket_handler, SslContextHandler* ssl_context_handler, ResponseMapper* response_mapper, Util* util, Settings* settings, Logger* logger, std::atomic_bool* should_terminate) {
	this->socket_handler = socket_handler;
	this->ssl_context_handler = ssl_context_handler;
	this->response_mapper = response_mapper;
	this->util = util;
	this->logger = logger;
	this->settings = settings;
	this->should_terminate = should_terminate;
	this->ssl_context = settings->get_internal_ssl_enabled() ? this->ssl_context_handler->create_ssl_context(true) : nullptr;
	this->failed_to_create_ssl_context = settings->get_internal_ssl_enabled() && this->ssl_context.get() == NULL;
}

bool ConnectionsManager::receive_socket_buffer(SOCKET_ID socket, SSL* ssl, char* res_buf, unsigned int res_buf_len) {
	int res_code = ssl != NULL
		? this->ssl_context_handler->receive_ssl_buffer(ssl, res_buf, res_buf_len)
		: this->socket_handler->receive_socket_buffer(socket, res_buf, res_buf_len);

	bool is_connection_broken = res_code > 0
		? false
		: ssl != NULL
		? this->ssl_context_handler->is_connection_broken(res_code)
		: this->socket_handler->is_connection_broken(res_code);

	if (is_connection_broken) {
		if (ssl != NULL) this->ssl_context_handler->free_ssl(ssl);

		this->socket_handler->close_socket(socket);
	}

	if (res_code <= 0)
	{
		this->logger->log_error("Could not receive socket's data");
		this->close_socket_connection(socket, ssl);
	}
	else this->update_socket_heartbeat(socket);

	return res_code > 0;
}

bool ConnectionsManager::respond_to_socket(SOCKET_ID socket, SSL* ssl, char* res_buf, unsigned int res_buf_len) {
	try
	{
		int res_code = ssl != NULL
			? this->ssl_context_handler->respond_to_ssl(ssl, res_buf, res_buf_len)
			: this->socket_handler->respond_to_socket(socket, res_buf, res_buf_len);

		bool is_connection_broken = res_code > 0 
			? false
			: ssl != NULL
				? this->ssl_context_handler->is_connection_broken(res_code)
				: this->socket_handler->is_connection_broken(res_code);

		if (is_connection_broken) {
			if (ssl != NULL) this->ssl_context_handler->free_ssl(ssl);

			this->socket_handler->close_socket(socket);
		}

		if (res_code <= 0)
		{
			this->logger->log_error("Could not respond back to the socket");
			this->close_socket_connection(socket, ssl);
		}
		else this->update_socket_heartbeat(socket);

		return res_code > 0;
	}
	catch (const std::exception& ex)
	{
		this->logger->log_error("Error occured while responding to socket. " + ((std::string)ex.what()));
		return false;
	}
}

bool ConnectionsManager::respond_to_socket_with_error(SOCKET_ID socket, SSL* ssl, ErrorCode error_code, const std::string& error_message) {
	long err_buf_size = sizeof(long) + sizeof(ErrorCode) + error_message.size() + sizeof(int) + sizeof(ResponseValueKey);

	int error_message_size = error_message.size();
	ResponseValueKey error_message_type = ResponseValueKey::ERROR_MESSAGE;

	long offset = 0;

	std::unique_ptr<char> err_buf = std::unique_ptr<char>(new char[err_buf_size]);

	memcpy_s(err_buf.get() + offset, sizeof(long), &err_buf_size, sizeof(long));
	offset += sizeof(long);

	memcpy_s(err_buf.get() + offset, sizeof(ErrorCode), &error_code, sizeof(ErrorCode));
	offset += sizeof(ErrorCode);

	memcpy_s(err_buf.get() + offset, sizeof(ResponseValueKey), &error_message_type, sizeof(ResponseValueKey));
	offset += sizeof(ResponseValueKey);

	memcpy_s(err_buf.get() + offset, sizeof(int), &error_message_size, sizeof(int));
	offset += sizeof(int);

	memcpy_s(err_buf.get() + offset, error_message_size, error_message.c_str(), error_message_size);

	return this->respond_to_socket(socket, ssl, err_buf.get(), err_buf_size);
}

// For internal communication only
std::tuple<std::shared_ptr<char>, long, bool> ConnectionsManager::send_request_to_socket(SOCKET_ID socket, SSL* ssl, char* buf, unsigned int buf_len, const std::string& internal_requets_type) {
	try
	{
		this->logger->log_info("Sending internal request of type " + internal_requets_type);

		bool success = this->respond_to_socket(socket, ssl, buf, buf_len);

		if (!success) return std::tuple<std::shared_ptr<char>, long, bool>(nullptr, -1, true);

		if (!this->should_wait_for_response(*((RequestType*)(buf + sizeof(long)))))
			return std::tuple<std::shared_ptr<char>, long, bool>(nullptr, 0, false);

		long response_size = 0;

		success = this->receive_socket_buffer(socket, ssl, (char*)&response_size, sizeof(long));

		if (!success) return std::tuple<std::shared_ptr<char>, long, bool>(nullptr, -1, true);

		response_size -= sizeof(long);

		std::shared_ptr<char> res_buf = std::shared_ptr<char>(new char[response_size]);

		success = this->receive_socket_buffer(socket, ssl, res_buf.get(), response_size);

		if (!success) return std::tuple<std::shared_ptr<char>, long, bool>(nullptr, -1, true);

		success = *((ErrorCode*)res_buf.get()) == ErrorCode::NONE;

		if (!success) {
			std::unique_ptr<ErrorResponse> res = this->response_mapper->to_error_response(res_buf.get(), response_size);
			std::string err_msg = std::string(res.get()->error_message, res.get()->error_message_size);
			this->logger->log_error("Internal request failed. " + err_msg);
			return std::tuple<std::shared_ptr<char>, long, bool>(nullptr, -1, false);
		}

		return std::tuple<std::shared_ptr<char>, long, bool>(res_buf, response_size, false);
	}
	catch (const std::exception& ex)
	{
		this->logger->log_error(ex.what());
		return std::tuple<std::shared_ptr<char>, long, bool>(nullptr, -1, true);
	}
}

std::tuple<std::shared_ptr<char>, long, bool> ConnectionsManager::send_request_to_socket(ConnectionPool* pool, int retries, char* buf, unsigned int buf_len, const std::string& internal_requets_type) {
	try
	{
		while (retries > 0) {
			std::shared_ptr<Connection> connection = pool->get_connection();

			if (connection == nullptr) {
				this->logger->log_error("No open connections found in pool");
				return std::tuple<std::shared_ptr<char>, long, bool>(nullptr, -1, false);
			}

			auto res_tup = this->send_request_to_socket(
				connection.get()->socket, 
				connection.get()->ssl,
				buf, 
				buf_len,
				internal_requets_type
			);

			connection.get()->last_used_timestamp = this->util->get_current_time_milli().count();

			pool->add_connection(std::get<2>(res_tup) ? nullptr : connection, true);

			if (!std::get<2>(res_tup)) return res_tup;

			retries--;
		}

		return std::tuple<std::shared_ptr<char>, long, bool>(nullptr, -1, false);
	}
	catch (const std::exception& ex)
	{
		this->logger->log_error("Error occured while responding to socket. " + ((std::string)ex.what()));
		return std::tuple<std::shared_ptr<char>, long, bool>(nullptr, -1, true);
	}
}

bool ConnectionsManager::should_wait_for_response(RequestType request_type) {
	//return request_type != RequestType::DATA_NODE_HEARTBEAT;
	return true;
}

void ConnectionsManager::initialize_controller_nodes_connections() {
	if (this->failed_to_create_ssl_context) {
		*this->should_terminate = true;
		return;
	}

	int failed_occurunces = 0;
	int successfull_occurunces = 0;

	for (auto controller : this->settings->get_controller_nodes()) {
		int node_id = std::get<0>(controller);
		std::shared_ptr<ConnectionInfo> info = std::get<1>(controller);

		if (this->settings->get_is_controller_node() && node_id == this->settings->get_node_id()) {
			successfull_occurunces++;
			continue;
		}

		bool succeed = this->setup_connection_pool(node_id, info, &this->controllers_mut, &this->controller_node_connections);

		failed_occurunces += (!succeed ? 1 : 0);
		successfull_occurunces += (int)succeed;
	}

	std::string log_msg = failed_occurunces > 0 && successfull_occurunces > 0
		? "Partial connections with controller quorum nodes established" 
		: failed_occurunces == 0
			? "All connections with controller quorum nodes established"
			: "No connection could be established with any controller quorum node";

	this->logger->log_info(log_msg);
}

bool ConnectionsManager::setup_connection_pool(int node_id, std::shared_ptr<ConnectionInfo> info, std::shared_mutex* connections_mut, std::map<int, std::shared_ptr<ConnectionPool>>* connections) {
	std::shared_ptr<ConnectionPool> connection_pool = std::shared_ptr<ConnectionPool>(new ConnectionPool(3, info));

	bool failed = this->create_node_connection_pool(node_id, connection_pool.get(), 3000);
	
	std::lock_guard<std::shared_mutex> lock(*connections_mut);
	(*connections)[node_id] = connection_pool;

	return failed;
}

bool ConnectionsManager::add_connection_to_pool(ConnectionPool* pool) {
	if (pool->get_connections_missing_count() == 0) return true;

	SOCKET_ID socket = this->socket_handler->get_connect_socket(pool->get_connection_info());

	if (socket == invalid_socket) return false;

	SSL* ssl = this->settings->get_internal_ssl_enabled()
		? this->ssl_context_handler->wrap_connection_with_ssl(this->ssl_context.get(), socket)
		: NULL;

	if (this->settings->get_internal_ssl_enabled() && ssl == NULL) {
		this->socket_handler->close_socket(socket);
		return false;
	}

	std::shared_ptr<Connection> connection = std::make_shared<Connection>();
	connection.get()->socket = socket;
	connection.get()->ssl = ssl;

	pool->add_connection(connection);

	return true;
}

bool ConnectionsManager::create_node_connection_pool(int node_id, ConnectionPool* pool, long milliseconds_to_wait) {
	try
	{
		int retries = 3;

		while (retries > 0) {
			bool connection_added = this->add_connection_to_pool(pool);

			if (!connection_added) {
				if (--retries == 0) throw std::runtime_error("Retries went to 0");
				std::this_thread::sleep_for(std::chrono::milliseconds(milliseconds_to_wait));
				continue;
			}

			break;
		}

		this->logger->log_info("Initialized connection pool successfully to node " + std::to_string(node_id));
		return true;
	}
	catch (const std::exception&)
	{
		this->logger->log_error(
			"Could not initialize connection pool to node "
			+ std::to_string(node_id)
			+ " on address "
			+ pool->get_connection_info()->address
			+ " and port "
			+ std::to_string(pool->get_connection_info()->port)
		);
	}

	return false;
}

void ConnectionsManager::terminate_connections() {
	this->logger->log_info("Terminating connections...");

	{
		std::lock_guard<std::shared_mutex> lock(this->controllers_mut);

		for (auto iter : this->controller_node_connections) {
			std::shared_ptr<ConnectionPool> connection_pool = iter.second;

			std::shared_ptr<Connection> connection = connection_pool.get()->get_connection();

			while (connection != nullptr) {
				if(!this->socket_handler->close_socket(connection.get()->socket))
					this->logger->log_error("Error occured while trying to close controller connection on socket " + std::to_string(connection.get()->socket));

				if (connection.get()->ssl != NULL && !this->ssl_context_handler->free_ssl(connection.get()->ssl))
					this->logger->log_error("Error occured while trying to close controller ssl connection wrap on socket " + std::to_string(connection.get()->socket));

				connection = connection_pool.get()->get_connection();
			}
		}

		this->controller_node_connections.clear();
	}

	{
		std::lock_guard<std::shared_mutex> lock(this->data_mut);

		for (auto iter : this->data_node_connections) {
			std::shared_ptr<ConnectionPool> connection_pool = iter.second;

			std::shared_ptr<Connection> connection = connection_pool.get()->get_connection();

			while (connection != nullptr) {
				if (!this->socket_handler->close_socket(connection.get()->socket))
					this->logger->log_error("Error occured while trying to close data node connection on socket " + std::to_string(connection.get()->socket));

				if (connection.get()->ssl != NULL && !this->ssl_context_handler->free_ssl(connection.get()->ssl))
					this->logger->log_error("Error occured while trying to close data node ssl connection wrap on socket " + std::to_string(connection.get()->socket));

				connection = connection_pool.get()->get_connection();
			}
		}

		this->data_node_connections.clear();
	}

	this->logger->log_info("Connections terminated");
}

void ConnectionsManager::keep_pool_connections_to_maximum() {
	while (!(this->should_terminate->load())) {
		try
		{
			this->add_connections_to_pools(&this->controllers_mut, &this->controller_node_connections);

			this->add_connections_to_pools(&this->data_mut, &this->data_node_connections);
		}
		catch (const std::exception& ex)
		{
			std::string err_msg = "Error occured while trying to keep pool connections to maximum. Reason: " + std::string(ex.what());
			this->logger->log_error(err_msg);
		}

		std::this_thread::sleep_for(std::chrono::milliseconds(5000));
	}
}

void ConnectionsManager::ping_pool_connections() {
	while (!(this->should_terminate->load())) {
		try
		{
			this->ping_connection_pools(&this->controllers_mut, &this->controller_node_connections);

			this->ping_connection_pools(&this->data_mut, &this->data_node_connections);
		}
		catch (const std::exception& ex)
		{
			std::string err_msg = "Error occured while trying to ping pool connections. Reason: " + std::string(ex.what());
			this->logger->log_error(err_msg);
		}

		std::this_thread::sleep_for(std::chrono::milliseconds(15000));
	}
}

void ConnectionsManager::add_connections_to_pools(std::shared_mutex* connections_mut, std::map<int, std::shared_ptr<ConnectionPool>>* connections) {
	std::shared_lock<std::shared_mutex> lock(*connections_mut);

	for (auto iter : (*connections)) {
		if (iter.second.get()->get_connections_missing_count() == 0) continue;

		int retries = 3;
		int connections_added = 0;
		while (retries > 0) {
			if (!this->add_connection_to_pool(iter.second.get()))
				retries--;
			else connections_added++;

			if (iter.second.get()->get_connections_missing_count() == 0) break;
		}

		if (retries == 0 && connections_added == 0)
			this->logger->log_error("Failed to increase connection pool connections to node " + std::to_string(iter.first));
		else if(connections_added > 0)
			this->logger->log_info(std::to_string(connections_added) + " connections added to node " + std::to_string(iter.first) + " connection pool");
	}
}

void ConnectionsManager::ping_connection_pools(std::shared_mutex* connections_mut, std::map<int, std::shared_ptr<ConnectionPool>>* connections) {
	std::shared_lock<std::shared_mutex> lock(*connections_mut);

	bool success = true;

	for (auto iter : (*connections))
		for (int i = 0; i < iter.second.get()->get_total_connections(); i++) {
			std::shared_ptr<Connection> conn = iter.second.get()->get_connection();

			if (conn == nullptr) break;

			if (!this->util->has_timeframe_expired(conn->last_used_timestamp, this->settings->get_idle_connection_timeout_ms() / 2))
			{
				iter.second.get()->add_connection(conn, true);
				continue;
			}
			
			// TODO: Ping connection here

			iter.second.get()->add_connection(success ? conn : nullptr, true);

			if (success)
				this->logger->log_info("Pinged connection pool socket " + std::to_string(conn->socket));
		}
}

std::shared_mutex* ConnectionsManager::get_controller_node_connections_mut() {
	return &this->controllers_mut;
}

std::shared_mutex* ConnectionsManager::get_data_node_connections_mut() {
	return &this->data_mut;
}

// will be called after mutex locking, no lock required here
std::map<int, std::shared_ptr<ConnectionPool>>* ConnectionsManager::get_controller_node_connections() {
	return &this->controller_node_connections;
}

// will be called after mutex locking, no lock required here
std::shared_ptr<ConnectionPool> ConnectionsManager::get_controller_node_connection(int node_id) {
	if (this->controller_node_connections.find(node_id) == this->controller_node_connections.end()) return nullptr;
	return this->controller_node_connections[node_id];
}

bool ConnectionsManager::add_socket_lock(SOCKET_ID socket) {
	std::lock_guard<std::mutex> lock(this->socket_locks_mut);
	if (this->locked_sockets.find(socket) != this->locked_sockets.end()) return false;
	this->locked_sockets.insert(socket);
	return true;
}

void ConnectionsManager::remove_socket_lock(SOCKET_ID socket) {
	std::lock_guard<std::mutex> lock(this->socket_locks_mut);
	this->locked_sockets.erase(socket);
}

void ConnectionsManager::remove_data_node_connections(int node_id) {
	std::lock_guard<std::shared_mutex> lock(this->data_mut);

	if (this->data_node_connections.find(node_id) == this->data_node_connections.end()) return;

	this->close_connection_pool(this->data_node_connections[node_id].get());

	this->data_node_connections.erase(node_id);
}

void ConnectionsManager::remove_controller_node_connections(int node_id) {
	std::lock_guard<std::shared_mutex> lock(this->controllers_mut);

	if (this->controller_node_connections.find(node_id) == this->controller_node_connections.end()) return;

	this->close_connection_pool(this->controller_node_connections[node_id].get());

	this->controller_node_connections.erase(node_id);
}

void ConnectionsManager::close_connection_pool(ConnectionPool* pool) {
	while (!pool->no_connections_left()) {
		std::shared_ptr<Connection> conn = pool->get_connection();

		if (conn.get() == NULL) continue;

		if (conn.get()->ssl != NULL)
			this->ssl_context_handler->free_ssl(conn.get()->ssl);

		this->socket_handler->close_socket(conn.get()->socket);

		pool->add_connection(nullptr, true);
	}
}

void ConnectionsManager::initialize_connection_heartbeat(SOCKET_ID socket, SSL* ssl) {
	std::lock_guard<std::mutex> lock(this->heartbeats_mut);
	this->connections_heartbeats[socket] = std::tuple<bool, std::chrono::milliseconds>(false, this->util->get_current_time_milli());

	if(ssl != NULL)
		this->connections_ssls[socket] = ssl;
}

void ConnectionsManager::remove_socket_connection_heartbeat(SOCKET_ID socket) {
	std::lock_guard<std::mutex> lock(this->heartbeats_mut);
	this->connections_heartbeats.erase(socket);
	this->connections_ssls.erase(socket);
}

bool ConnectionsManager::socket_expired(SOCKET_ID socket) {
	std::lock_guard<std::mutex> lock(this->heartbeats_mut);

	if (this->connections_heartbeats.find(socket) == this->connections_heartbeats.end()) return false;

	return std::get<0>(this->connections_heartbeats[socket]);
}

void ConnectionsManager::update_socket_heartbeat(SOCKET_ID socket) {
	std::lock_guard<std::mutex> lock(this->heartbeats_mut);

	if (this->connections_heartbeats.find(socket) == this->connections_heartbeats.end()) return;

	if (std::get<0>(this->connections_heartbeats[socket])) return;

	this->connections_heartbeats[socket] = std::tuple<bool, std::chrono::milliseconds>(false, this->util->get_current_time_milli());
}

void ConnectionsManager::check_connections_heartbeats() {
	std::vector<SOCKET_ID> to_expire;

	while (!(this->should_terminate->load())) {
		to_expire.clear();

		try
		{
			std::lock_guard<std::mutex> lock(this->heartbeats_mut);

			for (auto iter : this->connections_heartbeats)
				if (!std::get<0>(iter.second) && this->util->has_timeframe_expired(std::get<1>(iter.second), this->settings->get_idle_connection_timeout_ms()))
				{
					SSL* ssl = this->connections_ssls[iter.first];

					if (ssl != NULL) this->ssl_context_handler->free_ssl(ssl);

					this->socket_handler->close_socket(iter.first);

					to_expire.emplace_back(iter.first);
				}

			if (to_expire.size() > 0)
				for (SOCKET_ID socket : to_expire) {
					this->connections_heartbeats[socket] = std::tuple<bool, std::chrono::milliseconds>(true, std::chrono::milliseconds(0));
					this->logger->log_info("Socket " + std::to_string(socket) + " expired");
				}
		}
		catch (const std::exception& ex)
		{
			std::string err_msg = "Error occured while checking for dead connections. Reason: " + std::string(ex.what());
			this->logger->log_error(err_msg);
		} 

		std::this_thread::sleep_for(std::chrono::milliseconds(this->settings->get_idle_connection_check_ms()));
	}
}

bool ConnectionsManager::initialize_data_node_connection_pool(int node_id, std::shared_ptr<ConnectionInfo> info){
	return this->setup_connection_pool(node_id, info, &this->data_mut, &this->data_node_connections);
}

bool ConnectionsManager::initialize_controller_node_connection_pool(int node_id, std::shared_ptr<ConnectionInfo> info) {
	return this->setup_connection_pool(node_id, info, &this->controllers_mut, &this->controller_node_connections);
}

std::shared_ptr<ConnectionPool> ConnectionsManager::get_node_connection_pool(int node_id) {
	{
		std::shared_lock<std::shared_mutex> lock(this->controllers_mut);
		if (this->controller_node_connections.find(node_id) != this->controller_node_connections.end())
			return this->controller_node_connections[node_id];
	}

	{
		std::shared_lock<std::shared_mutex> lock(this->data_mut);
		if (this->data_node_connections.find(node_id) != this->data_node_connections.end())
			return this->data_node_connections[node_id];
	}

	return nullptr;
}

void ConnectionsManager::close_socket_connection(SOCKET_ID socket, SSL* ssl) {
	if (ssl != NULL) this->ssl_context_handler->free_ssl(ssl);
	this->socket_handler->close_socket(socket);
}