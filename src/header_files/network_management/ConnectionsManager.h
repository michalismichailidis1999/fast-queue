#pragma once
#include <map>
#include <set>
#include <mutex>
#include <queue>
#include <tuple>
#include <functional>
#include <chrono>
#include "../logging/Logger.h"
#include "./SocketHandler.h"
#include "./SslContextHandler.h"
#include "./Connection.h"
#include "../util/ConnectionPool.h"
#include "../requests_management/ResponseMapper.h"
#include "../requests_management/Responses.h"
#include "../Enums.h"
#include "../util/Util.h"

struct ConnectionInfo;
struct Connection;

class ConnectionsManager {
private:
	std::map<int, std::shared_ptr<ConnectionPool>> controller_node_connections;
	std::map<int, std::shared_ptr<ConnectionPool>> data_node_connections;

	std::map<SOCKET_ID, std::tuple<bool, std::chrono::milliseconds>> connections_heartbeats;
	std::mutex heartbeats_mut;

	std::map<SOCKET_ID, SSL*> connections_ssls;

	std::set<SOCKET_ID> locked_sockets;

	SocketHandler* socket_handler;
	SslContextHandler* ssl_context_handler;
	ResponseMapper* response_mapper;
	Util* util;
	Settings* settings;
	Logger* logger;

	std::shared_ptr<SSL_CTX> ssl_context;
	bool failed_to_create_ssl_context;

	std::mutex controllers_mut;
	std::mutex data_mut;
	std::mutex socket_locks_mut;

	std::atomic_bool* should_terminate;

	bool create_node_connection_pool(int node_id, ConnectionPool* pool, long milliseconds_to_wait = 1000);
	
	void add_connections_to_pools(std::mutex* connections_mut, std::map<int, std::shared_ptr<ConnectionPool>>* connections);

	bool setup_connection_pool(int node_id, std::shared_ptr<ConnectionInfo> info, std::mutex* connections_mut, std::map<int, std::shared_ptr<ConnectionPool>>* connections);

	bool should_wait_for_response(RequestType request_type);

	void update_socket_heartbeat(SOCKET_ID socket);
public:
	ConnectionsManager(SocketHandler* socket_handler, SslContextHandler* ssl_context_handler, ResponseMapper* response_mapper, Util* util, Settings* settings, Logger* logger, std::atomic_bool* should_terminate);

	bool receive_socket_buffer(SOCKET_ID socket, SSL* ssl, char* res_buf, long res_buf_len);
	bool respond_to_socket(SOCKET_ID socket, SSL* ssl, char* res_buf, long res_buf_len);
	bool respond_to_socket_with_error(SOCKET_ID socket, SSL* ssl, ErrorCode error_code, const std::string& error_message);

	std::tuple<std::shared_ptr<char>, long, bool> send_request_to_socket(SOCKET_ID socket, SSL* ssl, char* buf, long buf_len, const std::string& internal_requets_type);
	std::tuple<std::shared_ptr<char>, long, bool> send_request_to_socket(ConnectionPool* pool, int retries, char* buf, long buf_len, const std::string& internal_requets_type);

	bool connect_to_data_node(int node_id, std::shared_ptr<ConnectionInfo> info, long fail_wait_milli = 3000);

	void initialize_controller_nodes_connections();
	void terminate_connections();

	void keep_pool_connections_to_maximum();

	std::mutex* get_controller_node_connections_mut();
	std::mutex* get_data_node_connections_mut();

	bool add_socket_lock(SOCKET_ID socket);
	void remove_socket_lock(SOCKET_ID socket);

	void remove_data_node_connections(int node_id);
	void remove_controller_node_connections(int node_id);

	bool add_connection_to_pool(ConnectionPool* pool);
	void close_connection_pool(ConnectionPool* pool);

	void initialize_connection_heartbeat(SOCKET_ID socket, SSL* ssl);
	bool socket_expired(SOCKET_ID socket);
	void remove_socket_connection_heartbeat(SOCKET_ID socket);

	void check_connections_heartbeats();

	bool initialize_data_node_connection_pool(int node_id, std::shared_ptr<ConnectionInfo> info);
	bool initialize_controller_node_connection_pool(int node_id, std::shared_ptr<ConnectionInfo> info);

	std::map<int, std::shared_ptr<ConnectionPool>>* get_controller_node_connections(bool with_lock = true);

	std::shared_ptr<ConnectionPool> get_controller_node_connection(int node_id, bool with_lock = true);

	std::shared_ptr<ConnectionPool> get_node_connection_pool(int node_id);
}; 