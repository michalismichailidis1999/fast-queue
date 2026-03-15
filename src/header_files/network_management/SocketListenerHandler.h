#pragma once
#include <functional>
#include "../requests_management/RequestManager.h"

#include "../__linux/memcpy_s.h"

class SocketListenerHandler {
private:
	ConnectionsManager* cm;
	SocketHandler* socket_handler;
	RequestManager* rm;
	Logger* logger;
	Settings* settings;

	alignas(64) std::atomic_int total_connections;

	std::atomic_bool* should_terminate;

	std::function<void(SocketSession*, char*, int)> execute_request_fn;

	void accept_connection(tcp::acceptor* acceptor, bool internal_communication);
	void check_for_stop_request(boost::asio::executor_work_guard<boost::asio::io_context::executor_type>* work_guard, boost::asio::steady_timer* check_timer);
public:
	SocketListenerHandler(ConnectionsManager* cm, SocketHandler* socket_handler, RequestManager* rm, Logger* logger, Settings* settings, std::atomic_bool* should_terminate);

	void create_and_run_socket_listener(bool internal_communication);
};