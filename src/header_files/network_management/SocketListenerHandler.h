#pragma once
#include "./ConnectionsManager.h"
#include "./SocketHandler.h"
#include "./SslContextHandler.h"
#include "../requests_management/RequestManager.h"
#include "../util/ThreadPool.h"
#include "../logging/Logger.h"
#include "../Settings.h"

#include "../__linux/memcpy_s.h"

class SocketListenerHandler {
private:
	ConnectionsManager* cm;
	SocketHandler* socket_handler;
	SslContextHandler* ssl_context_handler;
	RequestManager* rm;
	Logger* logger;
	Settings* settings;

	std::atomic_int total_connections;

	std::atomic_bool* should_terminate;

public:
	SocketListenerHandler(ConnectionsManager* cm, SocketHandler* socket_handler, SslContextHandler* ssl_context_handler, RequestManager* rm, Logger* logger, Settings* settings, std::atomic_bool* should_terminate);

	void create_and_run_socket_listener(ThreadPool* thread_pool, bool internal_communication);
};