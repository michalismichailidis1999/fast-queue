#pragma once
#include <tuple>
#include "./SocketSession.h"
#include "./Connection.h"
#include "../Settings.h"
#include "../logging/Logger.h"
#include "../__linux/memcpy_s.h"

class SocketHandler {
private:
	Settings* settings;
	Logger* logger;

	std::shared_ptr<boost::asio::io_context> external_connections_socket;
public:
	SocketHandler(Settings* settings, Logger* logger);

	std::shared_ptr<SocketSession> get_connect_socket(ConnectionInfo* info);

	std::shared_ptr<tcp::acceptor> get_tcp_acceptor(boost::asio::io_context* io_context, bool internal_communication);
};