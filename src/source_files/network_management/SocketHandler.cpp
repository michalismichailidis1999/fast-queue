#include "../../header_files/network_management/SocketHandler.h"

SocketHandler::SocketHandler(Settings* settings, Logger* logger) {
	this->settings = settings;
	this->logger = logger;
    this->external_connections_socket = std::make_shared<boost::asio::io_context>();
}

std::shared_ptr<SocketSession> SocketHandler::get_connect_socket(ConnectionInfo* info) {
    try {
        tcp::resolver resolver(*this->external_connections_socket.get());
        auto endpoints = resolver.resolve(info->address, std::to_string(info->port));

        tcp::socket socket(*this->external_connections_socket.get());

        boost::asio::connect(socket, endpoints);

        std::shared_ptr<SocketSession> socket_session = std::make_shared<SocketSession>(
            this->logger, true, this->settings->get_max_message_size(), std::move(socket)
        );

        return socket_session;
    }
    catch (std::exception& e) {
        std::string err_msg = "Failed to connect to address " + info->address + ":" + std::to_string(info->port) + ". Reason: " + std::string(e.what());
        this->logger->log_error(err_msg);
        return nullptr;
    }
}

std::shared_ptr<tcp::acceptor> SocketHandler::get_tcp_acceptor(boost::asio::io_context* io_context, bool internal_communication, int core_id) {
    int port = internal_communication 
        ? settings->get_internal_port() 
        : settings->get_external_port();


    // In case its windows this will handle socket load balancing to the other threads
    #if defined(_WIN32) || defined(_WIN64)
    port += core_id;
    #endif

    std::string ip = internal_communication 
        ? settings->get_internal_ip() 
        : settings->get_external_ip();

    bool bind_all_interfaces = internal_communication 
        ? settings->get_internal_bind_all_interfaces() 
        : settings->get_external_bind_all_interfaces();

    if (bind_all_interfaces)
        ip = "0.0.0.0";

    auto endpoint = tcp::endpoint(boost::asio::ip::make_address(ip), port);

    auto acceptor = std::make_shared<tcp::acceptor>(tcp::acceptor(*io_context));
    acceptor.get()->open(endpoint.protocol());
    acceptor.get()->set_option(tcp::acceptor::reuse_address(true));

    // SO_REUSEPORT -> Allowes multiple bindings to same port and handles load balancing of accepted connections
    #if defined(SO_REUSEPORT)
    acceptor->set_option(boost::asio::detail::socket_option::boolean<SOL_SOCKET, SO_REUSEPORT>(true));
    #endif

    acceptor.get()->bind(endpoint);
    acceptor.get()->listen();

    return acceptor;
}