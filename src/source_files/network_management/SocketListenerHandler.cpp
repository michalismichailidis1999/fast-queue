#include "../../header_files/network_management/SocketListenerHandler.h"

SocketListenerHandler::SocketListenerHandler(ConnectionsManager* cm, SocketHandler* socket_handler, RequestManager* rm, Logger* logger, Settings* settings, std::atomic_bool* should_terminate) {
	this->cm = cm;
	this->socket_handler = socket_handler;
	this->rm = rm;
    this->logger = logger;
	this->settings = settings;

    this->total_connections = 0;
    this->should_terminate = should_terminate;

    this->execute_request_fn = [&](SocketSession* socket_session, char* reqbuf, int req_buf_size) {
        this->rm->execute_request(socket_session, reqbuf, req_buf_size);
    };

    this->reduce_external_connections_count = [&]() {
        this->total_connections--;
    };

    this->ignore_connections_count = []() {};
}

void SocketListenerHandler::create_and_run_socket_listener(bool internal_communication) {
    boost::asio::io_context io_context;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_guard(boost::asio::make_work_guard(io_context));
    boost::asio::steady_timer check_timer(io_context);

    this->check_for_stop_request(&work_guard, &check_timer);

    std::shared_ptr<tcp::acceptor> acceptor = this->socket_handler->get_tcp_acceptor(&io_context, internal_communication);

    this->accept_connection(acceptor.get(), internal_communication);

    io_context.run();
}

void SocketListenerHandler::accept_connection(tcp::acceptor* acceptor, bool internal_communication) {
    acceptor->async_accept(
        [this, acceptor, internal_communication](boost::system::error_code ec, tcp::socket socket) {
            if (!ec)
            {
                auto new_socket = std::make_shared<SocketSession>(this->logger, internal_communication, this->settings->get_max_message_size(), std::move(socket));

                if (!internal_communication && this->total_connections.load() >= this->settings->get_maximum_connections()) {
                    this->cm->respond_to_socket_with_error(new_socket.get(), ErrorCode::MAX_CONNECTIONS_LIMIT_REACHED, "Maximum connections limit reached. Cannot accept any more connections");
                    new_socket->close();
                    this->accept_connection(acceptor, internal_communication);
                    return;
                }
                else
                    this->cm->respond_to_socket_with_error(new_socket.get(), ErrorCode::NONE, this->dummy_response_byte);
                
                new_socket->start_listening(
                    this->execute_request_fn,
                    !internal_communication
                        ? this->reduce_external_connections_count
                        : this->ignore_connections_count
                );

                if (!internal_communication)
                    this->total_connections++;

                this->cm->initialize_connection_heartbeat(new_socket.get());
            }
            
            this->accept_connection(acceptor, internal_communication);
        });
}

void SocketListenerHandler::check_for_stop_request(boost::asio::executor_work_guard<boost::asio::io_context::executor_type>* work_guard, boost::asio::steady_timer* check_timer) {
    check_timer->expires_after(std::chrono::milliseconds(10000)); // check every 10 seconds
    check_timer->async_wait([this, work_guard, check_timer](const boost::system::error_code& ec) {
        if (ec) return;

        if (this->should_terminate->load())
            work_guard->reset();
        else this->check_for_stop_request(work_guard, check_timer);
    });
}