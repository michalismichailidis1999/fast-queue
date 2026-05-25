#include "../../header_files/network_management/SocketSession.h"

SocketSession::SocketSession(Logger* logger, bool internal_communication, int max_allowed_request_size, tcp::socket socket_) : socket(std::move(socket_)) {
    this->logger = logger;
    this->internal_communication = internal_communication;
    this->max_allowed_request_size = max_allowed_request_size;
    this->fd = (int)this->socket.native_handle();
    this->fd_str = std::to_string(this->fd);
    this->reduce_external_connections_count = nullptr;
    this->remove_stored_socket_from_cache = nullptr;
    this->core_id = -1;
    this->is_closed.store(false);
}

SocketSession::~SocketSession() {
    this->close();
}

void SocketSession::start_listening(const std::function<void(SocketSession*, char*, int)>& execute_request_fn, const std::function<void()>& reduce_external_connections_count, std::function<void(int, long long)> remove_stored_socket_from_cache, int core_id) {
    this->core_id = core_id;
    this->execute_request_fn = execute_request_fn;
    this->reduce_external_connections_count = reduce_external_connections_count;
    this->remove_stored_socket_from_cache = remove_stored_socket_from_cache;
    this->logger->log_info("Socket " + this->fd_str + " connected successfully and is handled by Core " + std::to_string(core_id));
    this->read_request_length_async();
}

void SocketSession::read_request_length_async() {
    auto self(shared_from_this());

    auto req_size = std::make_shared<int>(0);

    boost::asio::async_read(this->socket, boost::asio::buffer(req_size.get(), sizeof(int)),
        [this, self, req_size](boost::system::error_code ec, std::size_t size) {
            if (!ec) {
                if (size != sizeof(int)) {
                    this->logger->log_error("SocketSession.read_request_length_async: Buffer size mismatching in socket " + this->fd_str);
                    this->read_request_length_async();
                    return;
                }

                int request_body_size = *req_size.get();

                if (request_body_size > max_allowed_request_size) {
                    this->logger->log_error("Received request with size > max allowed request size of " + std::to_string(max_allowed_request_size) + " bytes");
                    this->close();
                    return;
                }

                this->read_request_body_async(request_body_size);
            }
            else {
                this->close();

                std::string disconnect_msg = "Socket " + this->fd_str + " running on Core " + std::to_string(this->core_id) + " disconnected";
                
                if (ec != boost::asio::error::eof)
                    disconnect_msg += ". Error code " + std::to_string(ec.value()) + " occured in socket " + this->fd_str + " while reading request bytes asynchronously";

                if (ec != boost::asio::error::eof)
                    this->logger->log_error(disconnect_msg);
                else
                    this->logger->log_info(disconnect_msg);
            }
        });
}

void SocketSession::read_request_body_async(int req_body_size) {
    auto self(shared_from_this());

    auto req = std::make_shared<std::vector<char>>(req_body_size);

    boost::asio::async_read(this->socket, boost::asio::buffer(req.get()->data(), req_body_size),
        [this, self, req, req_body_size](boost::system::error_code ec, std::size_t size) {
            if (!ec) {
                if (size != req_body_size) {
                    this->logger->log_error("SocketSession.read_request_body_async: Buffer size mismatching in socket " + this->fd_str);
                    this->read_request_length_async();
                    return;
                }

                this->execute_request_fn(this, req.get()->data(), req_body_size);

                this->read_request_length_async();
            }
            else {
                this->close();

                std::string disconnect_msg = "Socket " + this->fd_str + " running on Core " + std::to_string(this->core_id) + " disconnected";

                if (ec != boost::asio::error::eof)
                    disconnect_msg += ". Error code " + std::to_string(ec.value()) + " occured in socket " + this->fd_str + " while reading request bytes asynchronously";

                if (ec != boost::asio::error::eof)
                    this->logger->log_error(disconnect_msg);
                else
                    this->logger->log_info(disconnect_msg);
            }
        });
}

bool SocketSession::read(char* buf, int buf_size) {
    bool success = true;

    try {
        std::size_t bytes_read = boost::asio::read(this->socket, boost::asio::buffer(buf, buf_size));

        if (bytes_read != buf_size)
            throw std::runtime_error("Failed to read request specified bytes");
    }
    catch (std::exception& e) {
        success = false;
        this->logger->log_error("Exception occured while reading data from socket " + this->fd_str);
    }

    return success;
}

bool SocketSession::write(std::shared_ptr<char> buf, int buf_size) {
    bool success = true;

    try {
        std::size_t bytes_written = boost::asio::write(this->socket, boost::asio::buffer(buf.get(), buf_size));

        if (bytes_written != buf_size)
            throw std::runtime_error("Failed to write all bytes to socket");
    }
    catch (std::exception& e) {
        success = false;
        this->logger->log_error("Exception occured while writing data to socket " + this->fd_str);
    }

    return success;
}

void SocketSession::close() {
    bool expected = false;

    if (!this->is_closed.compare_exchange_strong(expected, true))
        return;

    if (this->socket.is_open()) {
        boost::system::error_code ec;

        this->socket.shutdown(
            boost::asio::ip::tcp::socket::shutdown_both, ec
        );

        this->socket.close(ec);
    }

    if (this->reduce_external_connections_count)
        this->reduce_external_connections_count();

    if (this->remove_stored_socket_from_cache)
        this->remove_stored_socket_from_cache(this->fd, this->creation_time);
}