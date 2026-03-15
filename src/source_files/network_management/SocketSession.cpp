#include "../../header_files/network_management/SocketSession.h"

SocketSession::SocketSession(Logger* logger, bool internal_communication, int max_allowed_request_size, tcp::socket socket_) : socket(std::move(socket_)) {
    this->logger = logger;
    this->internal_communication = internal_communication;
    this->max_allowed_request_size = max_allowed_request_size;
    this->fd = (int)this->socket.native_handle();
    this->fd_str = std::to_string(this->fd);
}

void SocketSession::start_listening(const std::function<void(SocketSession*, char*, int)>& execute_request_fn) {
    this->execute_request_fn = execute_request_fn;
    this->read_request_length_async();
}

void SocketSession::read_request_length_async() {
    auto self(shared_from_this());

    auto req_size = std::make_shared<int>(0);

    boost::asio::async_read(this->socket, boost::asio::buffer(req_size.get(), sizeof(int)),
        [this, self, req_size](boost::system::error_code ec, std::size_t size) {
            if (size != sizeof(int)) {
                this->logger->log_error("SocketSession.read_request_length_async: Buffer size mismatching in socket " + this->fd_str);
                this->read_request_length_async();
                return;
            }

            if (!ec) {
                int request_body_size = *req_size.get();

                if (request_body_size > max_allowed_request_size) {
                    this->logger->log_error("Received request with size > max allowed request size of " + std::to_string(max_allowed_request_size) + " bytes");
                    this->close();
                    return;
                }

                this->read_request_body_async(request_body_size);
            }
            else if (ec == boost::asio::error::eof) {
                this->logger->log_info("Socket " + this->fd_str + " disconnected");
            }
            else if (ec) {
                this->logger->log_error("Error code " + std::to_string(ec.value()) + " occured in socket " + this->fd_str + " while reading request bytes asynchronously");
            }
        });
}

void SocketSession::read_request_body_async(int req_body_size) {
    auto self(shared_from_this());

    auto req = std::make_shared<std::vector<char>>(req_body_size);

    boost::asio::async_read(this->socket, boost::asio::buffer(req.get()->data(), req_body_size),
        [this, self, req, req_body_size](boost::system::error_code ec, std::size_t size) {
            if (size != req_body_size) {
                this->logger->log_error("SocketSession.read_request_body_async: Buffer size mismatching in socket " + this->fd_str);
                this->read_request_length_async();
                return;
            }

            if (!ec) this->execute_request_fn(this, req.get()->data(), req_body_size);
            else if (ec == boost::asio::error::eof) {
                this->logger->log_info("Socket " + this->fd_str + " disconnected");
            }
            else if (ec) {
                this->logger->log_error("Error code " + std::to_string(ec.value()) + " occured in socket " + this->fd_str + " while reading body asynchronously");
            }
        });
}

bool SocketSession::write_async(char* buf, int buf_size) {
    auto self(shared_from_this());

    boost::asio::async_write(this->socket, boost::asio::buffer(buf, buf_size),
        [this, self, buf_size](boost::system::error_code ec, std::size_t size) {
            if (size != buf_size) {
                this->logger->log_error("SocketSession.write_async: Buffer size mismatching in socket " + this->fd_str);
                return;
            }

            if (ec == boost::asio::error::eof) {
                this->logger->log_info("Socket " + this->fd_str + " disconnected");
            }
            else if (ec) {
                this->logger->log_error("Error code " + std::to_string(ec.value()) + " occured in socket " + this->fd_str + " while writing asynchronously");
            }
        });

    return true;
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
        this->logger->log_error("Exception occured while reading data from socket " + this->fd_str + ". Reason: " + std::string(e.what()));
    }

    return success;
}

bool SocketSession::write(char* buf, int buf_size) {
    bool success = true;

    try {
        std::size_t bytes_written = boost::asio::write(this->socket, boost::asio::buffer(buf, buf_size));

        if (bytes_written != buf_size)
            throw std::runtime_error("Failed to write all bytes to socket");
    }
    catch (std::exception& e) {
        success = false;
        this->logger->log_error("Exception occured while writing data to socket " + this->fd_str + ". Reason: " + std::string(e.what()));
    }

    return success;
}

void SocketSession::close() {
    if (!this->socket.is_open()) return;
    this->socket.close();
}