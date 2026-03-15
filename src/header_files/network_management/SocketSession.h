#pragma once
#include <boost/asio.hpp>
#include <memory>
#include <vector>
#include <functional>
#include "../logging/Logger.h"

using namespace boost::asio::ip;

class SocketSession : public std::enable_shared_from_this<SocketSession> {
private:
    Logger* logger;
    tcp::socket socket;
    int max_allowed_request_size;

    std::function<void(SocketSession*, char*, int)> execute_request_fn;

    void read_request_length_async();
    void read_request_body_async(int req_body_size);
public:
    int fd;
    std::string fd_str;
    bool internal_communication;

    SocketSession(Logger* logger, bool internal_communication, int max_allowed_request_size, tcp::socket socket);

    void start_listening(const std::function<void(SocketSession*, char*, int)>& execute_request_fn);

    bool write_async(char* buf, int buf_size);

    bool read(char* buf, int buf_size);
    bool write(char* buf, int buf_size);

    void close();
};