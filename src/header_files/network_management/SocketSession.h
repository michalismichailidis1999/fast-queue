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

    int core_id;

    std::function<void(SocketSession*, char*, int)> execute_request_fn;
    std::function<void()> reduce_external_connections_count;
    std::function<void(int, long long)> remove_stored_socket_from_cache;

    void read_request_length_async();
    void read_request_body_async(int req_body_size);

    bool connection_closed(boost::system::error_code ec);
public:
    int fd;
    std::string fd_str;
    bool internal_communication;
    long long creation_time;

    SocketSession(Logger* logger, bool internal_communication, int max_allowed_request_size, tcp::socket socket);
    ~SocketSession();

    void start_listening(const std::function<void(SocketSession*, char*, int)>& execute_request_fn, const std::function<void()>& reduce_external_connections_count, std::function<void(int, long long)> remove_stored_socket_from_cache, int core_id);

    bool write_async(char* buf, int buf_size);

    bool read(char* buf, int buf_size);
    bool write(char* buf, int buf_size);

    void close();
};