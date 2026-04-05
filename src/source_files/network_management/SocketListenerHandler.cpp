#include "../../header_files/network_management/SocketListenerHandler.h"

SocketListenerHandler::SocketListenerHandler(ConnectionsManager* cm, SocketHandler* socket_handler, RequestManager* rm, Logger* logger, Settings* settings, Util* util, std::atomic_bool* should_terminate, int total_cores) {
	this->cm = cm;
	this->socket_handler = socket_handler;
	this->rm = rm;
    this->logger = logger;
	this->settings = settings;

    this->total_connections = 0;
    this->should_terminate = should_terminate;

    this->total_cores = total_cores;
    this->next_internal_worker = -1;
    this->next_external_worker = -1;

    this->execute_request_fn = [&](SocketSession* socket_session, char* reqbuf, int req_buf_size) {
        this->rm->execute_request(socket_session, reqbuf, req_buf_size);
    };

    this->reduce_external_connections_count = [&]() {
        this->total_connections--;
    };

    this->ignore_connections_count = []() {};

    this->remove_stored_socket_from_cache = [this](int fd, long long creation_time) {
        this->remove_stored_socket(fd, creation_time);
    };
}

void SocketListenerHandler::create_and_run_socket_listener(bool internal_communication, hwloc_topology_t topo, int core_id) {
    std::shared_ptr<boost::asio::io_context> io_context = std::make_shared<boost::asio::io_context>();
    
    std::unordered_map<int, std::shared_ptr<boost::asio::io_context>>* workers_io_contexts = internal_communication
        ? &this->internal_contexts
        : &external_contexts;

    (*workers_io_contexts)[core_id] = io_context;

    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_guard(boost::asio::make_work_guard(*(io_context.get())));
    boost::asio::steady_timer check_timer(*(io_context.get()));

    this->check_for_stop_request(&work_guard, &check_timer);

    std::shared_ptr<tcp::acceptor> acceptor = this->socket_handler->get_tcp_acceptor(io_context.get(), internal_communication, core_id);

    this->accept_connection(acceptor.get(), internal_communication, core_id);

    // Pin socket listener to specific core to increase cache hit ratio
    hwloc_obj_t core_obj = hwloc_get_obj_by_type(topo, HWLOC_OBJ_CORE, core_id);

    if (!core_obj)
        throw std::runtime_error("Failed to get core object from core id " + std::to_string(core_id));

    if (hwloc_set_cpubind(topo, core_obj->cpuset, HWLOC_CPUBIND_THREAD) < 0)
        throw std::runtime_error("Failed to bind listener thread to core " + std::to_string(core_id));
    // =========================================

    this->logger->log_info("Initialized and pinned I/O Context for socket handling for " + ((std::string)(internal_communication ? "internal" : "external")) + " communication on Core " + std::to_string(core_id));

    io_context.get()->run();
}

void SocketListenerHandler::accept_connection(tcp::acceptor* acceptor, bool internal_communication, int core_id) {
    if (core_id == 0 && !internal_communication) {
        int temp = 1;
    }
    acceptor->async_accept(
        [this, acceptor, internal_communication, core_id](boost::system::error_code ec, tcp::socket socket) mutable {
            this->accept_connection(acceptor, internal_communication, core_id);

            if (!ec)
            {
                #if !defined(_WIN32) && !defined(_WIN64)
                this->handle_new_connected_socket(std::move(socket), internal_communication, core_id);
                #else
                auto next_io_context_tup = this->get_next_io_context(internal_communication, core_id);
                
                if (std::get<0>(next_io_context_tup) != nullptr)
                {
                    int new_core_id = std::get<1>(next_io_context_tup);
                    this->logger->log_info("Moving socket " + std::to_string((int)socket.native_handle()) + " to I/O Context running in pinned thread at Core " + std::to_string(new_core_id));
                    boost::asio::post(*std::get<0>(next_io_context_tup).get(), [this, internal_communication, core_id, new_core_id, socket = std::move(socket)]() mutable {
                        this->handle_new_connected_socket(std::move(socket), internal_communication, new_core_id);
                    });
                    return;
                }
                else
                    this->handle_new_connected_socket(std::move(socket), internal_communication, core_id);
                #endif
            }
            else
                this->logger->log_error("Failed to accept socket. Received error code: " + std::to_string(ec.value()));
        });
}

void SocketListenerHandler::handle_new_connected_socket(tcp::socket socket, bool internal_communication, int core_id) {
    auto new_socket = std::make_shared<SocketSession>(this->logger, internal_communication, this->settings->get_max_message_size(), std::move(socket));
    new_socket.get()->creation_time = this->util->get_current_time_milli().count();

    if (!internal_communication && this->total_connections.load() >= this->settings->get_maximum_connections()) {
        this->cm->respond_to_socket_with_error(new_socket.get(), ErrorCode::MAX_CONNECTIONS_LIMIT_REACHED, "Maximum connections limit reached. Cannot accept any more connections");
        new_socket->close();
        return;
    }
    else
        this->cm->respond_to_socket_with_error(new_socket.get(), ErrorCode::NONE, this->dummy_response_byte);

    new_socket->start_listening(
        this->execute_request_fn,
        !internal_communication
            ? this->reduce_external_connections_count
            : this->ignore_connections_count,
        this->remove_stored_socket_from_cache,
        core_id
    );

    this->store_new_socket(new_socket);

    if (!internal_communication)
        this->total_connections++;

    this->cm->initialize_connection_heartbeat(new_socket.get());
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

std::tuple<std::shared_ptr<boost::asio::io_context>, int> SocketListenerHandler::get_next_io_context(bool internal_communication, int core_id) {
    int* next_worker = internal_communication ? &this->next_internal_worker : &this->next_external_worker;

    std::unordered_map<int, std::shared_ptr<boost::asio::io_context>>* workers_io_contexts = internal_communication
        ? &this->internal_contexts
        : &external_contexts;

    *next_worker = (*next_worker + 1) % this->total_cores;

    if (*next_worker == core_id) return std::tuple<std::shared_ptr<boost::asio::io_context>, int>(nullptr, core_id);

    return std::tuple<std::shared_ptr<boost::asio::io_context>, int>((*workers_io_contexts)[*next_worker], *next_worker);
}

std::shared_ptr<boost::asio::io_context> SocketListenerHandler::get_io_context_by_core_id(bool internal_communication, int core_id) {
    std::unordered_map<int, std::shared_ptr<boost::asio::io_context>>* workers_io_contexts = internal_communication
        ? &this->internal_contexts
        : &external_contexts;

    return (*workers_io_contexts)[core_id];
}

void SocketListenerHandler::store_new_socket(std::shared_ptr<SocketSession> socket) {
    std::lock_guard<std::mutex> lock(this->open_sockets_mut);
    this->open_sockets[socket->fd] = socket;
}

void SocketListenerHandler::remove_stored_socket(int fd, long long creation_time) {
    std::lock_guard<std::mutex> lock(this->open_sockets_mut);
    if (this->open_sockets.find(fd) == this->open_sockets.end()) return;
    if (this->open_sockets[fd].get()->creation_time != creation_time) return;
    this->open_sockets.erase(fd);
}