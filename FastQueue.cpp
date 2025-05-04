#include "FastQueue.h"

std::atomic_bool should_terminate(false);
std::atomic_int total_connections(0);

Logger* _logger;

void terminationSignalHandler(int signum) {
    switch (signum) {
    case SIGSEGV:
        _logger->log_error("Segmentation error occured. Shutting down immediatelly...");
        exit(1);
    case SIGINT:
    case SIGTERM:
        _logger->log_info("Termination signal received. Initiating graceful shutdown...");
        break;
    default:
        return;
    }

    should_terminate = true;  // Set the termination flag
}

std::unique_ptr<Settings> setup_settings(FileHandler* fh, const std::string& config_path) {
    if (!fh->check_if_exists(config_path)) {
        printf("Invalid configuration file path %s\n", config_path.c_str());
        throw std::exception();
    }

    printf("Setting up server using configuration file %s...\n", config_path.c_str());

    std::tuple<long, std::shared_ptr<char>> config_res = fh->get_complete_file_content(config_path);

    std::unique_ptr<Settings> settings = std::unique_ptr<Settings>(new Settings(std::get<1>(config_res).get(), std::get<0>(config_res)));

    return settings;
}  

void create_and_run_socket_listener(ConnectionsManager* cm, SocketHandler* socket_handler, SslContextHandler* ssl_context_handler, Settings* settings, RequestManager* rm, ThreadPool* thread_pool, bool internal_communication) {
    std::shared_ptr<SSL_CTX> ctx = nullptr;

    bool ssl_enabled = internal_communication
        ? settings->get_internal_ssl_enabled()
        : settings->get_external_ssl_enabled();

    if (ssl_enabled) {
        _logger->log_info("Enabling SSL...");

        std::shared_ptr<SSL_CTX> ctx = ssl_context_handler->create_ssl_context(internal_communication);

        if (!ctx.get()) {
            _logger->log_error("Unable to create SSL context");
            ERR_print_errors_fp(stderr);
            should_terminate = true;
            return;
        }

        _logger->log_info("SSL enabled");
    }

    SOCKET_ID listen_socket = socket_handler->get_listen_socket(internal_communication);

    if (listen_socket == invalid_socket) {
        should_terminate = true;
        return;
    }

    {
        std::string communication_info = internal_communication ? "internal" : "external";

        _logger->log_info("Server started and listening for " + communication_info + " connections");
    }

    std::vector<POLLED_FD> fds;
    fds.push_back({ listen_socket, POLLIN_EVENT, 0 });


    std::map<SOCKET_ID, SSL*> fds_ssls;

    if (ssl_enabled)
        fds_ssls[listen_socket] = NULL;

    while (should_terminate == 0) {
        int pollResult = socket_handler->poll_events(&fds);

        if (pollResult > 0)
            for (int i = 0; i < fds.size(); ++i) {
                if (socket_handler->pollin_event_occur(&fds[i]) && !socket_handler->error_event_occur(&fds[i])) {
                    if (i == 0) {
                        // Handle new incoming connection
                        SOCKET_ID newConn = socket_handler->accept_connection(listen_socket);

                        if ((++total_connections) > settings->get_maximum_connections()) {
                            total_connections--;

                            if (!socket_handler->close_socket(fds[i].fd))
                                _logger->log_error("Socket closure failed with error");

                            _logger->log_error("Could not accept any more connections.");
                            continue;
                        }

                        if (newConn != invalid_socket) {
                            SSL* ssl = NULL;

                            if (ssl_enabled) {
                                ssl = ssl_context_handler->wrap_connection_with_ssl(ctx.get(), newConn);

                                if (ssl == NULL) {
                                    socket_handler->close_socket(newConn);
                                    continue;
                                }

                                fds_ssls[newConn] = ssl;
                            }

                            fds.push_back({ newConn, POLLIN, 0 });
                            cm->initialize_connection_heartbeat(newConn, ssl);
                            _logger->log_info("New connection accepted");
                        }
                        else
                            _logger->log_error("Failed to accept new connection");

                        cm->remove_socket_lock(newConn);
                    }
                    else {
                        SOCKET_ID socket = fds[i].fd;

                        if (!cm->add_socket_lock(socket)) continue;

                        SSL* ssl = ssl_enabled ? fds_ssls[i] : NULL;

                        thread_pool->enqueue([&, socket, internal_communication]() {
                            try
                            {
                                rm->execute_request(socket, ssl, internal_communication);
                            }
                            catch (const std::exception& ex)
                            {
                                _logger->log_error(ex.what());
                            }
                            });
                    }
                }
                else if (socket_handler->error_event_occur(&fds[i])) {
                    SOCKET_ID socket = fds[i].fd;
                    bool socket_expired = cm->socket_expired(socket);

                    cm->remove_socket_lock(socket);

                    if (!socket_expired && !socket_handler->close_socket(socket))
                        _logger->log_error("Socket closure failed with error");

                    _logger->log_error("Connection from socket " + std::to_string(socket) + " removed");

                    fds.erase(fds.begin() + i);
                    total_connections--;

                    if (i > 0 && ssl_enabled && fds_ssls.find(socket) != fds_ssls.end()) {
                        if (!socket_expired && !ssl_context_handler->free_ssl(fds_ssls[socket]))
                            _logger->log_error("Failed to cleanup socket ssl");

                        fds_ssls.erase(socket);
                    }

                    if (i == 0) {
                        should_terminate = true;
                        break;
                    }

                    --i;
                }
            }
        else if (pollResult == poll_error) _logger->log_error("Request polling failed");

        std::this_thread::sleep_for(std::chrono::milliseconds(150));
    }

    if (fds.size() > 0)
        for (int i = 0; i < fds.size(); i++) {
            bool socket_expired = cm->socket_expired(fds[i].fd);

            if (!socket_expired && !socket_handler->close_socket(fds[i].fd))
                _logger->log_error("Socket closure failed with error");

            if (i > 0 && ssl_enabled && !socket_expired)
                SSL_free(fds_ssls[i]);
        }

    socket_handler->socket_cleanup();

    thread_pool->stop_workers();
}

std::string get_config_path(int argc, char* argv[]) {
    const char* settings_path = std::getenv("CONFIGURATION_PATH");

    if (argc <= 1 && settings_path == NULL) throw std::exception("Configuration file path is missing");
    else return argc > 1 ? std::string(argv[1]) : std::string(settings_path);
}

int main(int argc, char* argv[])
{
    std::unique_ptr<Util> util = std::unique_ptr<Util>(new Util());

    std::unique_ptr<FileHandler> fh = std::unique_ptr<FileHandler>(new FileHandler());

    std::string config_path = get_config_path(argc, argv);;

    std::unique_ptr<Settings> settings = setup_settings(fh.get(), config_path);

    std::unique_ptr<Logger> server_logger = std::unique_ptr<Logger>(new Logger("server", fh.get(), util.get(), settings.get()));
    std::unique_ptr<Logger> controller_logger = std::unique_ptr<Logger>(new Logger("controller", fh.get(), util.get(), settings.get()));
    
    _logger = server_logger.get();

    // Register signal handlers
    signal(SIGINT, terminationSignalHandler);
    signal(SIGTERM, terminationSignalHandler);
    signal(SIGSEGV, terminationSignalHandler);
    // ======================================

    std::unique_ptr<QueueSegmentFilePathMapper> pm = std::unique_ptr<QueueSegmentFilePathMapper>(new QueueSegmentFilePathMapper(util.get(), settings.get()));

    std::unique_ptr<QueueManager> qm = std::unique_ptr<QueueManager>(new QueueManager(fh.get(), pm.get(), server_logger.get()));

    std::unique_ptr<SocketHandler> socket_handler = std::unique_ptr<SocketHandler>(new SocketHandler(settings.get(), server_logger.get()));
    std::unique_ptr<SslContextHandler> ssl_context_handler = std::unique_ptr<SslContextHandler>(new SslContextHandler(settings.get(), server_logger.get()));

    std::unique_ptr<ClassToByteTransformer> transformer = std::unique_ptr<ClassToByteTransformer>(new ClassToByteTransformer());
    std::unique_ptr<RequestMapper> request_mapper = std::unique_ptr<RequestMapper>(new RequestMapper());
    std::unique_ptr<ResponseMapper> response_mapper = std::unique_ptr<ResponseMapper>(new ResponseMapper());

    std::unique_ptr<DiskFlusher> df = std::unique_ptr<DiskFlusher>(new DiskFlusher(qm.get(), fh.get(), server_logger.get(), settings.get(), &should_terminate));
    std::unique_ptr<DiskReader> dr = std::unique_ptr<DiskReader>(new DiskReader(fh.get(), server_logger.get(), settings.get()));

    std::unique_ptr<BPlusTreeIndexHandler> ih = std::unique_ptr<BPlusTreeIndexHandler>(new BPlusTreeIndexHandler(df.get(), dr.get()));
    std::unique_ptr<SegmentMessageMap> smm = std::unique_ptr<SegmentMessageMap>(new SegmentMessageMap(df.get(), dr.get()));

    std::unique_ptr<SegmentAllocator> sa = std::unique_ptr<SegmentAllocator>(new SegmentAllocator(smm.get(), df.get()));
    std::unique_ptr<MessagesHandler> mh = std::unique_ptr<MessagesHandler>(new MessagesHandler(df.get(), dr.get(), pm.get(), sa.get(), smm.get(), ih.get(), settings.get()));

    std::unique_ptr<ConnectionsManager> cm = std::unique_ptr<ConnectionsManager>(new ConnectionsManager(socket_handler.get(), ssl_context_handler.get(), response_mapper.get(), util.get(), settings.get(), server_logger.get(), &should_terminate));

    std::unique_ptr<ClusterMetadata> cluster_metadata = std::unique_ptr<ClusterMetadata>(new ClusterMetadata(settings.get()->get_node_id()));
    std::unique_ptr<ClusterMetadata> future_cluster_metadata = std::unique_ptr<ClusterMetadata>(new ClusterMetadata());

    std::unique_ptr<BeforeServerStartupHandler> startup_handler = std::unique_ptr<BeforeServerStartupHandler>(
        new BeforeServerStartupHandler(
            qm.get(),
            cluster_metadata.get(),
            future_cluster_metadata.get(),
            fh.get(),
            pm.get(),
            util.get(),
            server_logger.get(),
            settings.get()
        )
    );

    // needs to run before controller is created since it initialized some values based on existing queues metadata
    startup_handler.get()->initialize_required_folders_and_queues();

    std::unique_ptr<Controller> controller = settings.get()->get_is_controller_node()
        ? std::unique_ptr<Controller>(new Controller(cm.get(), qm.get(), mh.get(), response_mapper.get(), transformer.get(), util.get(), controller_logger.get(), settings.get(), cluster_metadata.get(), future_cluster_metadata.get(), &should_terminate))
        : nullptr;

    std::unique_ptr data_node = std::unique_ptr<DataNode>(new DataNode(controller.get(), cm.get(), cluster_metadata.get(), response_mapper.get(), transformer.get(), settings.get(), server_logger.get()));

    std::unique_ptr<ClientRequestExecutor> client_request_executor = std::unique_ptr<ClientRequestExecutor>(new ClientRequestExecutor(cm.get(), qm.get(), controller.get(), cluster_metadata.get(), transformer.get(), fh.get(), util.get(), settings.get(), server_logger.get()));
    std::unique_ptr<InternalRequestExecutor> internal_request_executor = std::unique_ptr<InternalRequestExecutor>(new InternalRequestExecutor(settings.get(), server_logger.get(), cm.get(), fh.get(), controller.get(), transformer.get()));
    std::unique_ptr<RequestManager> rm = std::unique_ptr<RequestManager>(new RequestManager(cm.get(), settings.get(), client_request_executor.get(), internal_request_executor.get(), request_mapper.get(), server_logger.get()));

    startup_handler.get()->rebuild_cluster_metadata();

    server_logger->log_info("Server starting...");

    try
    {
        ssl_context_handler.get()->initialize_ssl();

        auto keep_connections_to_maximum = [&]() {
            cm.get()->keep_pool_connections_to_maximum();
        };

        auto notify_other_nodes = [&]() {
            data_node.get()->notify_controllers_about_node_existance(&should_terminate);
        };

        auto check_connections_heartbeat = [&]() {
            cm.get()->check_connections_heartbeats();
        };

        auto flush_to_disk_periodically = [&](int milliseconds) {
            df.get()->flush_to_disk_periodically(milliseconds);
        };

        auto run_controller_quorum_communication = [&]() {
            controller.get()->run_controller_quorum_communication();
        };

        auto check_dead_data_nodes = [&]() {
            controller.get()->check_for_dead_data_nodes();
        };

        auto check_for_commit_and_last_applied_diff = [&]() {
            controller.get()->check_for_commit_and_last_applied_diff();
        };

        auto make_lagging_followers_catchup = [&]() {
            controller.get()->make_lagging_followers_catchup();
        };

        auto send_heartbeats_to_leader = [&]() {
            data_node.get()->send_heartbeats_to_leader(&should_terminate);
        };

        std::unique_ptr<ThreadPool> thread_pool = std::unique_ptr<ThreadPool>(new ThreadPool(settings.get()->get_request_parallelism()));

        std::thread internal_listener_thread = std::thread(create_and_run_socket_listener, cm.get(), socket_handler.get(), ssl_context_handler.get(), settings.get(), rm.get(), thread_pool.get(), true);

        std::thread external_listener_thread = std::thread(create_and_run_socket_listener, cm.get(), socket_handler.get(), ssl_context_handler.get(), settings.get(), rm.get(), thread_pool.get(), false);

        cm.get()->initialize_controller_nodes_connections();

        std::thread connection_pools_thread = std::thread(keep_connections_to_maximum);

        std::thread notify_other_nodes_thread = std::thread(notify_other_nodes);

        std::thread check_connections_heartbeat_thread = std::thread(check_connections_heartbeat);

        std::thread disk_flushing_thread(flush_to_disk_periodically, settings.get()->get_flush_to_disk_after_ms());

        std::thread run_quorum_communication_thread;
        std::thread check_dead_data_nodes_thread;
        std::thread check_for_commit_and_last_applied_diff_thread;
        std::thread make_lagging_followers_catchup_thread;
        std::thread send_heartbeats_to_leader_thread;

        if (settings.get()->get_is_controller_node()) {
            run_quorum_communication_thread = std::thread(run_controller_quorum_communication);
            check_dead_data_nodes_thread = std::thread(check_dead_data_nodes);
            check_for_commit_and_last_applied_diff_thread = std::thread(check_for_commit_and_last_applied_diff);
            make_lagging_followers_catchup_thread = std::thread(make_lagging_followers_catchup);
        }

        if (!settings.get()->get_is_controller_node())
            send_heartbeats_to_leader_thread = std::thread(send_heartbeats_to_leader);

        if (run_quorum_communication_thread.joinable())
            run_quorum_communication_thread.join();

        if (check_dead_data_nodes_thread.joinable())
            check_dead_data_nodes_thread.join();

        if (check_for_commit_and_last_applied_diff_thread.joinable())
            check_for_commit_and_last_applied_diff_thread.join();

        if (make_lagging_followers_catchup_thread.joinable())
            make_lagging_followers_catchup_thread.join();

        if (send_heartbeats_to_leader_thread.joinable())
            send_heartbeats_to_leader_thread.join();

        notify_other_nodes_thread.join();
        check_connections_heartbeat_thread.join();

        internal_listener_thread.join();
        external_listener_thread.join();
        connection_pools_thread.join();
        disk_flushing_thread.join();
    }
    catch (const std::exception&) {
        should_terminate = true;
    }

    cm.get()->terminate_connections();

    ssl_context_handler->cleanup_ssl();

	return 0;
}
