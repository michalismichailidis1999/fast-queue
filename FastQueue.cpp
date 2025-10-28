#include "FastQueue.h"

std::atomic_bool should_terminate(false);

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
        throw std::runtime_error("Could not find configuration file path");
    }

    printf("Setting up server using configuration file %s...\n", config_path.c_str());

    std::tuple<long, std::shared_ptr<char>> config_res = fh->get_complete_file_content(config_path);

    std::unique_ptr<Settings> settings = std::unique_ptr<Settings>(new Settings(std::get<1>(config_res).get(), std::get<0>(config_res)));

    return settings;
}  

std::string get_config_path(int argc, char* argv[]) {
    const char* settings_path = std::getenv("CONFIGURATION_PATH");

    if (argc <= 1 && settings_path == NULL) throw std::runtime_error("Configuration file path is missing");
    else return argc > 1 ? std::string(argv[1]) : std::string(settings_path);
}

int main(int argc, char* argv[])
{
    std::unique_ptr<Util> util = std::unique_ptr<Util>(new Util());

    std::unique_ptr<FileHandler> fh = std::unique_ptr<FileHandler>(new FileHandler());

    std::string config_path = get_config_path(argc, argv);;

    std::unique_ptr<Settings> settings = setup_settings(fh.get(), config_path);

    std::unique_ptr<Logger> server_logger = std::unique_ptr<Logger>(new Logger("server", fh.get(), util.get(), settings.get()));
    
    _logger = server_logger.get();

    // Register signal handlers
    signal(SIGINT, terminationSignalHandler);
    signal(SIGTERM, terminationSignalHandler);
    signal(SIGSEGV, terminationSignalHandler);
    // ======================================

    std::unique_ptr<QueueSegmentFilePathMapper> pm = std::unique_ptr<QueueSegmentFilePathMapper>(new QueueSegmentFilePathMapper(util.get(), settings.get()));

    std::unique_ptr<SocketHandler> socket_handler = std::unique_ptr<SocketHandler>(new SocketHandler(settings.get(), server_logger.get()));
    std::unique_ptr<SslContextHandler> ssl_context_handler = std::unique_ptr<SslContextHandler>(new SslContextHandler(settings.get(), server_logger.get()));

    std::unique_ptr<ClassToByteTransformer> transformer = std::unique_ptr<ClassToByteTransformer>(new ClassToByteTransformer());
    std::unique_ptr<RequestMapper> request_mapper = std::unique_ptr<RequestMapper>(new RequestMapper(server_logger.get()));
    std::unique_ptr<ResponseMapper> response_mapper = std::unique_ptr<ResponseMapper>(new ResponseMapper(server_logger.get()));

    std::unique_ptr<ConnectionsManager> cm = std::unique_ptr<ConnectionsManager>(new ConnectionsManager(socket_handler.get(), ssl_context_handler.get(), response_mapper.get(), util.get(), settings.get(), server_logger.get(), &should_terminate));

    std::unique_ptr<CacheHandler> cache_handler = std::unique_ptr<CacheHandler>(new CacheHandler(util.get(), settings.get()));

    std::unique_ptr<DiskFlusher> df = std::unique_ptr<DiskFlusher>(new DiskFlusher(fh.get(), cache_handler.get(), server_logger.get(), settings.get(), &should_terminate));
    std::unique_ptr<DiskReader> dr = std::unique_ptr<DiskReader>(new DiskReader(fh.get(), cache_handler.get(), server_logger.get(), settings.get()));

    std::unique_ptr<MessageOffsetAckHandler> oah = std::unique_ptr<MessageOffsetAckHandler>(new MessageOffsetAckHandler(fh.get(), pm.get()));

    std::unique_ptr<BPlusTreeIndexHandler> ih = std::unique_ptr<BPlusTreeIndexHandler>(new BPlusTreeIndexHandler(df.get(), dr.get()));
    std::unique_ptr<SegmentMessageMap> smm = std::unique_ptr<SegmentMessageMap>(new SegmentMessageMap(df.get(), dr.get(), pm.get()));

    std::unique_ptr<QueueManager> qm = std::unique_ptr<QueueManager>(new QueueManager(smm.get(), fh.get(), pm.get(), server_logger.get()));

    std::unique_ptr<SegmentLockManager> lm = std::unique_ptr<SegmentLockManager>(new SegmentLockManager());

    std::unique_ptr<SegmentAllocator> sa = std::unique_ptr<SegmentAllocator>(new SegmentAllocator(smm.get(), lm.get(), pm.get(), df.get(), server_logger.get()));
    
    std::unique_ptr<MessagesHandler> mh = std::unique_ptr<MessagesHandler>(new MessagesHandler(df.get(), dr.get(), pm.get(), sa.get(), smm.get(), lm.get(), ih.get(), util.get(), settings.get(), server_logger.get()));

    std::unique_ptr<ClusterMetadataApplyHandler> cmah = std::unique_ptr<ClusterMetadataApplyHandler>(new ClusterMetadataApplyHandler(qm.get(), cm.get(), fh.get(), pm.get(), settings.get(), server_logger.get()));
    
    std::unique_ptr<Controller> controller = std::unique_ptr<Controller>(new Controller(cm.get(), qm.get(), mh.get(), cmah.get(), response_mapper.get(), transformer.get(), util.get(), server_logger.get(), settings.get(), &should_terminate));

    std::unique_ptr<RetentionHandler> rh = std::unique_ptr<RetentionHandler>(new RetentionHandler(qm.get(), lm.get(), fh.get(), pm.get(), util.get(), server_logger.get(), settings.get()));
    std::unique_ptr<CompactionHandler> ch = std::unique_ptr<CompactionHandler>(new CompactionHandler(controller.get(), qm.get(), mh.get(), lm.get(), cmah.get(), fh.get(), pm.get(), server_logger.get(), settings.get()));

    std::unique_ptr<DataNode> data_node = std::unique_ptr<DataNode>(new DataNode(controller.get(), cm.get(), qm.get(), mh.get(), oah.get(), request_mapper.get(), response_mapper.get(), transformer.get(), util.get(), fh.get(), settings.get(), server_logger.get()));
    
    std::unique_ptr<TransactionHandler> th = std::unique_ptr<TransactionHandler>(
        new TransactionHandler(cm.get(), fh.get(), pm.get(), controller.get()->get_cluster_metadata(), response_mapper.get(), transformer.get(), util.get(), settings.get(), server_logger.get())
    );

    mh.get()->set_transaction_handler(th.get());

    controller.get()->set_transaction_handler(th.get());

    std::unique_ptr<BeforeServerStartupHandler> startup_handler = std::unique_ptr<BeforeServerStartupHandler>(
        new BeforeServerStartupHandler(
            controller.get(),
            data_node.get(),
            cmah.get(),
            th.get(),
            qm.get(),
            oah.get(),
            sa.get(),
            smm.get(),
            fh.get(),
            pm.get(),
            util.get(),
            server_logger.get(),
            settings.get()
        )
    );

    // needs to run before controller is created since it initialized some values based on existing queues metadata
    startup_handler.get()->initialize_required_folders_and_queues();

    controller.get()->init_commit_index_and_last_applied();

    startup_handler.get()->rebuild_cluster_metadata();
    
    controller.get()->update_quorum_communication_values();

    std::unique_ptr<ClientRequestExecutor> client_request_executor = std::unique_ptr<ClientRequestExecutor>(new ClientRequestExecutor(mh.get(), oah.get(), cm.get(), qm.get(), controller.get(), data_node.get(), transformer.get(), settings.get(), server_logger.get()));
    std::unique_ptr<InternalRequestExecutor> internal_request_executor = std::unique_ptr<InternalRequestExecutor>(new InternalRequestExecutor(settings.get(), server_logger.get(), cm.get(), fh.get(), controller.get(), data_node.get(), qm.get(), mh.get(), transformer.get()));
    std::unique_ptr<RequestManager> rm = std::unique_ptr<RequestManager>(new RequestManager(cm.get(), settings.get(), client_request_executor.get(), internal_request_executor.get(), request_mapper.get(), server_logger.get()));

    std::unique_ptr<ThreadPool> thread_pool = std::unique_ptr<ThreadPool>(new ThreadPool(settings.get()->get_request_parallelism()));

    std::unique_ptr<SocketListenerHandler> slh = std::unique_ptr<SocketListenerHandler>(
        new SocketListenerHandler(
            cm.get(),
            socket_handler.get(),
            ssl_context_handler.get(),
            rm.get(),
            thread_pool.get(),
            server_logger.get(),
            settings.get(),
            &should_terminate
        )
    );

    server_logger->log_info("Server starting...");

    try
    {
        ssl_context_handler.get()->initialize_ssl();

        auto create_and_run_socket_listener = [&](bool internal_communication) {
            slh.get()->create_and_run_socket_listener(internal_communication);
        };

        auto keep_connections_to_maximum = [&]() {
            cm.get()->keep_pool_connections_to_maximum();
        };

        auto ping_pool_connections = [&]() {
            cm.get()->ping_pool_connections();
        };

        auto check_connections_heartbeat = [&]() {
            cm.get()->check_connections_heartbeats();
        };

        auto flush_to_disk_periodically = [&]() {
            df.get()->flush_to_disk_periodically();
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

        auto send_heartbeats_to_leader = [&]() {
            data_node.get()->send_heartbeats_to_leader(&should_terminate);
        };

        auto compact_closed_segments = [&]() {
            ch.get()->compact_closed_segments(&should_terminate);
        };

        auto remove_expired_segments = [&]() {
            rh.get()->remove_expired_segments(&should_terminate);
        };

        auto retrieve_cluster_metadata_updates = [&]() {
            data_node.get()->retrieve_cluster_metadata_updates(&should_terminate);
        };

        auto check_for_dead_consumers = [&]() {
            data_node.get()->check_for_dead_consumer(&should_terminate);
        };

        auto fetch_data_from_partition_leaders = [&]() {
            data_node.get()->fetch_data_from_partition_leaders(&should_terminate);
        };

        auto check_for_lagging_followers = [&]() {
            data_node.get()->check_for_lagging_followers(&should_terminate);
        };

        std::thread internal_listener_thread = std::thread(create_and_run_socket_listener, true);
        std::thread external_listener_thread = std::thread(create_and_run_socket_listener, false);

        cm.get()->initialize_controller_nodes_connections();

        std::thread connection_pools_thread = std::thread(keep_connections_to_maximum);
        std::thread ping_pool_connections_thread = std::thread(ping_pool_connections);
        std::thread check_connections_heartbeat_thread = std::thread(check_connections_heartbeat);
        std::thread disk_flushing_thread = std::thread(flush_to_disk_periodically);
        std::thread run_quorum_communication_thread = std::thread(run_controller_quorum_communication);
        std::thread check_dead_data_nodes_thread = std::thread(check_dead_data_nodes);
        std::thread check_for_commit_and_last_applied_diff_thread = std::thread(check_for_commit_and_last_applied_diff);
        std::thread send_heartbeats_to_leader_thread = std::thread(send_heartbeats_to_leader);
        std::thread compact_closed_segments_thread = std::thread(compact_closed_segments);
        std::thread remove_expired_segments_thread = std::thread(remove_expired_segments);
        std::thread retrieve_cluster_metadata_updates_thread = std::thread(retrieve_cluster_metadata_updates);
        std::thread check_for_dead_consumers_thread = std::thread(check_for_dead_consumers);
        std::thread fetch_data_from_partition_leaders_thread = std::thread(fetch_data_from_partition_leaders);
        std::thread check_for_lagging_followers_thread = std::thread(check_for_lagging_followers);

        internal_listener_thread.join();
        external_listener_thread.join();
        connection_pools_thread.join();
        ping_pool_connections_thread.join();
        check_connections_heartbeat_thread.join();
        disk_flushing_thread.join();
        run_quorum_communication_thread.join();
        check_dead_data_nodes_thread.join();
        check_for_commit_and_last_applied_diff_thread.join();
        send_heartbeats_to_leader_thread.join();
        compact_closed_segments_thread.join();
        remove_expired_segments_thread.join();
        retrieve_cluster_metadata_updates_thread.join();
        check_for_dead_consumers_thread.join();
        fetch_data_from_partition_leaders_thread.join();
        check_for_lagging_followers_thread.join();
    }
    catch (const std::exception&) {
        should_terminate = true;
    }

    cm.get()->terminate_connections();

    ssl_context_handler->cleanup_ssl();

	return 0;
}
