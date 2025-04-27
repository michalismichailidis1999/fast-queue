#include "FastQueue.h"

std::atomic_bool should_terminate(false);
std::atomic_int total_connections(0);

Logger* _logger;

void terminationSignalHandler(int signum) {
    if (signum == SIGSEGV) {
        _logger->log_error("Segmentation error occured. Shutting down...");
        should_terminate = true;
        return;
    }

    if (signum != SIGINT && signum != SIGTERM) return;

    _logger->log_info("Termination signal received. Initiating graceful shutdown...");
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

std::shared_ptr<QueueMetadata> get_queue_metadata(
    FileHandler* fh,
    QueueSegmentFilePathMapper* pm,
    const std::string& queue_metadata_file_path,
    const std::string& queue_name,
    bool must_exist = false
) {
    if (!fh->check_if_exists(queue_metadata_file_path) && !must_exist) {
        std::shared_ptr<QueueMetadata> metadata = std::shared_ptr<QueueMetadata>(
            new QueueMetadata(queue_name)
        );

        if (queue_name == CLUSTER_METADATA_QUEUE_NAME)
            metadata.get()->set_status(Status::ACTIVE);

        std::tuple<long, std::shared_ptr<char>> bytes_tup = metadata.get()->get_metadata_bytes();

        fh->create_new_file(
            queue_metadata_file_path,
            std::get<0>(bytes_tup),
            std::get<1>(bytes_tup).get(),
            "",
            true
        );

        return metadata;
    }

    if (must_exist) return nullptr;

    std::unique_ptr<char> data = std::unique_ptr<char>(new char[QUEUE_METADATA_TOTAL_BYTES]);

    fh->read_from_file(
        queue_name,
        queue_metadata_file_path,
        QUEUE_METADATA_TOTAL_BYTES,
        0,
        data.get()
    );

    return std::shared_ptr<QueueMetadata>(new QueueMetadata(data.get()));
}

void set_segment_index(PartitionSegment* segment) {

}

void set_partition_active_segment(FileHandler* fh, QueueSegmentFilePathMapper* pm, Partition* partition, const std::string& queue_name, bool is_cluster_metadata_queue) {
    std::shared_ptr<PartitionSegment> segment = nullptr;

    std::string segment_key = partition->get_current_segment() > 0
        ? pm->get_file_key(queue_name, partition->get_current_segment())
        : "";

    std::string segment_path = partition->get_current_segment() > 0
        ? pm->get_file_path(queue_name, partition->get_current_segment(), is_cluster_metadata_queue ? -1 : partition->get_partition_id())
        : "";

    if (partition->get_current_segment() > 0 && fh->check_if_exists(segment_path)) {
        std::unique_ptr<char> bytes = std::unique_ptr<char>(new char[SEGMENT_METADATA_TOTAL_BYTES]);

        fh->read_from_file(
            segment_key,
            segment_path,
            SEGMENT_METADATA_TOTAL_BYTES,
            0,
            bytes.get()
        );

        segment = std::shared_ptr<PartitionSegment>(new PartitionSegment(bytes.get(), segment_key, segment_path));

        if (!segment.get()->get_is_read_only()) {
            partition->set_active_segment(segment);
            set_segment_index(segment.get());
            return;
        }

        // TODO: Set segment largest message id to index mapper
    }
    
    unsigned long long new_segment_id = partition->get_current_segment() + 1;

    std::string new_segment_key = pm->get_file_key(queue_name, new_segment_id);
    std::string new_segment_path = pm->get_file_path(queue_name, new_segment_id, is_cluster_metadata_queue ? -1 : partition->get_partition_id());
    
    segment = std::shared_ptr<PartitionSegment>(
        new PartitionSegment(new_segment_id, new_segment_key, new_segment_path)
    );
    
    std::tuple<long, std::shared_ptr<char>> bytes_tup = segment.get()->get_metadata_bytes();

    fh->create_new_file(
        new_segment_path,
        std::get<0>(bytes_tup),
        std::get<1>(bytes_tup).get(),
        pm->get_file_key(queue_name, new_segment_id),
        true
    );

    partition->set_active_segment(segment);
    set_segment_index(segment.get());
}

void clear_unnecessary_files_and_initialize_queues(Settings* settings, FileHandler* fh, QueueManager* qm, QueueSegmentFilePathMapper* pm, Util* util) {
    std::regex get_queue_name_rgx(settings->get_log_path() + "/((__|)[a-zA-Z][a-zA-Z0-9_-]*)$", std::regex_constants::icase);
    std::regex get_segment_num_rgx("_cluster_snap_0*([1-9][0-9]*).*|0*([1-9][0-9]*).*$", std::regex_constants::icase);
    std::regex partition_match_rgx("partition-([0-9]|[1-9][0-9]+)$", std::regex_constants::icase);
    std::regex ignore_file_deletion_rgx("_cluster_snap_0*[1-9][0-9]*$" + FILE_EXTENSION + "$", std::regex_constants::icase);
    std::regex is_skippable_file_rgx("metadata" + FILE_EXTENSION + "$", std::regex_constants::icase);

    int partition_id = 0;
    bool is_cluster_metadata_queue = false;
    std::string queue_name = "";

    std::string ignored_snapshot = "";
    unsigned long long last_ignored_snapshot_segment_id = 0;

    std::shared_ptr<Queue> queue = nullptr;
    std::shared_ptr<QueueMetadata> metadata = nullptr;
    std::unordered_map<unsigned int, std::shared_ptr<Partition>> partitions;
    std::shared_ptr<PartitionSegment> segment = nullptr;

    auto queue_partition_segment_func = [&](const std::filesystem::directory_entry& dir_entry) {
        const std::string& path = fh->get_dir_entry_path(dir_entry);

        std::smatch match;

        if (std::regex_search(path, match, is_skippable_file_rgx)) return;

        if (!std::regex_search(path, match, get_segment_num_rgx)) {
            _logger->log_warning("Incorrect file name detected at path " + path + ". Deleting it since it won't be used by server");
            fh->delete_dir_or_file(path);
            return;
        }

        if (match.size() < 2) return;

        unsigned long long segment_id = std::stoull(match[1].matched ? match[1] : match[2]);

        // keep only last cluster metadata snapshot created
        if (is_cluster_metadata_queue && std::regex_search(path, match, ignore_file_deletion_rgx)) {
            if (ignored_snapshot == "") {
                ignored_snapshot = path;
                last_ignored_snapshot_segment_id = segment_id;
            }
            else if (segment_id > last_ignored_snapshot_segment_id) {
                fh->delete_dir_or_file(ignored_snapshot);
                ignored_snapshot = path;
                last_ignored_snapshot_segment_id = segment_id;
                _logger->log_info("Deleted old cluster metadata snapshot with path " + ignored_snapshot);
            }
            else {
                fh->delete_dir_or_file(path);
                _logger->log_info("Deleted old cluster metadata snapshot with path " + ignored_snapshot);
            }

            return;
        }

        Partition* partition = partitions[partition_id].get();

        if (partition->get_oldest_segment() > segment_id || partition->get_oldest_segment() == 0)
            partition->set_oldest_segment(segment_id);

        if (partition->get_current_segment() < segment_id || partition->get_current_segment() == 0)
            partition->set_current_segment(segment_id);
    };

    auto queue_partition_func = [&](const std::filesystem::directory_entry& dir_entry) {
        const std::string& path = fh->get_dir_entry_path(dir_entry);

        std::smatch match;

        if (!std::regex_search(path, match, partition_match_rgx)) return;

        partition_id = (unsigned int)std::stoi((std::string(match[1])));

        if (partition_id >= metadata.get()->get_partitions()) {
            _logger->log_warning(
                "Partition with id "
                + std::to_string(partition_id)
                + " found in queue's "
                + metadata.get()->get_name()
                + " folder, but queue has total "
                + std::to_string(metadata.get()->get_partitions())
                + " partitions. Deleting this incorrect folder."
            );

            fh->delete_dir_or_file(path);

            _logger->log_info("Deleted incorrect partition path " + path);

            return;
        }

        std::shared_ptr<Partition> partition = std::shared_ptr<Partition>(new Partition(partition_id, queue_name));

        partitions[partition_id] = partition;

        fh->execute_action_to_dir_subfiles(path, queue_partition_segment_func);
    };

    auto queue_func = [&](const std::filesystem::directory_entry& dir_entry) {
        const std::string& path = fh->get_dir_entry_path(dir_entry);

        std::smatch match;

        if (!std::regex_search(path, match, get_queue_name_rgx)) {
            _logger->log_warning("Unknown queue folder detected with path " + path);
            fh->delete_dir_or_file(path);
            _logger->log_warning("Folder/File with path " + path + " deleted.");
            return;
        }

        queue_name = match[1];

        if (queue_name.size() >= 2 && queue_name[0] == '_' && queue_name[1] == '_' && !Helper::is_internal_queue(queue_name)) {
            _logger->log_warning("Unknown queue folder detected with path " + path + ".Only internal queues can start with __ in their name.");
            fh->delete_dir_or_file(path);
            _logger->log_warning("Folder/File with path " + path + " deleted.");
            return;
        }

        std::string metadata_file_path = path + "/metadata" + FILE_EXTENSION;

        if (queue_name == CLUSTER_METADATA_QUEUE_NAME) {
            partition_id = 0;
            is_cluster_metadata_queue = true;
            partitions[0] = std::shared_ptr<Partition>(new Partition(0U, CLUSTER_METADATA_QUEUE_NAME));
        }
        else is_cluster_metadata_queue = false;

        metadata = get_queue_metadata(fh, pm, metadata_file_path, queue_name, !is_cluster_metadata_queue);

        if (metadata == nullptr)
        {
            // metadata is needed to know how many partitions to expect in user created queue
            _logger->log_warning("Queue folder found with no metadata file inside it. Cleaning this folder with path " + path + " so it can be rebuild if no corruption occured.");
            fh->delete_dir_or_file(path);
            return;
        }

        if (!is_cluster_metadata_queue)
            fh->execute_action_to_dir_subfiles(path, queue_partition_func);
        else
            fh->execute_action_to_dir_subfiles(path, queue_partition_segment_func);

        if (partitions.size() < metadata.get()->get_partitions()) {
            _logger->log_error(
                "Found "
                + std::to_string(partitions.size())
                + " partitions in queue's "
                + metadata.get()->get_name()
                + " folder, but queue has total "
                + std::to_string(metadata.get()->get_partitions())
                + " partitions"
            );

            fh->delete_dir_or_file(path);

            return;
        }

        queue = std::shared_ptr<Queue>(new Queue(metadata));

        for (auto& iter : partitions) {
            set_partition_active_segment(fh, pm, iter.second.get(), queue_name, is_cluster_metadata_queue);
            queue->add_partition(iter.second);
        }

        qm->add_queue(queue);
    };

    fh->execute_action_to_dir_subfiles(settings->get_log_path(), queue_func);
}

void initialize_required_folders_and_queues(Settings* settings, FileHandler* fh, QueueSegmentFilePathMapper* pm, QueueManager* qm, Util* util) {
    _logger->log_info("Scanning path " + settings->get_log_path() + " to initialize data and clean unnecessary files...");
    
    // Creating parent folders
    fh->create_directory(settings->get_log_path());

    std::string cluster_metadata_queue_dir = settings->get_log_path() + "\\" + CLUSTER_METADATA_QUEUE_NAME;

    // Creating required subfolders to run the broker
    fh->create_directory(cluster_metadata_queue_dir);

    clear_unnecessary_files_and_initialize_queues(settings, fh, qm, pm, util);

    _logger->log_info("Path scanning completed");
}

void rebuild_cluster_metadata(QueueManager* qm, ClusterMetadata* cluster_metadata, ClusterMetadata* future_cluster_metadata) {
    _logger->log_info("Rebuilding cluster metadata...");

    // TODO: Read from __cluster_metadata queue to rebuild cluster metadata

    future_cluster_metadata->copy_from(cluster_metadata);

    _logger->log_info("Clustere metadata rebuilt successfully");
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

    std::unique_ptr<QueueSegmentFilePathMapper> pm = std::unique_ptr<QueueSegmentFilePathMapper>(new QueueSegmentFilePathMapper(util.get(), settings.get()));

    std::unique_ptr<QueueManager> qm = std::unique_ptr<QueueManager>(new QueueManager(fh.get(), pm.get(), server_logger.get()));

    initialize_required_folders_and_queues(settings.get(), fh.get(), pm.get(), qm.get(), util.get());

    std::unique_ptr<SocketHandler> socket_handler = std::unique_ptr<SocketHandler>(new SocketHandler(settings.get(), server_logger.get()));
    std::unique_ptr<SslContextHandler> ssl_context_handler = std::unique_ptr<SslContextHandler>(new SslContextHandler(settings.get(), server_logger.get()));

    std::unique_ptr<ClassToByteTransformer> transformer = std::unique_ptr<ClassToByteTransformer>(new ClassToByteTransformer());
    std::unique_ptr<RequestMapper> request_mapper = std::unique_ptr<RequestMapper>(new RequestMapper());
    std::unique_ptr<ResponseMapper> response_mapper = std::unique_ptr<ResponseMapper>(new ResponseMapper());

    std::unique_ptr<DiskFlusher> df = std::unique_ptr<DiskFlusher>(new DiskFlusher(qm.get(), fh.get(), server_logger.get(), settings.get(), &should_terminate));
    std::unique_ptr<DiskReader> dr = std::unique_ptr<DiskReader>(new DiskReader(fh.get(), server_logger.get(), settings.get()));

    std::unique_ptr<SegmentAllocator> sa = std::unique_ptr<SegmentAllocator>(new SegmentAllocator(fh.get()));
    std::unique_ptr<BPlusTreeIndexHandler> ih = std::unique_ptr<BPlusTreeIndexHandler>(new BPlusTreeIndexHandler(df.get(), dr.get()));
    std::unique_ptr<MessagesHandler> mh = std::unique_ptr<MessagesHandler>(new MessagesHandler(df.get(), dr.get(), pm.get(), sa.get(), ih.get(), settings.get()));

    std::unique_ptr<ConnectionsManager> cm = std::unique_ptr<ConnectionsManager>(new ConnectionsManager(socket_handler.get(), ssl_context_handler.get(), response_mapper.get(), util.get(), settings.get(), server_logger.get(), &should_terminate));

    std::unique_ptr<ClusterMetadata> cluster_metadata = std::unique_ptr<ClusterMetadata>(new ClusterMetadata(settings.get()->get_node_id()));
    std::unique_ptr<ClusterMetadata> future_cluster_metadata = std::unique_ptr<ClusterMetadata>(new ClusterMetadata());

    std::unique_ptr<Controller> controller = settings.get()->get_is_controller_node()
        ? std::unique_ptr<Controller>(new Controller(cm.get(), qm.get(), mh.get(), response_mapper.get(), transformer.get(), util.get(), controller_logger.get(), settings.get(), cluster_metadata.get(), future_cluster_metadata.get(), &should_terminate))
        : nullptr;

    std::unique_ptr data_node = std::unique_ptr<DataNode>(new DataNode(controller.get(), cm.get(), cluster_metadata.get(), response_mapper.get(), transformer.get(), settings.get(), server_logger.get()));

    std::unique_ptr<ClientRequestExecutor> client_request_executor = std::unique_ptr<ClientRequestExecutor>(new ClientRequestExecutor(cm.get(), qm.get(), controller.get(), cluster_metadata.get(), transformer.get(), fh.get(), util.get(), settings.get(), server_logger.get()));
    std::unique_ptr<InternalRequestExecutor> internal_request_executor = std::unique_ptr<InternalRequestExecutor>(new InternalRequestExecutor(settings.get(), server_logger.get(), cm.get(), fh.get(), controller.get(), transformer.get()));
    std::unique_ptr<RequestManager> rm = std::unique_ptr<RequestManager>(new RequestManager(cm.get(), settings.get(), client_request_executor.get(), internal_request_executor.get(), request_mapper.get(), server_logger.get()));

    server_logger->log_info("Server starting...");

    rebuild_cluster_metadata(qm.get(), cluster_metadata.get(), future_cluster_metadata.get());

    // Register signal handlers
    signal(SIGINT, terminationSignalHandler);
    signal(SIGTERM, terminationSignalHandler);  // Or other relevant signals
    signal(SIGSEGV, terminationSignalHandler);  // Or other relevant signals

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
