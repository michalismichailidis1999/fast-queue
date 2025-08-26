#include "../header_files/BeforeServerStartupHandler.h"

BeforeServerStartupHandler::BeforeServerStartupHandler(Controller* controller, DataNode* data_node, ClusterMetadataApplyHandler* cmah, QueueManager* qm, MessageOffsetAckHandler* oah, SegmentAllocator* sa, SegmentMessageMap* smm, FileHandler* fh, QueueSegmentFilePathMapper* pm, Util* util, Logger* logger, Settings* settings) {
    this->controller = controller;
    this->data_node = data_node;
    this->cmah = cmah;
    this->qm = qm;
    this->oah = oah;
    this->sa = sa;
    this->smm = smm;
	this->fh = fh;
	this->pm = pm;
	this->util = util;
	this->logger = logger;
	this->settings = settings;
}

// Public Methods

void BeforeServerStartupHandler::initialize_required_folders_and_queues() {
    this->logger->log_info("Scanning path " + settings->get_log_path() + " to initialize data and clean unnecessary files...");

    // Creating parent folders
    this->fh->create_directory(this->settings->get_log_path());

    // Creating required subfolders to run the broker
    this->fh->create_directory(this->pm->get_queue_folder_path(CLUSTER_METADATA_QUEUE_NAME));

    this->clear_unnecessary_files_and_initialize_queues();

    this->logger->log_info("Path scanning completed");
}

void BeforeServerStartupHandler::rebuild_cluster_metadata() {
    this->logger->log_info("Rebuilding cluster metadata...");

    std::shared_ptr<Queue> queue = this->qm->get_queue(CLUSTER_METADATA_QUEUE_NAME);

    std::shared_ptr<Partition> partition = queue.get()->get_partition(0);

    unsigned long long smallest_segment_id = partition->get_smallest_segment_id();
    unsigned long long current_segment_id = partition->get_current_segment_id();

    std::unordered_map<int, Command> registered_nodes;
    std::unordered_map<std::string, Command> registered_consumers;

    while (smallest_segment_id > 0 && smallest_segment_id <= current_segment_id) {
        this->cmah->apply_commands_from_segment(
            this->controller->get_cluster_metadata(), 
            smallest_segment_id, 
            this->controller->get_last_command_applied(),
            &registered_nodes,
            &registered_consumers,
            this->controller->get_future_cluster_metadata()
        );

        if (smallest_segment_id < current_segment_id) smallest_segment_id++;
        else break;
    }

    for (auto& iter : registered_nodes)
    {
        this->cmah->apply_command(this->controller->get_cluster_metadata(), &iter.second);
        this->controller->update_data_node_heartbeat(iter.first, NULL, true);
    }

    for (auto& iter : registered_consumers)
    {
        RegisterConsumerGroupCommand* command = (RegisterConsumerGroupCommand*)iter.second.get_command_info();
        this->cmah->apply_command(this->controller->get_cluster_metadata(), &iter.second, true);
        this->data_node->update_consumer_heartbeat(command->get_queue_name(), command->get_group_id(), command->get_consumer_id());
    }

    std::vector<std::string> queue_names;

    this->qm->get_all_queue_names(&queue_names);

    for (const std::string& queue_name : queue_names) {
        if (queue_name == CLUSTER_METADATA_QUEUE_NAME) continue;

        std::shared_ptr<Queue> queue = this->qm->get_queue(queue_name);

        int total_partitions = queue.get()->get_metadata()->get_partitions();

        for (int i = 0; i < total_partitions; i++) {
            std::shared_ptr<Partition> partition = queue.get()->get_partition(i);

            if (partition == nullptr) continue;

            this->oah->assign_latest_offset_to_partition_consumers(partition.get());

            auto consumers = partition.get()->get_all_consumers();

            for (auto& consumer : consumers)
                this->data_node->update_consumer_heartbeat(queue_name, consumer.get()->get_group_id(), consumer.get()->get_id());
        }
    }

    this->logger->log_info("Cluster metadata rebuilt successfully");
}

// ========================================================

// Private Methods

void BeforeServerStartupHandler::handle_compacted_segment(const std::string& queue_name, int partition_id, unsigned long long segment_id, bool is_internal_queue) {
    std::string segment_path = this->pm->get_file_path(
        queue_name, 
        segment_id,
        is_internal_queue ? -1 : partition_id
    );

    std::string index_path = this->pm->get_file_path(
        queue_name,
        segment_id,
        is_internal_queue ? -1 : partition_id,
        true
    );

    std::string compacted_segment_path = this->pm->get_compacted_file_path(
        queue_name,
        segment_id,
        is_internal_queue ? -1 : partition_id
    );

    std::string compacted_index_path = this->pm->get_compacted_file_path(
        queue_name,
        segment_id,
        is_internal_queue ? -1 : partition_id,
        true
    );

    if (this->fh->check_if_exists(segment_path) && this->fh->check_if_exists(index_path)) {
        this->fh->delete_dir_or_file(compacted_segment_path);
        this->fh->delete_dir_or_file(compacted_index_path);
        return;
    }

    this->fh->delete_dir_or_file(segment_path);
    this->fh->delete_dir_or_file(index_path);

    this->fh->rename_file("", compacted_segment_path, segment_path);
    this->fh->rename_file("", compacted_index_path, index_path);
}

void BeforeServerStartupHandler::clear_unnecessary_files_and_initialize_queues() {
    std::regex get_queue_name_rgx("((__|)[a-zA-Z][a-zA-Z0-9_-]*)$", std::regex_constants::icase);
    std::regex get_segment_num_rgx("0*([1-9][0-9]*)\\" + FILE_EXTENSION + "$", std::regex_constants::icase);
    std::regex get_compacted_segment_num_rgx("0*([1-9][0-9]*)_compacted\\" + FILE_EXTENSION + "$", std::regex_constants::icase);
    std::regex is_segment_index("index_0*([1-9][0-9]*)\\" + FILE_EXTENSION + "$", std::regex_constants::icase);
    std::regex is_compacted_segment_index("index_0*([1-9][0-9]*)_compacted\\" + FILE_EXTENSION + "$", std::regex_constants::icase);
    std::regex partition_match_rgx("partition-([0-9]|[1-9][0-9]+)$", std::regex_constants::icase);
    std::regex is_metadata_file_rgx("metadata\\" + FILE_EXTENSION + "$", std::regex_constants::icase);
    std::regex is_offsets_file_rgx("__offsets\\" + FILE_EXTENSION + "$", std::regex_constants::icase);

    int partition_id = 0;
    bool is_cluster_metadata_queue = false;
    std::string queue_name = "";

    std::shared_ptr<Queue> queue = nullptr;
    std::shared_ptr<QueueMetadata> metadata = nullptr;
    std::unordered_map<unsigned int, std::shared_ptr<Partition>> partitions;
    std::shared_ptr<PartitionSegment> segment = nullptr;

    bool compacted_segment = false;
    unsigned long long segment_id = 0;

    std::vector<std::tuple<std::string, int, unsigned long long, bool>> compacted_segments_to_handle;

    auto queue_partition_segment_func = [&](const std::filesystem::directory_entry& dir_entry) {
        const std::string& path = this->fh->get_dir_entry_path(dir_entry);

        compacted_segment = false;

        std::smatch match;

        if (std::regex_search(path, match, is_metadata_file_rgx)) return;

        if (std::regex_search(path, match, is_offsets_file_rgx)) return;

        if (std::regex_search(path, match, is_segment_index))  return;

        if (std::regex_search(path, match, is_compacted_segment_index)) {
            segment_id = std::stoull(match[1]);
            compacted_segments_to_handle.emplace_back(queue_name, partition_id, segment_id, is_cluster_metadata_queue);
            return;
        }

        if (!std::regex_search(path, match, get_segment_num_rgx)) {
            if (!std::regex_search(path, match, get_compacted_segment_num_rgx)) return;
            compacted_segment = true;
        }

        if (match.size() < 1) return;

        segment_id = std::stoull(match[1]);

        if(compacted_segment)
            compacted_segments_to_handle.emplace_back(queue_name, partition_id, segment_id, is_cluster_metadata_queue);

        Partition* partition = partitions[partition_id].get();

        if (partition->get_current_segment_id() < segment_id || partition->get_current_segment_id() == 0)
            partition->set_current_segment_id(segment_id);

        if (partition->get_smallest_segment_id() == 0 || partition->get_smallest_segment_id() > segment_id) {
            partition->set_smallest_segment_id(segment_id);

            if(partition->get_smallest_uncompacted_segment_id() == 0 || partition->get_smallest_uncompacted_segment_id() > segment_id)
                partition->set_smallest_uncompacted_segment_id(segment_id);
        }
    };

    auto queue_partition_func = [&](const std::filesystem::directory_entry& dir_entry) {
        const std::string& path = this->fh->get_dir_entry_path(dir_entry);

        std::smatch match;

        if (!std::regex_search(path, match, partition_match_rgx)) return;

        partition_id = (unsigned int)std::stoi((std::string(match[1])));

        if (partition_id >= metadata.get()->get_partitions()) {
            this->logger->log_warning(
                "Partition with id "
                + std::to_string(partition_id)
                + " found in queue's "
                + metadata.get()->get_name()
                + " folder, but queue has total "
                + std::to_string(metadata.get()->get_partitions())
                + " partitions. Deleting this incorrect folder."
            );

            this->fh->delete_dir_or_file(path);

            this->logger->log_info("Deleted incorrect partition path " + path);

            return;
        }

        std::shared_ptr<Partition> partition = std::shared_ptr<Partition>(new Partition(partition_id, queue_name));

        partitions[partition_id] = partition;

        this->set_partition_segment_message_map(partition.get(), false);

        this->fh->execute_action_to_dir_subfiles(path, queue_partition_segment_func);
    };

    auto queue_func = [&](const std::filesystem::directory_entry& dir_entry) {
        const std::string& path = this->fh->get_dir_entry_path(dir_entry);

        partitions.clear();

        std::smatch match;

        if (!std::regex_search(path, match, get_queue_name_rgx)) {
            this->logger->log_warning("Unknown queue folder detected with path " + path);
            this->fh->delete_dir_or_file(path);
            this->logger->log_warning("Folder/File with path " + path + " deleted.");
            return;
        }

        queue_name = match[1];

        if (queue_name.size() >= 2 && queue_name[0] == '_' && queue_name[1] == '_' && !Helper::is_internal_queue(queue_name)) {
            this->logger->log_warning("Unknown queue folder detected with path " + path + ".Only internal queues can start with __ in their name.");
            this->fh->delete_dir_or_file(path);
            this->logger->log_warning("Folder/File with path " + path + " deleted.");
            return;
        }

        std::string metadata_file_path = path + "/metadata" + FILE_EXTENSION;

        if (queue_name == CLUSTER_METADATA_QUEUE_NAME) {
            partition_id = 0;
            is_cluster_metadata_queue = true;
            partitions[0] = std::shared_ptr<Partition>(new Partition(0U, CLUSTER_METADATA_QUEUE_NAME));
            this->set_partition_segment_message_map(partitions[0].get(), true);
        }
        else is_cluster_metadata_queue = false;

        metadata = this->get_queue_metadata(metadata_file_path, queue_name, !is_cluster_metadata_queue);

        if (metadata == nullptr)
        {
            // metadata is needed to know how many partitions to expect in user created queue
            this->logger->log_warning("Queue folder found with no metadata file inside it. Cleaning this folder with path " + path + " so it can be rebuild if no corruption occured.");
            this->fh->delete_dir_or_file(path);
            return;
        }

        if (!is_cluster_metadata_queue)
            this->fh->execute_action_to_dir_subfiles(path, queue_partition_func);
        else
            this->fh->execute_action_to_dir_subfiles(path, queue_partition_segment_func);

        queue = std::shared_ptr<Queue>(new Queue(metadata));

        for (auto& iter : partitions) {
            this->set_partition_active_segment(iter.second.get(), is_cluster_metadata_queue);

            if (!is_cluster_metadata_queue)
                this->set_partition_replicated_offset(iter.second.get());

            queue->add_partition(iter.second);
        }

        qm->add_queue(queue);
    };

    this->fh->execute_action_to_dir_subfiles(settings->get_log_path(), queue_func);

    for (auto& compacted_segment : compacted_segments_to_handle)
        this->handle_compacted_segment(
            std::get<0>(compacted_segment),
            std::get<1>(compacted_segment),
            std::get<2>(compacted_segment),
            std::get<3>(compacted_segment)
        );
}

std::shared_ptr<QueueMetadata> BeforeServerStartupHandler::get_queue_metadata(const std::string& queue_metadata_file_path, const std::string& queue_name, bool must_exist) {
    if (!this->fh->check_if_exists(queue_metadata_file_path) && must_exist) return nullptr;
    
    if (!this->fh->check_if_exists(queue_metadata_file_path) && !must_exist) {
        std::shared_ptr<QueueMetadata> metadata = std::shared_ptr<QueueMetadata>(
            new QueueMetadata(queue_name, 1, 1, CleanupPolicyType::COMPACT_SEGMENTS)
        );

        if (queue_name == CLUSTER_METADATA_QUEUE_NAME)
            metadata.get()->set_status(Status::ACTIVE);

        std::tuple<long, std::shared_ptr<char>> bytes_tup = metadata.get()->get_metadata_bytes();

        this->fh->create_new_file(
            queue_metadata_file_path,
            std::get<0>(bytes_tup),
            std::get<1>(bytes_tup).get(),
            queue_name == CLUSTER_METADATA_QUEUE_NAME ? this->pm->get_metadata_file_key(queue_name) : "",
            true
        );

        return metadata;
    }

    std::unique_ptr<char> data = std::unique_ptr<char>(new char[QUEUE_METADATA_TOTAL_BYTES]);

    this->fh->read_from_file(
        queue_name == CLUSTER_METADATA_QUEUE_NAME ? this->pm->get_metadata_file_key(queue_name) : "",
        queue_metadata_file_path,
        QUEUE_METADATA_TOTAL_BYTES,
        0,
        data.get()
    );

    return std::shared_ptr<QueueMetadata>(new QueueMetadata(data.get()));
}

void BeforeServerStartupHandler::set_partition_segment_message_map(Partition* partition, bool is_cluster_metadata_queue) {
    std::string file_key = this->pm->get_segment_message_map_key(
        partition->get_queue_name(),
        is_cluster_metadata_queue ? -1 : partition->get_partition_id()
    );

    std::string file_path = this->pm->get_segment_message_map_path(
        partition->get_queue_name(),
        is_cluster_metadata_queue ? -1 : partition->get_partition_id()
    );

    if (!this->fh->check_if_exists(file_path)) {
        std::unique_ptr<char> data = std::unique_ptr<char>(new char[MESSAGES_LOC_MAP_PAGE_SIZE]);

        this->smm->fill_new_page_with_values(data.get(), 1);


        this->fh->create_new_file(
            file_path,
            MESSAGES_LOC_MAP_PAGE_SIZE,
            data.get(),
            file_key,
            true
        );
    }

    partition->set_message_map(file_key, file_path);
}

void BeforeServerStartupHandler::set_partition_replicated_offset(Partition* partition) {
    std::string offsets_key = this->pm->get_partition_offsets_key(partition->get_queue_name(), partition->get_partition_id());
    std::string offsets_path = this->pm->get_partition_offsets_path(partition->get_queue_name(), partition->get_partition_id());

    std::string temp_offsets_path = this->pm->get_partition_offsets_path(partition->get_queue_name(), partition->get_partition_id(), true);

    partition->set_offsets(offsets_key, offsets_path);

    unsigned long long last_replicated_offset = 0;

    if (!this->fh->check_if_exists(offsets_path) && this->fh->check_if_exists(temp_offsets_path)) {
        this->fh->rename_file("", temp_offsets_path, offsets_path);
    }

    if (this->fh->check_if_exists(offsets_path)) {
        this->fh->read_from_file(
            offsets_key,
            offsets_path,
            sizeof(unsigned long long),
            0,
            &last_replicated_offset
        );

        this->fh->delete_dir_or_file(temp_offsets_path);

        partition->set_last_replicated_offset(last_replicated_offset);

        return;
    }

    unsigned int total_bytes = sizeof(unsigned long long) + sizeof(unsigned int);

    std::unique_ptr<char> buff = std::unique_ptr<char>(new char[total_bytes]);

    unsigned long long zero_val = 0;

    memcpy_s(buff.get(), sizeof(unsigned long long), &zero_val, sizeof(unsigned long long));
    memcpy_s(buff.get() + sizeof(unsigned long long), sizeof(unsigned int), &zero_val, sizeof(unsigned int));

    this->fh->create_new_file(
        offsets_path,
        total_bytes,
        buff.get(),
        offsets_key,
        true
    );
}

void BeforeServerStartupHandler::set_partition_active_segment(Partition* partition, bool is_cluster_metadata_queue) {
    std::shared_ptr<PartitionSegment> segment = nullptr;

    std::string segment_key = partition->get_current_segment_id() > 0
        ? this->pm->get_file_key(partition->get_queue_name(), partition->get_current_segment_id(), is_cluster_metadata_queue ? -1 : partition->get_partition_id())
        : "";

    std::string segment_path = partition->get_current_segment_id() > 0
        ? this->pm->get_file_path(partition->get_queue_name(), partition->get_current_segment_id(), is_cluster_metadata_queue ? -1 : partition->get_partition_id())
        : "";

    if (partition->get_current_segment_id() > 0 && this->fh->check_if_exists(segment_path)) {
        std::unique_ptr<char> bytes = std::unique_ptr<char>(new char[SEGMENT_METADATA_TOTAL_BYTES]);

        this->fh->read_from_file(
            segment_key,
            segment_path,
            SEGMENT_METADATA_TOTAL_BYTES,
            0,
            bytes.get()
        );

        segment = std::shared_ptr<PartitionSegment>(new PartitionSegment(bytes.get(), segment_key, segment_path));
        partition->set_active_segment(segment);

        if (!segment.get()->get_is_read_only()) {
            this->set_segment_index(partition->get_queue_name(), segment.get(), is_cluster_metadata_queue ? -1 : partition->get_partition_id());
            this->set_segment_last_message_offset_and_timestamp(partition, segment.get());
            return;
        }
    }

    this->sa->allocate_new_segment(partition);
}

void BeforeServerStartupHandler::set_segment_index(const std::string& queue_name, PartitionSegment* segment, int partition) {
    std::string index_file_key = this->pm->get_file_key(queue_name, segment->get_id(), partition, true);
    std::string index_file_path = this->pm->get_file_path(queue_name, segment->get_id(), partition, true);

    segment->set_index(index_file_key, index_file_path);

    if (this->fh->check_if_exists(index_file_path)) {
        std::string partition_info = partition >= 0
            ? " partition " + std::to_string(partition) + " "
            : " ";

        this->logger->log_info("Setting up last index page offset for queue's " + queue_name + partition_info + "segment " + std::to_string(segment->get_id()));

        std::unique_ptr<char> index_page = std::unique_ptr<char>(new char[INDEX_PAGE_SIZE]);
        std::unique_ptr<BTreeNode> node = nullptr;

        unsigned int page_offset = 0;

        while (true) {
            this->fh->read_from_file(
                index_file_key,
                index_file_path,
                INDEX_PAGE_SIZE,
                page_offset * INDEX_PAGE_SIZE,
                index_page.get()
            );

            if (!Helper::has_valid_checksum(index_page.get()))
                throw CorruptionException("Corrupted index page detected while setting segment index");

            node = std::unique_ptr<BTreeNode>(new BTreeNode(index_page.get()));

            if (node.get()->get_page_type() == PageType::LEAF) break;

            page_offset = node.get()->get_next_page_offset() == -1
                ? node.get()->get_last_child()->val_pos
                : node.get()->get_next_page_offset();
        }

        segment->set_last_index_page_offset(page_offset);

        this->logger->log_info("Last index page offset was set successfully");

        return;
    }

    this->fh->create_new_file(
        index_file_path,
        INDEX_PAGE_SIZE,
        std::get<0>(BTreeNode(PageType::LEAF).get_page_bytes()).get(),
        index_file_key,
        true
    );
}

void BeforeServerStartupHandler::set_segment_last_message_offset_and_timestamp(Partition* partition, PartitionSegment* segment) {
    unsigned int batch_size = READ_MESSAGES_BATCH_SIZE;
    std::unique_ptr<char> read_batch = std::unique_ptr<char>(new char[batch_size]);

    unsigned long long read_pos = SEGMENT_METADATA_TOTAL_BYTES;

    unsigned long bytes_read = 0;

    unsigned int offset = 0;

    unsigned int message_bytes = 0;
    unsigned long long message_id = 0;
    unsigned long long message_timestamp = 0;
    unsigned long long message_leader_epoch = 0;

    unsigned long long total_written_bytes = 0;

    while (true) {
        bytes_read = this->fh->read_from_file(
            segment->get_segment_key(),
            segment->get_segment_path(),
            batch_size,
            read_pos,
            read_batch.get()
        );

        if (bytes_read == 0) break;

        offset = 0;

        while (offset <= bytes_read - MESSAGE_TOTAL_BYTES) {
            memcpy_s(&message_bytes, TOTAL_METADATA_BYTES, read_batch.get() + offset + TOTAL_METADATA_BYTES_OFFSET, TOTAL_METADATA_BYTES);

            if (offset + message_bytes <= batch_size && !Helper::has_valid_checksum(read_batch.get() + offset))
                throw CorruptionException("Corrupted messages detected while setting segment's last message offset and timestamp");

            if (offset + message_bytes > bytes_read) break;

            memcpy_s(&message_id, MESSAGE_ID_SIZE, read_batch.get() + offset + MESSAGE_ID_OFFSET, MESSAGE_ID_SIZE);
            memcpy_s(&message_timestamp, MESSAGE_TIMESTAMP_SIZE, read_batch.get() + offset + MESSAGE_TIMESTAMP_OFFSET, MESSAGE_TIMESTAMP_SIZE);
            memcpy_s(&message_leader_epoch, MESSAGE_LEADER_ID_SIZE, read_batch.get() + offset + MESSAGE_LEADER_ID_OFFSET, MESSAGE_LEADER_ID_SIZE);

            offset += message_bytes;
            total_written_bytes += message_bytes;
        }

        segment->set_last_message_offset(message_id);
        segment->set_last_message_timestamp(message_timestamp);

        if (bytes_read < READ_MESSAGES_BATCH_SIZE) {
            if (offset < bytes_read && offset + message_bytes > bytes_read) {
                std::unique_ptr<char> corrupted_message = std::unique_ptr<char>(new char[message_bytes]);
                bool ia_active = false;

                memcpy_s(corrupted_message.get(), message_bytes, read_batch.get() + offset, message_bytes);
                memcpy_s(corrupted_message.get() + MESSAGE_IS_ACTIVE_OFFSET, MESSAGE_IS_ACTIVE_SIZE, &ia_active, MESSAGE_IS_ACTIVE_SIZE);

                this->fh->write_to_file(
                    segment->get_segment_key(),
                    segment->get_segment_path(),
                    message_bytes,
                    read_pos + offset,
                    corrupted_message.get(),
                    true
                );
            }

            break;
        }

        if (message_bytes > batch_size) {
            batch_size = message_bytes;
            read_batch = std::unique_ptr<char>(new char[batch_size]);
        }
        else if (message_bytes < batch_size && batch_size > READ_MESSAGES_BATCH_SIZE) {
            batch_size = READ_MESSAGES_BATCH_SIZE;
            read_batch = std::unique_ptr<char>(new char[batch_size]);
        }

        read_pos += offset;
    }

    segment->set_total_written_bytes(total_written_bytes);
    partition->set_last_message_offset(segment->get_last_message_offset());
    partition->set_last_message_leader_epoch(message_leader_epoch);
}

// ========================================================