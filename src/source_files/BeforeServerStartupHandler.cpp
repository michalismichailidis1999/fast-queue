#include "../header_files/BeforeServerStartupHandler.h"

BeforeServerStartupHandler::BeforeServerStartupHandler(QueueManager* qm, SegmentAllocator* sa, ClusterMetadata* cluster_metadata, ClusterMetadata* future_cluster_metadata, FileHandler* fh, QueueSegmentFilePathMapper* pm, Util* util, Logger* logger, Settings* settings) {
	this->qm = qm;
    this->sa = sa;
    this->cluster_metadata = cluster_metadata;
    this->future_cluster_metadata = future_cluster_metadata;
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

    std::string cluster_metadata_queue_dir = this->settings->get_log_path() + "\\" + CLUSTER_METADATA_QUEUE_NAME;

    // Creating required subfolders to run the broker
    this->fh->create_directory(cluster_metadata_queue_dir);

    this->clear_unnecessary_files_and_initialize_queues();

    this->logger->log_info("Path scanning completed");
}

void BeforeServerStartupHandler::rebuild_cluster_metadata() {
    this->logger->log_info("Rebuilding cluster metadata...");

    // TODO: Read from __cluster_metadata queue to rebuild cluster metadata

    this->future_cluster_metadata->copy_from(cluster_metadata);

    this->logger->log_info("Clustere metadata rebuilt successfully");
}

// ========================================================

// Private Methods

void BeforeServerStartupHandler::clear_unnecessary_files_and_initialize_queues() {
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
        const std::string& path = this->fh->get_dir_entry_path(dir_entry);

        std::smatch match;

        if (std::regex_search(path, match, is_skippable_file_rgx)) return;

        if (!std::regex_search(path, match, get_segment_num_rgx)) {
            this->logger->log_warning("Incorrect file name detected at path " + path + ". Deleting it since it won't be used by server");
            this->fh->delete_dir_or_file(path);
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
                this->fh->delete_dir_or_file(ignored_snapshot);
                ignored_snapshot = path;
                last_ignored_snapshot_segment_id = segment_id;
                this->logger->log_info("Deleted old cluster metadata snapshot with path " + ignored_snapshot);
            }
            else {
                this->fh->delete_dir_or_file(path);
                this->logger->log_info("Deleted old cluster metadata snapshot with path " + ignored_snapshot);
            }

            return;
        }

        Partition* partition = partitions[partition_id].get();

        if (partition->get_current_segment_id() < segment_id || partition->get_current_segment_id() == 0)
            partition->set_current_segment_id(segment_id);
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

        if (partitions.size() < metadata.get()->get_partitions()) {
            this->logger->log_error(
                "Found "
                + std::to_string(partitions.size())
                + " partitions in queue's "
                + metadata.get()->get_name()
                + " folder, but queue has total "
                + std::to_string(metadata.get()->get_partitions())
                + " partitions"
            );

            this->fh->delete_dir_or_file(path);

            return;
        }

        queue = std::shared_ptr<Queue>(new Queue(metadata));

        for (auto& iter : partitions) {
            this->set_partition_active_segment(iter.second.get(), is_cluster_metadata_queue);
            queue->add_partition(iter.second);
        }

        qm->add_queue(queue);
    };

    this->fh->execute_action_to_dir_subfiles(settings->get_log_path(), queue_func);
}

std::shared_ptr<QueueMetadata> BeforeServerStartupHandler::get_queue_metadata(const std::string& queue_metadata_file_path, const std::string& queue_name, bool must_exist) {
    if (!this->fh->check_if_exists(queue_metadata_file_path) && !must_exist) {
        std::shared_ptr<QueueMetadata> metadata = std::shared_ptr<QueueMetadata>(
            new QueueMetadata(queue_name)
        );

        if (queue_name == CLUSTER_METADATA_QUEUE_NAME)
            metadata.get()->set_status(Status::ACTIVE);

        std::tuple<long, std::shared_ptr<char>> bytes_tup = metadata.get()->get_metadata_bytes();

        this->fh->create_new_file(
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

    this->fh->read_from_file(
        queue_name,
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
        std::unique_ptr<char> data = std::unique_ptr<char>(new char[MAPPED_SEGMENTS_PER_PAGE]);

        unsigned long long zero_value = 0;

        for (unsigned int i = 1; i < MAPPED_SEGMENTS_PER_PAGE; i++)
            memcpy_s(data.get() + i * sizeof(unsigned long long), sizeof(unsigned long long), &zero_value, sizeof(unsigned long long));

        this->fh->create_new_file(
            file_path,
            MAPPED_SEGMENTS_PER_PAGE,
            data.get(),
            file_key,
            true
        );
    }

    partition->set_message_map(file_key, file_path);
}

void BeforeServerStartupHandler::set_partition_active_segment(Partition* partition, bool is_cluster_metadata_queue) {
    std::shared_ptr<PartitionSegment> segment = nullptr;

    std::string segment_key = partition->get_current_segment_id() > 0
        ? this->pm->get_file_key(partition->get_queue_name(), partition->get_current_segment_id())
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

        if (!segment.get()->get_is_read_only()) {
            partition->set_active_segment(segment);
            this->set_segment_index(partition->get_queue_name(), segment.get(), is_cluster_metadata_queue ? -1 : partition->get_partition_id());
            return;
        }
    }

    this->sa->allocate_new_segment(partition);
}

void BeforeServerStartupHandler::set_segment_index(const std::string& queue_name, PartitionSegment* segment, int partition) {
    std::string index_file_key = this->pm->get_file_key(queue_name, segment->get_id(), true);
    std::string index_file_path = this->pm->get_file_path(queue_name, segment->get_id(), partition, true);

    segment->set_index(index_file_key, index_file_path);

    if (this->fh->check_if_exists(index_file_path)) return;

    this->fh->create_new_file(
        index_file_path,
        INDEX_PAGE_SIZE,
        std::get<0>(BTreeNode(PageType::LEAF).get_page_bytes()).get(),
        index_file_key,
        true
    );
}

// ========================================================