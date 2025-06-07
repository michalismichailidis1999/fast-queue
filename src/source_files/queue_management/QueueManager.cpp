#include "../../header_files/queue_management/QueueManager.h"

QueueManager::QueueManager(SegmentMessageMap* smm, FileHandler* fh, QueueSegmentFilePathMapper* pm, Logger* logger) {
	this->smm = smm;
	this->fh = fh;
	this->pm = pm;
	this->logger = logger;
}

void QueueManager::add_queue(std::shared_ptr<Queue> queue) {
	std::lock_guard<std::shared_mutex> lock(this->mut);
	this->queues[queue.get()->get_metadata()->get_name()] = queue;
}

bool QueueManager::has_queue(const std::string& queue_name) {
	std::shared_lock<std::shared_mutex> lock(this->mut);
	return this->queues.find(queue_name) != this->queues.end();
}

std::shared_ptr<Queue> QueueManager::get_queue(const std::string& queue_name) {
	std::shared_lock<std::shared_mutex> lock(this->mut);

	if (this->queues.find(queue_name) == this->queues.end()) return nullptr;

	return this->queues[queue_name];
}

void QueueManager::remove_queue(const std::string& queue_name) {
	std::lock_guard<std::shared_mutex> lock(this->mut);
	this->queues.erase(queue_name);
}

void QueueManager::get_all_queue_names(std::vector<std::string>* queues_list) {
	std::shared_lock<std::shared_mutex> lock(this->mut);

	for (auto iter : this->queues)
		(*queues_list).push_back(iter.first);
}

void QueueManager::create_queue(QueueMetadata* metadata) {
	if (Helper::is_internal_queue(metadata->get_name())) {
		this->logger->log_error("Invalid action. Cannot create queue, name is used for internal queue");
		return;
	}

	std::string queue_path = this->pm->get_queue_folder_path(metadata->get_name());

	if (!this->fh->check_if_exists(queue_path))
		this->fh->create_directory(queue_path);

	std::string queue_metadata_path = this->pm->get_metadata_file_path(metadata->get_name());

	if (!this->fh->check_if_exists(queue_metadata_path)) {
		std::tuple<long, std::shared_ptr<char>> bytes_tup = metadata->get_metadata_bytes();

		this->fh->create_new_file(
			queue_metadata_path,
			std::get<0>(bytes_tup),
			std::get<1>(bytes_tup).get(),
			"",
			true
		);
	}

	std::shared_ptr<QueueMetadata> new_metadata = std::shared_ptr<QueueMetadata>(
		new QueueMetadata(metadata->get_name(), metadata->get_partitions(), metadata->get_replication_factor())
	);

	new_metadata.get()->set_status(Status::ACTIVE);

	std::shared_ptr<Queue> queue = std::shared_ptr<Queue>(new Queue(new_metadata));

	this->add_queue(queue);
}

void QueueManager::delete_queue(const std::string& queue_name) {
	if (Helper::is_internal_queue(queue_name)) {
		this->logger->log_error("Invalid action. Cannot delete internal queue");
		return;
	}

	std::shared_ptr<Queue> queue = this->get_queue(queue_name);

	queue->get_metadata()->set_status(Status::PENDING_DELETION);

	std::string& queue_folder = this->pm->get_queue_folder_path(queue_name);

	this->fh->delete_dir_or_file(queue_folder, "", queue_name);

	this->remove_queue(queue_name);
}

void QueueManager::add_assigned_partition_to_queue(const std::string& queue_name, unsigned int partition_id) {
	if (Helper::is_internal_queue(queue_name)) {
		this->logger->log_error("Invalid action. Cannot assign partition to internal queue");
		return;
	}

	std::shared_ptr<Queue> queue = this->get_queue(queue_name);

	if (queue == nullptr) {
		std::string err_msg = "Could not assign partition to queue " + queue_name + ". Queue not found";
		throw std::exception(err_msg.c_str());
	}

	this->fh->create_directory(this->pm->get_partition_folder_path(queue_name, partition_id));

	std::shared_ptr<Partition> partition = std::shared_ptr<Partition>(new Partition(partition_id, queue_name));

	std::string segment_key = this->pm->get_file_key(queue_name, 1, partition_id);
	std::string segment_path = this->pm->get_file_path(queue_name, 1, partition_id);

	std::string index_key = this->pm->get_file_key(queue_name, 1, partition_id, true);
	std::string index_path = this->pm->get_file_path(queue_name, 1, partition_id, true);

	std::string mm_key = this->pm->get_segment_message_map_key(queue_name, partition_id);
	std::string mm_path = this->pm->get_segment_message_map_path(queue_name, partition_id);

	std::shared_ptr<PartitionSegment> segment = std::shared_ptr<PartitionSegment>(new PartitionSegment(1, segment_key, segment_path));

	segment.get()->set_index(index_key, index_path);

	partition->set_active_segment(segment);

	if (!this->fh->check_if_exists(segment_path))
	{
		std::tuple<long, std::shared_ptr<char>> bytes_tup = segment.get()->get_metadata_bytes();

		this->fh->create_new_file(
			segment_path,
			std::get<0>(bytes_tup),
			std::get<1>(bytes_tup).get(),
			segment_key,
			true
		);
	}

	if (!this->fh->check_if_exists(index_path))
		this->fh->create_new_file(
			index_path,
			INDEX_PAGE_SIZE,
			std::get<0>(BTreeNode(PageType::LEAF).get_page_bytes()).get(),
			index_key,
			true
		);

	if (!this->fh->check_if_exists(mm_path))
	{
		std::unique_ptr<char> data = std::unique_ptr<char>(new char[MAPPED_SEGMENTS_PER_PAGE]);

		this->smm->fill_new_page_with_values(data.get(), 1);

		this->fh->create_new_file(
			mm_path,
			MAPPED_SEGMENTS_PER_PAGE,
			data.get(),
			mm_key,
			true
		);
	}

	queue->add_partition(partition);
}

void QueueManager::remove_assigned_partition_from_queue(const std::string& queue_name, unsigned int partition_id) {
	if (Helper::is_internal_queue(queue_name)) {
		this->logger->log_error("Invalid action. Cannot remove partition from internal queue");
		return;
	}

	std::shared_ptr<Queue> queue = this->get_queue(queue_name);

	if (queue == nullptr) {
		std::string err_msg = "Could not assign partition to queue " + queue_name + ". Queue not found";
		throw std::exception(err_msg.c_str());
	}

	std::string part_key = this->pm->get_partition_folder_key(queue_name, partition_id);
	std::string part_folder = this->pm->get_partition_folder_path(queue_name, partition_id);
	
	this->fh->delete_dir_or_file(part_folder, "", part_key);

	queue.get()->remove_partition(partition_id);
}