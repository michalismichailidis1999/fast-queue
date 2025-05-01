#include "../../../../header_files/queue_management/messages_management/index_management/BPlusTreeIndexHandler.h"

BPlusTreeIndexHandler::BPlusTreeIndexHandler(DiskFlusher* disk_flusher, DiskReader* disk_reader) {
	this->disk_flusher = disk_flusher;
	this->disk_reader = disk_reader;
}

void BPlusTreeIndexHandler::add_message_to_index(Partition* partition, unsigned long long message_id, unsigned int message_pos) {
	PartitionSegment* segment = partition->get_active_segment();

	std::shared_ptr<char> node_data = std::shared_ptr<char>(new char[INDEX_PAGE_SIZE]);
	std::shared_ptr<BTreeNode> node_to_insert = nullptr;

	unsigned int page_offset = 0;
	bool node_to_insert_found = false;

	while (!node_to_insert_found) {
		this->disk_reader->read_data_from_disk(
			segment->get_index_key(),
			segment->get_index_path(),
			node_data.get(),
			INDEX_PAGE_SIZE,
			page_offset,
			Helper::is_internal_queue(partition->get_queue_name())
		);

		if (!Helper::has_valid_checksum(node_data.get()))
			throw std::exception("Index page was corrupted");

		std::shared_ptr<BTreeNode> node_to_insert = std::shared_ptr<BTreeNode>(new BTreeNode(node_data.get()));

		if (node_to_insert.get()->type == PageType::LEAF) node_to_insert_found = true;
		else page_offset = node_to_insert.get()->rows[node_to_insert.get()->rows_num].val_pos;
	}

	bool insertion_completed = false;

	auto row_to_insert = BTreeNodeRow{ message_id, message_pos };

	while (!insertion_completed) {
		auto split_res = node_to_insert.get()->insert(row_to_insert);

		std::shared_ptr<BTreeNode> left = std::get<0>(split_res);
		std::shared_ptr<BTreeNode> right = std::get<1>(split_res);

		if (left == nullptr) break;

		node_to_insert = right; // TODO: This must become the parent of current node_to_insert

		left.get()->page_offset = segment->get_last_index_page_offset(true);
		right.get()->page_offset = segment->get_last_index_page_offset(true);
	}
}