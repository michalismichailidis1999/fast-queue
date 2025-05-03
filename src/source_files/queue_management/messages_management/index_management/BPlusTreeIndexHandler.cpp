#include "../../../../header_files/queue_management/messages_management/index_management/BPlusTreeIndexHandler.h"

BPlusTreeIndexHandler::BPlusTreeIndexHandler(DiskFlusher* disk_flusher, DiskReader* disk_reader) {
	this->disk_flusher = disk_flusher;
	this->disk_reader = disk_reader;
}

void BPlusTreeIndexHandler::add_message_to_index(Partition* partition, unsigned long long message_id, unsigned int message_pos) {
	PartitionSegment* segment = partition->get_active_segment();

	std::shared_ptr<char> node_data = std::shared_ptr<char>(new char[INDEX_PAGE_SIZE]);
	std::shared_ptr<BTreeNode> node_to_insert = nullptr;
	std::shared_ptr<BTreeNode> parent_node = nullptr;

	unsigned int page_offset = segment->get_last_index_non_leaf_offset();
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

		if (node_to_insert.get()->type == PageType::LEAF) {
			node_to_insert_found = true;
			continue;
		}
		
		page_offset = node_to_insert.get()->rows[node_to_insert.get()->rows_num].val_pos;
		parent_node = node_to_insert;
	}

	bool is_internal_queue = Helper::is_internal_queue(partition->get_queue_name());

	auto row_to_insert = BTreeNodeRow{ message_id, message_pos };

	bool node_is_not_full = node_to_insert.get()->insert(row_to_insert);

	if (node_is_not_full) {
		this->disk_flusher->flush_data_to_disk(
			segment->get_index_key(),
			segment->get_index_path(),
			std::get<0>(node_to_insert.get()->get_page_bytes()).get(),
			INDEX_PAGE_SIZE,
			node_to_insert.get()->page_offset * INDEX_PAGE_SIZE,
			is_internal_queue
		);

		return;
	}

	std::shared_ptr<BTreeNode> left_node = node_to_insert;
	std::shared_ptr<BTreeNode> right_node = std::shared_ptr<BTreeNode>(new BTreeNode(PageType::LEAF));

	left_node.get()->page_offset = segment->get_last_index_page_offset(true);
	left_node.get()->parent_offset = parent_node.get()->page_offset;

	right_node.get()->page_offset = segment->get_last_index_page_offset(true);
	right_node.get()->parent_offset = parent_node.get()->page_offset;
	right_node.get()->insert(row_to_insert);

	auto key_pos_to_insert = BTreeNodeRow{ left_node.get()->max_key, left_node.get()->page_offset };

	bool parent_node_is_not_full = parent_node.get()->insert(key_pos_to_insert);

	this->disk_flusher->flush_data_to_disk(
		segment->get_segment_key(),
		segment->get_segment_path(),
		std::get<1>(segment->get_metadata_bytes()).get(),
		SEGMENT_METADATA_TOTAL_BYTES,
		0,
		is_internal_queue
	);
}