#include "../../../../header_files/queue_management/messages_management/index_management/BPlusTreeIndexHandler.h"

BPlusTreeIndexHandler::BPlusTreeIndexHandler(DiskFlusher* disk_flusher, DiskReader* disk_reader) {
	this->disk_flusher = disk_flusher;
	this->disk_reader = disk_reader;
}

unsigned int BPlusTreeIndexHandler::find_message_location(PartitionSegment* segment, unsigned long long read_from_message_id) {
	std::unique_ptr<char> node_data = std::unique_ptr<char>(new char[INDEX_PAGE_SIZE]);
	std::shared_ptr<BTreeNode> node = nullptr;

	unsigned int page_offset = 0;

	while (true) {
		this->read_index_page_from_disk(segment, node_data.get(), page_offset);

		node = std::shared_ptr<BTreeNode>(new BTreeNode(node_data.get()));

		if (node.get()->next_page_offset == 0) break;

		if (node.get()->max_key >= read_from_message_id && node.get()->min_key <= read_from_message_id) break;

		page_offset = node.get()->next_page_offset;
	}

	if (node.get()->type == PageType::NON_LEAF) {
		// TODO: Find leaf node where message location exists
	}

	return this->find_message_location(node.get(), read_from_message_id);
}

void BPlusTreeIndexHandler::add_message_to_index(Partition* partition, unsigned long long message_id, unsigned int message_pos) {
	PartitionSegment* segment = partition->get_active_segment();

	bool is_internal_queue = Helper::is_internal_queue(partition->get_queue_name());

	auto node_to_insert_tup = this->find_node_to_insert(segment);

	std::shared_ptr<BTreeNode> node_to_insert = std::get<0>(node_to_insert_tup);
	std::shared_ptr<BTreeNode> parent_node = std::get<1>(node_to_insert_tup);

	auto row_to_insert = BTreeNodeRow{ message_id, message_pos };

	if (!node_to_insert.get()->is_full()) {
		node_to_insert.get()->insert(row_to_insert);
		this->flush_node_to_disk(segment, node_to_insert.get());
		return;
	}

	std::shared_ptr<BTreeNode> new_node = nullptr;

	std::vector<BTreeNode*> nodes_to_flush;

	nodes_to_flush.emplace_back(parent_node.get());

	if (parent_node == nullptr) {
		parent_node = this->add_new_parent_to_node(segment, node_to_insert.get());
		parent_node.get()->insert(BTreeNodeRow{ node_to_insert.get()->max_key, node_to_insert.get()->page_offset });
		nodes_to_flush.emplace_back(node_to_insert.get());
	}

	if (parent_node.get()->is_full()) {
		new_node = this->create_new_node_pointer(segment, parent_node.get());
		new_node.get()->insert(row_to_insert);
	}
	else {
		new_node = this->create_new_child(segment, parent_node.get(), node_to_insert.get());
		new_node.get()->insert(row_to_insert);
		parent_node.get()->insert(BTreeNodeRow{ new_node.get()->max_key, new_node.get()->page_offset });

		if(nodes_to_flush.size() == 1) 
			nodes_to_flush.emplace_back(node_to_insert.get());
	}

	nodes_to_flush.emplace_back(new_node.get());

	this->flush_segment_updated_metadata(segment);

	this->flush_nodes_to_disk(segment, &nodes_to_flush);
}

std::tuple<std::shared_ptr<BTreeNode>, std::shared_ptr<BTreeNode>> BPlusTreeIndexHandler::find_node_to_insert(PartitionSegment* segment) {
	std::unique_ptr<char> node_data = std::unique_ptr<char>(new char[INDEX_PAGE_SIZE]);
	std::shared_ptr<BTreeNode> node_to_insert = nullptr;
	std::shared_ptr<BTreeNode> prev_node = nullptr;
	std::shared_ptr<BTreeNode> parent_node = nullptr;

	unsigned int page_offset = segment->get_last_index_non_leaf_offset();
	bool node_to_insert_found = false;

	while (!node_to_insert_found) {
		this->read_index_page_from_disk(segment, node_data.get(), page_offset);

		prev_node = node_to_insert;
		std::shared_ptr<BTreeNode> node_to_insert = std::shared_ptr<BTreeNode>(new BTreeNode(node_data.get()));

		if (node_to_insert.get()->type == PageType::LEAF) {
			node_to_insert_found = true;
			parent_node = prev_node != nullptr && node_to_insert.get()->parent_offset == prev_node.get()->page_offset
				? node_to_insert
				: nullptr;
			continue;
		}

		page_offset = node_to_insert.get()->rows[node_to_insert.get()->rows_num].val_pos;
	}

	return std::tuple<std::shared_ptr<BTreeNode>, std::shared_ptr<BTreeNode>>(node_to_insert, parent_node);
}

std::shared_ptr<BTreeNode> BPlusTreeIndexHandler::add_new_parent_to_node(PartitionSegment* segment, BTreeNode* node) {
	std::shared_ptr<BTreeNode> parent_node = std::shared_ptr<BTreeNode>(new BTreeNode(PageType::NON_LEAF));
	parent_node.get()->prev_page_offset = node->prev_page_offset;
	parent_node.get()->page_offset = node->page_offset;

	node->page_offset = segment->get_last_index_page_offset(true);
	node->parent_offset = parent_node.get()->page_offset;
	node->prev_page_offset = 0;

	return parent_node;
}

std::shared_ptr<BTreeNode> BPlusTreeIndexHandler::create_new_child(PartitionSegment* segment, BTreeNode* parent_node, BTreeNode* prev_node) {
	std::shared_ptr<BTreeNode> new_node = std::shared_ptr<BTreeNode>(new BTreeNode(PageType::LEAF));

	new_node.get()->parent_offset = parent_node->page_offset;
	new_node.get()->page_offset = segment->get_last_index_page_offset(true);
	new_node.get()->prev_page_offset = prev_node->page_offset;

	prev_node->next_page_offset = new_node.get()->page_offset;

	return new_node;
}

std::shared_ptr<BTreeNode> BPlusTreeIndexHandler::create_new_node_pointer(PartitionSegment* segment, BTreeNode* current_parent) {
	std::shared_ptr<BTreeNode> new_node = std::shared_ptr<BTreeNode>(new BTreeNode(PageType::LEAF));

	new_node.get()->prev_page_offset = current_parent->prev_page_offset;
	new_node.get()->page_offset = segment->get_last_index_page_offset(true);

	current_parent->next_page_offset = new_node.get()->page_offset;

	return new_node;
}

void BPlusTreeIndexHandler::flush_segment_updated_metadata(PartitionSegment* segment) {
	this->disk_flusher->flush_metadata_updates_to_disk(segment);
}

void BPlusTreeIndexHandler::flush_nodes_to_disk(PartitionSegment* segment, std::vector<BTreeNode*>* nodes) {
	for(int i = nodes->size(); i >= 0; i--)
		this->flush_node_to_disk(segment, (*nodes)[i]);
}

void BPlusTreeIndexHandler::flush_node_to_disk(PartitionSegment* segment, BTreeNode* node) {
	this->disk_flusher->write_data_to_specific_file_location(
		segment->get_index_key(),
		segment->get_index_path(),
		std::get<0>(node->get_page_bytes()).get(),
		INDEX_PAGE_SIZE,
		node->page_offset * INDEX_PAGE_SIZE
	);
}

void BPlusTreeIndexHandler::read_index_page_from_disk(PartitionSegment* segment, void* node_data, unsigned int page_offset) {
	this->disk_reader->read_data_from_disk(
		segment->get_index_key(),
		segment->get_index_path(),
		node_data,
		INDEX_PAGE_SIZE,
		page_offset
	);

	if (!Helper::has_valid_checksum(node_data))
		throw std::exception("Index page was corrupted");
}

unsigned int BPlusTreeIndexHandler::find_message_location(BTreeNode* node, unsigned long long message_id) {
	return 0;
}