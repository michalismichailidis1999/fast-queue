#include "../../../../header_files/queue_management/messages_management/index_management/BPlusTreeIndexHandler.h"

BPlusTreeIndexHandler::BPlusTreeIndexHandler(DiskFlusher* disk_flusher, DiskReader* disk_reader) {
	this->disk_flusher = disk_flusher;
	this->disk_reader = disk_reader;
}

unsigned long long BPlusTreeIndexHandler::find_message_location(Partition* partition, PartitionSegment* segment, unsigned long long read_from_message_id, bool remove_everything_after_match) {
	std::unique_ptr<char> node_data = std::unique_ptr<char>(new char[INDEX_PAGE_SIZE]);
	std::shared_ptr<BTreeNode> node = nullptr;
	std::shared_ptr<BTreeNode> prev_node = nullptr;
	std::shared_ptr<BTreeNode> parent_node = nullptr;

	unsigned int page_offset = 0;
	unsigned int iter = 0;

	while (true) {
		this->read_index_page_from_disk(partition, segment, node_data.get(), page_offset);

		prev_node = node;
		node = std::shared_ptr<BTreeNode>(new BTreeNode(node_data.get()));

		if (node.get()->get_page_type() == PageType::NON_LEAF && node.get()->rows_num == 0)
			throw CorruptionException("Corrupted index page detected");

		if (node.get()->next_page_offset == -1) break;

		if (node.get()->max_key >= read_from_message_id && node.get()->min_key <= read_from_message_id) break;

		page_offset = node.get()->next_page_offset;
		iter++;
	}

	if (node.get()->get_page_type() == PageType::LEAF && node.get()->rows_num == 0 && iter > 0) node = prev_node;

	if (node.get()->get_page_type() == PageType::LEAF && node.get()->rows_num == 0) {
		if (prev_node == nullptr) return SEGMENT_METADATA_TOTAL_BYTES;
		if (remove_everything_after_match) this->clear_index_values(partition, segment, NULL, node.get(), read_from_message_id);
		return prev_node.get()->get_last_child()->val_pos;
	}

	if (prev_node != nullptr && prev_node.get()->max_key < read_from_message_id && read_from_message_id < node.get()->min_key)
		node = prev_node;

	if (node.get()->type == PageType::NON_LEAF) {
		parent_node = node;
		page_offset = this->find_message_location(node.get(), read_from_message_id);
		this->read_index_page_from_disk(partition, segment, node_data.get(), page_offset);
		node = std::shared_ptr<BTreeNode>(new BTreeNode(node_data.get()));
	}

	if (remove_everything_after_match)
		this->clear_index_values(partition, segment, parent_node != nullptr ? parent_node.get() : NULL, node.get(), read_from_message_id);

	return this->find_message_location(node.get(), read_from_message_id);
}

void BPlusTreeIndexHandler::add_message_to_index(Partition* partition, unsigned long long message_id, long long message_pos, bool cache_pages) {
	PartitionSegment* segment = partition->get_active_segment();

	bool is_internal_queue = Helper::is_internal_queue(partition->get_queue_name());

	auto node_to_insert_tup = this->find_node_to_insert(partition, segment);

	std::shared_ptr<BTreeNode> node_to_insert = std::get<0>(node_to_insert_tup);
	std::shared_ptr<BTreeNode> parent_node = std::get<1>(node_to_insert_tup);

	if (node_to_insert == nullptr)
		throw std::runtime_error("Could not find index node to insert");

	auto row_to_insert = BTreeNodeRow{ message_id, message_pos };

	std::vector<BTreeNode*> nodes_to_flush;

	if (node_to_insert.get()->insert(row_to_insert)) {
		if (parent_node != nullptr) {
			parent_node.get()->max_key = row_to_insert.key;
			parent_node.get()->get_last_child()->key = row_to_insert.key;
			nodes_to_flush.emplace_back(parent_node.get());
		}

		nodes_to_flush.emplace_back(node_to_insert.get());

		this->flush_nodes_to_disk(partition, segment, &nodes_to_flush, cache_pages);

		return;
	}

	std::shared_ptr<BTreeNode> new_node = nullptr;

	if(parent_node != nullptr)
		nodes_to_flush.emplace_back(parent_node.get());
	else {
		parent_node = this->add_new_parent_to_node(segment, node_to_insert.get());
		auto row = BTreeNodeRow{ node_to_insert.get()->max_key, node_to_insert.get()->page_offset };
		parent_node.get()->insert(row);
		nodes_to_flush.emplace_back(parent_node.get());
		nodes_to_flush.emplace_back(node_to_insert.get());
	}

	if (parent_node.get()->is_full()) {
		new_node = this->create_new_node_pointer(segment, parent_node.get());
		new_node.get()->insert(row_to_insert);
	}
	else {
		new_node = this->create_new_child(segment, parent_node.get(), node_to_insert.get());
		new_node.get()->insert(row_to_insert);

		auto row = BTreeNodeRow{ new_node.get()->max_key, new_node.get()->page_offset };
		parent_node.get()->insert(row);

		if(nodes_to_flush.size() == 1)
			nodes_to_flush.emplace_back(node_to_insert.get());
	}
	
	nodes_to_flush.emplace_back(new_node.get());

	this->flush_nodes_to_disk(partition, segment, &nodes_to_flush, cache_pages);
}

std::tuple<std::shared_ptr<BTreeNode>, std::shared_ptr<BTreeNode>> BPlusTreeIndexHandler::find_node_to_insert(Partition* partition, PartitionSegment* segment) {
	std::unique_ptr<char> node_data = std::unique_ptr<char>(new char[INDEX_PAGE_SIZE]);

	std::shared_ptr<BTreeNode> parent_node = nullptr;
	std::shared_ptr<BTreeNode> node_to_insert = nullptr;

	unsigned long long page_offset = 0;

	while (true) {
		this->read_index_page_from_disk(partition, segment, node_data.get(), page_offset);

		std::shared_ptr<BTreeNode> node_to_insert = std::shared_ptr<BTreeNode>(new BTreeNode(node_data.get()));

		if (node_to_insert.get()->get_page_type() == PageType::NON_LEAF && node_to_insert.get()->rows_num == 0)
			throw CorruptionException("Corrupted index page detected");

		if (node_to_insert.get()->get_page_type() == PageType::LEAF)
			return std::tuple<std::shared_ptr<BTreeNode>, std::shared_ptr<BTreeNode>>(node_to_insert, parent_node);

		page_offset = node_to_insert.get()->get_next_page_offset() >= 0 
			? node_to_insert.get()->get_next_page_offset() 
			: node_to_insert.get()->get_last_child()->val_pos;

		parent_node = node_to_insert;
	}

	return std::tuple<std::shared_ptr<BTreeNode>, std::shared_ptr<BTreeNode>>(nullptr, nullptr);
}

std::shared_ptr<BTreeNode> BPlusTreeIndexHandler::add_new_parent_to_node(PartitionSegment* segment, BTreeNode* node) {
	std::shared_ptr<BTreeNode> parent_node = std::shared_ptr<BTreeNode>(new BTreeNode(PageType::NON_LEAF));
	parent_node.get()->prev_page_offset = node->prev_page_offset;
	parent_node.get()->page_offset = node->page_offset;

	node->page_offset = segment->get_last_index_page_offset(true);
	node->parent_offset = parent_node.get()->page_offset;
	node->prev_page_offset = -1;

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

	new_node.get()->prev_page_offset = current_parent->page_offset;
	new_node.get()->page_offset = segment->get_last_index_page_offset(true);

	current_parent->next_page_offset = new_node.get()->page_offset;

	return new_node;
}

void BPlusTreeIndexHandler::flush_nodes_to_disk(Partition* partition, PartitionSegment* segment, std::vector<BTreeNode*>* nodes, bool cache_pages) {
	for (auto iter = nodes->rbegin(); iter != nodes->rend(); iter++)
		this->flush_node_to_disk(partition, segment, *iter, cache_pages);
}

void BPlusTreeIndexHandler::flush_node_to_disk(Partition* partition, PartitionSegment* segment, BTreeNode* node, bool cache_page) {
	CacheKeyInfo cache_key_info = {
		partition->get_queue_name(),
		partition->get_partition_id(),
		segment->get_id(),
		node->get_page_offset()
	};

	this->disk_flusher->write_data_to_specific_file_location(
		segment->get_index_key(),
		segment->get_index_path(),
		std::get<0>(node->get_page_bytes()).get(),
		INDEX_PAGE_SIZE,
		node->page_offset * INDEX_PAGE_SIZE,
		Helper::is_internal_queue(partition->get_queue_name()),
		false,
		cache_page ? &cache_key_info : NULL
	);
}

void BPlusTreeIndexHandler::read_index_page_from_disk(Partition* partition, PartitionSegment* segment, void* node_data, unsigned long long page_offset) {
	std::shared_ptr<char> cached_page = this->disk_reader->read_index_page_from_cache(
		partition, segment, page_offset
	);

	if (cached_page != nullptr) {
		if (!Helper::has_valid_checksum(cached_page.get()))
			throw CorruptionException("Index page was corrupted");

		memcpy_s(node_data, INDEX_PAGE_SIZE, cached_page.get(), INDEX_PAGE_SIZE);

		return;
	}
	
	unsigned long bytes_read = this->disk_reader->read_data_from_disk(
		segment->get_index_key(),
		segment->get_index_path(),
		node_data,
		INDEX_PAGE_SIZE,
		page_offset * INDEX_PAGE_SIZE
	);

	if (bytes_read != INDEX_PAGE_SIZE || !Helper::has_valid_checksum(node_data))
		throw CorruptionException("Index page was corrupted");
}

unsigned long long BPlusTreeIndexHandler::find_message_location(BTreeNode* node, unsigned long long message_id) {
	// This will only be true for first index node
	if (node->type == PageType::LEAF && message_id < node->min_key)
		return SEGMENT_METADATA_TOTAL_BYTES;

	if (message_id <= node->min_key) return node->rows[0].val_pos;
	if (message_id >= node->max_key) return node->rows[node->rows_num - 1].val_pos;

	if (node->rows_num <= 2) return node->rows[0].val_pos;

	unsigned int start_pos = 1;
	unsigned int end_pos = node->rows_num - 1;

	unsigned int pos = start_pos + (end_pos - start_pos) / 2;

	while (start_pos <= end_pos) {
		if (message_id == node->rows[pos].key) return node->rows[pos].val_pos;

		if (start_pos == pos - 1 && end_pos == pos + 1 && node->rows[start_pos].key < message_id && message_id < node->rows[pos].key)
			return pos;

		if (message_id > node->rows[pos].key) start_pos = pos + 1;
		else end_pos = pos - 1;

		if(start_pos == end_pos) return start_pos;

		if (start_pos > end_pos) break;

		pos = start_pos + (end_pos - start_pos) / 2;
	}

	return node->rows[pos].val_pos;
}

void BPlusTreeIndexHandler::clear_index_values(Partition* partition, PartitionSegment* segment, BTreeNode* parent_node, BTreeNode* node, unsigned long long message_id) {
	node->remove_from_key_and_after(message_id);

	if (node->rows_num > 0 || parent_node == NULL) {
		this->flush_node_to_disk(partition, segment, node, true);
		return;
	}

	std::shared_ptr<BTreeNode> node_ref = nullptr;
	BTreeNode* node_to_remove_rows = parent_node;

	std::unique_ptr<char> node_data = std::unique_ptr<char>(new char[INDEX_PAGE_SIZE]);

	while (node_to_remove_rows != NULL) {
		node_to_remove_rows->remove_from_key_and_after(message_id);

		if (node_to_remove_rows->rows_num > 0) {
			this->flush_node_to_disk(partition, segment, node_to_remove_rows, true);
			return;
		}
		
		if (node_to_remove_rows->prev_page_offset == -1) {
			BTreeNode new_leaf_node = BTreeNode(PageType::LEAF);
			this->flush_node_to_disk(partition, segment, &new_leaf_node, true);
			return;
		}
		
		this->read_index_page_from_disk(partition, segment, node_data.get(), node_to_remove_rows->prev_page_offset);
		node_ref = std::shared_ptr<BTreeNode>(new BTreeNode(node_data.get()));
		node_to_remove_rows = node_ref.get();
	}
}