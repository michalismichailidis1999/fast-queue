#pragma once
#include <memory>
#include <vector>
#include "../../../file_management/DiskFlusher.h"
#include "../../../file_management/DiskReader.h"
#include "../../Partition.h"
#include "./BTreeNode.h"
#include "../../../util/Helper.h"
#include "../../../Constants.h"
#include "../../../exceptions/CurruptionException.h"

#include "../../../__linux/memcpy_s.h"

class BPlusTreeIndexHandler {
private:
	DiskFlusher* disk_flusher;
	DiskReader* disk_reader;

	std::shared_ptr<BTreeNode> add_new_parent_to_node(PartitionSegment* segment, BTreeNode* node);

	std::shared_ptr<BTreeNode> create_new_child(PartitionSegment* segment, BTreeNode* parent_node, BTreeNode* prev_node);

	std::shared_ptr<BTreeNode> create_new_node_pointer(PartitionSegment* segment, BTreeNode* current_parrent);

	// returns both node to insert and its parent
	// tuple( node to insert, parent node )
	std::tuple<std::shared_ptr<BTreeNode>, std::shared_ptr<BTreeNode>> find_node_to_insert(Partition* partition, PartitionSegment* segment);

	void flush_nodes_to_disk(Partition* partition, PartitionSegment* segment, std::vector<BTreeNode*>* nodes, bool cache_pages);

	void flush_node_to_disk(Partition* partition, PartitionSegment* segment, BTreeNode* node, bool cache_page);

	void read_index_page_from_disk(Partition* partition, PartitionSegment* segment, void* node_data, unsigned long long page_offset);

	unsigned long long find_message_location(BTreeNode* node, unsigned long long message_id);

	void clear_index_values(Partition* partition, PartitionSegment* segment, BTreeNode* parent_node, BTreeNode* node, unsigned long long message_id);
public:
	BPlusTreeIndexHandler(DiskFlusher* disk_flusher, DiskReader* disk_reader);

	unsigned long long find_message_location(Partition* partition, PartitionSegment* segment, unsigned long long read_from_message_id, bool remove_everything_after_match = false);
	
	void add_message_to_index(Partition* partition, unsigned long long message_id, long long message_pos, bool cache_pages = true);
};