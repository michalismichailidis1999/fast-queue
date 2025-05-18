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

class BPlusTreeIndexHandler {
private:
	DiskFlusher* disk_flusher;
	DiskReader* disk_reader;

	std::shared_ptr<BTreeNode> add_new_parent_to_node(PartitionSegment* segment, BTreeNode* node);

	std::shared_ptr<BTreeNode> create_new_child(PartitionSegment* segment, BTreeNode* parent_node, BTreeNode* prev_node);

	std::shared_ptr<BTreeNode> create_new_node_pointer(PartitionSegment* segment, BTreeNode* current_parrent);

	// returns both node to insert and its parent
	// tuple( node to insert, parent node )
	std::tuple<std::shared_ptr<BTreeNode>, std::shared_ptr<BTreeNode>> find_node_to_insert(PartitionSegment* segment);

	void flush_segment_updated_metadata(PartitionSegment* segment);

	void flush_nodes_to_disk(PartitionSegment* segment, std::vector<BTreeNode*>* nodes);

	void flush_node_to_disk(PartitionSegment* segment, BTreeNode* node);

	void read_index_page_from_disk(PartitionSegment* segment, void* node_data, unsigned int page_offset);

	unsigned int find_message_location(BTreeNode* node, unsigned long long message_id);
public:
	BPlusTreeIndexHandler(DiskFlusher* disk_flusher, DiskReader* disk_reader);

	unsigned int find_message_location(PartitionSegment* segment, unsigned long long read_from_message_id);
	
	void add_message_to_index(Partition* partition, unsigned long long message_id, unsigned int message_pos);
};