#pragma once
#include <memory>
#include <tuple>
#include "../../../Enums.h"
#include "../../../Constants.h"
#include "../../../util/Helper.h"

typedef struct {
	unsigned long long key;
	unsigned int val_pos;
} BTreeNodeRow;

class BTreeNode {
private:
	unsigned int page_offset;
	PageType type;
	unsigned long long min_key;
	unsigned long long max_key;
	unsigned int rows_num;

	BTreeNodeRow rows[INDEX_PAGE_TOTAL_ROWS];

public:
	BTreeNode(PageType type);

	BTreeNode(void* metadata);

	std::tuple<std::shared_ptr<char>, unsigned int> get_page_bytes();

	friend class BPlusTreeIndexHandler;
};