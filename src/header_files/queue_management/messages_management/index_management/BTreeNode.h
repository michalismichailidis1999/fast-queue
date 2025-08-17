#pragma once
#include <memory>
#include <tuple>
#include "../../../Enums.h"
#include "../../../Constants.h"
#include "../../../util/Helper.h"

#include "../../../__linux/memcpy_s.h"

typedef struct {
	unsigned long long key;
	long long val_pos;
} BTreeNodeRow;

class BTreeNode {
private:
	long long page_offset;
	PageType type;
	unsigned long long min_key;
	unsigned long long max_key;
	unsigned int rows_num;

	// will only be used by leaf nodes
	long long parent_offset;

	// will only be used in same page types
	long long prev_page_offset;
	long long next_page_offset;

	BTreeNodeRow rows[INDEX_PAGE_TOTAL_ROWS];
public:
	BTreeNode(PageType type);

	BTreeNode(void* metadata);

	// returns true if node is not full and row inserted
	bool insert(BTreeNodeRow& row);

	std::tuple<std::shared_ptr<char>, unsigned int> get_page_bytes();

	bool is_full();

	PageType get_page_type();

	long long get_page_offset();
	long long get_parent_offset();
	long long get_prev_page_offset();
	long long get_next_page_offset();

	BTreeNodeRow* get_first_child();
	BTreeNodeRow* get_last_child();
	BTreeNodeRow* get_nth_child(unsigned int index);

	unsigned int get_total_rows_count();

	void remove_from_key_and_after(unsigned long long key);

	friend class BPlusTreeIndexHandler;
};