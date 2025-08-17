#include "../../../../header_files/queue_management/messages_management/index_management/BTreeNode.h"

BTreeNode::BTreeNode(PageType type) {
	this->page_offset = 0;
	this->type = type;
	this->min_key = 0;
	this->max_key = 0;
	this->rows_num = 0;
	this->parent_offset = -1;
	this->prev_page_offset = -1;
	this->next_page_offset = -1;
}

BTreeNode::BTreeNode(void* metadata) {
	memcpy_s(&this->page_offset, INDEX_PAGE_OFFSET_SIZE, (char*)metadata + INDEX_PAGE_OFFSET_OFFSET, INDEX_PAGE_OFFSET_SIZE);
	memcpy_s(&this->type, INDEX_PAGE_TYPE_SIZE, (char*)metadata + INDEX_PAGE_TYPE_OFFSET, INDEX_PAGE_TYPE_SIZE);
	memcpy_s(&this->min_key, INDEX_PAGE_MIN_KEY_SIZE, (char*)metadata + INDEX_PAGE_MIN_KEY_OFFSET, INDEX_PAGE_MIN_KEY_SIZE);
	memcpy_s(&this->max_key, INDEX_PAGE_MAX_KEY_SIZE, (char*)metadata + INDEX_PAGE_MAX_KEY_OFFSET, INDEX_PAGE_MAX_KEY_SIZE);
	memcpy_s(&this->rows_num, INDEX_PAGE_NUM_OF_ROWS_SIZE, (char*)metadata + INDEX_PAGE_NUM_OF_ROWS_OFFSET, INDEX_PAGE_NUM_OF_ROWS_SIZE);
	memcpy_s(&this->parent_offset, INDEX_PAGE_PARENT_PAGE_SIZE, (char*)metadata + INDEX_PAGE_PARENT_PAGE_OFFSET, INDEX_PAGE_PARENT_PAGE_SIZE);
	memcpy_s(&this->prev_page_offset, INDEX_PAGE_PREV_PAGE_SIZE, (char*)metadata + INDEX_PAGE_PREV_PAGE_OFFSET, INDEX_PAGE_PREV_PAGE_SIZE);
	memcpy_s(&this->next_page_offset, INDEX_PAGE_NEXT_PAGE_SIZE, (char*)metadata + INDEX_PAGE_NEXT_PAGE_OFFSET, INDEX_PAGE_NEXT_PAGE_SIZE);

	unsigned int offset = INDEX_PAGE_METADATA_SIZE;

	for (int i = 0; i < this->rows_num; i++) {
		memcpy_s(&this->rows[i].key, INDEX_KEY_SIZE, (char*)metadata + offset + INDEX_KEY_OFFSET, INDEX_KEY_SIZE);
		memcpy_s(&this->rows[i].val_pos, INDEX_VALUE_POSITION_SIZE, (char*)metadata + offset + INDEX_VALUE_POSITION_OFFSET, INDEX_VALUE_POSITION_SIZE);
		offset += INDEX_KEY_VALUE_METADATA_SIZE;
	}

	for (int i = this->rows_num; i < INDEX_PAGE_TOTAL_ROWS; i++) {
		this->rows[i].key = 0;
		this->rows[i].val_pos = 0;
	}
}

std::tuple<std::shared_ptr<char>, unsigned int> BTreeNode::get_page_bytes() {
	std::shared_ptr<char> bytes = std::shared_ptr<char>(new char[INDEX_PAGE_SIZE]);

	memcpy_s(bytes.get() + INDEX_PAGE_OFFSET_OFFSET, INDEX_PAGE_OFFSET_SIZE, &this->page_offset, INDEX_PAGE_OFFSET_SIZE);
	memcpy_s(bytes.get() + INDEX_PAGE_TYPE_OFFSET, INDEX_PAGE_TYPE_SIZE, &this->type, INDEX_PAGE_TYPE_SIZE);
	memcpy_s(bytes.get() + INDEX_PAGE_MIN_KEY_OFFSET, INDEX_PAGE_MIN_KEY_SIZE, &this->min_key, INDEX_PAGE_MIN_KEY_SIZE);
	memcpy_s(bytes.get() + INDEX_PAGE_MAX_KEY_OFFSET, INDEX_PAGE_MAX_KEY_SIZE, &this->max_key, INDEX_PAGE_MAX_KEY_SIZE);
	memcpy_s(bytes.get() + INDEX_PAGE_NUM_OF_ROWS_OFFSET, INDEX_PAGE_NUM_OF_ROWS_SIZE, &this->rows_num, INDEX_PAGE_NUM_OF_ROWS_SIZE);
	memcpy_s(bytes.get() + INDEX_PAGE_PARENT_PAGE_OFFSET, INDEX_PAGE_PARENT_PAGE_SIZE, &this->parent_offset, INDEX_PAGE_PARENT_PAGE_SIZE);
	memcpy_s(bytes.get() + INDEX_PAGE_PREV_PAGE_OFFSET, INDEX_PAGE_PREV_PAGE_SIZE, &this->prev_page_offset, INDEX_PAGE_PREV_PAGE_SIZE);
	memcpy_s(bytes.get() + INDEX_PAGE_NEXT_PAGE_OFFSET, INDEX_PAGE_NEXT_PAGE_SIZE, &this->next_page_offset, INDEX_PAGE_NEXT_PAGE_SIZE);

	unsigned int offset = INDEX_PAGE_METADATA_SIZE;

	unsigned long long zero_value = 0;

	for (int i = 0; i < INDEX_PAGE_TOTAL_ROWS; i++) {
		if (i < this->rows_num) {
			memcpy_s(bytes.get() + offset + INDEX_KEY_OFFSET, INDEX_KEY_SIZE, &this->rows[i].key, INDEX_KEY_SIZE);
			memcpy_s(bytes.get() + offset + INDEX_VALUE_POSITION_OFFSET, INDEX_VALUE_POSITION_SIZE, &this->rows[i].val_pos, INDEX_VALUE_POSITION_SIZE);
		}
		else {
			memcpy_s(bytes.get() + offset + INDEX_KEY_OFFSET, INDEX_KEY_SIZE, &zero_value, INDEX_KEY_SIZE);
			memcpy_s(bytes.get() + offset + INDEX_VALUE_POSITION_OFFSET, INDEX_VALUE_POSITION_SIZE, &zero_value, INDEX_VALUE_POSITION_SIZE);
		}
		
		offset += INDEX_KEY_VALUE_METADATA_SIZE;
	}

	Helper::add_common_metadata_values(bytes.get(), INDEX_PAGE_SIZE);

	return std::tuple<std::shared_ptr<char>, unsigned int>(bytes, INDEX_PAGE_SIZE);
}

bool BTreeNode::insert(BTreeNodeRow& row) {
	if (this->is_full()) return false;

	if (this->rows_num == 0) {
		this->min_key = row.key;
		this->max_key = row.key;
	} else if(this->min_key > row.key)
		this->min_key = row.key;
	else if(this->max_key < row.key)
		this->max_key = row.key;

	this->rows[this->rows_num++] = row;

	return true;
}

bool BTreeNode::is_full() {
	return this->rows_num == INDEX_PAGE_TOTAL_ROWS;
}

PageType BTreeNode::get_page_type() {
	return this->type;
}

long long BTreeNode::get_page_offset() {
	return this->page_offset;
}

long long BTreeNode::get_parent_offset() {
	return this->parent_offset;
}

long long BTreeNode::get_prev_page_offset() {
	return this->prev_page_offset;
}

long long BTreeNode::get_next_page_offset() {
	return this->next_page_offset;
}

BTreeNodeRow* BTreeNode::get_first_child() {
	return &this->rows[0];
}

BTreeNodeRow* BTreeNode::get_last_child() {
	return &this->rows[this->rows_num - 1];
}

BTreeNodeRow* BTreeNode::get_nth_child(unsigned int index) {
	if (index >= this->rows_num) return NULL;

	return &this->rows[index];
}

unsigned int BTreeNode::get_total_rows_count() {
	return this->rows_num;
}

void BTreeNode::remove_from_key_and_after(unsigned long long key) {
	unsigned int end_pos = 0;

	unsigned long long new_max = 0;
	unsigned int new_rows_num = 0;

	for (unsigned int i = 0; i < this->rows_num; i++) {
		auto& row = this->rows[i];

		if (row.key < key) {
			end_pos = i;
			new_max = row.key;
			new_rows_num++;
		}
		else break;
	}
	
	for (unsigned int i = end_pos + 1; i < this->rows_num; i++)
	{
		this->rows[i].key = 0;
		this->rows[i].val_pos = 0;
	}

	this->min_key = new_rows_num > 0 ? this->min_key : 0;
	this->max_key = new_max;
	this->rows_num = new_rows_num;
}