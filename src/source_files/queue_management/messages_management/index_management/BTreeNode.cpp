#include "../../../../header_files/queue_management/messages_management/index_management/BTreeNode.h"

BTreeNode::BTreeNode(PageType type) {
	this->page_offset = 0;
	this->type = type;
	this->min_key = 0;
	this->max_key = 0;
	this->rows_num = 0;
}

BTreeNode::BTreeNode(void* metadata) {
	memcpy_s(&this->page_offset, INDEX_PAGE_OFFSET_SIZE, (char*)metadata + INDEX_PAGE_OFFSET_OFFSET, INDEX_PAGE_OFFSET_SIZE);
	memcpy_s(&this->type, INDEX_PAGE_TYPE_SIZE, (char*)metadata + INDEX_PAGE_TYPE_OFFSET, INDEX_PAGE_TYPE_SIZE);
	memcpy_s(&this->min_key, INDEX_PAGE_MIN_KEY_SIZE, (char*)metadata + INDEX_PAGE_MIN_KEY_OFFSET, INDEX_PAGE_MIN_KEY_SIZE);
	memcpy_s(&this->max_key, INDEX_PAGE_MAX_KEY_SIZE, (char*)metadata + INDEX_PAGE_MAX_KEY_OFFSET, INDEX_PAGE_MAX_KEY_SIZE);
	memcpy_s(&this->rows_num, INDEX_PAGE_NUM_OF_ROWS_SIZE, (char*)metadata + INDEX_PAGE_NUM_OF_ROWS_OFFSET, INDEX_PAGE_NUM_OF_ROWS_SIZE);

	unsigned int offset = INDEX_PAGE_METADATA_SIZE;

	for (int i = 0; i < this->rows_num; i++) {
		memcpy_s(&this->rows[i].key, INDEX_KEY_SIZE, (char*)metadata + offset + INDEX_KEY_OFFSET, INDEX_KEY_SIZE);
		memcpy_s(&this->rows[i].val_pos, INDEX_VALUE_POSITION_SIZE, (char*)metadata + offset + INDEX_VALUE_POSITION_OFFSET, INDEX_VALUE_POSITION_SIZE);
		offset += INDEX_KEY_VALUE_METADATA_SIZE;
	}
}

std::tuple<std::shared_ptr<char>, unsigned int> BTreeNode::get_page_bytes() {
	std::shared_ptr<char> bytes = std::shared_ptr<char>(new char[INDEX_PAGE_SIZE]);

	memcpy_s(bytes.get() + INDEX_PAGE_OFFSET_OFFSET, INDEX_PAGE_OFFSET_SIZE, &this->page_offset, INDEX_PAGE_OFFSET_SIZE);
	memcpy_s(bytes.get() + INDEX_PAGE_TYPE_OFFSET, INDEX_PAGE_TYPE_SIZE, &this->type, INDEX_PAGE_TYPE_SIZE);
	memcpy_s(bytes.get() + INDEX_PAGE_MIN_KEY_OFFSET, INDEX_PAGE_MIN_KEY_SIZE, &this->min_key, INDEX_PAGE_MIN_KEY_SIZE);
	memcpy_s(bytes.get() + INDEX_PAGE_MAX_KEY_OFFSET, INDEX_PAGE_MAX_KEY_SIZE, &this->max_key, INDEX_PAGE_MAX_KEY_SIZE);
	memcpy_s(bytes.get() + INDEX_PAGE_NUM_OF_ROWS_OFFSET, INDEX_PAGE_NUM_OF_ROWS_SIZE, &this->rows_num, INDEX_PAGE_NUM_OF_ROWS_SIZE);

	unsigned int offset = INDEX_PAGE_METADATA_SIZE;

	for (int i = 0; i < INDEX_PAGE_TOTAL_ROWS; i++) {
		memcpy_s(bytes.get() + offset + INDEX_KEY_OFFSET, INDEX_KEY_SIZE, &this->rows[i].key, INDEX_KEY_SIZE);
		memcpy_s(bytes.get() + offset + INDEX_VALUE_POSITION_OFFSET, INDEX_VALUE_POSITION_SIZE, &this->rows[i].val_pos, INDEX_VALUE_POSITION_SIZE);
		offset += INDEX_KEY_VALUE_METADATA_SIZE;
	}

	Helper::add_common_metadata_values(bytes.get(), INDEX_PAGE_SIZE, ObjectType::BTREE_PAGE);

	return std::tuple<std::shared_ptr<char>, unsigned int>(bytes, INDEX_PAGE_SIZE);
}

bool BTreeNode::insert(BTreeNodeRow& row) {
	if (this->rows_num == INDEX_PAGE_TOTAL_ROWS) return false;

	if (this->rows_num == 0) this->min_key = row.key;

	this->rows[this->rows_num++] = row;

	this->max_key = row.key;

	return true;
}