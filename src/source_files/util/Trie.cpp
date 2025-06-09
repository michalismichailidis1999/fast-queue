#include "../../header_files/util/Trie.h"

Trie::Trie(TriePageType type, unsigned int page_offset) {
	this->type = type;
	this->rows_num = type == TriePageType::ROOT ? 26 : 0;
	this->page_offset = page_offset;
	this->next_page = 0;
}

Trie::Trie(void* page_data) {
	// TODO: Complete logic here
}

void Trie::set_next_page(unsigned int next_page) {
	this->next_page = next_page;
}

unsigned int Trie::get_next_page() {
	return this->next_page;
}

void Trie::add_next_trie(std::shared_ptr<Trie> trie) {
	this->next_tries.emplace_back(trie);
}

unsigned int Trie::add_word(const std::string& word) {
	unsigned int remaining_characters = word.size();

	Trie* trie = this;

	unsigned int next_index = 0;

	while (trie != NULL) {
		trie = this->next_tries[next_index++].get();
	}

	return remaining_characters;
}

bool Trie::contains_word(const std::string& word) {
	if (word.size() == 0) return false;

	if (!(this->rows[this->char_to_index(word[0])].flags & TRIE_NODE_HAS_VALUE_FLAG)) return false;

	Trie* trie = this;

	unsigned int next_index = 0;

	while (trie != NULL) {
		trie = this->next_tries[next_index++].get();
	}

	return false;
}

int Trie::char_to_index(char c) {
	if (c >= 'a' && c <= 'z') return c - 'a';

	if (c >= 'A' && c <= 'Z') return c - 'A';

	if (c >= '0' && c <= '9') return 26 + (c - '0');

	return -1;
}