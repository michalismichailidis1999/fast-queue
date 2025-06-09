#pragma once
#include <cstdint>
#include <vector>
#include <memory>
#include "../Enums.h"
#include "../Constants.h"

typedef struct {
	char val;
	int next; // if -1 then has no next
	uint8_t flags; // (starting from rights to left) bit 0: if 1 has value, bit 1: if 1 is word, bit 2: if 1 next value applies to next page
} TrieNode;

class Trie {
private:
	unsigned int page_offset;
	unsigned int rows_num;
	unsigned int next_page;

	TriePageType type;

	TrieNode rows[TRIE_PAGE_TOTAL_ROWS];

	std::vector<std::shared_ptr<Trie>> next_tries;

	int char_to_index(char c);
public:
	Trie(TriePageType type, unsigned int page_offset = 0);

	Trie(void* page_data);

	void set_next_page(unsigned int next_page);
	unsigned int get_next_page();

	void add_next_trie(std::shared_ptr<Trie> trie);

	unsigned int add_word(const std::string& word);

	bool contains_word(const std::string& word);
};