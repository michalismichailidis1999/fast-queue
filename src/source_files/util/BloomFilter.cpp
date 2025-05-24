#include "../../header_files/util/BloomFilter.h"

BitArray::BitArray() {
    this->size = BLOOM_FILTER_BIT_ARRAY_COMPRESSED_BITS;
    this->bits = std::vector<uint8_t>(this->size, 0);

    std::random_device rd;
    this->seed = rd();
}

BitArray::BitArray(unsigned int size) {
    this->size = size / sizeof(uint8_t);
    this->bits = std::vector<uint8_t>(this->size, 0);

    std::random_device rd;
    this->seed = rd();
}

void BitArray::add(void* key, unsigned int key_bytes) {
    unsigned int pos = this->murmur3_32(key, key_bytes);

    unsigned int index = pos / this->size;
    uint8_t byte_pos = pos % sizeof(uint8_t);

    this->bits[index] |= byte_pos;
}

bool BitArray::has(void* key, unsigned int key_bytes) {
    unsigned int pos = this->murmur3_32(key, key_bytes);

    unsigned int index = pos / this->size;
    uint8_t byte_pos = pos % sizeof(uint8_t);

    return this->bits[index] & byte_pos;
}

unsigned int BitArray::murmur3_32(void* key, unsigned int key_bytes) {
    unsigned int h = seed;
    const unsigned int c1 = 0xcc9e2d51;
    const unsigned int c2 = 0x1b873593;

    unsigned int i = 0;
    while (i + 4 <= key_bytes) {
        unsigned int k;
        memcpy_s(&k, sizeof(unsigned int), (char*)key + i, sizeof(unsigned int));
        i += 4;

        k *= c1;
        k = (k << 15) | (k >> 17);
        k *= c2;

        h ^= k;
        h = (h << 13) | (h >> 19);
        h = h * 5 + 0xe6546b64;
    }

    unsigned int k1 = 0;
    switch (key_bytes & 3) {
    case 3: k1 ^= *((uint8_t*)((char*)key + i + 2)) << 16;
    case 2: k1 ^= *((uint8_t*)((char*)key + i + 1)) << 8;
    case 1: k1 ^= *((uint8_t*)((char*)key + i));
        k1 *= c1;
        k1 = (k1 << 15) | (k1 >> 17);
        k1 *= c2;
        h ^= k1;
    }

    h ^= key_bytes;
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;

    return h;
}

void BitArray::reset() {
    for (unsigned int i = 0; i < this->size; i++)
        this->bits[i] = 0;
}

BloomFilter::BloomFilter(unsigned int size, unsigned int hash_functions) {
    this->hash_functions = hash_functions;

	this->arrays = std::vector<BitArray>(hash_functions);

    unsigned int total_bits = size / sizeof(uint8_t) / hash_functions;

    for (unsigned int i = 0; i < hash_functions; i++)
        this->arrays[i] = BitArray(total_bits);
}

void BloomFilter::add(void* key, unsigned int key_bytes) {
    for (unsigned int i = 0; i < hash_functions; i++)
        this->arrays[i].add(key, key_bytes);
}

bool BloomFilter::has(void* key, unsigned int key_bytes) {
    unsigned int exists = 0;

    for (unsigned int i = 0; i < hash_functions; i++)
        if (this->arrays[i].has(key, key_bytes))
            exists++;

    return exists == this->hash_functions;
}

void BloomFilter::reset() {
    for (auto& arr : this->arrays)
        arr.reset();
}