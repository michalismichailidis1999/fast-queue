#pragma once
#include <memory>
#include "Enums.h"

class DataCompressor {
private:
	CompressionAlgorithm algorithm;
public:
	DataCompressor(CompressionAlgorithm algorithm = CompressionAlgorithm::LZ4);

	std::shared_ptr<unsigned char> compress_data(void* data);
};