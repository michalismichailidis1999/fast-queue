#include "Consumer.h"
#include <cstddef>

Consumer::Consumer() {
}

std::tuple<int, char*> Consumer::consume() {
	return std::tuple(0, nullptr);
}