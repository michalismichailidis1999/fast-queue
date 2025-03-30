#pragma once
#include <tuple>

class Consumer {
public:
	Consumer();

	std::tuple<int, char*> consume();
};