#pragma once
#include <tuple>
#include <memory>
#include "../Enums.h"
#include "./Requests.h"
#include "./Responses.h"

class ClassToByteTransformer {
public:
    ClassToByteTransformer();

    // Requests
    std::tuple<long, std::shared_ptr<char>> transform(AppendEntriesRequest* obj);

    std::tuple<long, std::shared_ptr<char>> transform(RequestVoteRequest* obj);

    std::tuple<long, std::shared_ptr<char>> transform(DataNodeConnectionRequest* obj);

    std::tuple<long, std::shared_ptr<char>> transform(DataNodeHeartbeatRequest* obj);
    // ------------------

    // Responses
    std::tuple<long, std::shared_ptr<char>> transform(AppendEntriesResponse* obj);

    std::tuple<long, std::shared_ptr<char>> transform(RequestVoteResponse* obj);

    std::tuple<long, std::shared_ptr<char>> transform(DataNodeConnectionResponse* obj);

    std::tuple<long, std::shared_ptr<char>> transform(DataNodeHeartbeatResponse* obj);

    std::tuple<long, std::shared_ptr<char>> transform(CreateQueueResponse* obj);

    std::tuple<long, std::shared_ptr<char>> transform(GetControllerConnectionInfoResponse* obj);

    std::tuple<long, std::shared_ptr<char>> transform(GetLeaderIdResponse* obj);
    // ------------------
};