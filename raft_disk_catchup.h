#pragma once 


#include <memory>
#include <functional>
#include <stdint.h>
#include "raft.pb.h"
#include "replicate.h"


namespace raft {


using ReadHandler = 
    std::function<std::tuple<
        int, std::unique_ptr<raft::HardState>>(uint64_t, uint64_t, int)>;


class RaftDiskCatchUp {


public:
    RaftDiskCatchUp(
            uint64_t logid, 
            uint32_t selfid, 
            uint32_t catch_up_id, 
            uint64_t term, 
            uint64_t max_catch_up_index, 
            uint64_t min_index, 
            ReadHandler readcb);

    ~RaftDiskCatchUp();

    std::tuple<int, std::unique_ptr<raft::Message>>
        Step(const raft::Message& msg);

private:

    raft::MessageType step(const raft::Message& msg);

    std::tuple<int, std::unique_ptr<raft::Message>>
        buildRspMsg(
                const raft::Message& req_msg, 
                raft::MessageType rsp_msg_type);

private:
    const uint64_t logid_;
    const uint32_t selfid_;
    const uint32_t catch_up_id_;
    
    const uint64_t term_;
    const uint64_t max_index_;
    const uint64_t min_index_;

    ReadHandler readcb_;
    raft::Replicate replicate_;
}; // class RaftDiskCatchUp



} // namespace raft


