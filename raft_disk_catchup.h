#pragma once 


#include <memory>
#include <functional>
#include <stdint.h>
#include "raft.pb.h"


namespace raft {


using ReadHandler = 
    std::function<std::tuple<
        int, std::unique_ptr<raft::HardState>>(uint64_t, uint64_t, int)>;


class RaftDiskCatchUp {


public:
    RaftDiskCatchUp(
            uint64_t logid, 
            uint32_t selfid, 
            // uint32_t catch_up_id, 
            uint64_t term, 
            uint64_t max_catch_up_index, 
            uint64_t min_index, 
			uint64_t disk_commit_index, 
            ReadHandler readcb);

    ~RaftDiskCatchUp();

	// err, rsp_msg, need_mem
    std::tuple<int, std::unique_ptr<raft::Message>, bool>
        Step(const raft::Message& msg);

	void Update(uint64_t term, 
			uint64_t max_catch_up_index, 
			uint64_t min_disk_index, 
			uint64_t disk_commit_index);

private:

	std::tuple<raft::MessageType, bool> step(const raft::Message& msg);

    std::tuple<int, std::unique_ptr<raft::Message>>
        buildRspMsg(
                const raft::Message& req_msg, 
                raft::MessageType rsp_msg_type);

private:
    const uint64_t logid_;
    const uint32_t selfid_;
    
    uint64_t term_;
    uint64_t max_index_;
    uint64_t min_index_;
	uint64_t disk_commit_index_;

    ReadHandler readcb_;
}; // class RaftDiskCatchUp



} // namespace raft


