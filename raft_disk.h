#pragma once

#include <memory>
#include <functional>
#include <mutex>
#include <stdint.h>
#include "raft.pb.h"

namespace raft {

enum class RaftRole : uint32_t;

class Replicate;


using ReadHandler = 
    std::function<std::tuple<
        int, std::unique_ptr<raft::HardState>>(uint64_t, uint64_t, int)>;


// catch-up use only!
class RaftDisk {

public:
    RaftDisk(
            uint64_t logid, 
            uint32_t selfid, 
            ReadHandler readcb);

    ~RaftDisk();

    bool Update(
            raft::RaftRole new_role, 
            uint64_t new_term, 
            uint64_t new_commit_index, 
            uint64_t new_min_index, uint64_t new_max_index);

    std::tuple<int, std::unique_ptr<raft::Message>> 
        Step(
            uint32_t follower_id, 
            uint64_t next_log_index, 
            bool reject, raft::MessageType rsp_msg_type);

public:
    
    raft::RaftRole GetRole();
    uint64_t GetTerm();
    uint64_t GetCommit();
    uint64_t GetMinIndex();
    uint64_t GetMaxIndex();

private:

    uint64_t getMinIndex() const {
        return min_index_;
    }

    uint64_t getMaxIndex() const {
        return max_index_;
    }

private:
    bool updateTerm(uint64_t new_term);
    bool updateRole(raft::RaftRole new_role);
    bool updateCommit(uint64_t new_commit_index);
    bool updateMinIndex(uint64_t new_min_index);
    bool updateMaxIndex(uint64_t new_max_index);

    std::tuple<int, std::unique_ptr<raft::Message>>
        stepHearbeatMsg(uint32_t follower_id);

    std::tuple<int, std::unique_ptr<raft::Message>>
        stepAppMsg(uint32_t follower_id);

private:
    const uint64_t logid_ = 0ull;
    const uint32_t selfid_ = 0u;
    
    ReadHandler readcb_;

    std::mutex mutex_;

    raft::RaftRole role_;
    uint64_t term_ = 0;
    uint64_t commit_ = 0;
    uint64_t min_index_ = 0;
    uint64_t max_index_ = 0;

    std::unique_ptr<raft::Replicate> replicate_;
    std::set<uint32_t> replicate_follower_set_;
}; // class RaftDisk;


} // namespace raft



