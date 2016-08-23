#pragma once


namespace raft {

class Replicate {


public:
    bool UpdateReplicateState(
            uint32_t follower_id, 
            bool accepted, 
            uint64_t next_log_index);

    uint64_t GetCommit() const;

}; // class Replicate

} // namespace raft

