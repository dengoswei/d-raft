#pragma once


namespace raft {

class Replicate {


public:

    Replicate();

    bool UpdateReplicateState(
            uint32_t follower_id, 
            bool accepted, 
            uint64_t next_log_index);

    uint64_t GetCommit() const;

    uint64_t GetAcceptedIndex(uint32_t follower_id) const;

    uint64_t GetRejectedIndex(uint32_t follower_id) const;

}; // class Replicate

} // namespace raft

