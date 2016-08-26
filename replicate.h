#pragma once

#include <map>
#include <set>
#include <stdint.h>


namespace raft {

class Replicate {

public:

    Replicate();

    ~Replicate();

    bool UpdateReplicateState(
            uint32_t follower_id, 
            bool accepted, 
            uint64_t next_log_index);

    std::map<uint64_t, int> 
        GetAcceptedDistributionOn(
                const std::set<uint32_t>& follower_set) const;

    uint64_t GetAcceptedIndex(uint32_t follower_id) const;

    uint64_t GetRejectedIndex(uint32_t follower_id) const;

    void Reset(uint64_t commit_index);

    uint64_t NextExploreIndex(
            uint32_t follower_id, 
            uint64_t min_index, uint64_t max_index) const;

private:
    bool updateAcceptedMap(uint32_t follower_id, uint64_t next_log_index);

    bool updateRejectedMap(uint32_t follower_id, uint64_t next_log_index);

private:

    std::map<uint32_t, uint64_t> accepted_map_;
    std::map<uint32_t, uint64_t> rejected_map_;
}; // class Replicate

} // namespace raft

