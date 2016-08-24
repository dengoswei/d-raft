#include <cassert>
#include "replicate.h"


namespace raft {


Replicate::Replicate()
{

}


Replicate::~Replicate() = default;


bool Replicate::UpdateReplicateState(
        uint32_t follower_id, bool accepted, uint64_t next_log_index)
{
    assert(0 < follower_id);
    assert(0 < next_log_index);
    if (accepted) {
        return updateAcceptedMap(follower_id, next_log_index);
    }

    return updateRejectedMap(follower_id, next_log_index);
}

bool Replicate::updateAcceptedMap(
        uint32_t follower_id, uint64_t next_log_index)
{
    assert(0 < follower_id);
    assert(0 < next_log_index);
    uint64_t new_accepted = next_log_index - 1;
    assert(0 <= new_accepted);
    uint64_t prev_accepted = GetAcceptedIndex(follower_id);
    if (new_accepted <= prev_accepted) {
        return false; // update nothing 
    }

    assert(new_accepted > prev_accepted);
    accepted_map_[follower_id] = new_accepted;
    return true;
}

bool Replicate::updateRejectedMap(
        uint32_t follower_id, uint64_t next_log_index)
{
    assert(0 < follower_id);
    assert(0 < next_log_index);
    uint64_t new_rejected = next_log_index - 1;
    assert(0 < new_rejected);
    uint64_t prev_rejected = GetRejectedIndex(follower_id);

    if (0 != prev_rejected && new_rejected >= prev_rejected) {
        // update nothing
        return false; 
    }

    assert(0 == prev_rejected || new_rejected < prev_rejected);
    if (new_rejected == prev_rejected) {
        return false; // update nothing
    }
    
    assert(new_rejected < prev_rejected);
    rejected_map_[follower_id] = new_rejected;
    return true;
}

std::map<uint64_t, int>
Replicate::GetAcceptedDistributionOn(
        const std::set<uint32_t>& follower_set) const
{
    std::map<uint64_t, int> accepted_distribution;
    for (auto follower_id : follower_set) {
        auto accepted_index = GetAcceptedIndex(follower_id);
        if (0 == accepted_index) {
            continue;
        }
        
        assert(0 < accepted_index);
        ++accepted_distribution[accepted_index];
    }
    
    return accepted_distribution;
}

uint64_t Replicate::GetAcceptedIndex(uint32_t follower_id) const
{
    if (accepted_map_.end() == accepted_map_.find(follower_id)) {
        return 0;
    }

    return accepted_map_.at(follower_id);
}

uint64_t Replicate::GetRejectedIndex(uint32_t follower_id) const
{
    if (rejected_map_.end() == rejected_map_.find(follower_id)) {
        return 0;
    }

    uint64_t accepted_index = GetAcceptedIndex(follower_id);
    return accepted_index >= 
        rejected_map_.at(follower_id) ? 0 : rejected_map_.at(follower_id);
}

void Replicate::Reset(uint64_t commit_index)
{
    rejected_map_.clear();
    for (auto& id_idx_pair : accepted_map_) {
        if (id_idx_pair.second > commit_index) {
            id_idx_pair.second = commit_index;
        }
    }
}

} // namespace raft


