#include <cassert>
#include "replicate.h"
#include "log_utils.h"


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
    logerr("INFO: update accepted_map_ follower_id %u new_accepted %" PRIu64, 
            follower_id, new_accepted);
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

    assert(0 == prev_rejected | new_rejected < prev_rejected);
    rejected_map_[follower_id] = new_rejected;
    logerr("INFO: update rejected_map_ follower_id %u new_rejected %" PRIu64, 
            follower_id, new_rejected);
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

void Replicate::Fix(uint64_t fix_index)
{
    std::set<uint32_t> erase_set;
    for (auto& id_idx_pair : accepted_map_) {
        if (id_idx_pair.second > fix_index) {
            id_idx_pair.second = fix_index;
        }

        if (0 == id_idx_pair.second) {
            erase_set.insert(id_idx_pair.first);
        }
    }

    for (auto id : erase_set) {
        accepted_map_.erase(id);
    }
}


// only use for hb explore
uint64_t Replicate::NextExploreIndex(
        uint32_t follower_id, 
        uint64_t min_index, uint64_t max_index) const
{
    assert(min_index <= max_index);
    if (0 == min_index) {
        assert(0 == max_index);
        return 0; // no need explore
    }

    assert(0 < min_index);
    if (min_index == max_index) {
        // no need explore
        return 0;
    }

    if (accepted_map_.end() == accepted_map_.find(follower_id)) {
        if (rejected_map_.end() == rejected_map_.find(follower_id)) {
            // not in accepted_map_ && not in rejected_map_
            return max_index + 1;
        }

        // only in rejected 
        uint64_t rejected_index = rejected_map_.at(follower_id);
        assert(0 < rejected_index);
        if (rejected_index <= min_index + 1) {
            logerr("INFO follower_id %u rejected_index %" PRIu64 " "
                    " min_index %" PRIu64, 
                    follower_id, rejected_index, min_index);
            return 0;
        }

        assert(min_index + 1 < rejected_index);
        return (min_index + rejected_index) / 2 + 1;
    }

    uint64_t accepted_index = std::max(accepted_map_.at(follower_id), min_index);
    if (rejected_map_.end() == rejected_map_.find(follower_id) || 
            rejected_map_.at(follower_id) == accepted_index + 1) {
        // no need explore..
        return 0; // no need explore ?
    }

    if (rejected_map_.at(follower_id) <= accepted_index) {
        logerr("IMPORTANT: follower_id %u rejected_index %" PRIu64 " "
                " accepted_index %" PRIu64 " accepted_map_ %" PRIu64, 
                follower_id, rejected_map_.at(follower_id), 
                accepted_index, accepted_map_.at(follower_id));
        return 0; 
    }

    // both in accepted && rejected_
    assert(rejected_map_.at(follower_id) > accepted_index + 1);
    uint64_t next_explore_index = (
            accepted_index + rejected_map_.at(follower_id)) / 2 + 1;
    assert(next_explore_index > accepted_index + 1);
    assert(next_explore_index < rejected_map_.at(follower_id) + 1);
    return next_explore_index;
}

uint64_t Replicate::NextCatchUpIndex(
        uint32_t follower_id, 
        uint64_t min_index, uint64_t max_index) const
{
    assert(min_index <= max_index);
    if (0 == min_index) {
        return 0; // no need catchup
    }

    assert(0 < min_index);
    auto next_explore_index = 
        NextExploreIndex(follower_id, min_index, max_index);
    if (0 < next_explore_index) {
        return 0; // need explore first;
    }

    // else =>
    assert(0 == next_explore_index);
    if (accepted_map_.end() == accepted_map_.find(follower_id)) {
        // no info in accepted_map_ yet;
        if (rejected_map_.end() != rejected_map_.find(follower_id) &&
                min_index >= rejected_map_.at(follower_id)) {
            if (size_t{1} == min_index) {
                return 1;
            }

            logerr("IMPORTANT CATCH-UP HUP follower_id %u min_index %" 
                    PRIu64 " rejected_index %" PRIu64, 
                    follower_id, min_index, rejected_map_.at(follower_id));
            return 0;
        }

        return min_index;  // must be the case;
    }

    assert(accepted_map_.end() != accepted_map_.find(follower_id));
    if (accepted_map_.at(follower_id) < min_index) {
        logerr("IMPORTANT CATCH-UP HUP follower_id %u min_index %" PRIu64 " "
                "accepted_index %" PRIu64, 
                follower_id, min_index, accepted_map_.at(follower_id));
        return 0;
    }

    assert(accepted_map_.at(follower_id) >= min_index);
    if (accepted_map_.at(follower_id) == max_index) {
        logerr("CATCH-UP STOP follower_id %u at max_index %" PRIu64, 
                follower_id, max_index);
        return 0; // no need catch-up
    }

    assert(accepted_map_.at(follower_id) < max_index);
    return accepted_map_.at(follower_id) + 1;
}

} // namespace raft


