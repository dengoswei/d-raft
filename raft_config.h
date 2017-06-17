#pragma once

#include <stdint.h>
#include <memory>
#include <set>
#include "raft.pb.h"



namespace raft {

class ConfState;


// IMPORTANT: ONLY ONE OPERATOR !!
//
// If server adopted Cnew only when they learned that Cnew
// was commited, Raft leaders would have a difficult time knowing
// when a majority of the old cluster had adopted it.
class RaftConfig {

public:
    RaftConfig();

    ~RaftConfig();

    void Apply(
            const raft::SoftState& soft_state, 
            uint64_t commited_index);

    const raft::ClusterConfig* GetConfig() const;

    const raft::ClusterConfig* GetCommitConfig() const;
    const raft::ClusterConfig* GetPendingConfig() const;

private:
    std::unique_ptr<raft::ClusterConfig> config_;
    std::unique_ptr<raft::ClusterConfig> pending_config_;
}; // class RaftConfig


bool is_valid(const raft::ClusterConfig& config);

std::tuple<bool, raft::Node> 
get(const raft::ClusterConfig& config, uint32_t peer);

} // namespace raft


