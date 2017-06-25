#pragma once 

#include <stdint.h>
#include <memory>
#include <map>
#include "raft.pb.h"


namespace raft {

    struct RaftOption;
    class RaftMem;
} // namespace raft


raft::RaftOption default_option();

raft::ClusterConfig build_fake_config(size_t node_cnt);

void add_config(
        raft::HardState& hard_state, 
        uint64_t term, 
        const raft::ClusterConfig& config);


