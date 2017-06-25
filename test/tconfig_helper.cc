#include <string>
#include <algorithm>
#include <cassert>
#include "tconfig_helper.h"
#include "raft_mem.h"
#include "mem_utils.h"


namespace {

void add_fake_nodes(
        raft::ClusterConfig& config, uint32_t svr_id)
{
    assert(svr_id > config.max_id());
    auto node = config.add_nodes();
    assert(nullptr != node);
    node->set_svr_id(svr_id);
    node->set_svr_ip(svr_id);
    node->set_svr_port(svr_id);
    config.set_max_id(std::max(config.max_id(), svr_id));
}


} // namespace .. 

raft::RaftOption default_option()
{
    raft::RaftOption option;
    option.election_tick = 10;
    option.hb_tick = 3;
    return option;
}


raft::ClusterConfig build_fake_config(size_t node_cnt)
{
    assert(size_t{3} <= node_cnt);
    raft::ClusterConfig config;
    config.set_index(1);
    
    for (size_t id = 0; id < node_cnt; ++id) {
        add_fake_nodes(config, id + 1);
    }

    return config;
}

void add_config(
        raft::HardState& hard_state, 
        uint64_t term, 
        const raft::ClusterConfig& config)
{
    auto entry = hard_state.add_entries();
    assert(nullptr != entry);
    entry->set_type(raft::EntryType::EntryConfChange);
    entry->set_term(term);
    entry->set_index(config.index());
    entry->set_reqid(0);
    std::string raw_data;
    assert(config.SerializeToString(&raw_data));
    assert(false == raw_data.empty());
    entry->set_data(std::move(raw_data));
}






