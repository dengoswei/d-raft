#include <memory>
#include <cassert>
#include "gtest/gtest.h"
#include "raft_mem.h"
#include "raft.pb.h"
#include "raft_config.h"
#include "mem_utils.h"


namespace {

void add_fake_nodes(raft::ClusterConfig& config, uint32_t svr_id)
{
    auto node = config.add_nodes();
    assert(nullptr != node);
    node->set_svr_id(svr_id);
    node->set_svr_ip(svr_id);
    node->set_svr_port(svr_id);
    config.set_max_id(std::max(config.max_id(), svr_id));
}

}


TEST(RaftConfigTest, Construct)
{
    raft::RaftMem raft(1, 1, 10, 2);
    
    raft::ClusterConfig config;
    {
        config.set_index(1);
        add_fake_nodes(config, 1);
        add_fake_nodes(config, 2);
        add_fake_nodes(config, 3);
    }

    auto hs = cutils::make_unique<raft::HardState>();
    assert(nullptr != hs);
    {
        auto meta = hs->mutable_meta();
        assert(nullptr != meta);
        meta->set_term(1);
        meta->set_commit(1);
        meta->set_min_index(1);
        meta->set_max_index(1);

        auto entry = hs->add_entries();
        assert(nullptr != entry);
        entry->set_type(raft::EntryType::EntryConfChange);
        entry->set_term(1);
        entry->set_index(1);
        entry->set_reqid(0);
        std::string raw_data;
        assert(config.SerializeToString(&raw_data));
        assert(false == raw_data.empty());
        entry->set_data(std::move(raw_data));
    }

    assert(0 == raft.Init(config, std::move(hs)));
    assert(1 == raft.GetMaxIndex());
    assert(1 == raft.GetMinIndex());
    assert(1 == raft.GetCommit());
}
