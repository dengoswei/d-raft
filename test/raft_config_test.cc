#include <memory>
#include <cassert>
#include "gtest/gtest.h"
#include "raft_mem.h"
#include "raft.pb.h"
#include "raft_config.h"
#include "mem_utils.h"
#include "tconfig_helper.h"


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
    raft::RaftMem raft(1, 1, default_option());
    
    raft::ClusterConfig config = build_fake_config(3);;
       
    auto hs = cutils::make_unique<raft::HardState>();
    assert(nullptr != hs);
    {
        auto meta = hs->mutable_meta();
        assert(nullptr != meta);
        meta->set_term(1);
        meta->set_commit(1);
        meta->set_min_index(1);
        meta->set_max_index(1);

        add_config(*hs, meta->term(), config);
    }

    assert(0 == raft.Init(config, std::move(hs)));
    assert(1 == raft.GetMaxIndex());
    assert(1 == raft.GetMinIndex());
    assert(1 == raft.GetCommit());
}

TEST(RaftConfigTest, Construct2)
{
    std::map<uint32_t, std::unique_ptr<raft::RaftMem>> map_raft;
    for (uint32_t svr_id = 1; svr_id <= 3; ++svr_id) {
        auto raft_mem = build_raft_mem(svr_id, 3, 1, 2);
        assert(nullptr != raft_mem);

        map_raft[svr_id] = std::move(raft_mem);
    }
}
