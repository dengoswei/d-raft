#include <memory>
#include <cassert>
#include "gtest/gtest.h"
#include "raft_mem.h"
#include "raft.pb.h"
#include "raft_config.h"
#include "mem_utils.h"
#include "tconfig_helper.h"
#include "test_helper.h"


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

TEST(RaftConfigTest, Construct3)
{
    raft::RaftMem raft(1, 4, default_option());
    
    raft::ClusterConfig config = build_fake_config(3);

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

TEST(RaftConfigTest, AddConfig)
{
    auto raft_mem = build_raft_mem(1, 3, 1, 1);
    assert(nullptr != raft_mem);
    
    make_fake_leader(*raft_mem);

    raft::Node new_node;
    new_node.set_svr_id(4);
    new_node.set_svr_ip(4);
    new_node.set_svr_port(4);

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    auto ret = raft_mem->AddConfig(hard_state, soft_state, new_node);
    assert(0 == ret);
    assert(nullptr != hard_state);
    assert(1 == hard_state->entries_size());
    assert(raft_mem->GetMaxIndex() + 1 == hard_state->entries(0).index());
    assert(nullptr != soft_state);
    assert(0 < soft_state->configs_size());
    raft_mem->ApplyState(std::move(hard_state), std::move(soft_state));
    assert(nullptr != raft_mem->GetConfig());
    assert(nullptr != raft_mem->GetPendingConfig());
    assert(raft_mem->IsMember(4));
}

TEST(RaftConfigTest, DelConfig)
{
    auto raft_mem = build_raft_mem(1, 3, 1, 1);
    assert(nullptr != raft_mem);

    make_fake_leader(*raft_mem);

    raft::Node del_node;
    del_node.set_svr_id(2);
    del_node.set_svr_ip(2);
    del_node.set_svr_port(2);

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    auto ret = raft_mem->DelConfig(hard_state, soft_state, del_node);
    assert(0 == ret);
    assert(nullptr != hard_state);
    assert(1 == hard_state->entries_size());
    assert(nullptr != soft_state);
    assert(0 < soft_state->configs_size());
    raft_mem->ApplyState(std::move(hard_state), std::move(soft_state));
    assert(nullptr != raft_mem->GetConfig());
    assert(nullptr != raft_mem->GetPendingConfig());
    assert(false == raft_mem->IsMember(2));
}
