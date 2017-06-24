#include "gtest/gtest.h"
#include "raft_mem.h"
#include "raft.pb.h"
#include "hassert.h"
#include "mem_utils.h"
#include "test_helper.h"
#include "tconfig_helper.h"




TEST(ElectionTest, FollowerStepTimeout)
{
    auto raft_mem = build_raft_mem(1, 3, 1, 1);
    assert(nullptr != raft_mem);

    auto& raft = *raft_mem;
    assert(raft::RaftRole::FOLLOWER == raft.GetRole());
    assert(false == raft.HasTimeout());
    assert(1 == raft.GetTerm());
    assert(1 == raft.GetMinIndex());
    assert(1 == raft.GetMaxIndex());

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    raft::MessageType rsp_msg_type = raft::MessageType::MsgNull;

    // case 1: no timeout
    {
        std::tie(hard_state, 
                soft_state, 
                mark_broadcast, rsp_msg_type) = raft.CheckTimeout(false);
        assert(nullptr == hard_state);
        assert(nullptr == soft_state);
        assert(false == mark_broadcast);
        assert(raft::MessageType::MsgNull == rsp_msg_type);
    }

    // case 2: timeout
    {
        std::tie(hard_state, 
                soft_state, 
                mark_broadcast, rsp_msg_type) = raft.CheckTimeout(true);
        assert(nullptr != hard_state);
        assert(nullptr != soft_state);
        assert(true == mark_broadcast);
        assert(raft::MessageType::MsgVote == rsp_msg_type);

        assert(hard_state->has_meta());
        assert(raft.GetTerm() + 1 == hard_state->meta().term());
        assert(hard_state->meta().has_vote());
        assert(false == hard_state->meta().has_commit());
        assert(0 == hard_state->meta().vote());
        assert(0 == hard_state->entries_size());

        assert(raft::RaftRole::CANDIDATE == 
                static_cast<raft::RaftRole>(soft_state->role()));
        assert(0 == soft_state->leader_id());
        
        raft.ApplyState(std::move(hard_state), std::move(soft_state));

        auto rsp_msg = raft.BuildBroadcastRspMsg(rsp_msg_type);
        assert(nullptr != rsp_msg);
        assert(rsp_msg_type == rsp_msg->type());
        assert(rsp_msg->logid() == raft.GetLogId());
        assert(0 == rsp_msg->to());
        assert(rsp_msg->from() == raft.GetSelfId());
        assert(raft.GetTerm() == rsp_msg->term());
        assert(0 == rsp_msg->entries_size());

        assert(raft::RaftRole::CANDIDATE == raft.GetRole());
    }
}

