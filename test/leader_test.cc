#include "gtest/gtest.h"
#include "raft_mem.h"
#include "raft.pb.h"
#include "mem_utils.h"
#include "test_helper.h"


TEST(LeaderTest, SimpleConstruct)
{
    auto raft_mem = build_raft_mem(1, 1, raft::RaftRole::LEADER);
    assert(nullptr != raft_mem);
    assert(raft::RaftRole::LEADER == raft_mem->GetRole());
}

TEST(LeaderTest, IgnoreMsg)
{
    auto raft_mem = build_raft_mem(3, 1, raft::RaftRole::LEADER);
    assert(nullptr != raft_mem);

    auto null_msg = build_from_msg(
            *raft_mem, raft::MessageType::MsgNull, 2);
    assert(nullptr != null_msg);

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;

    std::tie(hard_state, 
            soft_state, mark_broadcast, rsp_msg_type) = 
        raft_mem->Step(*null_msg, nullptr, nullptr);
    assert(nullptr == hard_state);
    assert(nullptr == soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgNull == rsp_msg_type);
}

TEST(LeaderTest, InvalidTerm)
{
    auto raft_mem = build_raft_mem(3, 1, raft::RaftRole::LEADER);
    assert(nullptr != raft_mem);

    auto null_msg = build_from_msg(
            *raft_mem, raft::MessageType::MsgNull, 2);
    assert(nullptr != null_msg);

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;

    // case 1
    {
        // revert to follower
        null_msg->set_term(raft_mem->GetTerm() + 1);
        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type) = 
            raft_mem->Step(*null_msg, nullptr, nullptr);
        assert(nullptr != hard_state);
        assert(nullptr != soft_state);
        assert(false == mark_broadcast);
        assert(raft::MessageType::MsgNull == rsp_msg_type);
        assert(null_msg->term() == hard_state->term());
        assert(raft::RaftRole::FOLLOWER == 
                static_cast<raft::RaftRole>(soft_state->role()));
        assert(false == soft_state->has_leader_id());
    }

    // case 2
    {
        null_msg->set_term(raft_mem->GetTerm() - 1);
        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type) = 
            raft_mem->Step(*null_msg, nullptr, nullptr);
        assert(nullptr == hard_state);
        assert(nullptr == soft_state);
        assert(false == mark_broadcast);
        assert(raft::MessageType::MsgHeartbeat == rsp_msg_type);

        auto rsp_msg = raft_mem->BuildRspMsg(
                *null_msg, nullptr, nullptr, mark_broadcast, rsp_msg_type);
        assert(nullptr != rsp_msg);
        assert(rsp_msg_type == rsp_msg->type());
        assert(raft_mem->GetTerm() == rsp_msg->term());
        assert(raft_mem->GetMaxIndex() + 1 == rsp_msg->index());
        assert(rsp_msg->has_log_term());
        assert(rsp_msg->has_commit_index());
        assert(rsp_msg->has_commit_term());
        assert(raft_mem->GetCommit() == rsp_msg->commit_index());
    }
}

