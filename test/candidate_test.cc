#include "gtest/gtest.h"
#include "raft_mem.h"
#include "raft.pb.h"
#include "mem_utils.h"
#include "test_helper.h"

namespace {


} // namespace


TEST(CandidateTest, SimpleConstruct)
{
    auto raft_mem = build_raft_mem(1, 1, raft::RaftRole::CANDIDATE);
    assert(nullptr != raft_mem);
    assert(raft::RaftRole::CANDIDATE == raft_mem->GetRole());

    assert(0 == raft_mem->GetVoteCount());
}

TEST(CandidateTest, IgnoreMsg)
{
    auto raft_mem = build_raft_mem(3, 1, raft::RaftRole::CANDIDATE);
    assert(nullptr != raft_mem);

    auto null_msg = build_from_msg(
            *raft_mem, raft::MessageType::MsgNull, 2);
    assert(nullptr != null_msg);

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;
    bool need_disk_replicate = false;

    std::tie(hard_state, 
            soft_state, mark_broadcast, rsp_msg_type, 
            need_disk_replicate) = 
        raft_mem->Step(*null_msg, nullptr, nullptr);
    assert(nullptr == hard_state);
    assert(nullptr == soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgNull == rsp_msg_type);
    assert(false == need_disk_replicate);
}

TEST(CandidateTest, InvalidTerm)
{
    auto raft_mem = build_raft_mem(3, 1, raft::RaftRole::CANDIDATE);
    assert(nullptr != raft_mem);

    auto null_msg = build_from_msg(
            *raft_mem, raft::MessageType::MsgNull, 2);
    assert(nullptr != null_msg);
    
    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;
    bool need_disk_replicate = false;
    // case 1:
    {
        // revert to follower
        null_msg->set_term(raft_mem->GetTerm() + 1);
        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type, 
                need_disk_replicate) = 
            raft_mem->Step(*null_msg, nullptr, nullptr);
        assert(nullptr != hard_state);
        assert(nullptr != soft_state);
        assert(false == mark_broadcast);
        assert(raft::MessageType::MsgNull == rsp_msg_type);
        assert(false == need_disk_replicate);

        assert(hard_state->has_meta());
        assert(hard_state->meta().has_term());
        assert(null_msg->term() == hard_state->meta().term());
        assert(raft::RaftRole::FOLLOWER == 
                static_cast<raft::RaftRole>(soft_state->role()));
        assert(false == soft_state->has_leader_id());
    }

    // case 2:
    {
        // rsp with MsgVote
        null_msg->set_term(raft_mem->GetTerm() - 1);
        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type, 
                need_disk_replicate) = 
            raft_mem->Step(*null_msg, nullptr, nullptr);
        assert(nullptr == hard_state);
        assert(nullptr == soft_state);
        assert(false == mark_broadcast);
        assert(raft::MessageType::MsgVote == rsp_msg_type);
        assert(false == need_disk_replicate);

        auto rsp_msg = raft_mem->BuildRspMsg(
                *null_msg, nullptr, nullptr, mark_broadcast, rsp_msg_type);
        assert(nullptr != rsp_msg);
        assert(rsp_msg_type == rsp_msg->type());
        assert(raft_mem->GetTerm() == rsp_msg->term());
        assert(raft_mem->GetMaxIndex() + 1 == rsp_msg->index());
        assert(rsp_msg->has_log_term());
    }
}

TEST(CandidateTest, RepeateBroadcastVote)
{
    auto raft_mem = build_raft_mem(3, 1, raft::RaftRole::CANDIDATE);
    assert(nullptr != raft_mem);

    assert(0 == raft_mem->GetVoteCount());

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;

    uint64_t term = raft_mem->GetTerm();
    for (int testime = 0; testime < 10; ++testime) {
        assert(term == raft_mem->GetTerm());
        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type) = 
            raft_mem->CheckTimeout(true);
        assert(nullptr == hard_state);
        assert(nullptr == soft_state);
        assert(true == mark_broadcast);
        assert(raft::MessageType::MsgVote == rsp_msg_type);

        raft::Message fake_msg;
        fake_msg.set_type(raft::MessageType::MsgNull);
        fake_msg.set_logid(raft_mem->GetLogId());
        fake_msg.set_to(raft_mem->GetSelfId());
        fake_msg.set_term(raft_mem->GetTerm());
        auto rsp_msg = raft_mem->BuildRspMsg(
                fake_msg, nullptr, nullptr, mark_broadcast, rsp_msg_type);
        assert(nullptr != rsp_msg);
        assert(rsp_msg_type == rsp_msg->type());
        assert(raft_mem->GetTerm() == rsp_msg->term());
        assert(raft_mem->GetMaxIndex() + 1 == rsp_msg->index());
        assert(rsp_msg->has_log_term());

        raft_mem->ApplyState(nullptr, nullptr);
        assert(0 == raft_mem->GetVoteCount());
    }
}

TEST(CandidateTest, BecomeLeader)
{
    auto raft_mem = build_raft_mem(3, 1, raft::RaftRole::CANDIDATE);
    assert(nullptr != raft_mem);

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;
    bool need_disk_replicate = false;

    auto vote_rsp_msg = build_from_msg(
            *raft_mem, raft::MessageType::MsgVoteResp, 2);
    assert(nullptr != vote_rsp_msg);
    vote_rsp_msg->set_reject(false);

    std::tie(hard_state, 
            soft_state, mark_broadcast, rsp_msg_type, 
            need_disk_replicate) = 
        raft_mem->Step(*vote_rsp_msg, nullptr, nullptr);
    assert(nullptr != hard_state);
    assert(nullptr != soft_state);
    assert(true == mark_broadcast);
    assert(raft::MessageType::MsgHeartbeat == rsp_msg_type);
    assert(false == need_disk_replicate);

    assert(hard_state->has_meta());
    assert(hard_state->meta().has_vote());
    assert(hard_state->meta().has_term());
    assert(raft_mem->GetSelfId() == hard_state->meta().vote());
    assert(0 == hard_state->entries_size());

    assert(raft::RaftRole::LEADER == 
            static_cast<raft::RaftRole>(soft_state->role()));
    assert(soft_state->has_leader_id());
    assert(raft_mem->GetSelfId() == soft_state->leader_id());

    auto rsp_msg = raft_mem->BuildRspMsg(
            *vote_rsp_msg, hard_state, soft_state, 
            mark_broadcast, rsp_msg_type);
    assert(nullptr != rsp_msg);
    assert(rsp_msg_type == rsp_msg->type());
    assert(0 == rsp_msg->to());
    assert(raft_mem->GetMaxIndex() + 1 == rsp_msg->index());
    assert(rsp_msg->has_log_term());
    assert(rsp_msg->has_commit_index());
    assert(rsp_msg->has_commit_term());
    assert(raft_mem->GetCommit() == rsp_msg->commit_index());


    raft_mem->ApplyState(std::move(hard_state), std::move(soft_state));
    assert(raft::RaftRole::LEADER == raft_mem->GetRole());
}
