#include "gtest/gtest.h"
#include "raft_mem.h"
#include "raft.pb.h"
#include "hassert.h"
#include "mem_utils.h"

namespace {

std::unique_ptr<raft::Message> build_vote_rsp(
        raft::RaftMem& raft_mem, uint32_t follower_id, bool reject)
{
    auto vote_rsp = cutils::make_unique<raft::Message>();
    assert(nullptr != vote_rsp);
    vote_rsp->set_type(raft::MessageType::MsgVoteResp);
    vote_rsp->set_logid(raft_mem.GetLogId());
    vote_rsp->set_to(raft_mem.GetSelfId());
    vote_rsp->set_from(follower_id);
    vote_rsp->set_term(raft_mem.GetTerm());
    vote_rsp->set_reject(reject);
    return vote_rsp;
}

} // namespace 

TEST(ElectionTest, FollowerStepTimeout)
{
    raft::RaftMem raft(1, 1, 100);
    assert(raft::RaftRole::FOLLOWER == raft.GetRole());
    assert(false == raft.HasTimeout());
    assert(0 == raft.GetTerm());
    assert(0 == raft.GetMinIndex());
    assert(0 == raft.GetMaxIndex());

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    raft::MessageType rsp_msg_type = raft::MessageType::MsgNull;

    // case 1: no timeout
    {
        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type) = raft.CheckTimeout(false);
        assert(nullptr == hard_state);
        assert(nullptr == soft_state);
        assert(false == mark_broadcast);
        assert(raft::MessageType::MsgNull == rsp_msg_type);

        raft::Message fake_msg;
        fake_msg.set_logid(raft.GetLogId());
        fake_msg.set_to(raft.GetSelfId());
        fake_msg.set_from(0);
        auto rsp_msg = raft.BuildRspMsg(
                fake_msg, hard_state, soft_state, mark_broadcast, rsp_msg_type);
        assert(nullptr == rsp_msg);
    }

    // case 2: timeout
    {
        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type) = raft.CheckTimeout(true);
        assert(nullptr != hard_state);
        assert(nullptr != soft_state);
        assert(true == mark_broadcast);
        assert(raft::MessageType::MsgVote == rsp_msg_type);

        assert(raft.GetTerm() + 1 == hard_state->term());
        assert(0 == hard_state->vote());
        assert(0 == hard_state->entries_size());
        assert(0 == hard_state->commit());

        assert(raft::RaftRole::CANDIDATE == 
                static_cast<raft::RaftRole>(soft_state->role()));
        assert(0 == soft_state->leader_id());

        raft::Message fake_msg;
        fake_msg.set_logid(raft.GetLogId());
        fake_msg.set_to(raft.GetSelfId());
        fake_msg.set_from(0);
        auto rsp_msg = raft.BuildRspMsg(
                fake_msg, hard_state, soft_state, mark_broadcast, rsp_msg_type);
        assert(nullptr != rsp_msg);

        assert(rsp_msg->type() == rsp_msg_type);
        assert(rsp_msg->logid() == raft.GetLogId());
        assert(0 == rsp_msg->to());
        assert(raft.GetSelfId() == rsp_msg->from());
        assert(hard_state->term() == rsp_msg->term());
        assert(rsp_msg->has_index());
        assert(1 == rsp_msg->index());
        assert(rsp_msg->has_log_term());
        assert(0 == rsp_msg->log_term());
        assert(0 == rsp_msg->entries_size());

        raft.ApplyState(std::move(hard_state), std::move(soft_state));
        assert(nullptr == hard_state);
        assert(nullptr == soft_state);
        assert(uint64_t{1} == raft.GetTerm());
        assert(raft::RaftRole::CANDIDATE == raft.GetRole());
    }
}

TEST(ElectionTest, ZeroSuccElection)
{
    raft::RaftMem raft(1, 1, 100);

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    raft::MessageType rsp_msg_type = raft::MessageType::MsgNull;

    // step 1
    {
        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type) = raft.CheckTimeout(true);
        raft.ApplyState(std::move(hard_state), std::move(soft_state));
    }

    assert(0 < raft.GetTerm());
    assert(raft::RaftRole::CANDIDATE == raft.GetRole());
    // step 2
    {
        raft::Message vote_rsp;
        vote_rsp.set_type(raft::MessageType::MsgVoteResp);
        vote_rsp.set_logid(raft.GetLogId());
        vote_rsp.set_to(raft.GetSelfId());
        vote_rsp.set_from(2);
        vote_rsp.set_term(raft.GetTerm());
        vote_rsp.set_reject(false);

        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type) = raft.Step(vote_rsp, nullptr, nullptr);
        assert(nullptr != hard_state);
        assert(nullptr != soft_state);
        assert(true == mark_broadcast);
        assert(raft::MessageType::MsgHeartbeat == rsp_msg_type);

        assert(raft.GetTerm() == hard_state->term());
        assert(raft.GetSelfId() == hard_state->vote());
        assert(0 == hard_state->entries_size());
        assert(0 == hard_state->commit());

        assert(raft::RaftRole::LEADER == 
                static_cast<raft::RaftRole>(soft_state->role()));
        assert(raft.GetSelfId() == soft_state->leader_id());

        auto rsp_msg = raft.BuildRspMsg(
                vote_rsp, hard_state, soft_state, mark_broadcast, rsp_msg_type);
        assert(nullptr != rsp_msg);

        assert(rsp_msg->type() == rsp_msg_type);
        assert(rsp_msg->logid() == raft.GetLogId());
        assert(0 == rsp_msg->to());
        assert(raft.GetSelfId() == rsp_msg->from());
        assert(raft.GetTerm() == rsp_msg->term());
        assert(rsp_msg->has_index());
        assert(1 == rsp_msg->index());
        assert(rsp_msg->has_log_term());
        assert(0 == rsp_msg->log_term());
        assert(0 == rsp_msg->entries_size());

        assert(rsp_msg->has_commit_index());
        assert(rsp_msg->has_commit_term());
        assert(0 == rsp_msg->commit_index());
        assert(0 == rsp_msg->commit_term());

        raft.ApplyState(std::move(hard_state), std::move(soft_state));
        assert(nullptr == hard_state);
        assert(nullptr == soft_state);
        assert(uint64_t{1} == raft.GetTerm());
        assert(raft::RaftRole::LEADER == raft.GetRole());
        assert(raft.GetSelfId() == raft.GetVote(raft.GetTerm()));
        assert(raft.GetSelfId() == raft.GetLeaderId(raft.GetTerm()));
    }
}

TEST(ElectionTest, OneRejectElection)
{
    raft::RaftMem raft_mem(1, 1, 100);

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    raft::MessageType rsp_msg_type = raft::MessageType::MsgNull;

    // 1. timeout
    {
        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type) = raft_mem.CheckTimeout(true);
        raft_mem.ApplyState(std::move(hard_state), std::move(soft_state));
    }

    // 2.
    {
        auto vote_rsp = build_vote_rsp(raft_mem, 2, true);
        assert(nullptr != vote_rsp);

        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type) = 
            raft_mem.Step(*vote_rsp, nullptr, nullptr);
        assert(nullptr == hard_state);
        assert(nullptr == soft_state);
        assert(false == mark_broadcast);
        assert(raft::MessageType::MsgNull == rsp_msg_type);
        
        raft_mem.ApplyState(nullptr, nullptr);
        assert(raft::RaftRole::CANDIDATE == raft_mem.GetRole());
    }

    // 3.
    {
        auto vote_rsp = build_vote_rsp(raft_mem, 3, false);
        assert(nullptr != vote_rsp);

        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type) = 
            raft_mem.Step(*vote_rsp, nullptr, nullptr);
        assert(nullptr != hard_state);
        assert(nullptr != soft_state);
        assert(true == mark_broadcast);
        assert(raft::MessageType::MsgHeartbeat == rsp_msg_type);

        raft_mem.ApplyState(std::move(hard_state), std::move(soft_state));
        assert(raft::RaftRole::LEADER == raft_mem.GetRole());
        assert(raft_mem.GetSelfId() == raft_mem.GetVote(raft_mem.GetTerm()));
    }
}

TEST(ElectionTest, AllRejectElection)
{
    raft::RaftMem raft_mem(1, 1, 100);

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    raft::MessageType rsp_msg_type = raft::MessageType::MsgNull;

    // 1. 
    {
        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type) = raft_mem.CheckTimeout(true);
        raft_mem.ApplyState(std::move(hard_state), std::move(soft_state));
    }

    // 2. 
    {
        for (auto follower_id = 2; follower_id <= 3; ++follower_id) {
            auto vote_rsp = build_vote_rsp(raft_mem, follower_id, true);
            assert(nullptr != vote_rsp);

            std::tie(hard_state, 
                    soft_state, 
                    mark_broadcast, rsp_msg_type) = raft_mem.Step(*vote_rsp, nullptr, nullptr);
            assert(nullptr == hard_state);
            assert(nullptr == soft_state);
            assert(false == mark_broadcast);
            assert(raft::MessageType::MsgNull == rsp_msg_type);

            raft_mem.ApplyState(nullptr, nullptr);
            assert(raft::RaftRole::CANDIDATE == raft_mem.GetRole());
        }
    }
    assert(raft::RaftRole::CANDIDATE == raft_mem.GetRole());
    std::tie(hard_state, 
            soft_state, mark_broadcast , rsp_msg_type) = raft_mem.CheckTimeout(false);
    assert(nullptr == hard_state);
    assert(nullptr == soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgNull == rsp_msg_type);
    assert(uint64_t{1} == raft_mem.GetTerm());
}

TEST(ElectionTest, StepTimeoutNothing)
{
    raft::RaftMem raft_mem(1, 1, 100);

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    raft::MessageType rsp_msg_type = raft::MessageType::MsgNull;

    {
        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type) = 
            raft_mem.CheckTimeout(true);
        raft_mem.ApplyState(
                std::move(hard_state), std::move(soft_state));
    }

    {
        auto vote_rsp = build_vote_rsp(raft_mem, 2, true);
        assert(nullptr != vote_rsp);

        std::tie(hard_state, 
                soft_state, 
                mark_broadcast, rsp_msg_type) = 
            raft_mem.Step(*vote_rsp, nullptr, nullptr);
        assert(nullptr == hard_state);
        assert(nullptr == soft_state);
        assert(false == mark_broadcast);
        assert(raft::MessageType::MsgNull == rsp_msg_type);

        raft_mem.ApplyState(nullptr, nullptr);
    }

    {
        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type) = 
            raft_mem.CheckTimeout(true);
        assert(nullptr == hard_state);
        assert(nullptr == soft_state);
        assert(true == mark_broadcast);
        assert(raft::MessageType::MsgVote == rsp_msg_type);
    }
}

TEST(ElectionTest, StepTimeoutAfterAllReject)
{
    raft::RaftMem raft_mem(1, 1, 100);

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    raft::MessageType rsp_msg_type = raft::MessageType::MsgNull;

    {
        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type) = 
            raft_mem.CheckTimeout(true);
        raft_mem.ApplyState(
                std::move(hard_state), std::move(soft_state));
        assert(0 == raft_mem.GetVoteCount());
    }

    {
        for (uint32_t follower_id = 2; follower_id <= 3; ++follower_id) {
            auto vote_rsp = build_vote_rsp(raft_mem, follower_id, true);
            assert(nullptr != vote_rsp);

            std::tie(hard_state, 
                    soft_state, 
                    mark_broadcast, rsp_msg_type) = 
                raft_mem.Step(*vote_rsp, nullptr, nullptr);
            assert(nullptr == hard_state);
            assert(nullptr == soft_state);
            assert(false == mark_broadcast);
            assert(raft::MessageType::MsgNull == rsp_msg_type);

            raft_mem.ApplyState(nullptr, nullptr);
        }
        assert(2 == raft_mem.GetVoteCount());
    }

    {
        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type) = 
            raft_mem.CheckTimeout(true);
        assert(nullptr != hard_state);
        assert(nullptr == soft_state);
        assert(true == mark_broadcast);
        assert(raft::MessageType::MsgVote == rsp_msg_type);
        assert(uint64_t{2} == hard_state->term());
        assert(0 == hard_state->vote());

        assert(0 == raft_mem.GetVoteCount());

        raft_mem.ApplyState(
                std::move(hard_state), nullptr);
        assert(uint64_t{2} == raft_mem.GetTerm());
    }
}

