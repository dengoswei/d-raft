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

TEST(ElectionTest, ZeroSuccElection)
{
    auto raft_mem = build_raft_mem(1, 3, 1, 1);
    assert(nullptr != raft_mem);

    auto& raft = *raft_mem;

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    raft::MessageType rsp_msg_type = raft::MessageType::MsgNull;
    bool need_disk_replicate = false;

    // step 1
    {
        std::tie(hard_state, 
                soft_state, 
				mark_broadcast, rsp_msg_type) = raft.CheckTimeout(true);
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

        std::tie(mark_broadcast, rsp_msg_type, need_disk_replicate)
            = raft.Step(vote_rsp, hard_state, soft_state);
        assert(nullptr != hard_state);
        assert(nullptr != soft_state);
        assert(true == mark_broadcast);
        assert(raft::MessageType::MsgApp == rsp_msg_type);
        assert(false == need_disk_replicate);

        assert(hard_state->has_meta());
        assert(raft.GetTerm() == hard_state->meta().term());
        assert(raft.GetSelfId() == hard_state->meta().vote());
        assert(0 == hard_state->entries_size());
        assert(false == hard_state->meta().has_commit());

        assert(raft::RaftRole::LEADER == 
                static_cast<raft::RaftRole>(soft_state->role()));
        assert(raft.GetSelfId() == soft_state->leader_id());

        raft.ApplyState(std::move(hard_state), std::move(soft_state));
        auto rsp_msg = raft.BuildBroadcastRspMsg(rsp_msg_type);
        assert(nullptr != rsp_msg);
        assert(2 == rsp_msg->nodes_size());
        assert(rsp_msg->type() == rsp_msg_type);
        assert(rsp_msg->logid() == raft.GetLogId());
        assert(0 == rsp_msg->to());
        assert(raft.GetSelfId() == rsp_msg->from());
        assert(raft.GetTerm() == rsp_msg->term());
        assert(0 < rsp_msg->index());
        assert(0 < rsp_msg->log_term());
        assert(0 < rsp_msg->commit_index());
        assert(0 < rsp_msg->commit_term());

        assert(raft::RaftRole::LEADER == raft.GetRole());
        assert(raft.GetSelfId() == raft.GetVote(raft.GetTerm()));
        assert(raft.GetSelfId() == raft.GetLeaderId(raft.GetTerm()));
    }
}

TEST(ElectionTest, OneRejectElection)
{
    auto raft_mem = build_raft_mem(1, 3, 1, 1);
    assert(nullptr != raft_mem);

    auto& raft = *raft_mem;

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    raft::MessageType rsp_msg_type = raft::MessageType::MsgNull;
    bool need_disk_replicate = false;

    // 1. timeout
    {
        std::tie(hard_state, 
                soft_state, 
                mark_broadcast, 
                rsp_msg_type) = raft.CheckTimeout(true);
        assert(nullptr != hard_state);
        raft.ApplyState(std::move(hard_state), std::move(soft_state));
    }

    // 2. reject ?
    {
        auto votersp_msg = build_votersp_msg(raft, 2);
        votersp_msg.set_reject(true);

        std::tie(mark_broadcast, rsp_msg_type, need_disk_replicate)
            = raft.Step(votersp_msg, hard_state, soft_state);
        assert(nullptr == hard_state);
        assert(nullptr == soft_state);
        assert(false == mark_broadcast);
        assert(raft::MessageType::MsgNull == rsp_msg_type);
        assert(false == need_disk_replicate);
        assert(raft::RaftRole::CANDIDATE == raft.GetRole());
    }

    // 3.
    {
        auto votersp_msg = build_votersp_msg(raft, 3);
        votersp_msg.set_reject(false);

        std::tie(mark_broadcast, rsp_msg_type, need_disk_replicate)
            = raft.Step(votersp_msg, hard_state, soft_state);
        assert(nullptr != hard_state);
        assert(nullptr != soft_state);
        assert(true == mark_broadcast);
        assert(raft::MessageType::MsgApp == rsp_msg_type);
        assert(false == need_disk_replicate);

        raft.ApplyState(std::move(hard_state), std::move(soft_state));
        assert(raft::RaftRole::LEADER == raft.GetRole());
        assert(raft.GetSelfId() == raft.GetVote(raft.GetTerm()));
    }
}

TEST(ElectionTest, AllRejectElection)
{
    auto raft_mem = build_raft_mem(1, 3, 1, 1);
    assert(nullptr != raft_mem);

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    raft::MessageType rsp_msg_type = raft::MessageType::MsgNull;
    bool need_disk_replicate = false;

    // 1. 
    {
        std::tie(hard_state, 
                soft_state, 
                mark_broadcast, 
                rsp_msg_type) = raft_mem->CheckTimeout(true);
        raft_mem->ApplyState(
                std::move(hard_state), std::move(soft_state));
    }

    // 2. 
    {
        for (auto follower_id = 2; follower_id <= 3; ++follower_id) {
            auto vote_rsp = build_votersp_msg(*raft_mem, follower_id);
            vote_rsp.set_reject(true);

            std::tie(mark_broadcast, rsp_msg_type, need_disk_replicate)
                = raft_mem->Step(vote_rsp, hard_state, soft_state);
            assert(nullptr == hard_state);
            assert(nullptr == soft_state);
            assert(false == mark_broadcast);
            assert(raft::MessageType::MsgNull == rsp_msg_type);
            assert(false == need_disk_replicate);

            assert(raft::RaftRole::CANDIDATE == raft_mem->GetRole());
        }
    }

    assert(raft::RaftRole::CANDIDATE == raft_mem->GetRole());
    std::tie(hard_state, 
            soft_state, 
            mark_broadcast, 
            rsp_msg_type) = raft_mem->CheckTimeout(false);
    assert(nullptr == hard_state);
    assert(nullptr == soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgNull == rsp_msg_type);
    assert(uint64_t{2} == raft_mem->GetTerm());
}

TEST(ElectionTest, StepTimeoutNothing)
{
    auto raft_mem = build_raft_mem(1, 3, 1, 1);
    assert(nullptr != raft_mem);

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    raft::MessageType rsp_msg_type = raft::MessageType::MsgNull;
    bool need_disk_replicate = false;

    {
        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type) = 
            raft_mem->CheckTimeout(true);
        assert(nullptr != hard_state);
        assert(nullptr != soft_state);
        raft_mem->ApplyState(
                std::move(hard_state), std::move(soft_state));
    }

    {
        auto votersp_msg = build_votersp_msg(*raft_mem, 2);
        votersp_msg.set_reject(true);

        std::tie(mark_broadcast, rsp_msg_type, need_disk_replicate)
            = raft_mem->Step(votersp_msg, hard_state, soft_state);
        assert(nullptr == hard_state);
        assert(nullptr == soft_state);
        assert(false == mark_broadcast);
        assert(raft::MessageType::MsgNull == rsp_msg_type);
    }

    {
        std::tie(hard_state, 
                soft_state, 
                mark_broadcast, rsp_msg_type) = 
            raft_mem->CheckTimeout(true);
        assert(nullptr == hard_state);
        assert(nullptr == soft_state);
        assert(true == mark_broadcast);
        assert(raft::MessageType::MsgVote == rsp_msg_type);
    }
}

TEST(ElectionTest, StepTimeoutAfterAllReject)
{
    auto raft_mem = build_raft_mem(1, 3, 1, 1);
    assert(nullptr != raft_mem);

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    raft::MessageType rsp_msg_type = raft::MessageType::MsgNull;
    bool need_disk_replicate = false;

    {
        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type) = 
            raft_mem->CheckTimeout(true);
        assert(nullptr != hard_state);
        raft_mem->ApplyState(
                std::move(hard_state), std::move(soft_state));
        assert(0 == raft_mem->GetVoteCount());
    }

    {
        for (uint32_t follower_id = 2; follower_id <= 3; ++follower_id) {
            auto votersp_msg = build_votersp_msg(*raft_mem, follower_id);
            votersp_msg.set_reject(true);

            std::tie(mark_broadcast, rsp_msg_type, need_disk_replicate)
                = raft_mem->Step(votersp_msg, hard_state, soft_state);
            assert(nullptr == hard_state);
            assert(nullptr == soft_state);
            assert(false == mark_broadcast);
            assert(raft::MessageType::MsgNull == rsp_msg_type);
        }
        assert(2 == raft_mem->GetVoteCount());
    }

    {
        std::tie(hard_state, 
                soft_state, 
                mark_broadcast, rsp_msg_type) = 
            raft_mem->CheckTimeout(true);
        assert(nullptr != hard_state);
        assert(nullptr == soft_state);
        assert(true == mark_broadcast);
        assert(raft::MessageType::MsgVote == rsp_msg_type);
        assert(hard_state->has_meta());
        assert(uint64_t{3} == hard_state->meta().term());
        assert(hard_state->meta().has_vote());
        assert(0 == hard_state->meta().vote());

        assert(0 == raft_mem->GetVoteCount());
        raft_mem->ApplyState(
                std::move(hard_state), nullptr);
        assert(uint64_t{3} == raft_mem->GetTerm());
    }
}


TEST(ElectionTest, Random3Election)
{
    auto map_raft = build_raft_mem(3, 1, 1);
    assert(size_t{3} == map_raft.size());

    std::vector<uint32_t> nodes = {1, 2, 3};
    for (int testtime = 0; testtime < 30; ++testtime) {
        std::random_shuffle(nodes.begin(), nodes.end());
        
        auto& candidate = map_raft.at(nodes[0]);
        if (raft::RaftRole::LEADER == candidate->GetRole()) {
            continue;
        }

        std::unique_ptr<raft::HardState> hard_state;
        std::unique_ptr<raft::SoftState> soft_state;
        bool mark_broadcast = false;
        auto rsp_msg_type = raft::MessageType::MsgNull;
        bool need_disk_replicate = false;

        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type)
            = candidate->CheckTimeout(true);
        assert(nullptr != hard_state);
        candidate->ApplyState(
                std::move(hard_state), std::move(soft_state));
        assert(true == mark_broadcast);
        assert(raft::MessageType::MsgVote == rsp_msg_type);

        auto vote_msg = candidate->BuildBroadcastRspMsg(rsp_msg_type);
        assert(nullptr != vote_msg);

        vote_msg->set_to(nodes[1]);

        std::tie(mark_broadcast, rsp_msg_type, need_disk_replicate)
            = map_raft.at(nodes[1])->Step(
                    *vote_msg, hard_state, soft_state);
        assert(false == mark_broadcast);
        assert(raft::MessageType::MsgVoteResp == rsp_msg_type);
        assert(false == need_disk_replicate);

        map_raft.at(nodes[1])->ApplyState(
                std::move(hard_state), std::move(soft_state));

        auto votersp_msg 
            = map_raft.at(nodes[1])->BuildRspMsg(*vote_msg, rsp_msg_type);
        assert(nullptr != votersp_msg);
        assert(nodes[0] == votersp_msg->to());

        std::tie(mark_broadcast, rsp_msg_type, need_disk_replicate)
            = candidate->Step(*votersp_msg, hard_state, soft_state);
        assert(nullptr != hard_state);
        assert(nullptr != soft_state);
        candidate->ApplyState(
                std::move(hard_state), std::move(soft_state));
        assert(raft::RaftRole::LEADER == candidate->GetRole());
        assert(true == mark_broadcast);
        assert(raft::MessageType::MsgApp == rsp_msg_type);

        auto app_msg = candidate->BuildBroadcastRspMsg(rsp_msg_type);
        assert(nullptr != app_msg);
        assert(0 == app_msg->to());
        for (int idx = 0; idx < app_msg->nodes_size(); ++idx) {
            uint32_t svr_id = app_msg->nodes(idx).svr_id();
            assert(nodes[0] != svr_id);
            app_msg->set_to(svr_id);

            std::tie(mark_broadcast, rsp_msg_type, need_disk_replicate)
                = map_raft.at(svr_id)->Step(
                        *app_msg, hard_state, soft_state);
            assert(raft::MessageType::MsgAppResp == rsp_msg_type);
            map_raft.at(svr_id)->ApplyState(
                    std::move(hard_state), std::move(soft_state));
        }
    }
}






