#include "gtest/gtest.h"
#include "raft_mem.h"
#include "raft.pb.h"
#include "mem_utils.h"
#include "test_helper.h"

namespace {

} // namespace

TEST(FollowerTest, SimpleConstruct)
{
    raft::RaftMem raft(1, 1, 100);

    assert(raft::RaftRole::FOLLOWER == raft.GetRole());
    assert(0 == raft.GetTerm());
    assert(0 == raft.GetMinIndex());
    assert(0 == raft.GetMaxIndex());
    assert(0 == raft.GetVoteCount());
}

TEST(FollowerTest, IgnoreMsg)
{
    raft::RaftMem raft_mem(1, 1, 100);

    {
        std::unique_ptr<raft::HardState> hard_state = 
            cutils::make_unique<raft::HardState>();
        assert(nullptr != hard_state);
        hard_state->set_term(1);
        hard_state->set_vote(0);
        hard_state->set_commit(0);
        raft_mem.ApplyState(std::move(hard_state), nullptr);
        assert(uint64_t{1} == raft_mem.GetTerm());
    }

    raft::Message null_msg;
    null_msg.set_type(raft::MessageType::MsgNull);
    null_msg.set_logid(raft_mem.GetLogId());
    null_msg.set_to(raft_mem.GetSelfId());
    null_msg.set_term(1);

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    raft::MessageType rsp_msg_type = raft::MessageType::MsgNull;

    std::tie(hard_state, 
            soft_state, mark_broadcast, rsp_msg_type) = 
        raft_mem.Step(null_msg, nullptr, nullptr);
    assert(nullptr == hard_state);
    assert(nullptr == soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgNull == rsp_msg_type);
}

TEST(FollowerTest, InvalidTerm)
{
    raft::RaftMem raft_mem(1, 1, 100);

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    raft::MessageType rsp_msg_type = raft::MessageType::MsgNull;

    // case 1
    raft::Message null_msg;
    null_msg.set_type(raft::MessageType::MsgNull);
    null_msg.set_logid(raft_mem.GetLogId());
    null_msg.set_to(raft_mem.GetSelfId());
    null_msg.set_term(2);
    std::tie(hard_state, 
            soft_state, mark_broadcast, rsp_msg_type) = 
        raft_mem.Step(null_msg, nullptr, nullptr);
    assert(nullptr != hard_state);
    assert(nullptr == soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgNull == rsp_msg_type);
    assert(null_msg.term() == hard_state->term());
    raft_mem.ApplyState(std::move(hard_state), nullptr);

    // case 2
    null_msg.set_term(1);
    std::tie(hard_state, 
            soft_state, mark_broadcast, rsp_msg_type) = 
        raft_mem.Step(null_msg, nullptr, nullptr);
    assert(nullptr == hard_state);
    assert(nullptr == soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgInvalidTerm == rsp_msg_type);

    auto rsp_msg = raft_mem.BuildRspMsg(
            null_msg, hard_state, soft_state, mark_broadcast, rsp_msg_type);
    assert(nullptr != rsp_msg);
    assert(rsp_msg_type == rsp_msg->type());
    assert(rsp_msg->term() > null_msg.term());
    assert(rsp_msg->term() == raft_mem.GetTerm());
}

TEST(FollowerTest, MsgVoteYes)
{
    auto raft_mem = build_raft_mem(1, 0, raft::RaftRole::FOLLOWER);
    assert(nullptr != raft_mem);

    raft::Message vote_msg;
    vote_msg.set_type(raft::MessageType::MsgVote);
    vote_msg.set_logid(raft_mem->GetLogId());
    vote_msg.set_to(raft_mem->GetSelfId());
    vote_msg.set_from(2);
    vote_msg.set_term(raft_mem->GetTerm());
    vote_msg.set_index(raft_mem->GetCommit() + 1);
    vote_msg.set_log_term(raft_mem->GetTerm());

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    raft::MessageType rsp_msg_type = raft::MessageType::MsgNull;

    std::tie(hard_state, 
            soft_state, mark_broadcast, rsp_msg_type) = 
        raft_mem->Step(vote_msg, nullptr, nullptr);
    assert(nullptr != hard_state);
    assert(nullptr == soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgVoteResp == rsp_msg_type);
    assert(2 == hard_state->vote());
    assert(raft_mem->GetCommit() == hard_state->commit());
    assert(raft_mem->GetTerm() == hard_state->term());
    assert(0 == hard_state->entries_size());

    auto rsp_msg = raft_mem->BuildRspMsg(
            vote_msg, hard_state, soft_state, mark_broadcast, rsp_msg_type);
    assert(nullptr != rsp_msg);
    assert(rsp_msg_type == rsp_msg->type());
    assert(vote_msg.logid() == rsp_msg->logid());
    assert(vote_msg.to() == rsp_msg->from());
    assert(vote_msg.from() == rsp_msg->to());
    assert(vote_msg.term() == rsp_msg->term());
    assert(false == rsp_msg->reject());

    raft_mem->ApplyState(std::move(hard_state), std::move(soft_state));
    assert(2 == raft_mem->GetVote(raft_mem->GetTerm()));
}


TEST(FollowerTest, MsgVoteReject)
{
    auto raft_mem = build_raft_mem(1, 10, raft::RaftRole::FOLLOWER);
    assert(nullptr != raft_mem);

    auto vote_msg = build_from_msg(
            *raft_mem, raft::MessageType::MsgVote, 2);
    assert(nullptr != vote_msg);
    vote_msg->set_index(raft_mem->GetCommit());
    vote_msg->set_log_term(raft_mem->GetTerm());

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;

    // reject by rsp nothing
    std::tie(hard_state, 
            soft_state, mark_broadcast, rsp_msg_type) = 
        raft_mem->Step(*vote_msg, nullptr, nullptr);
    assert(nullptr == hard_state);
    assert(nullptr == soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgNull == rsp_msg_type);

    auto rsp_msg = raft_mem->BuildRspMsg(
            *vote_msg, hard_state, soft_state, 
            mark_broadcast, rsp_msg_type);
    assert(nullptr == rsp_msg);

    raft_mem->ApplyState(std::move(hard_state), std::move(soft_state));
    assert(0 == raft_mem->GetVote(raft_mem->GetTerm()));
}


TEST(FollowerTest, NormalHeartBeat)
{
    auto raft_mem = build_raft_mem(1, 0, raft::RaftRole::FOLLOWER);
    assert(nullptr != raft_mem);

    auto hb_msg = build_from_msg(
            *raft_mem, raft::MessageType::MsgHeartbeat, 2);
    assert(nullptr != hb_msg);
    hb_msg->set_index(1);
    hb_msg->set_log_term(0);
    hb_msg->set_commit_index(0);
    hb_msg->set_commit_term(0);

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;

    std::tie(hard_state, 
            soft_state, mark_broadcast, rsp_msg_type) = 
        raft_mem->Step(*hb_msg, nullptr, nullptr);
    assert(nullptr == hard_state);
    assert(nullptr != soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgHeartbeatResp == rsp_msg_type);

    assert(false == soft_state->has_role());
    assert(soft_state->has_leader_id());
    assert(hb_msg->from() == soft_state->leader_id());

    auto rsp_msg = raft_mem->BuildRspMsg(
            *hb_msg, hard_state, soft_state, mark_broadcast, rsp_msg_type);
    assert(nullptr != rsp_msg);
    assert(rsp_msg_type == rsp_msg->type());
    assert(false == rsp_msg->reject());
    assert(raft_mem->GetMaxIndex() + 1 == rsp_msg->index());

    raft_mem->ApplyState(std::move(hard_state), std::move(soft_state));
    assert(hb_msg->from() == raft_mem->GetLeaderId(raft_mem->GetTerm()));
}

TEST(FollowerTest, HeartBeatRspReject)
{
    auto raft_mem = build_raft_mem(1, 10, raft::RaftRole::FOLLOWER);
    assert(nullptr != raft_mem);

    {
        std::unique_ptr<raft::HardState> 
            hard_state = cutils::make_unique<raft::HardState>();
        assert(nullptr != hard_state);
        auto entry = hard_state->add_entries();
        assert(nullptr != entry);
        entry->set_type(raft::EntryType::EntryNormal);
        entry->set_term(raft_mem->GetTerm());
        entry->set_index(raft_mem->GetCommit() + 1);
        entry->set_reqid(0);

        hard_state->set_term(raft_mem->GetTerm() + 1);
        raft_mem->ApplyState(std::move(hard_state), nullptr);
        assert(uint64_t{11} == raft_mem->GetMaxIndex());
    }

    auto hb_msg = build_from_msg(
            *raft_mem, raft::MessageType::MsgHeartbeat, 2);
    assert(nullptr != hb_msg);
    hb_msg->set_index(raft_mem->GetMaxIndex() + 1);
    hb_msg->set_log_term(raft_mem->GetTerm());
    assert(0 < hb_msg->log_term());
    hb_msg->set_commit_index(0);
    hb_msg->set_commit_term(0);

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;

    std::tie(hard_state, 
            soft_state, mark_broadcast, rsp_msg_type) = 
        raft_mem->Step(*hb_msg, nullptr, nullptr);
    assert(nullptr == hard_state);
    assert(nullptr != soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgHeartbeatResp == rsp_msg_type);

    auto rsp_msg = raft_mem->BuildRspMsg(
            *hb_msg, hard_state, soft_state, mark_broadcast, rsp_msg_type);
    assert(nullptr != rsp_msg);
    assert(rsp_msg_type == rsp_msg->type());
    assert(true == rsp_msg->reject());
    assert(raft_mem->GetMaxIndex() + 1 == rsp_msg->index());
}

TEST(FollowerTest, OutdateHeartBeat)
{
    auto raft_mem = build_raft_mem(1, 10, raft::RaftRole::FOLLOWER);
    assert(nullptr != raft_mem);

    {
        std::unique_ptr<raft::HardState> 
            hard_state = cutils::make_unique<raft::HardState>();
        assert(nullptr != hard_state);
        hard_state->set_term(raft_mem->GetTerm() + 10);
        raft_mem->ApplyState(std::move(hard_state), nullptr);
    }

    auto hb_msg = build_from_msg(
            *raft_mem, raft::MessageType::MsgHeartbeat, 2);
    assert(nullptr != hb_msg);
    hb_msg->set_index(1);
    hb_msg->set_log_term(0);
    hb_msg->set_commit_index(0);
    hb_msg->set_commit_term(0);

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;

    std::tie(hard_state, 
            soft_state, mark_broadcast, rsp_msg_type) = 
        raft_mem->Step(*hb_msg, nullptr, nullptr);
    assert(nullptr == hard_state);
    assert(nullptr != soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgHeartbeatResp == rsp_msg_type);

    auto rsp_msg = raft_mem->BuildRspMsg(
            *hb_msg, hard_state, soft_state, mark_broadcast, rsp_msg_type);
    assert(nullptr != rsp_msg);
    assert(rsp_msg_type == rsp_msg->type());
    assert(false == rsp_msg->reject());
    assert(rsp_msg->index() == raft_mem->GetCommit() + 1);


    hb_msg->set_index(3);
    hb_msg->set_log_term(1);
    assert(hb_msg->log_term() < raft_mem->GetTerm());
    hb_msg->set_commit_index(2);
    hb_msg->set_commit_term(1);

    std::tie(hard_state, 
            soft_state, mark_broadcast, rsp_msg_type) = 
        raft_mem->Step(*hb_msg, nullptr, nullptr);
    assert(nullptr == hard_state);
    assert(nullptr != soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgHeartbeatResp == rsp_msg_type);

    rsp_msg = raft_mem->BuildRspMsg(
            *hb_msg, hard_state, soft_state, mark_broadcast, rsp_msg_type);
    assert(nullptr != rsp_msg);
    assert(rsp_msg_type == rsp_msg->type());
    assert(false == rsp_msg->reject());
    assert(rsp_msg->index() == raft_mem->GetCommit() + 1);
}

TEST(FollowerTest, NormalApp)
{
    auto raft_mem = build_raft_mem(1, 0, raft::RaftRole::FOLLOWER);
    assert(nullptr != raft_mem);

    auto app_msg = build_from_msg(
            *raft_mem, raft::MessageType::MsgApp, 2);
    assert(nullptr != app_msg);
    app_msg->set_index(1);
    app_msg->set_log_term(0);
    app_msg->set_commit_index(0);
    app_msg->set_commit_term(0);
    {
        auto entry = app_msg->add_entries();
        assert(nullptr != entry);
        entry->set_type(raft::EntryType::EntryNormal);
        entry->set_term(raft_mem->GetTerm());
        entry->set_index(app_msg->index());
        entry->set_reqid(0);
    }
    assert(0 < app_msg->entries_size());

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;

    std::tie(hard_state, 
            soft_state, mark_broadcast, rsp_msg_type) = 
        raft_mem->Step(*app_msg, nullptr, nullptr);
    assert(nullptr != hard_state);
    assert(nullptr != soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgAppResp == rsp_msg_type);

    assert(false == soft_state->has_role());
    assert(soft_state->has_leader_id());
    assert(app_msg->from() == soft_state->leader_id());

    assert(0 < hard_state->entries_size());
    
    auto rsp_msg = raft_mem->BuildRspMsg(
            *app_msg, hard_state, soft_state, mark_broadcast, rsp_msg_type);
    assert(nullptr != rsp_msg);
    assert(rsp_msg_type == rsp_msg->type());
    assert(false == rsp_msg->reject());

    raft_mem->ApplyState(std::move(hard_state), std::move(soft_state));
    assert(app_msg->index() == raft_mem->GetMaxIndex());
    assert(app_msg->from() == raft_mem->GetLeaderId(app_msg->term()));

    assert(raft_mem->GetMaxIndex() + 1 == rsp_msg->index());
}

TEST(FollowerTest, AppReject)
{
    auto raft_mem = build_raft_mem(1, 0, raft::RaftRole::FOLLOWER);
    assert(nullptr != raft_mem);
    {
        std::unique_ptr<raft::HardState>
            hard_state = cutils::make_unique<raft::HardState>();
        assert(nullptr != hard_state);
        add_entries(hard_state, raft_mem->GetTerm(), 1);
        hard_state->set_term(raft_mem->GetTerm() + 1);
        raft_mem->ApplyState(std::move(hard_state), nullptr);
    }

    auto app_msg = build_from_msg(
            *raft_mem, raft::MessageType::MsgApp, 2);
    assert(nullptr != app_msg);
    app_msg->set_index(2);
    app_msg->set_log_term(raft_mem->GetTerm());
    app_msg->set_commit_index(0);
    app_msg->set_commit_term(0);
    add_entries(app_msg, raft_mem->GetTerm(), 2);

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;

    std::tie(hard_state, 
            soft_state, mark_broadcast, rsp_msg_type) = 
        raft_mem->Step(*app_msg, nullptr, nullptr);
    assert(nullptr == hard_state);
    assert(nullptr != soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgAppResp == rsp_msg_type);

    auto rsp_msg = raft_mem->BuildRspMsg(
            *app_msg, hard_state, soft_state, mark_broadcast, rsp_msg_type);
    assert(nullptr != rsp_msg);
    assert(rsp_msg_type == rsp_msg->type());
    assert(true == rsp_msg->reject());
    assert(rsp_msg->index() == app_msg->index());

    app_msg = build_from_msg(
            *raft_mem, raft::MessageType::MsgApp, 2);
    assert(nullptr != app_msg);
    app_msg->set_index(1);
    app_msg->set_log_term(0);
    app_msg->set_commit_index(0);
    app_msg->set_commit_term(0);
    add_entries(app_msg, raft_mem->GetTerm(), 1);

    std::tie(hard_state, 
            soft_state, mark_broadcast, rsp_msg_type) = 
        raft_mem->Step(*app_msg, nullptr, nullptr);
    assert(nullptr != hard_state);
    assert(nullptr != soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgAppResp == rsp_msg_type);

    rsp_msg = raft_mem->BuildRspMsg(
            *app_msg, hard_state, soft_state, mark_broadcast, rsp_msg_type);
    assert(nullptr != rsp_msg);
    assert(rsp_msg_type == rsp_msg->type());
    assert(false == rsp_msg->reject());
    printf ( "%d %d\n", static_cast<int>(app_msg->index()), static_cast<int>(rsp_msg->index()) );
    assert(app_msg->index() + 1 == rsp_msg->index());
}

TEST(FollowerTest, OutdateApp)
{
    auto raft_mem = build_raft_mem(1, 10, raft::RaftRole::FOLLOWER);
    assert(nullptr != raft_mem);

    update_term(raft_mem, 11);

    auto app_msg = build_from_msg(
            *raft_mem, raft::MessageType::MsgApp, 2);
    assert(nullptr != app_msg);
    app_msg->set_index(2);
    app_msg->set_log_term(1);
    app_msg->set_commit_index(1);
    app_msg->set_commit_term(1);
    
    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;

    std::tie(hard_state, 
            soft_state, mark_broadcast, rsp_msg_type) = 
        raft_mem->Step(*app_msg, nullptr, nullptr);
    assert(nullptr == hard_state);
    assert(nullptr != soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgAppResp == rsp_msg_type);

    auto rsp_msg = raft_mem->BuildRspMsg(
            *app_msg, hard_state, soft_state, mark_broadcast, rsp_msg_type);
    assert(nullptr != rsp_msg);
    assert(rsp_msg_type == rsp_msg->type());
    assert(false == rsp_msg->reject());
    assert(rsp_msg->index() == raft_mem->GetCommit() + 1);

    add_entries(app_msg, raft_mem->GetTerm(), 2);
    add_entries(app_msg, raft_mem->GetTerm(), 3);
    std::tie(hard_state, 
            soft_state, mark_broadcast, rsp_msg_type) = 
        raft_mem->Step(*app_msg, nullptr, nullptr);
    assert(nullptr == hard_state);
    assert(nullptr != soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgAppResp == rsp_msg_type);

    rsp_msg = raft_mem->BuildRspMsg(
            *app_msg, hard_state, soft_state, mark_broadcast, rsp_msg_type);
    assert(nullptr != rsp_msg);
    assert(rsp_msg_type == rsp_msg->type());
    assert(false == rsp_msg->reject());
    assert(rsp_msg->index() == raft_mem->GetCommit() + 1);
}

TEST(FollowerTest, FollowerToCandidate)
{
    auto raft_mem = build_raft_mem(1, 1, raft::RaftRole::FOLLOWER);
    assert(nullptr != raft_mem);
    assert(raft::RaftRole::FOLLOWER == raft_mem->GetRole());

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;

    std::tie(hard_state, 
            soft_state, mark_broadcast, rsp_msg_type) = 
        raft_mem->CheckTimeout(true);
    assert(nullptr != hard_state);
    assert(nullptr != soft_state);
    assert(true == mark_broadcast);
    assert(raft::MessageType::MsgVote == rsp_msg_type);

    assert(raft_mem->GetTerm() + 1 == hard_state->term());
    assert(0 == hard_state->vote());
    assert(0 == hard_state->entries_size());

    assert(raft::RaftRole::CANDIDATE == 
            static_cast<raft::RaftRole>(soft_state->role()));
    assert(false == soft_state->has_leader_id());
    
    raft::Message fake_msg;
    fake_msg.set_type(raft::MessageType::MsgNull);
    fake_msg.set_logid(raft_mem->GetLogId());
    fake_msg.set_to(raft_mem->GetSelfId());
    fake_msg.set_term(hard_state->term());
    auto rsp_msg = raft_mem->BuildRspMsg(
            fake_msg, hard_state, soft_state, mark_broadcast, rsp_msg_type);
    assert(nullptr != rsp_msg);
    assert(rsp_msg_type == rsp_msg->type());
    assert(raft_mem->GetTerm() + 1 == rsp_msg->term());
    assert(raft_mem->GetSelfId() == rsp_msg->from());
    assert(0 == rsp_msg->to());
    assert(raft_mem->GetMaxIndex() + 1 == rsp_msg->index());
    assert(rsp_msg->has_log_term());
}



