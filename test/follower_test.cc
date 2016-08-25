#include "gtest/gtest.h"
#include "raft_mem.h"
#include "raft.pb.h"
#include "mem_utils.h"

namespace {

std::unique_ptr<raft::RaftMem> 
    build_raft_mem(
            uint64_t term, uint64_t commit_index, raft::RaftRole role)
{
    std::unique_ptr<raft::RaftMem> raft_mem = 
        cutils::make_unique<raft::RaftMem>(1, 1, 100);
    assert(nullptr != raft_mem);

    std::unique_ptr<raft::HardState> hard_state = 
        cutils::make_unique<raft::HardState>();
    assert(nullptr != hard_state);
    hard_state->set_term(term);
    hard_state->set_commit(commit_index);
    hard_state->set_vote(0);
    if (0 != commit_index) {
        raft::Entry* entry = hard_state->add_entries();
        assert(nullptr != entry);
        entry->set_type(raft::EntryType::EntryNormal);
        entry->set_term(term);
        entry->set_index(commit_index);
        entry->set_reqid(0);
    }

    std::unique_ptr<raft::SoftState> soft_state = 
        cutils::make_unique<raft::SoftState>();
    assert(nullptr != soft_state);
    soft_state->set_role(static_cast<uint32_t>(role));

    raft_mem->ApplyState(std::move(hard_state), std::move(soft_state));
    assert(raft_mem->GetRole() == role);
    assert(raft_mem->GetTerm() == term);
    assert(raft_mem->GetCommit() == commit_index);
    assert(raft_mem->GetMaxIndex() == commit_index);
    assert(raft_mem->GetMinIndex() == commit_index);
    return raft_mem;
}

std::unique_ptr<raft::Message> build_to_msg(
        const raft::RaftMem& raft_mem, 
        raft::MessageType msg_type, 
        uint32_t follower_id)
{
    std::unique_ptr<raft::Message> msg =
        cutils::make_unique<raft::Message>();
    assert(nullptr != msg);

    msg->set_type(msg_type);
    msg->set_logid(raft_mem.GetLogId());
    msg->set_term(raft_mem.GetTerm());
    msg->set_to(follower_id);
    msg->set_from(raft_mem.GetSelfId());
    return msg;
}

std::unique_ptr<raft::Message> build_from_msg(
        const raft::RaftMem& raft_mem, 
        raft::MessageType msg_type, 
        uint32_t follower_id)
{
    auto msg = cutils::make_unique<raft::Message>();
    assert(nullptr != msg);

    msg->set_type(msg_type);
    msg->set_logid(raft_mem.GetLogId());
    msg->set_term(raft_mem.GetTerm());
    msg->set_to(raft_mem.GetSelfId());
    msg->set_from(follower_id);
    return msg;
}

}

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
    // TODO
    assert(false == rsp_msg->reject());
    assert(raft_mem->GetMaxIndex() + 1 == rsp_msg->index());

    raft_mem->ApplyState(std::move(hard_state), std::move(soft_state));
    assert(hb_msg->from() == raft_mem->GetLeaderId(raft_mem->GetTerm()));
}

