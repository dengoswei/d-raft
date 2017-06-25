#include "gtest/gtest.h"
#include "raft_mem.h"
#include "raft.pb.h"
#include "mem_utils.h"
#include "test_helper.h"
#include "tconfig_helper.h"

namespace {

} // namespace

TEST(FollowerTest, SimpleConstruct)
{
    auto raft_mem = build_raft_mem(1, 3, 1, 1);
    assert(nullptr != raft_mem);

    auto& raft = *raft_mem;
    assert(raft::RaftRole::FOLLOWER == raft.GetRole());
    assert(1 == raft.GetTerm());
    assert(1 == raft.GetMinIndex());
    assert(1 == raft.GetMaxIndex());
    assert(0 == raft.GetVoteCount());
}

TEST(FollowerTest, IgnoreMsg)
{
    auto raft_mem = build_raft_mem(1, 3, 1, 1);
    assert(nullptr != raft_mem);

    auto null_msg = build_null_msg(
            raft_mem->GetLogId(), raft_mem->GetTerm(), 
            2, raft_mem->GetSelfId());

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    raft::MessageType rsp_msg_type = raft::MessageType::MsgNull;
    bool need_disk_replicate = false;

    std::tie(mark_broadcast, 
            rsp_msg_type, 
            need_disk_replicate) 
        = raft_mem->Step(null_msg, hard_state, soft_state);
    assert(nullptr == hard_state);
    assert(nullptr == soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgNull == rsp_msg_type);
    assert(false == need_disk_replicate);
}

TEST(FollowerTest, InvalidTerm)
{
    auto raft_mem = build_raft_mem(1, 3, 1, 1);

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    raft::MessageType rsp_msg_type = raft::MessageType::MsgNull;
    bool need_disk_replicate = false;

    // case 1
    auto null_msg = build_null_msg(
            raft_mem->GetLogId(), 
            raft_mem->GetTerm(), 
            2, raft_mem->GetSelfId());
    null_msg.set_term(2);

    std::tie(mark_broadcast, 
            rsp_msg_type, 
            need_disk_replicate) 
        = raft_mem->Step(null_msg, hard_state, soft_state);
    assert(nullptr != hard_state);
    assert(nullptr == soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgNull == rsp_msg_type);
    assert(false == need_disk_replicate);
    assert(hard_state->has_meta());
    assert(null_msg.term() == hard_state->meta().term());

    raft_mem->ApplyState(std::move(hard_state), nullptr);

    // case 2
    null_msg.set_term(1);
    std::tie(mark_broadcast, 
            rsp_msg_type, 
            need_disk_replicate)
        = raft_mem->Step(null_msg, hard_state, soft_state);
    assert(nullptr == hard_state);
    assert(nullptr == soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgInvalidTerm == rsp_msg_type);
    assert(false == need_disk_replicate);

    auto rsp_msg = raft_mem->BuildRspMsg(null_msg, rsp_msg_type);
    assert(nullptr != rsp_msg);
    assert(rsp_msg_type == rsp_msg->type());
    assert(rsp_msg->term() > null_msg.term());
    assert(rsp_msg->term() == raft_mem->GetTerm());
}

TEST(FollowerTest, MsgVoteYes)
{
    auto raft_mem = build_raft_mem(1, 3, 1, 1);
    assert(nullptr != raft_mem);

    auto vote_msg = build_vote_msg(*raft_mem, 2);
    
    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    raft::MessageType rsp_msg_type = raft::MessageType::MsgNull;
    bool need_disk_replicate = false;

    std::tie(mark_broadcast, 
            rsp_msg_type, 
            need_disk_replicate) 
        = raft_mem->Step(vote_msg, hard_state, soft_state);
    assert(nullptr != hard_state);
    assert(nullptr == soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgVoteResp == rsp_msg_type);
    assert(false == need_disk_replicate);
    assert(hard_state->has_meta());
    assert(2 == hard_state->meta().vote());
    assert(raft_mem->GetTerm() == hard_state->meta().term());
    assert(0 == hard_state->entries_size());

    raft_mem->ApplyState(std::move(hard_state), std::move(soft_state));
    auto rsp_msg = raft_mem->BuildRspMsg(vote_msg, rsp_msg_type);
    assert(nullptr != rsp_msg);
    assert(rsp_msg_type == rsp_msg->type());
    assert(vote_msg.logid() == rsp_msg->logid());
    assert(vote_msg.to() == rsp_msg->from());
    assert(vote_msg.from() == rsp_msg->to());
    assert(vote_msg.term() == rsp_msg->term());
    assert(false == rsp_msg->reject());

    assert(2 == raft_mem->GetVote(raft_mem->GetTerm()));
}


TEST(FollowerTest, MsgVoteReject)
{
    auto raft_mem = build_raft_mem(1, 3, 1, 1);
    assert(nullptr != raft_mem);

    auto vote_msg = build_vote_msg(*raft_mem, 2);
    vote_msg.set_index(raft_mem->GetMaxIndex() - 1);
    vote_msg.set_log_term(0);

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;
    bool need_disk_replicate = false;

    // reject by rsp nothing
    std::tie(mark_broadcast, 
            rsp_msg_type, 
            need_disk_replicate)
        = raft_mem->Step(vote_msg, hard_state, soft_state);
    assert(nullptr == hard_state);
    assert(nullptr == soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgNull == rsp_msg_type);
    assert(false == need_disk_replicate);

    raft_mem->ApplyState(std::move(hard_state), std::move(soft_state));
    assert(0 == raft_mem->GetVote(raft_mem->GetTerm()));

    auto rsp_msg = raft_mem->BuildRspMsg(vote_msg, rsp_msg_type);
    assert(nullptr == rsp_msg);
}

TEST(FollowerTest, NormalHeartBeat)
{
    auto raft_mem = build_raft_mem(1, 3, 1, 1);
    assert(nullptr != raft_mem);

    auto hb_msg = build_hb_msg(*raft_mem, 2);

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;
    bool need_disk_replicate = false;

    std::tie(mark_broadcast, 
            rsp_msg_type, 
            need_disk_replicate)
        = raft_mem->Step(hb_msg, hard_state, soft_state);
    assert(nullptr == hard_state);
    assert(nullptr != soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgHeartbeatResp == rsp_msg_type);
    assert(false == need_disk_replicate);

    assert(false == soft_state->has_role());
    assert(soft_state->has_leader_id());
    assert(hb_msg.from() == soft_state->leader_id());
    
    raft_mem->ApplyState(
            std::move(hard_state), std::move(soft_state));

    auto rsp_msg = raft_mem->BuildRspMsg(hb_msg, rsp_msg_type);
    assert(nullptr != rsp_msg);
    assert(rsp_msg_type == rsp_msg->type());
	assert(rsp_msg->index() == hb_msg.index());

    assert(hb_msg.from() == raft_mem->GetLeaderId(raft_mem->GetTerm()));
}

TEST(FollowerTest, OutdateHeartBeat)
{
    auto raft_mem = build_raft_mem(1, 3, 1, 1);
    assert(nullptr != raft_mem);

    {
        std::unique_ptr<raft::HardState> 
            hard_state = cutils::make_unique<raft::HardState>();
        assert(nullptr != hard_state);
        auto meta = hard_state->mutable_meta();
        assert(nullptr != meta);
        meta->set_term(raft_mem->GetTerm() + 10);
        raft_mem->ApplyState(std::move(hard_state), nullptr);
    }

    auto hb_msg = build_hb_msg(*raft_mem, 2);
    hb_msg.set_term(raft_mem->GetTerm() - 4);

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;
    bool need_disk_replicate = false;

    std::tie(mark_broadcast, 
            rsp_msg_type, 
            need_disk_replicate) 
        = raft_mem->Step(hb_msg, hard_state, soft_state);
    assert(nullptr == hard_state);
    assert(nullptr == soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgInvalidTerm == rsp_msg_type);
    assert(false == need_disk_replicate);

    hb_msg.set_term(raft_mem->GetTerm());

    std::tie(mark_broadcast, 
            rsp_msg_type, 
            need_disk_replicate)
        = raft_mem->Step(hb_msg, hard_state, soft_state);
    assert(nullptr == hard_state);
    // mark leader;
    assert(nullptr != soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgHeartbeatResp == rsp_msg_type);
    assert(false == need_disk_replicate);
}


TEST(FollowerTest, NormalApp)
{
    auto raft_mem = build_raft_mem(1, 3, 1, 1);
    assert(nullptr != raft_mem);

    auto app_msg = build_app_msg(*raft_mem, 1, 2, 1);
    assert(0 < app_msg.entries_size());

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;
    bool need_disk_replicate = false;

    std::tie(mark_broadcast, 
            rsp_msg_type, 
            need_disk_replicate)
        = raft_mem->Step(app_msg, hard_state, soft_state);
    assert(nullptr != hard_state);
    assert(nullptr != soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgAppResp == rsp_msg_type);
    assert(false == need_disk_replicate);

    assert(false == soft_state->has_role());
    assert(soft_state->has_leader_id());
    assert(app_msg.from() == soft_state->leader_id());

    assert(0 < hard_state->entries_size());
    raft_mem->ApplyState(std::move(hard_state), std::move(soft_state)); 
    auto rsp_msg = raft_mem->BuildRspMsg(app_msg, rsp_msg_type);
    assert(nullptr != rsp_msg);
    assert(rsp_msg_type == rsp_msg->type());
    assert(false == rsp_msg->reject());
	assert(app_msg.entries(
                app_msg.entries_size()-1).index() == rsp_msg->index());

    assert(app_msg.index() + 1 == raft_mem->GetMaxIndex());
	assert(rsp_msg->index() == raft_mem->GetMaxIndex());
    assert(app_msg.from() == raft_mem->GetLeaderId(app_msg.term()));

    assert(raft_mem->GetMaxIndex() == rsp_msg->index());
}

TEST(FollowerTest, AppReject)
{
    auto raft_mem = build_raft_mem(1, 3, 1, 1);
    assert(nullptr != raft_mem);
    {
        std::unique_ptr<raft::HardState>
            hard_state = cutils::make_unique<raft::HardState>();
        assert(nullptr != hard_state);
        add_entry(*hard_state, raft_mem->GetTerm(), 2, "");

        auto meta = hard_state->mutable_meta();
        assert(nullptr != meta);
        meta->set_term(raft_mem->GetTerm() + 1);
        raft_mem->ApplyState(std::move(hard_state), nullptr);
    }

    auto app_msg = build_app_msg(*raft_mem, 2, 2, 3);
    app_msg.set_log_term(raft_mem->GetTerm());

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;
    bool need_disk_replicate = false;

	// missing log entry: index 1 => rejected
    std::tie(mark_broadcast, 
            rsp_msg_type, 
            need_disk_replicate) 
        = raft_mem->Step(app_msg, hard_state, soft_state);
    if (nullptr != hard_state) {
        printf ( "has_meta %d entries_size %d\n", 
                hard_state->has_meta(), hard_state->entries_size() );
    }
    assert(nullptr == hard_state);
    assert(nullptr != soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgAppResp == rsp_msg_type);
    assert(false == need_disk_replicate);
    
    raft_mem->ApplyState(std::move(hard_state), std::move(soft_state));
    auto rsp_msg = raft_mem->BuildRspMsg(app_msg, rsp_msg_type);
    assert(nullptr != rsp_msg);
    assert(rsp_msg_type == rsp_msg->type());
    assert(true == rsp_msg->reject());
    printf ( "rsp index %lu app_msg index %lu max %lu\n", 
            rsp_msg->index(), app_msg.index(), raft_mem->GetMaxIndex() );
	assert(rsp_msg->reject_hint() == raft_mem->GetCommit());

	// build new log entry: index 1 with different term => over-write;
    app_msg = build_app_msg(*raft_mem, 1, 2, 4);

    std::tie(mark_broadcast, 
            rsp_msg_type, 
            need_disk_replicate)
        = raft_mem->Step(app_msg, hard_state, soft_state);
    assert(nullptr != hard_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgAppResp == rsp_msg_type);
    assert(false == need_disk_replicate);

    raft_mem->ApplyState(std::move(hard_state), std::move(soft_state));
    rsp_msg = raft_mem->BuildRspMsg(app_msg, rsp_msg_type);
    assert(nullptr != rsp_msg);
    assert(rsp_msg_type == rsp_msg->type());
    assert(false == rsp_msg->reject());
}

TEST(FollowerTest, OutdateApp)
{
    auto raft_mem = build_raft_mem(1, 3, 1, 1);
    assert(nullptr != raft_mem);

    {
        std::unique_ptr<raft::HardState>
            hard_state = cutils::make_unique<raft::HardState>();
        assert(nullptr != hard_state);

        auto meta = hard_state->mutable_meta();
        assert(nullptr != meta);
        meta->set_term(raft_mem->GetTerm() + 10);
        raft_mem->ApplyState(std::move(hard_state), nullptr);
    }

    auto app_msg = build_app_msg(*raft_mem, 1, 2, 3);
    app_msg.set_term(3);
    assert(app_msg.term() < raft_mem->GetTerm());
    
    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;
    bool need_disk_replicate = false;

    std::tie(mark_broadcast, 
            rsp_msg_type, 
            need_disk_replicate) 
        = raft_mem->Step(app_msg, hard_state, soft_state);
    assert(nullptr == hard_state);
    assert(nullptr == soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgInvalidTerm == rsp_msg_type);
    assert(false == need_disk_replicate);
}

TEST(FollowerTest, FollowerToCandidate)
{
    auto raft_mem = build_raft_mem(1, 3, 1, 1);
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

    assert(hard_state->has_meta());
    assert(raft_mem->GetTerm() + 1 == hard_state->meta().term());
    assert(hard_state->meta().has_vote());
    assert(0 == hard_state->meta().vote());
    assert(0 == hard_state->entries_size());

    assert(raft::RaftRole::CANDIDATE == 
            static_cast<raft::RaftRole>(soft_state->role()));
    assert(false == soft_state->has_leader_id());
    
    raft_mem->ApplyState(std::move(hard_state), std::move(soft_state));

    auto rsp_msg = raft_mem->BuildBroadcastRspMsg(rsp_msg_type);
    assert(nullptr != rsp_msg);
    assert(2 == rsp_msg->nodes_size());

    assert(rsp_msg_type == rsp_msg->type());
    assert(raft_mem->GetTerm() == rsp_msg->term());
    assert(raft_mem->GetSelfId() == rsp_msg->from());
    assert(0 == rsp_msg->to());
    assert(raft_mem->GetMaxIndex() == rsp_msg->index());
    assert(rsp_msg->has_log_term());
}


