#include "gtest/gtest.h"
#include "raft_mem.h"
#include "raft.pb.h"
#include "mem_utils.h"
#include "test_helper.h"
#include "replicate.h"


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

        null_msg->set_index(1);
        auto rsp_msg = raft_mem->BuildRspMsg(
                *null_msg, nullptr, nullptr, mark_broadcast, rsp_msg_type);
        assert(nullptr != rsp_msg);
        assert(rsp_msg_type == rsp_msg->type());
        assert(raft_mem->GetTerm() == rsp_msg->term());
        printf ( "req_msg.index %d maxindex %d rsp_msg->index %d\n", 
                static_cast<int>(null_msg->index()), 
                static_cast<int>(raft_mem->GetMaxIndex()), 
                static_cast<int>(rsp_msg->index()));
        assert(raft_mem->GetMaxIndex() + 1 == rsp_msg->index());
        assert(rsp_msg->has_log_term());
        assert(rsp_msg->has_commit_index());
        assert(rsp_msg->has_commit_term());
        assert(raft_mem->GetCommit() == rsp_msg->commit_index());
    }
}

TEST(LeaderTest, RepeateBroadcastHeartBeat)
{
    auto raft_mem = build_raft_mem(3, 1, raft::RaftRole::LEADER);
    assert(nullptr != raft_mem);

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
        assert(raft::MessageType::MsgHeartbeat == rsp_msg_type);

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
        assert(rsp_msg->has_commit_index());
        assert(rsp_msg->has_commit_term());
        assert(raft_mem->GetCommit() == rsp_msg->commit_index());

        raft_mem->ApplyState(nullptr, nullptr);
    }
}

TEST(LeaderTest, MsgProp)
{
    auto raft_mem = build_raft_mem(3, 1, raft::RaftRole::LEADER);
    assert(nullptr != raft_mem);

    for (int testime = 0; testime < 10; ++testime) {
        auto prop_msg = build_from_msg(
                *raft_mem, raft::MessageType::MsgProp, 2);
        assert(nullptr != prop_msg);
        prop_msg->set_index(raft_mem->GetMaxIndex() + 1);
        add_entries(prop_msg, raft_mem->GetTerm(), prop_msg->index());

        std::unique_ptr<raft::HardState> hard_state;
        std::unique_ptr<raft::SoftState> soft_state;
        bool mark_broadcast = false;
        auto rsp_msg_type = raft::MessageType::MsgNull;

        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type) = 
            raft_mem->Step(*prop_msg, nullptr, nullptr);
        assert(nullptr != hard_state);
        assert(nullptr == soft_state);
        assert(true == mark_broadcast);
        assert(raft::MessageType::MsgApp == rsp_msg_type);

        assert(prop_msg->entries_size() == hard_state->entries_size());
        assert(prop_msg->entries(0) == hard_state->entries(0));

        auto rsp_msg = raft_mem->BuildRspMsg(
                *prop_msg, hard_state, soft_state, mark_broadcast, rsp_msg_type);
        assert(nullptr != rsp_msg);
        assert(rsp_msg_type == rsp_msg->type());
        assert(rsp_msg->term() == raft_mem->GetTerm());
        assert(rsp_msg->index() == raft_mem->GetMaxIndex() + 1);
        assert(rsp_msg->log_term() == raft_mem->GetTerm());
        assert(rsp_msg->entries_size() == 1);
        assert(rsp_msg->entries(0) == hard_state->entries(0));
        assert(raft_mem->GetCommit() == rsp_msg->commit_index());
        assert(raft_mem->GetTerm() == rsp_msg->commit_term());

        raft_mem->ApplyState(std::move(hard_state), std::move(soft_state));
    }

    // batch
    for (int testime = 0; testime < 10; ++testime) {
        auto prop_msg = build_from_msg(
                *raft_mem, raft::MessageType::MsgProp, 2);
        assert(nullptr != prop_msg);
        prop_msg->set_index(raft_mem->GetMaxIndex() + 1);

        for (int entries_size = 0; 
                entries_size <= testime; ++entries_size) {
            add_entries(prop_msg, 
                    raft_mem->GetTerm(), prop_msg->index() + entries_size);
        }

        std::unique_ptr<raft::HardState> hard_state;
        std::unique_ptr<raft::SoftState> soft_state;
        bool mark_broadcast = false;
        auto rsp_msg_type = raft::MessageType::MsgNull;

        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type) = 
            raft_mem->Step(*prop_msg, nullptr, nullptr);
        assert(nullptr != hard_state);
        assert(nullptr == soft_state);
        assert(true == mark_broadcast);
        assert(raft::MessageType::MsgApp == rsp_msg_type);

        assert(prop_msg->entries_size() == hard_state->entries_size());
        for (int idx = 0; idx < prop_msg->entries_size(); ++idx) {
            assert(prop_msg->entries(idx) == hard_state->entries(idx));
        }
        
        auto rsp_msg = raft_mem->BuildRspMsg(
                *prop_msg, hard_state, soft_state, mark_broadcast, rsp_msg_type);
        assert(nullptr != rsp_msg);
        assert(rsp_msg_type == rsp_msg->type());
        assert(rsp_msg->term() == raft_mem->GetTerm());
        assert(rsp_msg->index() == raft_mem->GetMaxIndex() + 1);
        assert(rsp_msg->log_term() == raft_mem->GetTerm());
        assert(prop_msg->entries_size() == rsp_msg->entries_size());
        for (int idx = 0; idx < rsp_msg->entries_size(); ++idx) {
            assert(prop_msg->entries(idx) == rsp_msg->entries(idx));
        }
        assert(raft_mem->GetCommit() == rsp_msg->commit_index());
        assert(raft_mem->GetTerm() == rsp_msg->commit_term());

        raft_mem->ApplyState(std::move(hard_state), std::move(soft_state));
    }
}

TEST(LeaderTest, MsgHearbeatUntilMatch)
{
    // case 1
    {
        auto raft_mem = build_raft_mem(3, 1, raft::RaftRole::LEADER);
        assert(nullptr != raft_mem);

        auto hard_state = cutils::make_unique<raft::HardState>();
        assert(nullptr != hard_state);
        for (int idx = 0; idx < 10; ++idx) {
            add_entries(hard_state, raft_mem->GetTerm(), raft_mem->GetMaxIndex() + 1 + idx);
        }

        raft_mem->ApplyState(std::move(hard_state), nullptr);
        assert(raft_mem->GetMaxIndex() == 1 + 10);

        auto hb_rsp_msg = build_from_msg(
                *raft_mem, raft::MessageType::MsgHeartbeatResp, 2);
        assert(nullptr != hb_rsp_msg);
        hb_rsp_msg->set_index(raft_mem->GetMaxIndex() + 1);
        hb_rsp_msg->set_reject(true);

        hard_state = nullptr;
        std::unique_ptr<raft::SoftState> soft_state;
        bool mark_broadcast = false;
        auto rsp_msg_type = raft::MessageType::MsgNull;

        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type) = 
            raft_mem->Step(*hb_rsp_msg, nullptr, nullptr);
        assert(nullptr == hard_state);
        assert(nullptr == soft_state);
        assert(false == mark_broadcast);
        assert(raft::MessageType::MsgHeartbeat == rsp_msg_type);
        printf ( "hb_rsp_msg %d replicate->rejected %d\n", 
                static_cast<int>(hb_rsp_msg->index()), 
                raft_mem->GetReplicate()->GetRejectedIndex(hb_rsp_msg->from()) );
        // TODO: FIX
        assert(hb_rsp_msg->index() - 1 == 
                raft_mem->GetReplicate()->GetRejectedIndex(hb_rsp_msg->from()));
        // <1, ..., 6, ..., 11> 
        auto rsp_msg = raft_mem->BuildRspMsg(
                *hb_rsp_msg, nullptr, nullptr, mark_broadcast, rsp_msg_type);
        assert(nullptr != rsp_msg);
        assert(rsp_msg_type == rsp_msg->type());
        assert(6 + 1 == rsp_msg->index());

        hb_rsp_msg->set_index(rsp_msg->index());
        hb_rsp_msg->set_reject(true);

        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type) = 
            raft_mem->Step(*hb_rsp_msg, nullptr, nullptr);
        assert(nullptr == hard_state);
        assert(nullptr == soft_state);
        assert(false == mark_broadcast);
        assert(raft::MessageType::MsgHeartbeat == rsp_msg_type);
        assert(hb_rsp_msg->index() - 1 == 
                raft_mem->GetReplicate()->GetRejectedIndex(hb_rsp_msg->from()));

        // <1, ..., 3, ..., 6>
        rsp_msg = raft_mem->BuildRspMsg(
                *hb_rsp_msg, nullptr, nullptr, mark_broadcast, rsp_msg_type);
        assert(nullptr != rsp_msg);
        assert(rsp_msg_type == rsp_msg->type());
        assert(3 + 1 == rsp_msg->index());

        hb_rsp_msg->set_index(rsp_msg->index());
        hb_rsp_msg->set_reject(false);

        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type) = 
            raft_mem->Step(*hb_rsp_msg, nullptr, nullptr);
        assert(nullptr == hard_state);
        assert(nullptr == soft_state);
        assert(false == mark_broadcast);
        assert(raft::MessageType::MsgHeartbeat == rsp_msg_type);
        assert(hb_rsp_msg->index() - 1 == 
                raft_mem->GetReplicate()->GetAcceptedIndex(hb_rsp_msg->from()));

        rsp_msg = raft_mem->BuildRspMsg(
                *hb_rsp_msg, nullptr, nullptr, mark_broadcast, rsp_msg_type);
        assert(nullptr != rsp_msg);
        assert(rsp_msg_type == rsp_msg->type());
        assert(4 + 1 == rsp_msg->index());

        hb_rsp_msg->set_index(rsp_msg->index());
        hb_rsp_msg->set_reject(true);

        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type) = 
            raft_mem->Step(*hb_rsp_msg, nullptr, nullptr);
        assert(nullptr == hard_state);
        assert(nullptr == soft_state);
        assert(false == mark_broadcast);
        assert(raft::MessageType::MsgApp == rsp_msg_type);
        assert(hb_rsp_msg->index() - 1 ==
                raft_mem->GetReplicate()->GetRejectedIndex(hb_rsp_msg->from()));
    }
}

TEST(LeaderTest, OutdateMsgHeartbeatReject)
{
    auto raft_mem = build_raft_mem(3, 1, raft::RaftRole::LEADER);
    assert(nullptr != raft_mem);

    auto hard_state = cutils::make_unique<raft::HardState>();
    assert(nullptr != hard_state);
    hard_state->set_term(raft_mem->GetTerm() + 1);
    for (int idx = 0; idx < 10; ++idx) {
        add_entries(hard_state, 
                hard_state->term(), raft_mem->GetMaxIndex() + 1 + idx);
    }

    raft_mem->ApplyState(std::move(hard_state), nullptr);

    auto hb_rsp_msg = build_from_msg(
            *raft_mem, raft::MessageType::MsgHeartbeatResp, 2);
    assert(nullptr != hb_rsp_msg);
    hb_rsp_msg->set_index(raft_mem->GetMaxIndex() + 1);
    hb_rsp_msg->set_reject(false);
    
    hard_state = nullptr;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;

    std::tie(hard_state, 
            soft_state, mark_broadcast, rsp_msg_type) = 
        raft_mem->Step(*hb_rsp_msg, nullptr, nullptr);
    assert(nullptr == hard_state);
    assert(nullptr == soft_state);
    assert(raft::MessageType::MsgNull == rsp_msg_type);
    assert(hb_rsp_msg->index() - 1 ==
            raft_mem->GetReplicate()->GetAcceptedIndex(hb_rsp_msg->from()));

    hb_rsp_msg->set_index(5);
    hb_rsp_msg->set_reject(true);

    std::tie(hard_state, 
            soft_state, mark_broadcast, rsp_msg_type) = 
        raft_mem->Step(*hb_rsp_msg, nullptr, nullptr);
    assert(nullptr == hard_state);
    assert(nullptr == soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgNull == rsp_msg_type);
    assert(raft_mem->GetMaxIndex() == 
            raft_mem->GetReplicate()->GetAcceptedIndex(hb_rsp_msg->from()));
    assert(0 == raft_mem->GetReplicate()->GetRejectedIndex(hb_rsp_msg->from()));
}


TEST(LeaderTest, MsgAppReject)
{
    auto raft_mem = build_raft_mem(3, 1, raft::RaftRole::LEADER);
    assert(nullptr != raft_mem);

    auto hard_state = cutils::make_unique<raft::HardState>();
    assert(nullptr != hard_state);
    for (int idx = 0; idx < 10; ++idx) {
        add_entries(hard_state, raft_mem->GetTerm(), raft_mem->GetMaxIndex() + 1 + idx);
    }

    raft_mem->ApplyState(std::move(hard_state), nullptr);
    assert(raft_mem->GetMaxIndex() == 1 + 10);

    // case 1
    auto app_rsp_msg = build_from_msg(
            *raft_mem, raft::MessageType::MsgAppResp, 2);
    assert(nullptr != app_rsp_msg);
    app_rsp_msg->set_index(3);
    app_rsp_msg->set_reject(true);

    hard_state = nullptr;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;

    std::tie(hard_state, 
            soft_state, mark_broadcast, rsp_msg_type) = 
        raft_mem->Step(*app_rsp_msg, nullptr, nullptr);
    assert(nullptr == hard_state);
    assert(nullptr == soft_state);
    assert(false == mark_broadcast);
    // acccepted_ nothing, rejected: 2 => min_index 1 avaible
    assert(raft::MessageType::MsgApp == rsp_msg_type);
    assert(app_rsp_msg->index() - 1 ==
            raft_mem->GetReplicate()->GetRejectedIndex(app_rsp_msg->from()));

    // case 2
    app_rsp_msg->set_from(3);
    app_rsp_msg->set_index(raft_mem->GetMaxIndex() + 1);
    app_rsp_msg->set_reject(true);

    std::tie(hard_state, 
            soft_state, mark_broadcast, rsp_msg_type) = 
        raft_mem->Step(*app_rsp_msg, nullptr, nullptr);
    assert(nullptr == hard_state);
    assert(nullptr == soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgHeartbeat == rsp_msg_type);
    assert(app_rsp_msg->index() - 1 == 
            raft_mem->GetReplicate()->GetRejectedIndex(app_rsp_msg->from()));
}

TEST(LeaderTest, MsgAppAccepted)
{
    auto raft_mem = build_raft_mem(3, 1, raft::RaftRole::LEADER);
    assert(nullptr != raft_mem);

    auto hard_state = cutils::make_unique<raft::HardState>();
    assert(nullptr != hard_state);
    for (int idx = 0; idx < 10; ++idx) {
        add_entries(hard_state, 
                raft_mem->GetTerm(), raft_mem->GetMaxIndex() + 1 + idx);
    }

    raft_mem->ApplyState(std::move(hard_state), nullptr);
    assert(raft_mem->GetMaxIndex() == 1 + 10);

    auto app_rsp_msg = build_from_msg(
            *raft_mem, raft::MessageType::MsgAppResp, 2);
    assert(nullptr != app_rsp_msg);
    app_rsp_msg->set_index(3);
    app_rsp_msg->set_reject(false);

    hard_state = nullptr;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;

    std::tie(hard_state, 
            soft_state, mark_broadcast, rsp_msg_type) = 
        raft_mem->Step(*app_rsp_msg, nullptr, nullptr);
    assert(nullptr == hard_state);
    assert(nullptr == soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgApp == rsp_msg_type);
    assert(app_rsp_msg->index() - 1 ==
            raft_mem->GetReplicate()->GetAcceptedIndex(app_rsp_msg->from()));

    auto app_msg = raft_mem->BuildRspMsg(
            *app_rsp_msg, nullptr, nullptr, mark_broadcast, rsp_msg_type);
    assert(nullptr != app_msg);
    assert(rsp_msg_type == app_msg->type());
    assert(app_msg->index() == app_rsp_msg->index());
    assert(0 < app_msg->entries_size());
    for (int idx = 0; idx < app_msg->entries_size(); ++idx) {
        const auto& msg_entry = app_msg->entries(idx); 
        assert(msg_entry.index() == app_msg->index() + idx);

        int mem_idx = msg_entry.index() - raft_mem->GetMinIndex();
        assert(0 <= mem_idx);
        const auto mem_entry = raft_mem->At(mem_idx);
        assert(nullptr != mem_entry);
        assert(msg_entry == *mem_entry);
    }
}
