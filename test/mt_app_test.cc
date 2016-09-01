#include "gtest/gtest.h"
#include "raft_mem.h"
#include "raft.pb.h"
#include "hassert.h"
#include "mem_utils.h"
#include "random_utils.h"
#include "test_helper.h"


TEST(MTAppTest, PerfectApp)
{
    std::map<uint32_t, std::unique_ptr<raft::RaftMem>> mapRaft;
    for (auto id : {1, 2, 3}) {
        mapRaft[id] = build_raft_mem(id, 1, 0, raft::RaftRole::FOLLOWER);
        assert(nullptr != mapRaft[id]);
        assert(id == mapRaft[id]->GetSelfId());
    }

    assert(true == make_leader(mapRaft, 1));
    
    auto& leader = mapRaft.at(1);
    assert(raft::RaftRole::LEADER == leader->GetRole());
    cutils::Random64BitGen reqid_gen;
    std::vector<uint64_t> vec_reqid;
    for (int testime = 0; testime < 100; ++testime) {

        printf ( "TESTINFO: testtime %d\n", testime );
        auto reqid = reqid_gen.Next();
        vec_reqid.push_back(reqid);
        
        std::unique_ptr<raft::Message> prop_msg;
        std::unique_ptr<raft::HardState> hard_state;
        std::unique_ptr<raft::SoftState> soft_state;
        bool mark_broadcast = false;
        auto rsp_msg_type = raft::MessageType::MsgNull;

        std::tie(prop_msg, 
                hard_state, soft_state, mark_broadcast, rsp_msg_type)
            = leader->SetValue("", reqid);
        assert(nullptr != prop_msg);
        assert(nullptr != hard_state);
        assert(nullptr == soft_state);
        assert(true == mark_broadcast);
        assert(raft::MessageType::MsgApp == rsp_msg_type);

        auto app_msg = leader->BuildRspMsg(
                *prop_msg, hard_state, soft_state, 
                mark_broadcast, rsp_msg_type);
        assert(nullptr != app_msg);
        assert(app_msg->index() == prop_msg->index());
        assert(app_msg->log_term() == prop_msg->log_term());

        leader->ApplyState(std::move(hard_state), std::move(soft_state));
        std::vector<std::unique_ptr<raft::Message>> vec_msg;
        vec_msg.push_back(std::move(app_msg));

        uint64_t prev_commit = leader->GetCommit();
        loop_until(mapRaft, vec_msg);
        assert(leader->GetCommit() == prop_msg->index()); 
        if (prev_commit != leader->GetCommit()) {
            vec_msg.clear();
            auto hb_msg = leader->BroadcastHeartBeatMsg();
            assert(nullptr != hb_msg);
            assert(leader->GetCommit() == hb_msg->commit_index());
            vec_msg.push_back(std::move(hb_msg));
            loop_until(mapRaft, vec_msg);
        }

        for (auto id : {1, 2, 3}) {
            printf ( "id %u GetCommit %d\n", id, static_cast<int>(mapRaft.at(id)->GetCommit()) );
            assert(prop_msg->index() == mapRaft.at(id)->GetCommit());
        }
    }

    for (auto id : {1, 2, 3}) {
        auto& raft_mem = mapRaft.at(id);
        assert(nullptr != raft_mem);
        for (size_t idx = 0; idx < vec_reqid.size(); ++idx) {
            auto mem_entry = raft_mem->At(idx);
            assert(nullptr != mem_entry);
            assert(mem_entry->reqid() == vec_reqid[idx]);
            assert(true == mem_entry->data().empty());
        }
    }
}

TEST(MTAppTest, MultPefectApp)
{
    std::map<uint32_t, std::unique_ptr<raft::RaftMem>> mapRaft;
    for (auto id : {1, 2, 3}) {
        mapRaft[id] = build_raft_mem(id, 1, 0, raft::RaftRole::FOLLOWER);
        assert(nullptr != mapRaft[id]);
        assert(id == mapRaft[id]->GetSelfId());
    }

    assert(true == make_leader(mapRaft, 1));

    auto& leader = mapRaft.at(1);
    assert(raft::RaftRole::LEADER == leader->GetRole());

    cutils::Random64BitGen reqid_gen;
    std::vector<uint64_t> vec_reqid;
    for (int testime = 0; testime < 100; ++testime) {
        int entries_size = testime % 10 + 1;
        std::vector<std::string> small_vec_value(entries_size, "");
        std::vector<uint64_t> small_vec_reqid;
        for (int idx = 0; idx < entries_size; ++idx) {
            auto reqid = reqid_gen.Next();
            small_vec_reqid.push_back(reqid);
            vec_reqid.push_back(reqid);
        }

        assert(small_vec_reqid.size() == small_vec_value.size());

        std::unique_ptr<raft::Message> prop_msg;
        std::unique_ptr<raft::HardState> hard_state;
        std::unique_ptr<raft::SoftState> soft_state;
        bool mark_broadcast = false;
        auto rsp_msg_type = raft::MessageType::MsgNull;

        std::tie(prop_msg, 
                hard_state, soft_state, mark_broadcast, rsp_msg_type)
            = leader->SetValue(small_vec_value, small_vec_reqid);
        assert(nullptr != prop_msg);
        assert(nullptr != hard_state);
        assert(nullptr == soft_state);
        assert(true == mark_broadcast);
        assert(raft::MessageType::MsgApp == rsp_msg_type);

        auto app_msg = leader->BuildRspMsg(
                *prop_msg, hard_state, soft_state, 
                mark_broadcast, rsp_msg_type);
        assert(nullptr != app_msg);
        assert(app_msg->index() == prop_msg->index());
        assert(app_msg->log_term() == prop_msg->log_term());

        leader->ApplyState(std::move(hard_state), std::move(soft_state));
        std::vector<std::unique_ptr<raft::Message>> vec_msg;
        vec_msg.push_back(std::move(app_msg));

        auto prev_commit = leader->GetCommit();
        loop_until(mapRaft, vec_msg);

        assert(leader->GetCommit() == prop_msg->index() + prop_msg->entries_size() - 1);
        if (prev_commit != leader->GetCommit()) {
            vec_msg.clear();
            auto hb_msg = leader->BroadcastHeartBeatMsg();
            assert(nullptr != hb_msg);
            assert(leader->GetCommit() == hb_msg->commit_index());
            vec_msg.push_back(std::move(hb_msg));
            loop_until(mapRaft, vec_msg);
        }

        for (auto id : {1, 2, 3}) {
            assert(prop_msg->index() + prop_msg->entries_size() - 1 == mapRaft.at(id)->GetCommit());
        }
    }

    for (auto id : {1, 2, 3}) {
        auto& raft_mem = mapRaft.at(id);
        assert(nullptr != raft_mem);
        for (size_t idx = 0; idx < vec_reqid.size(); ++idx) {
            auto mem_entry = raft_mem->At(idx);
            assert(nullptr != mem_entry);
            assert(mem_entry->reqid() == vec_reqid[idx]);
            assert(true == mem_entry->data().empty());
        }
    }
}


TEST(MTAppTest, QuoAppHB)
{
    std::map<uint32_t, std::unique_ptr<raft::RaftMem>> mapRaft;
    for (auto id : {1, 3}) {
        mapRaft[id] = build_raft_mem(id, 1, 0, raft::RaftRole::FOLLOWER);
        assert(nullptr != mapRaft[id]);
        assert(id = mapRaft[id]->GetSelfId());
    }

    assert(true == make_leader(mapRaft, 1));

    auto& leader = mapRaft.at(1);
    assert(raft::RaftRole::LEADER == leader->GetRole());
    cutils::Random64BitGen reqid_gen;
    std::vector<uint64_t> vec_reqid;
    for (int testime = 0; testime < 100; ++testime) {
        auto reqid = reqid_gen.Next();
        vec_reqid.push_back(reqid);

        std::unique_ptr<raft::Message> prop_msg;
        std::unique_ptr<raft::HardState> hard_state;
        std::unique_ptr<raft::SoftState> soft_state;
        bool mark_broadcast = false;
        auto rsp_msg_type = raft::MessageType::MsgNull;

        std::tie(prop_msg, 
                hard_state, soft_state, mark_broadcast, rsp_msg_type)
            = leader->SetValue("", reqid);
        assert(nullptr != prop_msg);
        assert(nullptr != hard_state);
        assert(nullptr == soft_state);
        assert(true == mark_broadcast);
        assert(raft::MessageType::MsgApp == rsp_msg_type);

        auto app_msg = leader->BuildRspMsg(
                *prop_msg, hard_state, soft_state, 
                mark_broadcast, rsp_msg_type);
        assert(nullptr != app_msg);
        assert(app_msg->index() == prop_msg->index());
        assert(app_msg->log_term() == prop_msg->log_term());
        
        leader->ApplyState(std::move(hard_state), std::move(soft_state));
        std::vector<std::unique_ptr<raft::Message>> vec_msg;
        vec_msg.push_back(std::move(app_msg));

        uint64_t prev_commit = leader->GetCommit();
        loop_until(mapRaft, vec_msg);
        assert(leader->GetCommit() == prop_msg->index());
        if (prev_commit != leader->GetCommit()) {
            vec_msg.clear();
            auto hb_msg = leader->BroadcastHeartBeatMsg();
            assert(nullptr != hb_msg);
            assert(leader->GetCommit() == hb_msg->commit_index());
            vec_msg.push_back(std::move(hb_msg));
            loop_until(mapRaft, vec_msg);
        }

        for (auto id : {1, 3}) {
            assert(prop_msg->index() == mapRaft.at(id)->GetCommit());
        }
    }

    // add
    mapRaft[2] = build_raft_mem(2, 1, 0, raft::RaftRole::FOLLOWER);
    assert(nullptr != mapRaft[2]);
    assert(2 == mapRaft.at(2)->GetSelfId());

    auto hb_msg = leader->BroadcastHeartBeatMsg();
    assert(nullptr != hb_msg);

    hb_msg->set_to(2);
    auto rsp_msg = apply_msg(mapRaft, *hb_msg);
    assert(nullptr != rsp_msg);
    assert(raft::MessageType::MsgHeartbeatResp == rsp_msg->type());
    assert(true == rsp_msg->reject());
    assert(2 == rsp_msg->index());

    auto leader_rsp_msg = apply_msg(mapRaft, *rsp_msg);
    assert(nullptr != leader_rsp_msg);
    assert(raft::MessageType::MsgApp == leader_rsp_msg->type());
    assert(1 == leader_rsp_msg->index());
    assert(0 == leader_rsp_msg->log_term());
    assert(10 == leader_rsp_msg->entries_size());

    rsp_msg = apply_msg(mapRaft, *leader_rsp_msg);
    assert(nullptr != rsp_msg);
    assert(raft::MessageType::MsgAppResp == rsp_msg->type());
    assert(false == rsp_msg->reject());
    assert(leader_rsp_msg->index() + leader_rsp_msg->entries_size() == rsp_msg->index());

    std::vector<std::unique_ptr<raft::Message>> vec_msg;
    vec_msg.push_back(std::move(rsp_msg));
    loop_until(mapRaft, vec_msg);

    assert(mapRaft.at(2)->GetCommit() == leader->GetCommit());

    for (auto id : {1, 2, 3}) {
        auto& raft_mem = mapRaft.at(id);
        for (size_t idx = 0; idx < vec_reqid.size(); ++idx) {
            auto mem_entry = raft_mem->At(idx);
            assert(nullptr != mem_entry);
            assert(mem_entry->reqid() == vec_reqid[idx]);
            assert(true == mem_entry->data().empty());
        }
    }
}


TEST(MTAppTest, QuoAppApp)
{
    std::map<uint32_t, std::unique_ptr<raft::RaftMem>> mapRaft;
    for (auto id : {1, 2}) {
        mapRaft[id] = build_raft_mem(id, 1, 0, raft::RaftRole::FOLLOWER);
        assert(nullptr != mapRaft[id]);
        assert(id == mapRaft[id]->GetSelfId());
    }

    assert(true == make_leader(mapRaft, 1));
    auto& leader = mapRaft.at(1);
    cutils::Random64BitGen reqid_gen;
    std::vector<uint64_t> vec_reqid;
    for (int testime = 0; testime < 100; ++testime) {
        auto reqid = reqid_gen.Next();
        vec_reqid.push_back(reqid);

        auto app_msg = set_value(*leader, "", reqid);
        assert(nullptr != app_msg);
       
        std::vector<std::unique_ptr<raft::Message>> vec_msg;
        vec_msg.push_back(std::move(app_msg));
        loop_until(mapRaft, vec_msg);
    }

    // add
    mapRaft[3] = build_raft_mem(3, 1, 0, raft::RaftRole::FOLLOWER);
    assert(nullptr != mapRaft[3]);

    auto reqid = reqid_gen.Next();
    vec_reqid.push_back(reqid);

    auto app_msg = set_value(*leader, "", reqid);
    assert(nullptr != app_msg);

    app_msg->set_to(3);
    auto rsp_msg = apply_msg(mapRaft, *app_msg);
    assert(raft::MessageType::MsgAppResp == rsp_msg->type());
    assert(true == rsp_msg->reject());
    assert(2 == rsp_msg->index());

    auto leader_rsp_msg = apply_msg(mapRaft, *rsp_msg);
    assert(nullptr != leader_rsp_msg);
    assert(raft::MessageType::MsgApp == leader_rsp_msg->type());
    assert(1 == leader_rsp_msg->index());
    assert(0 == leader_rsp_msg->log_term());
    assert(10 == leader_rsp_msg->entries_size());



}
