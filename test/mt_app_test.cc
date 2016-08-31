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



