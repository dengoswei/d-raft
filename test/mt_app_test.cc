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

	set_progress_replicate(*leader);
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

		auto vec_app_msg = leader->BuildBroadcastRspMsg(
				*prop_msg, hard_state, soft_state, rsp_msg_type);
		assert(size_t{2} == vec_app_msg.size());
		for (auto& app_msg : vec_app_msg) {
			assert(nullptr != app_msg);
			assert(app_msg->index() == prop_msg->index());
			assert(app_msg->log_term() == prop_msg->log_term());
			assert(1 == app_msg->entries_size());
			assert(0 != app_msg->to());
		}

        leader->ApplyState(std::move(hard_state), std::move(soft_state));

        uint64_t prev_commit = leader->GetCommit();
        loop_until(mapRaft, vec_app_msg);
		assert(leader->GetCommit() == leader->GetMaxIndex());
        for (auto id : {1, 2, 3}) {
            printf ( "id %u GetCommit %d\n", id, static_cast<int>(mapRaft.at(id)->GetCommit()) );
            assert(leader->GetMaxIndex() == mapRaft.at(id)->GetCommit());
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

	set_progress_replicate(*leader);

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

		auto vec_app_msg = leader->BuildBroadcastRspMsg(
				*prop_msg, hard_state, soft_state, rsp_msg_type);
		assert(size_t{2} == vec_app_msg.size());
		for (auto& app_msg : vec_app_msg) {
			assert(nullptr != app_msg);
			assert(app_msg->index() == prop_msg->index());
			assert(app_msg->log_term() == prop_msg->log_term());
			assert(0 < app_msg->entries_size());
			assert(0 != app_msg->to());
		}

        leader->ApplyState(std::move(hard_state), std::move(soft_state));

        auto prev_commit = leader->GetCommit();
        loop_until(mapRaft, vec_app_msg);

		assert(leader->GetCommit() == leader->GetMaxIndex());
		assert(leader->GetCommit() == prop_msg->entries(
					prop_msg->entries_size() - 1).index());

        for (auto id : {1, 2, 3}) {
			assert(leader->GetMaxIndex() == mapRaft.at(id)->GetCommit());
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
	set_progress_replicate(*leader);

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

		auto vec_app_msg = leader->BuildBroadcastRspMsg(
				*prop_msg, hard_state, soft_state, rsp_msg_type);
		assert(size_t{2} == vec_app_msg.size());
		for (auto& app_msg : vec_app_msg) {
			assert(nullptr != app_msg);
			assert(app_msg->index() == prop_msg->index());
			assert(app_msg->log_term() == prop_msg->log_term());
			assert(0 < app_msg->entries_size());
			assert(0 != app_msg->to());
		}

        leader->ApplyState(std::move(hard_state), std::move(soft_state));

        uint64_t prev_commit = leader->GetCommit();
        loop_until(mapRaft, vec_app_msg);
		assert(leader->GetCommit() == leader->GetMaxIndex());
		assert(prev_commit < leader->GetCommit());

        for (auto id : {1, 3}) {
            assert(leader->GetCommit() == mapRaft.at(id)->GetCommit());
        }
    }

    // add
//    mapRaft[2] = build_raft_mem(2, 1, 0, raft::RaftRole::FOLLOWER);
//    assert(nullptr != mapRaft[2]);
//    assert(2 == mapRaft.at(2)->GetSelfId());
//
//    auto hb_msg = leader->BroadcastHeartBeatMsg();
//    assert(nullptr != hb_msg);
//
//    hb_msg->set_to(2);
//    auto rsp_msg = apply_msg(mapRaft, *hb_msg);
//    assert(nullptr != rsp_msg);
//    assert(raft::MessageType::MsgHeartbeatResp == rsp_msg->type());
//    assert(true == rsp_msg->reject());
//    assert(2 == rsp_msg->index());
//
//    auto leader_rsp_msg = apply_msg(mapRaft, *rsp_msg);
//    assert(nullptr != leader_rsp_msg);
//    assert(raft::MessageType::MsgApp == leader_rsp_msg->type());
//    assert(1 == leader_rsp_msg->index());
//    assert(0 == leader_rsp_msg->log_term());
//    assert(10 == leader_rsp_msg->entries_size());
//
//    rsp_msg = apply_msg(mapRaft, *leader_rsp_msg);
//    assert(nullptr != rsp_msg);
//    assert(raft::MessageType::MsgAppResp == rsp_msg->type());
//    assert(false == rsp_msg->reject());
//    assert(leader_rsp_msg->index() + leader_rsp_msg->entries_size() == rsp_msg->index());
//
//    std::vector<std::unique_ptr<raft::Message>> vec_msg;
//    vec_msg.push_back(std::move(rsp_msg));
//    loop_until(mapRaft, vec_msg);
//
//    assert(mapRaft.at(2)->GetCommit() == leader->GetCommit());
//
//    for (auto id : {1, 2, 3}) {
//        auto& raft_mem = mapRaft.at(id);
//        for (size_t idx = 0; idx < vec_reqid.size(); ++idx) {
//            auto mem_entry = raft_mem->At(idx);
//            assert(nullptr != mem_entry);
//            assert(mem_entry->reqid() == vec_reqid[idx]);
//            assert(true == mem_entry->data().empty());
//        }
//    }
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
	set_progress_replicate(*leader);

    cutils::Random64BitGen reqid_gen;
    std::vector<uint64_t> vec_reqid;
    for (int testime = 0; testime < 100; ++testime) {
        auto reqid = reqid_gen.Next();
        vec_reqid.push_back(reqid);

        auto vec_app_msg = set_value(*leader, "", reqid);
		assert(size_t{2} == vec_app_msg.size());
       
        loop_until(mapRaft, vec_app_msg);
    }

    // add
    mapRaft[3] = build_raft_mem(3, 1, 0, raft::RaftRole::FOLLOWER);
    assert(nullptr != mapRaft[3]);

    auto reqid = reqid_gen.Next();
    vec_reqid.push_back(reqid);

    auto vec_app_msg = set_value(*leader, "", reqid);
	assert(size_t{2} == vec_app_msg.size());

	auto app_msg = std::move(vec_app_msg[0]);
	assert(nullptr != app_msg);
	app_msg->set_to(3);

    auto vec_rsp_msg = apply_msg(mapRaft, *app_msg);
	assert(size_t{1} == vec_rsp_msg.size());
	auto rsp_msg = std::move(vec_rsp_msg[0]);

    assert(raft::MessageType::MsgAppResp == rsp_msg->type());
    assert(true == rsp_msg->reject());
	assert(app_msg->index() == rsp_msg->index());
	assert(0 == rsp_msg->reject_hint());

    vec_rsp_msg = apply_msg(mapRaft, *rsp_msg);
	assert(size_t{1} == vec_rsp_msg.size());
	auto leader_rsp_msg = std::move(vec_rsp_msg[0]);
    assert(nullptr != leader_rsp_msg);
    assert(raft::MessageType::MsgApp == leader_rsp_msg->type());
    assert(0 == leader_rsp_msg->index());
    assert(0 == leader_rsp_msg->log_term());
	printf ( "entries_size %d\n", leader_rsp_msg->entries_size() );
    assert(0 <= leader_rsp_msg->entries_size());
}
