#include <algorithm>
#include "gtest/gtest.h"
#include "raft_mem.h"
#include "raft.pb.h"
#include "hassert.h"
#include "mem_utils.h"
#include "random_utils.h"
#include "test_helper.h"
#include "tconfig_helper.h"


namespace {

uint32_t make_leader(
        std::map<uint32_t, 
            std::unique_ptr<raft::RaftMem>>& map_raft)
{
    std::vector<uint32_t> nodes;
    for (size_t idx = 0; idx < map_raft.size(); ++idx) {
        nodes.push_back(idx + 1);
    }

    std::random_shuffle(nodes.begin(), nodes.end());
    auto& candidate = map_raft.at(nodes[0]);
    
    if (raft::RaftRole::LEADER == candidate->GetRole()) {
        return nodes[0];
    }

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    auto rsp_msg_type = raft::MessageType::MsgNull;
    bool need_disk_replicate = false;

    std::tie(hard_state, 
            soft_state, 
            rsp_msg_type) = candidate->CheckTimeout(true);
    assert(nullptr != hard_state);
    candidate->ApplyState(
            std::move(hard_state), std::move(soft_state));
    assert(raft::MessageType::MsgVote == rsp_msg_type);

    auto vote_msg = candidate->BuildBroadcastRspMsg(rsp_msg_type);
    assert(nullptr != vote_msg);

    size_t major_cnt = nodes.size() / 2 + 1;
    assert(2 <= major_cnt);
    std::vector<std::unique_ptr<raft::Message>> vec_votersp_msg;
    for (int step_cnt = 1; step_cnt < major_cnt; ++step_cnt) {
        uint32_t peer = nodes[step_cnt];
        vote_msg->set_to(peer);
        auto rsp_msg = apply_msg(map_raft, *vote_msg);
        assert(nullptr != rsp_msg);
        vec_votersp_msg.push_back(std::move(rsp_msg));
    }

    loop_until(map_raft, vec_votersp_msg);
    assert(raft::RaftRole::LEADER == candidate->GetRole());
    return nodes[0];
}

}

TEST(MTAppTest, PerfectApp)
{
    auto map_raft = build_raft_mem(3, 1, 1);

    uint32_t leader_id = make_leader(map_raft);
    assert(0 < leader_id);
    
    auto& leader = map_raft.at(leader_id);
    assert(raft::RaftRole::LEADER == leader->GetRole());

    const uint64_t origin_index = leader->GetMaxIndex();
    cutils::Random64BitGen reqid_gen;
    cutils::RandomStrGen<10, 30> value_gen;
    std::vector<uint64_t> vec_reqid;
    std::vector<std::string> vec_value;
    for (int testime = 0; testime < 100; ++testime) {

        printf ( "TESTINFO: testtime %d\n", testime );
        auto reqid = reqid_gen.Next();
        auto value = value_gen.Next();

        vec_reqid.push_back(reqid);
        vec_value.push_back(value);
        
        std::unique_ptr<raft::HardState> hard_state;
        auto ret = leader->SetValue(hard_state, value, reqid);
        assert(0 == ret);

        assert(nullptr != hard_state);
        assert(0 < hard_state->entries_size());
        assert(hard_state->has_meta());
        leader->ApplyState(std::move(hard_state), nullptr);

        auto vec_app_msg = leader->BuildAppMsg();
        assert(size_t{2} == vec_app_msg.size());
		for (auto& app_msg : vec_app_msg) {
			assert(nullptr != app_msg);
            assert(app_msg->index() == leader->GetMaxIndex() - 1);
			assert(1 == app_msg->entries_size());
			assert(0 != app_msg->to());
		}

        // TODO
        uint64_t prev_commit = leader->GetCommit();
        loop_until(map_raft, vec_app_msg);
		assert(leader->GetCommit() == leader->GetMaxIndex());
        for (auto id : {1, 2, 3}) {
            printf ( "id %u GetCommit %d\n", id, static_cast<int>(map_raft.at(id)->GetCommit()) );
            assert(leader->GetMaxIndex() == map_raft.at(id)->GetCommit());
        }
    }

    for (auto id : {1, 2, 3}) {
        auto& raft_mem = map_raft.at(id);
        assert(nullptr != raft_mem);
        for (size_t idx = 0; idx < vec_reqid.size(); ++idx) {
            auto mem_entry = raft_mem->At(
                    origin_index + idx + 1 - raft_mem->GetMinIndex());
            assert(nullptr != mem_entry);
            assert(mem_entry->reqid() == vec_reqid[idx]);
            assert(mem_entry->data() == vec_value[idx]);
        }
    }
}

TEST(MTAppTest, MultPefectApp)
{
    auto map_raft = build_raft_mem(3, 1, 1);

    uint32_t leader_id = make_leader(map_raft);

    auto& leader = map_raft.at(leader_id);
    assert(raft::RaftRole::LEADER == leader->GetRole());

    const uint64_t origin_index = leader->GetMaxIndex();

    cutils::RandomStrGen<10, 30> value_gen;
    cutils::Random64BitGen reqid_gen;
    std::vector<std::string> vec_value;
    std::vector<uint64_t> vec_reqid;
    for (int testime = 0; testime < 100; ++testime) {
        int entries_size = testime % 10 + 1;
        std::vector<std::string> small_vec_value;
        std::vector<uint64_t> small_vec_reqid;
        for (int idx = 0; idx < entries_size; ++idx) {
            auto value = value_gen.Next();
            auto reqid = reqid_gen.Next();
            small_vec_reqid.push_back(reqid);
            small_vec_value.push_back(value);
            vec_reqid.push_back(reqid);
            vec_value.push_back(value);
        }

        assert(small_vec_reqid.size() == small_vec_value.size());

        std::unique_ptr<raft::HardState> hard_state;
    
        auto ret = leader->SetValue(
                hard_state, small_vec_value, small_vec_reqid);
        assert(0 == ret);
        assert(nullptr != hard_state);

        leader->ApplyState(std::move(hard_state), nullptr);

        auto vec_app_msg = leader->BuildAppMsg();
		assert(size_t{2} == vec_app_msg.size());
		for (auto& app_msg : vec_app_msg) {
			assert(nullptr != app_msg);
			assert(app_msg->index() == 
                    leader->GetMaxIndex() - small_vec_value.size());
			assert(0 < app_msg->entries_size());
			assert(0 != app_msg->to());
		}

        auto prev_commit = leader->GetCommit();
        loop_until(map_raft, vec_app_msg);

		assert(leader->GetCommit() == leader->GetMaxIndex());
        for (auto id : {1, 2, 3}) {
			assert(leader->GetMaxIndex() == map_raft.at(id)->GetCommit());
        }
    }

    for (auto id : {1, 2, 3}) {
        auto& raft_mem = map_raft.at(id);
        assert(nullptr != raft_mem);
        for (size_t idx = 0; idx < vec_reqid.size(); ++idx) {
            auto mem_entry = raft_mem->At(
                    origin_index + idx + 1 - raft_mem->GetMinIndex());
            assert(nullptr != mem_entry);
            assert(mem_entry->reqid() == vec_reqid[idx]);
            assert(mem_entry->data() == vec_value[idx]);
        }
    }
}


TEST(MTAppTest, QuoAppApp)
{
    auto map_raft = build_raft_mem(3, 1, 1);

    uint32_t leader_id = make_leader(map_raft);
    auto& leader = map_raft.at(leader_id);
    assert(raft::RaftRole::LEADER == leader->GetRole());

    cutils::RandomStrGen<10, 30> value_gen;
    cutils::Random64BitGen reqid_gen;
    for (int testime = 0; testime < 100; ++testime) {
        auto reqid = reqid_gen.Next();
        auto value = value_gen.Next();

        std::unique_ptr<raft::HardState> hard_state;
        auto ret = leader->SetValue(hard_state, value, reqid);
        assert(0 == ret);
        assert(nullptr != hard_state);

        leader->ApplyState(std::move(hard_state), nullptr);

        auto vec_app_msg = leader->BuildAppMsg();
		assert(size_t{2} == vec_app_msg.size());
		for (auto& app_msg : vec_app_msg) {
			assert(nullptr != app_msg);
			assert(0 < app_msg->entries_size());
			assert(0 != app_msg->to());
		}

        std::vector<std::unique_ptr<raft::Message>> halt;
        halt.push_back(std::move(vec_app_msg[0]));

        uint64_t prev_commit = leader->GetCommit();
        loop_until(map_raft, halt);
		assert(leader->GetCommit() == leader->GetMaxIndex());
		assert(prev_commit < leader->GetCommit());
        
        halt[0] = std::move(vec_app_msg[1]);
        loop_until(map_raft, halt);
        for (auto id : {1, 3}) {
            assert(leader->GetCommit() == map_raft.at(id)->GetCommit());
        }
    }
}


