#include "gtest/gtest.h"
#include "raft_mem.h"
#include "raft.pb.h"
#include "mem_utils.h"
#include "test_helper.h"
#include "progress.h"

namespace {


void update_progress_next(raft::RaftMem& raft_mem)
{
	for (auto& id_progress : raft_mem.GetProgress()) {
		uint32_t peer_id = id_progress.first;
		auto progress = id_progress.second.get();
		assert(nullptr != progress);
		if (peer_id == raft_mem.GetSelfId()) {
			continue;
		}

		progress->UpdateNext(raft_mem.GetMaxIndex() + 1);
	}
}

} // namespace


TEST(LeaderTest, SimpleConstruct)
{
    auto raft_mem = build_raft_mem(1, 3, 1, 1);
    assert(nullptr != raft_mem);

    assert(raft::RaftRole::FOLLOWER == raft_mem->GetRole());

    make_fake_leader(*raft_mem);
    assert(raft::RaftRole::LEADER == raft_mem->GetRole());
}

TEST(LeaderTest, IgnoreMsg)
{
    auto raft_mem = build_raft_mem(1, 3, 1, 1);
    assert(nullptr != raft_mem);

    make_fake_leader(*raft_mem);
    
    auto null_msg = build_null_msg(
            raft_mem->GetLogId(), 
            raft_mem->GetTerm(), 2, raft_mem->GetSelfId());

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;
    bool need_disk_replicate = false;

    std::tie(mark_broadcast, rsp_msg_type, need_disk_replicate)
        = raft_mem->Step(null_msg, hard_state, soft_state);
    assert(nullptr == hard_state);
    assert(nullptr == soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgNull == rsp_msg_type);
    assert(false == need_disk_replicate);
}

TEST(LeaderTest, InvalidTerm)
{
    auto raft_mem = build_raft_mem(1, 3, 2, 1);
    assert(nullptr != raft_mem);

    make_fake_leader(*raft_mem);

    auto null_msg = build_null_msg(
            raft_mem->GetLogId(), raft_mem->GetTerm(), 
            2, raft_mem->GetSelfId());

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;
    bool need_disk_replicate = false;

    // case 1
    {
        // revert to follower
        null_msg.set_term(raft_mem->GetTerm() + 1);
        std::tie(mark_broadcast, rsp_msg_type, need_disk_replicate)
            = raft_mem->Step(null_msg, hard_state, soft_state);
        assert(nullptr != hard_state);
        assert(nullptr != soft_state);
        assert(false == mark_broadcast);
        assert(raft::MessageType::MsgNull == rsp_msg_type);
        assert(false == need_disk_replicate);
        assert(hard_state->has_meta());
        assert(hard_state->meta().has_term());
        assert(null_msg.term() == hard_state->meta().term());
        assert(raft::RaftRole::FOLLOWER == 
                static_cast<raft::RaftRole>(soft_state->role()));
        assert(false == soft_state->has_leader_id());
    }

    // case 2
    {
        hard_state = nullptr;
        soft_state = nullptr;
        null_msg.set_term(raft_mem->GetTerm() - 1);
        std::tie(mark_broadcast, rsp_msg_type, need_disk_replicate)
            = raft_mem->Step(null_msg, hard_state, soft_state);
        assert(nullptr == hard_state);
        assert(nullptr == soft_state);
        assert(false == mark_broadcast);
        assert(raft::MessageType::MsgHeartbeat == rsp_msg_type);
        assert(false == need_disk_replicate);

        null_msg.set_index(1);
        auto rsp_msg = raft_mem->BuildRspMsg(
                null_msg, nullptr, nullptr, 
                null_msg.from(), rsp_msg_type, true);
        assert(rsp_msg_type == rsp_msg->type());
        assert(raft_mem->GetTerm() == rsp_msg->term());
        printf ( "req_msg.index %d maxindex %d rsp_msg->index %d\n", 
                static_cast<int>(null_msg.index()), 
                static_cast<int>(raft_mem->GetMaxIndex()), 
                static_cast<int>(rsp_msg->index()));
        assert(raft_mem->GetMaxIndex() == rsp_msg->index());
        assert(rsp_msg->has_log_term());
    }
}


TEST(LeaderTest, RepeateBroadcastHeartBeat)
{
    auto raft_mem = build_raft_mem(1, 3, 2, 1);
    assert(nullptr != raft_mem);

    make_fake_leader(*raft_mem);

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;

    uint64_t term = raft_mem->GetTerm();
    for (int testime = 0; testime < 10; ++testime) {
        assert(term == raft_mem->GetTerm());
        std::tie(hard_state, 
                soft_state, 
                mark_broadcast, 
                rsp_msg_type) = 
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
		auto vec_rsp_msg = raft_mem->BuildBroadcastRspMsg(
				fake_msg, nullptr, nullptr, rsp_msg_type);
		assert(size_t{2} == vec_rsp_msg.size());
		for (const auto& rsp_msg : vec_rsp_msg) {
			assert(nullptr != rsp_msg);
			assert(rsp_msg_type == rsp_msg->type());
			assert(raft_mem->GetTerm() == rsp_msg->term());
			assert(raft_mem->GetMaxIndex() == rsp_msg->index());
			assert(rsp_msg->has_log_term());
		}

        raft_mem->ApplyState(nullptr, nullptr);
    }
}

TEST(LeaderTest, MsgProp)
{
    auto raft_mem = build_raft_mem(1, 3, 2, 1);
    assert(nullptr != raft_mem);

    make_fake_leader(*raft_mem);

	set_progress_replicate(*raft_mem);
    for (int testime = 0; testime < 10; ++testime) {
        auto prop_msg = build_prop_msg(*raft_mem, 1);

        std::unique_ptr<raft::HardState> hard_state;
        std::unique_ptr<raft::SoftState> soft_state;
        bool mark_broadcast = false;
        auto rsp_msg_type = raft::MessageType::MsgNull;
        bool need_disk_replicate = false;

        std::tie(mark_broadcast, rsp_msg_type, need_disk_replicate)
            = raft_mem->Step(prop_msg, hard_state, soft_state);
        assert(nullptr != hard_state);
        assert(nullptr == soft_state);
        assert(true == mark_broadcast);
        assert(raft::MessageType::MsgApp == rsp_msg_type);
        assert(false == need_disk_replicate);

        assert(prop_msg.entries_size() == hard_state->entries_size());
        assert(prop_msg.entries(0) == hard_state->entries(0));

		auto vec_rsp_msg = raft_mem->BuildBroadcastRspMsg(
				prop_msg, 
                hard_state, soft_state, rsp_msg_type);
		assert(size_t{2} == vec_rsp_msg.size());
		for (const auto& rsp_msg : vec_rsp_msg) {
			assert(nullptr != rsp_msg);
			assert(rsp_msg_type == rsp_msg->type());
			assert(rsp_msg->term() == raft_mem->GetTerm());
			assert(rsp_msg->index() == raft_mem->GetMaxIndex());
			assert(rsp_msg->log_term() == raft_mem->GetTerm());
			assert(0 != rsp_msg->to());
			assert(rsp_msg->entries_size() == 1);
			assert(rsp_msg->entries(0) == hard_state->entries(0));
			assert(rsp_msg->entries(0).index() == 
                    raft_mem->GetMaxIndex() + 1);
			assert(raft_mem->GetCommit() == rsp_msg->commit_index());
			assert(raft_mem->GetTerm() == rsp_msg->commit_term());
		}

        raft_mem->ApplyState(std::move(hard_state), std::move(soft_state));
    }

    // batch
    for (int testime = 0; testime < 10; ++testime) {
        auto prop_msg = build_prop_msg(*raft_mem, testime + 1); 

        std::unique_ptr<raft::HardState> hard_state;
        std::unique_ptr<raft::SoftState> soft_state;
        bool mark_broadcast = false;
        auto rsp_msg_type = raft::MessageType::MsgNull;
        bool need_disk_replicate = false;

        std::tie(mark_broadcast, rsp_msg_type, need_disk_replicate)
            = raft_mem->Step(prop_msg, hard_state, soft_state);
        assert(nullptr != hard_state);
        assert(nullptr == soft_state);
        assert(true == mark_broadcast);
        assert(raft::MessageType::MsgApp == rsp_msg_type);
        assert(false == need_disk_replicate);

        assert(prop_msg.entries_size() == hard_state->entries_size());
        for (int idx = 0; idx < prop_msg.entries_size(); ++idx) {
            assert(prop_msg.entries(idx) == hard_state->entries(idx));
        }
        
		auto vec_rsp_msg = raft_mem->BuildBroadcastRspMsg(
				prop_msg, hard_state, soft_state, rsp_msg_type);
		assert(size_t{2} == vec_rsp_msg.size());
		for (const auto& rsp_msg : vec_rsp_msg) {
			assert(nullptr != rsp_msg);
			assert(rsp_msg_type == rsp_msg->type());
			assert(rsp_msg->term() == raft_mem->GetTerm());
			assert(rsp_msg->index() == raft_mem->GetMaxIndex());
			assert(rsp_msg->log_term() == raft_mem->GetTerm());
			assert(prop_msg.entries_size() == rsp_msg->entries_size());
			for (int idx = 0; idx < rsp_msg->entries_size(); ++idx) {
				assert(prop_msg.entries(idx) == rsp_msg->entries(idx));
			}
			assert(raft_mem->GetCommit() == rsp_msg->commit_index());
			assert(raft_mem->GetTerm() == rsp_msg->commit_term());
		}

        raft_mem->ApplyState(std::move(hard_state), std::move(soft_state));
    }
}

TEST(LeaderTest, MsgAppUntilMatch)
{
    // case 1
    {
        auto raft_mem = build_raft_mem(1, 3, 2, 5);
        assert(nullptr != raft_mem);

        make_fake_leader(*raft_mem);

        auto hard_state = cutils::make_unique<raft::HardState>();
        assert(nullptr != hard_state);
        cutils::RandomStrGen<10, 30> gen;
        for (int idx = 0; idx < 10; ++idx) {
            add_entry(*hard_state, 
                    raft_mem->GetTerm(), 
                    raft_mem->GetMaxIndex() + 1 + idx, 
                    gen.Next());
        }

        raft_mem->ApplyState(std::move(hard_state), nullptr);
        assert(raft_mem->GetMaxIndex() == 5 + 10);

		update_progress_next(*raft_mem);

        auto apprsp_msg = build_apprsp_msg(*raft_mem, 2);
        apprsp_msg.set_index(raft_mem->GetMaxIndex());
		apprsp_msg.set_reject(true);
		apprsp_msg.set_reject_hint(raft_mem->GetMaxIndex() / 2);

		auto progress = raft_mem->GetProgress(apprsp_msg.from());
		assert(nullptr != progress);
		assert(raft::ProgressState::PROBE == progress->GetState());
		assert(raft_mem->GetMaxIndex() + 1 == progress->GetNext());
		assert(false == progress->IsPause());

        hard_state = nullptr;
        std::unique_ptr<raft::SoftState> soft_state;
        bool mark_broadcast = false;
        auto rsp_msg_type = raft::MessageType::MsgNull;
        bool need_disk_replicate = false;

        std::tie(mark_broadcast, rsp_msg_type, need_disk_replicate) 
            = raft_mem->Step(apprsp_msg, hard_state, soft_state);
        assert(nullptr == hard_state);
        assert(nullptr == soft_state);
        assert(false == mark_broadcast);
        assert(raft::MessageType::MsgApp == rsp_msg_type);
        assert(false == need_disk_replicate);

		assert(progress->GetNext() == apprsp_msg.reject_hint() + 1);
		assert(progress->GetMatched() == 0);
		assert(raft::ProgressState::PROBE == progress->GetState());
		assert(false == progress->IsPause());

        auto rsp_msg = raft_mem->BuildRspMsg(
                apprsp_msg, nullptr, nullptr, 
                apprsp_msg.from(), rsp_msg_type, true);
        assert(nullptr != rsp_msg);
        assert(rsp_msg_type == rsp_msg->type());
		assert(progress->GetNext() == 
				rsp_msg->entries(rsp_msg->entries_size()-1).index() + 1);
		assert(0 < rsp_msg->entries_size());
		assert(raft::ProgressState::PROBE == progress->GetState());
		assert(progress->IsPause());

		uint64_t max_index = raft_mem->GetMaxIndex();
		{
            auto prop_msg = build_prop_msg(*raft_mem, 1);

			std::unique_ptr<raft::HardState> hard_state;
			std::unique_ptr<raft::SoftState> soft_state;
			bool mark_broadcast = false;
			auto rsp_msg_type = raft::MessageType::MsgNull;
			bool need_disk_replicate = false;
			
            std::tie(mark_broadcast, rsp_msg_type, need_disk_replicate)
                = raft_mem->Step(prop_msg, hard_state, soft_state);
			assert(nullptr != hard_state);
			assert(nullptr == soft_state);
			assert(true == mark_broadcast);
			assert(raft::MessageType::MsgApp == rsp_msg_type);
			assert(false == need_disk_replicate);

			// stop by pause
			auto rsp_msg = raft_mem->BuildRspMsg(
					prop_msg, nullptr, nullptr, 
                    2, rsp_msg_type, false);
			assert(nullptr == rsp_msg);
		}
		assert(max_index == raft_mem->GetMaxIndex());

		apprsp_msg.set_index(
                rsp_msg->entries(rsp_msg->entries_size()-1).index());
		assert(apprsp_msg.index() > rsp_msg->index());
		apprsp_msg.set_reject(false);

        hard_state = nullptr;
        soft_state = nullptr;
        std::tie(mark_broadcast, rsp_msg_type, need_disk_replicate)
            = raft_mem->Step(apprsp_msg, hard_state, soft_state);
        assert(nullptr != hard_state);
		assert(0 == hard_state->entries_size());
		assert(hard_state->has_meta());
		assert(raft_mem->GetMaxIndex() > hard_state->meta().commit());
        assert(nullptr == soft_state);
        assert(false == mark_broadcast);
        assert(raft::MessageType::MsgApp == rsp_msg_type);
        assert(false == need_disk_replicate);
		assert(raft::ProgressState::REPLICATE == progress->GetState());
		assert(apprsp_msg.index() == progress->GetMatched());
		assert(raft_mem->GetMaxIndex() > progress->GetMatched());
		assert(raft_mem->GetMaxIndex() + 1 > progress->GetNext());
		assert(false == progress->IsPause());

        rsp_msg = raft_mem->BuildRspMsg(
                apprsp_msg, hard_state, soft_state, 
                apprsp_msg.from(), rsp_msg_type, true);
        assert(nullptr != rsp_msg);
        assert(rsp_msg_type == rsp_msg->type());
		assert(raft_mem->GetMaxIndex() == 
                rsp_msg->entries(rsp_msg->entries_size()-1).index());
		assert(raft_mem->GetTerm() == rsp_msg->log_term());
		assert(0 < rsp_msg->entries_size());
		assert(raft_mem->GetTerm() == rsp_msg->commit_term());
		assert(raft_mem->GetMaxIndex() + 1 == progress->GetNext());
        raft_mem->ApplyState(std::move(hard_state), std::move(soft_state));

        apprsp_msg.set_index(
                rsp_msg->entries(rsp_msg->entries_size()-1).index());
        hard_state = nullptr;
        soft_state = nullptr;
        std::tie(mark_broadcast, rsp_msg_type, need_disk_replicate)
            = raft_mem->Step(apprsp_msg, hard_state, soft_state);;
        assert(nullptr != hard_state);
        assert(0 == hard_state->entries_size());
        assert(hard_state->has_meta());
        assert(raft_mem->GetMaxIndex() == hard_state->meta().commit());
    }
}


TEST(LeaderTest, OutdateMsgAppReject)
{
    auto raft_mem = build_raft_mem(1, 3, 2, 5);
    assert(nullptr != raft_mem);

    make_fake_leader(*raft_mem);

    auto hard_state = cutils::make_unique<raft::HardState>();
    assert(nullptr != hard_state);
    {
        auto meta = hard_state->mutable_meta();
        assert(nullptr != meta);
        meta->set_term(raft_mem->GetTerm() + 1);
        meta->set_vote(0);
    }

    cutils::RandomStrGen<10, 30> gen;
    for (int idx = 0; idx < 10; ++idx) {
        add_entry(*hard_state, 
                hard_state->meta().term(), 
                raft_mem->GetMaxIndex() + 1 + idx, 
                gen.Next());
    }

    raft_mem->ApplyState(std::move(hard_state), nullptr);

    auto apprsp_msg = build_apprsp_msg(*raft_mem, 2);
    apprsp_msg.set_index(raft_mem->GetMaxIndex());
    apprsp_msg.set_reject(false);

	auto progress = raft_mem->GetProgress(apprsp_msg.from());
	assert(nullptr != progress);
	assert(raft::ProgressState::PROBE == progress->GetState());
	assert(false == progress->IsPause());
    
    hard_state = nullptr;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;
    bool need_disk_replicate = false;

    std::tie(mark_broadcast, rsp_msg_type, need_disk_replicate)
        = raft_mem->Step(apprsp_msg, hard_state, soft_state);
	assert(nullptr != hard_state);
	assert(0 == hard_state->entries_size());
	assert(hard_state->has_meta());
	assert(raft_mem->GetMaxIndex() == hard_state->meta().commit());

    assert(nullptr == soft_state);
    assert(raft_mem->GetMaxIndex() + 1 == progress->GetNext());
    assert(raft::MessageType::MsgApp == rsp_msg_type);
    assert(false == need_disk_replicate);
	assert(raft::ProgressState::REPLICATE == progress->GetState());
	assert(false == progress->IsPause());
	assert(raft_mem->GetMaxIndex() == progress->GetMatched());

	raft_mem->ApplyState(std::move(hard_state), std::move(soft_state));

	apprsp_msg.set_index(5);
	apprsp_msg.set_reject(true);
	apprsp_msg.set_reject_hint(4);

    std::tie(mark_broadcast, rsp_msg_type, need_disk_replicate)
        = raft_mem->Step(apprsp_msg, hard_state, soft_state);
    assert(nullptr == hard_state);
    assert(nullptr == soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgNull == rsp_msg_type);
    assert(false == need_disk_replicate);
	assert(raft::ProgressState::REPLICATE == progress->GetState());
	assert(raft_mem->GetMaxIndex() == progress->GetMatched());
	assert(raft_mem->GetMaxIndex() + 1 == progress->GetNext());
}


TEST(LeaderTest, MsgAppAccepted)
{
    auto raft_mem = build_raft_mem(1, 3, 2, 5);
    assert(nullptr != raft_mem);

    make_fake_leader(*raft_mem);

    auto hard_state = cutils::make_unique<raft::HardState>();
    assert(nullptr != hard_state);

    cutils::RandomStrGen<10, 30> gen;
    for (int idx = 0; idx < 10; ++idx) {
        add_entry(*hard_state, 
                raft_mem->GetTerm(), 
                raft_mem->GetMaxIndex() + 1 + idx, 
                gen.Next());
    }

    raft_mem->ApplyState(std::move(hard_state), nullptr);
    assert(raft_mem->GetMaxIndex() == 5 + 10);

	update_progress_next(*raft_mem);

    auto apprsp_msg = build_apprsp_msg(*raft_mem, 2);
    apprsp_msg.set_index(7);
    apprsp_msg.set_reject(false);

	auto progress = raft_mem->GetProgress(apprsp_msg.from());
	assert(nullptr != progress);
	assert(raft::ProgressState::PROBE == progress->GetState());
	assert(0 == progress->GetMatched());
	assert(raft_mem->GetMaxIndex() + 1 == progress->GetNext());
	assert(false == progress->IsPause());
	assert(5 == raft_mem->GetCommit());

    hard_state = nullptr;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;
    bool need_disk_replicate = false;

    std::tie(mark_broadcast, rsp_msg_type, need_disk_replicate)
        = raft_mem->Step(apprsp_msg, hard_state, soft_state);
    assert(nullptr != hard_state);
    assert(nullptr == soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgApp == rsp_msg_type);
    assert(false == need_disk_replicate);
	assert(raft::ProgressState::REPLICATE == progress->GetState());
	assert(7 == progress->GetMatched());
	assert(8 == progress->GetNext());
	assert(7 == hard_state->meta().commit());

	raft_mem->ApplyState(std::move(hard_state), std::move(soft_state));

	{
		auto hard_state = cutils::make_unique<raft::HardState>();
		assert(nullptr != hard_state);
		auto meta = hard_state->mutable_meta();
		assert(nullptr != meta);
		meta->set_term(raft_mem->GetTerm() + 1);
		meta->set_vote(raft_mem->GetSelfId());

        add_entry(*hard_state, 
                meta->term(), raft_mem->GetMaxIndex() + 0, gen.Next());
		assert(1 == hard_state->entries_size());

		auto soft_state = cutils::make_unique<raft::SoftState>();
		assert(nullptr != soft_state);

		soft_state->set_role(static_cast<uint32_t>(raft::RaftRole::LEADER));
		soft_state->set_leader_id(raft_mem->GetSelfId());
		
		raft_mem->ApplyState(std::move(hard_state), std::move(soft_state));
	}

	apprsp_msg.set_term(raft_mem->GetTerm());
	apprsp_msg.set_index(11);
	apprsp_msg.set_reject(false);
    std::tie(mark_broadcast, rsp_msg_type, need_disk_replicate)
        = raft_mem->Step(apprsp_msg, hard_state, soft_state);
	assert(nullptr == hard_state);
	assert(nullptr == soft_state);
	assert(raft::MessageType::MsgApp == rsp_msg_type);
	assert(false == need_disk_replicate);
	assert(raft::ProgressState::REPLICATE == progress->GetState());
	assert(11 == progress->GetMatched());
	assert(12 == progress->GetNext());
	assert(7 == raft_mem->GetCommit());

	apprsp_msg.set_index(raft_mem->GetMaxIndex());
	apprsp_msg.set_reject(false);
    std::tie(mark_broadcast, rsp_msg_type, need_disk_replicate) 
        = raft_mem->Step(apprsp_msg, hard_state, soft_state);
	assert(nullptr != hard_state);
	assert(0 == hard_state->entries_size());
	assert(hard_state->has_meta());
	assert(raft_mem->GetMaxIndex() == hard_state->meta().commit());

	assert(nullptr == soft_state);
	assert(raft::MessageType::MsgApp == rsp_msg_type);
	assert(false == need_disk_replicate);
	assert(raft_mem->GetMaxIndex() == progress->GetMatched());
	assert(raft_mem->GetMaxIndex() + 1 == progress->GetNext());
	assert(raft_mem->GetMaxIndex() == hard_state->meta().commit());

	raft_mem->ApplyState(std::move(hard_state), std::move(soft_state));

    std::tie(mark_broadcast, rsp_msg_type, need_disk_replicate) 
        = raft_mem->Step(apprsp_msg, hard_state, soft_state);
	assert(nullptr == hard_state);
	assert(nullptr == soft_state);
	assert(raft::MessageType::MsgNull == rsp_msg_type);
	assert(false == need_disk_replicate);
}



