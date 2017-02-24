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
    bool need_disk_replicate = false;

    std::tie(hard_state, 
            soft_state, mark_broadcast, rsp_msg_type, 
            need_disk_replicate) = 
        raft_mem->Step(*null_msg, nullptr, nullptr);
    assert(nullptr == hard_state);
    assert(nullptr == soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgNull == rsp_msg_type);
    assert(false == need_disk_replicate);
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
    bool need_disk_replicate = false;

    // case 1
    {
        // revert to follower
        null_msg->set_term(raft_mem->GetTerm() + 1);
        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type, 
                need_disk_replicate) = 
            raft_mem->Step(*null_msg, nullptr, nullptr);
        assert(nullptr != hard_state);
        assert(nullptr != soft_state);
        assert(false == mark_broadcast);
        assert(raft::MessageType::MsgNull == rsp_msg_type);
        assert(false == need_disk_replicate);
        assert(hard_state->has_meta());
        assert(hard_state->meta().has_term());
        assert(null_msg->term() == hard_state->meta().term());
        assert(raft::RaftRole::FOLLOWER == 
                static_cast<raft::RaftRole>(soft_state->role()));
        assert(false == soft_state->has_leader_id());
    }

    // case 2
    {
        null_msg->set_term(raft_mem->GetTerm() - 1);
        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type, 
                need_disk_replicate) = 
            raft_mem->Step(*null_msg, nullptr, nullptr);
        assert(nullptr == hard_state);
        assert(nullptr == soft_state);
        assert(false == mark_broadcast);
        assert(raft::MessageType::MsgHeartbeat == rsp_msg_type);
        assert(false == need_disk_replicate);

        null_msg->set_index(1);
        auto rsp_msg = raft_mem->BuildRspMsg(
                *null_msg, nullptr, nullptr, null_msg->from(), rsp_msg_type);
        assert(nullptr != rsp_msg);
        assert(rsp_msg_type == rsp_msg->type());
        assert(raft_mem->GetTerm() == rsp_msg->term());
        printf ( "req_msg.index %d maxindex %d rsp_msg->index %d\n", 
                static_cast<int>(null_msg->index()), 
                static_cast<int>(raft_mem->GetMaxIndex()), 
                static_cast<int>(rsp_msg->index()));
        assert(raft_mem->GetMaxIndex() == rsp_msg->index());
        assert(rsp_msg->has_log_term());
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
    auto raft_mem = build_raft_mem(3, 1, raft::RaftRole::LEADER);
    assert(nullptr != raft_mem);

	set_progress_replicate(*raft_mem);
    for (int testime = 0; testime < 10; ++testime) {
        auto prop_msg = build_from_msg(
                *raft_mem, raft::MessageType::MsgProp, 2);
        assert(nullptr != prop_msg);
        prop_msg->set_index(raft_mem->GetMaxIndex());
        add_entries(prop_msg, raft_mem->GetTerm(), prop_msg->index() + 1);

        std::unique_ptr<raft::HardState> hard_state;
        std::unique_ptr<raft::SoftState> soft_state;
        bool mark_broadcast = false;
        auto rsp_msg_type = raft::MessageType::MsgNull;
        bool need_disk_replicate = false;

        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type, 
                need_disk_replicate) = 
            raft_mem->Step(*prop_msg, nullptr, nullptr);
        assert(nullptr != hard_state);
        assert(nullptr == soft_state);
        assert(true == mark_broadcast);
        assert(raft::MessageType::MsgApp == rsp_msg_type);
        assert(false == need_disk_replicate);

        assert(prop_msg->entries_size() == hard_state->entries_size());
        assert(prop_msg->entries(0) == hard_state->entries(0));

		auto vec_rsp_msg = raft_mem->BuildBroadcastRspMsg(
				*prop_msg, hard_state, soft_state, rsp_msg_type);
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
			assert(rsp_msg->entries(0).index() == raft_mem->GetMaxIndex() + 1);
			assert(raft_mem->GetCommit() == rsp_msg->commit_index());
			assert(raft_mem->GetTerm() == rsp_msg->commit_term());
		}

        raft_mem->ApplyState(std::move(hard_state), std::move(soft_state));
    }

    // batch
    for (int testime = 0; testime < 10; ++testime) {
        auto prop_msg = build_from_msg(
                *raft_mem, raft::MessageType::MsgProp, 2);
        assert(nullptr != prop_msg);
        prop_msg->set_index(raft_mem->GetMaxIndex());

        for (int entries_size = 0; 
                entries_size <= testime; ++entries_size) {
            add_entries(prop_msg, 
                    raft_mem->GetTerm(), prop_msg->index() + entries_size + 1);
        }

        std::unique_ptr<raft::HardState> hard_state;
        std::unique_ptr<raft::SoftState> soft_state;
        bool mark_broadcast = false;
        auto rsp_msg_type = raft::MessageType::MsgNull;
        bool need_disk_replicate = false;

        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type, 
                need_disk_replicate) = 
            raft_mem->Step(*prop_msg, nullptr, nullptr);
        assert(nullptr != hard_state);
        assert(nullptr == soft_state);
        assert(true == mark_broadcast);
        assert(raft::MessageType::MsgApp == rsp_msg_type);
        assert(false == need_disk_replicate);

        assert(prop_msg->entries_size() == hard_state->entries_size());
        for (int idx = 0; idx < prop_msg->entries_size(); ++idx) {
            assert(prop_msg->entries(idx) == hard_state->entries(idx));
        }
        
		auto vec_rsp_msg = raft_mem->BuildBroadcastRspMsg(
				*prop_msg, hard_state, soft_state, rsp_msg_type);
		assert(size_t{2} == vec_rsp_msg.size());
		for (const auto& rsp_msg : vec_rsp_msg) {
			assert(nullptr != rsp_msg);
			assert(rsp_msg_type == rsp_msg->type());
			assert(rsp_msg->term() == raft_mem->GetTerm());
			assert(rsp_msg->index() == raft_mem->GetMaxIndex());
			assert(rsp_msg->log_term() == raft_mem->GetTerm());
			assert(prop_msg->entries_size() == rsp_msg->entries_size());
			for (int idx = 0; idx < rsp_msg->entries_size(); ++idx) {
				assert(prop_msg->entries(idx) == rsp_msg->entries(idx));
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
        auto raft_mem = build_raft_mem(3, 1, raft::RaftRole::LEADER);
        assert(nullptr != raft_mem);

        auto hard_state = cutils::make_unique<raft::HardState>();
        assert(nullptr != hard_state);
        for (int idx = 0; idx < 10; ++idx) {
            add_entries(hard_state, raft_mem->GetTerm(), raft_mem->GetMaxIndex() + 1 + idx);
        }

        raft_mem->ApplyState(std::move(hard_state), nullptr);
        assert(raft_mem->GetMaxIndex() == 1 + 10);

		update_progress_next(*raft_mem);

        auto app_rsp_msg = build_from_msg(
                *raft_mem, raft::MessageType::MsgAppResp, 2);
        assert(nullptr != app_rsp_msg);
        app_rsp_msg->set_index(raft_mem->GetMaxIndex());
		app_rsp_msg->set_reject(true);
		app_rsp_msg->set_reject_hint(raft_mem->GetMaxIndex() / 2);

		auto progress = raft_mem->GetProgress(app_rsp_msg->from());
		assert(nullptr != progress);
		assert(raft::ProgressState::PROBE == progress->GetState());
		assert(raft_mem->GetMaxIndex() + 1 == progress->GetNext());
		assert(false == progress->IsPause());

        hard_state = nullptr;
        std::unique_ptr<raft::SoftState> soft_state;
        bool mark_broadcast = false;
        auto rsp_msg_type = raft::MessageType::MsgNull;
        bool need_disk_replicate = false;

        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type, 
                need_disk_replicate) = 
            raft_mem->Step(*app_rsp_msg, nullptr, nullptr);
        assert(nullptr == hard_state);
        assert(nullptr == soft_state);
        assert(false == mark_broadcast);
        assert(raft::MessageType::MsgApp == rsp_msg_type);
        assert(false == need_disk_replicate);

		assert(progress->GetNext() == app_rsp_msg->reject_hint() + 1);
		assert(progress->GetMatched() == 0);
		assert(raft::ProgressState::PROBE == progress->GetState());
		assert(false == progress->IsPause());

        auto rsp_msg = raft_mem->BuildRspMsg(
                *app_rsp_msg, nullptr, nullptr, app_rsp_msg->from(), rsp_msg_type);
        assert(nullptr != rsp_msg);
        assert(rsp_msg_type == rsp_msg->type());
		assert(progress->GetNext() == 
				rsp_msg->entries(rsp_msg->entries_size()-1).index() + 1);
		assert(progress->GetNext() == raft_mem->GetMaxIndex() + 1);
		assert(0 < rsp_msg->entries_size());
		assert(raft::ProgressState::PROBE == progress->GetState());
		assert(progress->IsPause());

		uint64_t max_index = raft_mem->GetMaxIndex();
		{
			auto prop_msg = build_from_msg(
					*raft_mem, raft::MessageType::MsgProp, 2);
			assert(nullptr != prop_msg);
			prop_msg->set_index(raft_mem->GetMaxIndex());
			add_entries(prop_msg, raft_mem->GetTerm(), prop_msg->index() + 1);

			std::unique_ptr<raft::HardState> hard_state;
			std::unique_ptr<raft::SoftState> soft_state;
			bool mark_broadcast = false;
			auto rsp_msg_type = raft::MessageType::MsgNull;
			bool need_disk_replicate = false;
			
			std::tie(hard_state, 
					soft_state, mark_broadcast, rsp_msg_type, 
					need_disk_replicate) = raft_mem->Step(*prop_msg, nullptr, nullptr);
			assert(nullptr != hard_state);
			assert(nullptr == soft_state);
			assert(true == mark_broadcast);
			assert(raft::MessageType::MsgApp == rsp_msg_type);
			assert(false == need_disk_replicate);

			// stop by pause
			auto rsp_msg = raft_mem->BuildRspMsg(
					*prop_msg, nullptr, nullptr, prop_msg->from(), rsp_msg_type);
			assert(nullptr == rsp_msg);
		}
		assert(max_index == raft_mem->GetMaxIndex());

		app_rsp_msg->set_index(rsp_msg->entries(rsp_msg->entries_size()-1).index());
		assert(app_rsp_msg->index() > rsp_msg->index());
		assert(app_rsp_msg->index() == raft_mem->GetMaxIndex());
		app_rsp_msg->set_reject(false);

        std::tie(hard_state, 
                soft_state, mark_broadcast, rsp_msg_type, 
                need_disk_replicate) = 
            raft_mem->Step(*app_rsp_msg, nullptr, nullptr);
        assert(nullptr != hard_state);
		assert(0 == hard_state->entries_size());
		assert(hard_state->has_meta());
		assert(raft_mem->GetMaxIndex() == hard_state->meta().commit());
        assert(nullptr == soft_state);
        assert(false == mark_broadcast);
        assert(raft::MessageType::MsgApp == rsp_msg_type);
        assert(false == need_disk_replicate);
		assert(raft::ProgressState::REPLICATE == progress->GetState());
		assert(app_rsp_msg->index() == progress->GetMatched());
		assert(raft_mem->GetMaxIndex() == progress->GetMatched());
		assert(raft_mem->GetMaxIndex() + 1 == progress->GetNext());
		assert(false == progress->IsPause());

        rsp_msg = raft_mem->BuildRspMsg(
                *app_rsp_msg, hard_state, soft_state, app_rsp_msg->from(), rsp_msg_type);
        assert(nullptr != rsp_msg);
        assert(rsp_msg_type == rsp_msg->type());
		assert(raft_mem->GetMaxIndex() == rsp_msg->index());
		assert(raft_mem->GetTerm() == rsp_msg->log_term());
		assert(0 == rsp_msg->entries_size());
		assert(raft_mem->GetMaxIndex() == rsp_msg->commit_index());
		assert(raft_mem->GetTerm() == rsp_msg->commit_term());
		assert(raft_mem->GetMaxIndex() + 1 == progress->GetNext());
    }
}

TEST(LeaderTest, OutdateMsgAppReject)
{
    auto raft_mem = build_raft_mem(3, 1, raft::RaftRole::LEADER);
    assert(nullptr != raft_mem);

    auto hard_state = cutils::make_unique<raft::HardState>();
    assert(nullptr != hard_state);
    {
        auto meta = hard_state->mutable_meta();
        assert(nullptr != meta);
        meta->set_term(raft_mem->GetTerm() + 1);
        meta->set_vote(0);
    }

    for (int idx = 0; idx < 10; ++idx) {
        add_entries(hard_state, 
                hard_state->meta().term(), raft_mem->GetMaxIndex() + 1 + idx);
    }

    raft_mem->ApplyState(std::move(hard_state), nullptr);

    auto app_rsp_msg = build_from_msg(
            *raft_mem, raft::MessageType::MsgAppResp, 2);
    assert(nullptr != app_rsp_msg);
    app_rsp_msg->set_index(raft_mem->GetMaxIndex());
    app_rsp_msg->set_reject(false);

	auto progress = raft_mem->GetProgress(app_rsp_msg->from());
	assert(nullptr != progress);
	assert(raft::ProgressState::PROBE == progress->GetState());
	assert(false == progress->IsPause());
    
    hard_state = nullptr;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;
    bool need_disk_replicate = false;

    std::tie(hard_state, 
            soft_state, mark_broadcast, rsp_msg_type, 
            need_disk_replicate) = 
        raft_mem->Step(*app_rsp_msg, nullptr, nullptr);
	assert(nullptr != hard_state);
	assert(0 == hard_state->entries_size());
	assert(hard_state->has_meta());
	assert(raft_mem->GetMaxIndex() == hard_state->meta().commit());

    assert(nullptr == soft_state);
    assert(raft::MessageType::MsgApp == rsp_msg_type);
    assert(false == need_disk_replicate);
	assert(raft::ProgressState::REPLICATE == progress->GetState());
	assert(false == progress->IsPause());
	assert(raft_mem->GetMaxIndex() == progress->GetMatched());

	raft_mem->ApplyState(std::move(hard_state), std::move(soft_state));

	app_rsp_msg->set_index(5);
	app_rsp_msg->set_reject(true);
	app_rsp_msg->set_reject_hint(4);

    std::tie(hard_state, 
            soft_state, mark_broadcast, rsp_msg_type, 
            need_disk_replicate) = 
        raft_mem->Step(*app_rsp_msg, nullptr, nullptr);
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

	update_progress_next(*raft_mem);

    auto app_rsp_msg = build_from_msg(
            *raft_mem, raft::MessageType::MsgAppResp, 2);
    assert(nullptr != app_rsp_msg);
    app_rsp_msg->set_index(3);
    app_rsp_msg->set_reject(false);

	auto progress = raft_mem->GetProgress(app_rsp_msg->from());
	assert(nullptr != progress);
	assert(raft::ProgressState::PROBE == progress->GetState());
	assert(0 == progress->GetMatched());
	assert(raft_mem->GetMaxIndex() + 1 == progress->GetNext());
	assert(false == progress->IsPause());
	assert(1 == raft_mem->GetCommit());

    hard_state = nullptr;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;
    bool need_disk_replicate = false;

    std::tie(hard_state, 
            soft_state, mark_broadcast, rsp_msg_type, 
            need_disk_replicate) = 
        raft_mem->Step(*app_rsp_msg, nullptr, nullptr);
    assert(nullptr != hard_state);
    assert(nullptr == soft_state);
    assert(false == mark_broadcast);
    assert(raft::MessageType::MsgApp == rsp_msg_type);
    assert(false == need_disk_replicate);
	assert(raft::ProgressState::REPLICATE == progress->GetState());
	assert(3 == progress->GetMatched());
	assert(4 == progress->GetNext());
	assert(3 == hard_state->meta().commit());

	raft_mem->ApplyState(std::move(hard_state), std::move(soft_state));

	{
		auto hard_state = cutils::make_unique<raft::HardState>();
		assert(nullptr != hard_state);
		auto meta = hard_state->mutable_meta();
		assert(nullptr != meta);
		meta->set_term(raft_mem->GetTerm() + 1);
		meta->set_vote(raft_mem->GetSelfId());

		add_entries(hard_state, meta->term(), raft_mem->GetMaxIndex() + 1);
		assert(1 == hard_state->entries_size());

		auto soft_state = cutils::make_unique<raft::SoftState>();
		assert(nullptr != soft_state);

		soft_state->set_role(static_cast<uint32_t>(raft::RaftRole::LEADER));
		soft_state->set_leader_id(raft_mem->GetSelfId());
		
		raft_mem->ApplyState(std::move(hard_state), std::move(soft_state));
	}

	app_rsp_msg->set_term(raft_mem->GetTerm());
	app_rsp_msg->set_index(5);
	app_rsp_msg->set_reject(false);
	std::tie(hard_state, 
			soft_state, mark_broadcast, rsp_msg_type, 
			need_disk_replicate) = raft_mem->Step(*app_rsp_msg, nullptr, nullptr);
	assert(nullptr == hard_state);
	assert(nullptr == soft_state);
	assert(raft::MessageType::MsgApp == rsp_msg_type);
	assert(false == need_disk_replicate);
	assert(raft::ProgressState::REPLICATE == progress->GetState());
	assert(5 == progress->GetMatched());
	assert(6 == progress->GetNext());
	assert(3 == raft_mem->GetCommit());

	app_rsp_msg->set_index(raft_mem->GetMaxIndex());
	app_rsp_msg->set_reject(false);
	std::tie(hard_state, soft_state, mark_broadcast, rsp_msg_type, 
			need_disk_replicate) = raft_mem->Step(*app_rsp_msg, nullptr, nullptr);
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

	std::tie(hard_state, soft_state, mark_broadcast, rsp_msg_type, 
			need_disk_replicate) = raft_mem->Step(*app_rsp_msg, nullptr, nullptr);
	assert(nullptr == hard_state);
	assert(nullptr == soft_state);
	assert(raft::MessageType::MsgNull == rsp_msg_type);
	assert(false == need_disk_replicate);
}
