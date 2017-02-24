#include <cassert>
#include "gtest/gtest.h"
#include "raft_mem.h"
#include "raft.pb.h"
#include "mem_utils.h"
#include "test_helper.h"
#include "progress.h"


TEST(TmpCacheTest, SimpleConstruct)
{
    raft::TmpEntryCache cache;
    assert(size_t{0} == cache.Size());
}

TEST(TmpCacheTest, InsertTest)
{
    raft::TmpEntryCache cache;
    assert(size_t{0} == cache.Size());

    raft::Entry test_entry;
    test_entry.set_term(1);
    for (size_t idx = 0; idx < 100; ++idx) {
        test_entry.set_index(idx + 1);
        cache.Insert(test_entry);
        assert(cache.Size() == idx + 1);
    }

    test_entry.set_term(2);
    test_entry.set_index(101);
    cache.Insert(test_entry);
    assert(cache.Size() == size_t{1});
}

TEST(TmpCacheTest, MayInvalid)
{
    raft::TmpEntryCache cache;
    assert(size_t{0} == cache.Size());

    cache.MayInvalid(1);
    assert(size_t{0} == cache.Size());

    raft::Entry test_entry;
    test_entry.set_term(2);
    for (size_t idx = 0; idx < 10; ++idx) {
        test_entry.set_index(idx + 1);
        cache.Insert(test_entry);
    }
    assert(size_t{10} == cache.Size());
    
    cache.MayInvalid(3);
    assert(size_t{0} == cache.Size());
    test_entry.set_term(3);
    cache.Insert(test_entry);
    assert(size_t{1} == cache.Size());
}

TEST(TmpCacheTest, SimpleExtract)
{
    auto cache = cutils::make_unique<raft::TmpEntryCache>();
    assert(nullptr != cache);

    raft::Entry test_entry;
    test_entry.set_term(1);
    for (size_t idx = 1; idx <= 10; ++idx) {
        test_entry.set_index(idx + idx);
        cache->Insert(test_entry);
    }

    assert(cache->Size() == size_t{10});

    auto hard_state = cutils::make_unique<raft::HardState>();
    assert(nullptr != hard_state);
    // case 1
    {
        // extract nothing;
        cache->Extract(1, *hard_state);
        assert(0 == hard_state->entries_size());
        assert(size_t{10} == cache->Size());
    }

    // case 2
    {
        // extract index 6;
        auto entry = hard_state->add_entries();
        assert(nullptr != entry);
        entry->set_term(1);
        entry->set_index(5);

        assert(size_t{10} == cache->Size());
        assert(1 == hard_state->entries_size());
        cache->Extract(1, *hard_state);
        assert(2 == hard_state->entries_size());
        assert(hard_state->entries(1).index() == 6);
        assert(hard_state->entries(1).term() == 1);
        assert(size_t{7} == cache->Size());
    }
}


TEST(TmpCacheTest, RaftTest)
{
	auto raft_mem = build_raft_mem(3, 1, raft::RaftRole::FOLLOWER);
	assert(nullptr != raft_mem);

	auto cache = raft_mem->GetTmpEntryCache();
	assert(nullptr != cache);
	assert(size_t{0} == cache->Size());

	auto app_msg = build_from_msg(
			*raft_mem, raft::MessageType::MsgApp, 2);
	assert(nullptr != app_msg);
	app_msg->set_index(2);
	app_msg->set_log_term(raft_mem->GetTerm());
	add_entries(app_msg, raft_mem->GetTerm(), 3);

	std::unique_ptr<raft::HardState> hard_state;
	std::unique_ptr<raft::SoftState> soft_state;
	bool mark_broadcast = false;
	auto rsp_msg_type = raft::MessageType::MsgNull;
	bool need_disk_replicate = false;

	std::tie(hard_state, soft_state, mark_broadcast, rsp_msg_type, 
			need_disk_replicate) = raft_mem->Step(*app_msg, nullptr, nullptr);
	assert(nullptr == hard_state);
	assert(nullptr != soft_state);
	assert(false == mark_broadcast);
	assert(raft::MessageType::MsgAppResp == rsp_msg_type);
	assert(false == need_disk_replicate);

	assert(size_t{1} == cache->Size());

	app_msg = build_from_msg(
			*raft_mem, raft::MessageType::MsgApp, 2);
	assert(nullptr != app_msg);
	app_msg->set_index(3);
	app_msg->set_log_term(raft_mem->GetTerm());
	add_entries(app_msg, raft_mem->GetTerm(), 4);

	std::tie(hard_state, soft_state, mark_broadcast, rsp_msg_type, 
			need_disk_replicate) = raft_mem->Step(*app_msg, nullptr, std::move(soft_state));
	assert(nullptr == hard_state);
	assert(nullptr != soft_state);
	assert(false == mark_broadcast);
	assert(raft::MessageType::MsgAppResp == rsp_msg_type);
	assert(false == need_disk_replicate);
	assert(size_t{2} == cache->Size());

	raft_mem->ApplyState(nullptr, std::move(soft_state));

	app_msg = build_from_msg(
			*raft_mem, raft::MessageType::MsgApp, 2);
	assert(nullptr != app_msg);
	app_msg->set_index(1);
	app_msg->set_log_term(raft_mem->GetTerm());
	add_entries(app_msg, raft_mem->GetTerm(), 2);

	std::tie(hard_state, soft_state, mark_broadcast, rsp_msg_type, 
			need_disk_replicate) = raft_mem->Step(*app_msg, nullptr, nullptr);
	assert(nullptr != hard_state);
	assert(3 == hard_state->entries_size());
	assert(nullptr == soft_state);
	assert(false == mark_broadcast);
	assert(raft::MessageType::MsgAppResp == rsp_msg_type);
	assert(false == need_disk_replicate);

	assert(size_t{0} == cache->Size());
	raft_mem->ApplyState(std::move(hard_state), nullptr);
	assert(uint64_t{4} == raft_mem->GetMaxIndex());

}


