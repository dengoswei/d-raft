#include "gtest/gtest.h"
#include "raft_mem.h"
#include "raft.pb.h"
#include "mem_utils.h"
#include "test_helper.h"
#include "replicate.h"




TEST(RaftDiskTest, SimpleConstruct)
{
    FakeDiskStorage storage;

    raft::RaftDisk raft_disk(1, 1, 
            [&](uint64_t logid, uint64_t log_index, int entries_size)
                -> std::tuple<int, std::unique_ptr<raft::HardState>> {
                return storage.Read(logid, log_index, entries_size);
            });
    assert(raft::RaftRole::FOLLOWER == raft_disk.GetRole());
    assert(0 == raft_disk.GetTerm());
    assert(0 == raft_disk.GetCommit());
    assert(0 == raft_disk.GetMinIndex());
    assert(0 == raft_disk.GetMaxIndex());
}

TEST(RaftDiskTest, UpdateTest)
{
    FakeDiskStorage storage;

    auto raft_disk = build_raft_disk(1, storage);
    assert(nullptr != raft_disk);

    assert(true == raft_disk->Update(
                raft::RaftRole::LEADER, 1, 0, 0, 0));
    assert(raft::RaftRole::LEADER == raft_disk->GetRole());
    assert(1 == raft_disk->GetTerm());
}

TEST(RaftDiskTest, StepHB)
{
    FakeDiskStorage storage;

    {
        auto hard_state = cutils::make_unique<raft::HardState>();
        assert(nullptr != hard_state);
        for (int testime = 0; testime < 100; ++testime) {
            add_entries(hard_state, 2, 1 + testime);
        }
        assert(100 == hard_state->entries_size());

        assert(0 == storage.Write(*hard_state));
    }

    // case 1
    {
        auto raft_disk = build_raft_disk(1, storage);
        assert(nullptr != raft_disk);

        assert(true == raft_disk->Update(
                    raft::RaftRole::LEADER, 2, 1, 1, 100));

        int ret = 0;
        std::unique_ptr<raft::Message> hb_msg;
        std::tie(ret, hb_msg) = raft_disk->Step(
                2, 101, true, raft::MessageType::MsgHeartbeat);
        assert(0 == ret);
        assert(nullptr != hb_msg);
        assert(raft::MessageType::MsgHeartbeat == hb_msg->type());
        assert(51 == hb_msg->index());
        assert(2 == hb_msg->term());
        assert(2 == hb_msg->log_term());
        assert(2 == hb_msg->to());

        std::tie(ret, hb_msg) = raft_disk->Step(
                2, 51, false, raft::MessageType::MsgHeartbeat);
        assert(0 == ret);
        assert(nullptr != hb_msg);
        assert(raft::MessageType::MsgHeartbeat == hb_msg->type());
        assert(76 == hb_msg->index());

        std::tie(ret, hb_msg) = raft_disk->Step(
                2, 76, false, raft::MessageType::MsgHeartbeat);
        assert(0 == ret);
        assert(nullptr != hb_msg);
        assert(raft::MessageType::MsgHeartbeat == hb_msg->type());
        assert(88 == hb_msg->index());
    }

    // case 2
    {

    }
}

TEST(RaftDiskTest, StepApp)
{
    FakeDiskStorage storage;

    {
        auto hard_state = cutils::make_unique<raft::HardState>();
        assert(nullptr != hard_state);
        for (int testime = 0; testime < 100; ++testime) {
            add_entries(hard_state, 2, 1 + testime);
        }

        assert(100 == hard_state->entries_size());
        assert(0 == storage.Write(*hard_state));
    }

    {
        auto raft_disk = build_raft_disk(1, storage);
        assert(nullptr != raft_disk);

        assert(true == raft_disk->Update(
                    raft::RaftRole::LEADER, 2, 1, 1, 100));

        int ret = 0;
        std::unique_ptr<raft::Message> app_msg;
        std::tie(ret, app_msg) = raft_disk->Step(
                2, 50, false, raft::MessageType::MsgApp);
        assert(0 == ret);
        assert(nullptr != app_msg);
        assert(raft::MessageType::MsgApp == app_msg->type());
        assert(50 == app_msg->index());
        assert(2 == app_msg->term());
        assert(2 == app_msg->log_term());
        assert(2 == app_msg->to());
        assert(10 == app_msg->entries_size());

        std::tie(ret, app_msg) = raft_disk->Step(
                2, 71, false, raft::MessageType::MsgApp);
        assert(0 == ret);
        assert(nullptr != app_msg);
        assert(raft::MessageType::MsgApp == app_msg->type());
        assert(71 == app_msg->index());
    }
}
