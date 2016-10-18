#include "gtest/gtest.h"
#include "raft.pb.h"
#include "mem_utils.h"
#include "test_helper.h"


TEST(RaftDiskCatchUpTest, SimpleConstruct)
{
    FakeDiskStorage storage;

    raft::RaftDiskCatchUp raft_disk_c(
            1, 1, 2, 1, 10, 1, 
            [&](uint64_t logid, uint64_t log_index, int entries_size)
                -> std::tuple<int, std::unique_ptr<raft::HardState>> {
                return storage.Read(logid, log_index, entries_size);
            });
}


TEST(RaftDiskCatchUpTest, StepInvalidMsg)
{
    // case 1: invalid term
    {
        FakeDiskStorage storage;
        auto raft_disk_c = build_raft_disk_c(
                1, 2, 4, 10, 1, storage);
        assert(nullptr != raft_disk_c);

        raft::Message invalid_term;
        invalid_term.set_disk_mark(true);
        invalid_term.set_term(3);
        int ret = 0; 
        std::unique_ptr<raft::Message> rsp_msg;

        std::tie(ret, rsp_msg) = raft_disk_c->Step(invalid_term);
        assert(0 == ret);
        assert(nullptr == rsp_msg);

        invalid_term.set_term(5);
        std::tie(ret, rsp_msg) = raft_disk_c->Step(invalid_term);
        assert(0 == ret);
        assert(nullptr == rsp_msg);
    }

    // case 2: dont have disk_mark
    {
        FakeDiskStorage storage;
        auto raft_disk_c = build_raft_disk_c(
                1, 2, 4, 10, 1, storage);
        assert(nullptr != raft_disk_c);

        raft::Message invalid_msg;
        invalid_msg.set_disk_mark(false);
        int ret = 0;
        std::unique_ptr<raft::Message> rsp_msg;
        std::tie(ret, rsp_msg) = raft_disk_c->Step(invalid_msg);
        assert(-1 == ret);
        assert(nullptr == rsp_msg);
    }

    // case 3
    {
        // non app resp or heartbeat rsp msg;
        uint64_t test_term = 4;
        FakeDiskStorage storage;
        auto raft_disk_c = build_raft_disk_c(
                1, 2, test_term, 10, 1, storage);
        assert(nullptr != raft_disk_c);

        raft::Message invalid_msg;
        invalid_msg.set_disk_mark(true);
        invalid_msg.set_term(test_term);
        invalid_msg.set_logid(1);
        invalid_msg.set_from(2);
        invalid_msg.set_to(1);
        invalid_msg.set_type(raft::MessageType::MsgApp);

        int ret = 0;
        std::unique_ptr<raft::Message> rsp_msg;
        std::tie(ret, rsp_msg) = raft_disk_c->Step(invalid_msg);
        assert(0 == ret);
        assert(nullptr == rsp_msg);
    }
}

TEST(RaftDiskCatchUpTest, StepMsgHeartBeat)
{
    const uint64_t test_term = 4;
    FakeDiskStorage storage;

    auto raft_disk_c = build_raft_disk_c(
            1, 2, test_term, 10, 1, storage);
    assert(nullptr != raft_disk_c);

    // fake storage
    {
        auto hs = cutils::make_unique<raft::HardState>();
        assert(nullptr != hs);

        for (size_t idx = 1; idx <= 10; ++idx) {
            add_entries(hs, test_term, idx);
        }

        assert(10 == hs->entries_size());
        assert(0 == storage.Write(*hs));
    }

    raft::Message hb_msg;
    hb_msg.set_disk_mark(true);
    hb_msg.set_logid(1);
    hb_msg.set_from(2);
    hb_msg.set_to(1);
    hb_msg.set_term(test_term);
    hb_msg.set_index(12);
    hb_msg.set_reject(true);
    hb_msg.set_type(raft::MessageType::MsgHeartbeatResp);

    int ret = 0;
    std::unique_ptr<raft::Message> rsp_msg;

    std::tie(ret, rsp_msg) = raft_disk_c->Step(hb_msg);
    assert(0 == ret);
    assert(nullptr != rsp_msg);
    assert(raft::MessageType::MsgHeartbeat == rsp_msg->type());
    assert(7 == rsp_msg->index());
    assert(rsp_msg->has_disk_mark());
    assert(true == rsp_msg->disk_mark());
    assert(test_term == rsp_msg->log_term());
    assert(test_term == rsp_msg->term());

    hb_msg.set_index(7);
    std::tie(ret, rsp_msg) = raft_disk_c->Step(hb_msg);
    assert(0 == ret);
    assert(nullptr != rsp_msg);
    assert(raft::MessageType::MsgHeartbeat == rsp_msg->type());
    assert(4 == rsp_msg->index());
    assert(rsp_msg->disk_mark());

    hb_msg.set_index(rsp_msg->index());
    hb_msg.set_reject(false);
    std::tie(ret, rsp_msg) = raft_disk_c->Step(hb_msg);
    assert(0 == ret);
    assert(nullptr != rsp_msg);
    assert(raft::MessageType::MsgHeartbeat == rsp_msg->type());
    assert(5 == rsp_msg->index());
    assert(rsp_msg->disk_mark());

    hb_msg.set_index(rsp_msg->index());
    std::tie(ret, rsp_msg) = raft_disk_c->Step(hb_msg);
    assert(0 == ret);
    assert(nullptr != rsp_msg);
    assert(6 == rsp_msg->index());
    assert(rsp_msg->disk_mark());

    hb_msg.set_index(rsp_msg->index());
    std::tie(ret, rsp_msg) = raft_disk_c->Step(hb_msg);
    assert(0 == ret);
    assert(nullptr != rsp_msg);
    assert(raft::MessageType::MsgApp == rsp_msg->type());
    assert(hb_msg.index() == rsp_msg->index());
    assert(test_term == rsp_msg->log_term());
    assert(0 < rsp_msg->entries_size());
    assert(10 == rsp_msg->entries(rsp_msg->entries_size()-1).index());

    printf ( "rsp_msg->entries_size %d index %u\n", 
            rsp_msg->entries_size(), (uint32_t)rsp_msg->entries(rsp_msg->entries_size() - 1).index());

    auto app_rsp_msg = hb_msg;
    app_rsp_msg.set_index(10+1);
    app_rsp_msg.set_type(raft::MessageType::MsgAppResp);
    app_rsp_msg.set_reject(false);
    std::tie(ret, rsp_msg) = raft_disk_c->Step(app_rsp_msg);
    assert(0 == ret);
    assert(nullptr == rsp_msg);
}

