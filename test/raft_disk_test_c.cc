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


