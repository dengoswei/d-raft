#include "gtest/gtest.h"
#include "raft.pb.h"
#include "mem_utils.h"
#include "test_helper.h"


TEST(RaftDiskCatchUpTest, SimpleConstruct)
{
    FakeDiskStorage storage;

    raft::RaftDiskCatchUp raft_disk_c(
            1, 1, 1, 10, 1, 10, 
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
        auto raft_disk_c = build_raft_disk_c(1, 4, 10, 1, 10, storage);
        assert(nullptr != raft_disk_c);

        raft::Message invalid_term;
        invalid_term.set_disk_mark(true);
        invalid_term.set_term(3);
        int ret = 0; 
        std::unique_ptr<raft::Message> rsp_msg;
		bool need_mem = false;

        std::tie(ret, rsp_msg, need_mem) = raft_disk_c->Step(invalid_term);
        assert(0 == ret);
        assert(nullptr == rsp_msg);
		assert(false == need_mem);

        invalid_term.set_term(5);
        std::tie(ret, rsp_msg, need_mem) = raft_disk_c->Step(invalid_term);
        assert(0 == ret);
        assert(nullptr == rsp_msg);
		assert(false == need_mem);
    }

    // case 2: dont have disk_mark
    {
        FakeDiskStorage storage;
        auto raft_disk_c = build_raft_disk_c(1, 4, 10, 1, 10, storage);
        assert(nullptr != raft_disk_c);

        raft::Message invalid_msg;
        invalid_msg.set_disk_mark(false);
        int ret = 0;
        std::unique_ptr<raft::Message> rsp_msg;
		bool need_mem = false;
        std::tie(ret, rsp_msg, need_mem) = raft_disk_c->Step(invalid_msg);
        assert(-1 == ret);
        assert(nullptr == rsp_msg);
		assert(false == need_mem);
    }

    // case 3
    {
        // non app resp or heartbeat rsp msg;
        uint64_t test_term = 4;
        FakeDiskStorage storage;
        auto raft_disk_c = build_raft_disk_c(1, test_term, 10, 1, 10, storage);
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
		bool need_mem = false;
        std::tie(ret, rsp_msg, need_mem) = raft_disk_c->Step(invalid_msg);
        assert(0 == ret);
        assert(nullptr == rsp_msg);
		assert(false == need_mem);
    }

	// case 4
	{
		uint64_t test_term = 4;
		FakeDiskStorage storage;
		auto raft_disk_c = build_raft_disk_c(1, test_term, 10, 1, 10, storage);
		assert(nullptr != raft_disk_c);

		raft::Message msg;
		msg.set_disk_mark(true);
		msg.set_term(test_term);
		msg.set_logid(1);
		msg.set_from(2);
		msg.set_to(1);
		msg.set_type(raft::MessageType::MsgHeartbeatResp);

		int ret = 0;
		std::unique_ptr<raft::Message> rsp_msg;
		bool need_mem = false;
		std::tie(ret, rsp_msg, need_mem) = raft_disk_c->Step(msg);
		assert(0 == ret);
		assert(nullptr == rsp_msg);
		assert(false == need_mem);

		msg.set_index(11);
		msg.set_type(raft::MessageType::MsgAppResp);

		std::tie(ret, rsp_msg, need_mem) = raft_disk_c->Step(msg);
		assert(0 == ret);
		assert(nullptr == rsp_msg);
		assert(true == need_mem);
	}
}


TEST(RaftDiskCatchUpTest, StepMsgApp)
{
    const uint64_t test_term = 4;
    FakeDiskStorage storage;

    auto raft_disk_c = build_raft_disk_c(1, test_term, 10, 1, 10, storage);
    assert(nullptr != raft_disk_c);

    // fake storage
    {
        auto hs = cutils::make_unique<raft::HardState>();
        assert(nullptr != hs);

        for (size_t idx = 1; idx <= 11; ++idx) {
            add_entries(hs, test_term, idx);
        }

        assert(11 == hs->entries_size());
        assert(0 == storage.Write(*hs));
    }

	raft::Message app_msg;
	app_msg.set_disk_mark(true);
	app_msg.set_logid(1);
	app_msg.set_from(2);
	app_msg.set_to(1);
	app_msg.set_term(test_term);
	app_msg.set_index(12);
	app_msg.set_reject(true);
	app_msg.set_reject_hint(10);
	app_msg.set_type(raft::MessageType::MsgAppResp);

    int ret = 0;
    std::unique_ptr<raft::Message> rsp_msg;
	bool need_mem = false;

    std::tie(ret, rsp_msg, need_mem) = raft_disk_c->Step(app_msg);
    assert(0 == ret);
    assert(nullptr != rsp_msg);
	assert(false == need_mem);
	assert(raft::MessageType::MsgApp == rsp_msg->type());
	assert(10 == rsp_msg->index());
	assert(1 == rsp_msg->entries_size());
    assert(true == rsp_msg->disk_mark());
    assert(test_term == rsp_msg->log_term());
    assert(test_term == rsp_msg->term());

	app_msg.set_reject_hint(1);
	std::tie(ret, rsp_msg, need_mem) = raft_disk_c->Step(app_msg);
	assert(0 == ret);
	assert(nullptr != rsp_msg);
	assert(false == need_mem);

	assert(raft::MessageType::MsgApp == rsp_msg->type());
	assert(1 == rsp_msg->index());
	assert(1 == rsp_msg->entries_size());
	assert(rsp_msg->index() + 1 == rsp_msg->entries(0).index());
    assert(rsp_msg->disk_mark());


	app_msg.set_reject(false);
	app_msg.set_index(8);
	std::tie(ret, rsp_msg, need_mem) = raft_disk_c->Step(app_msg);
	assert(0 == ret);
	assert(nullptr != rsp_msg);
	assert(raft::MessageType::MsgApp == rsp_msg->type());
	assert(8 == rsp_msg->index());
	assert(0 < rsp_msg->entries_size());
	assert(10 == rsp_msg->entries(rsp_msg->entries_size()-1).index());
	assert(10 == rsp_msg->commit_index());

	app_msg.set_index(11);
	std::tie(ret, rsp_msg, need_mem) = raft_disk_c->Step(app_msg);
	assert(0 == ret);
	assert(nullptr == rsp_msg);
	assert(true == need_mem);
}

