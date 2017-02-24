#include "raft_disk_catchup.h"
#include "log_utils.h"
#include "mem_utils.h"

namespace {

enum {

	DISK_SWITCH_TO_EXPLORE = 65, 
	DISK_SWITCH_TO_APP = 66, 
	DISK_KEEP_EXPLORE = 67, 
	DISK_SWITCH_TO_MEM = 70, 
};

}

namespace raft {


RaftDiskCatchUp::RaftDiskCatchUp(
        uint64_t logid, 
        uint32_t selfid, 
        uint64_t term, 
        uint64_t max_catch_up_index, 
        uint64_t min_index, 
		uint64_t disk_commit_index, 
        ReadHandler readcb)
    : logid_(logid)
    , selfid_(selfid)
    , term_(term)
    , max_index_(max_catch_up_index)
    , min_index_(min_index)
	, disk_commit_index_(disk_commit_index)
    , readcb_(readcb)
{
    assert(0 < logid_);
    assert(0 < selfid_);
    assert(0 < term_);
    assert(0 < max_index_);
    assert(nullptr != readcb_);
	if (0 == min_index_) {
		assert(0 == max_index_);
	}
}

RaftDiskCatchUp::~RaftDiskCatchUp() = default;


std::tuple<int, std::unique_ptr<raft::Message>, bool>
RaftDiskCatchUp::Step(const raft::Message& msg)
{
    if (false == msg.has_disk_mark() || 
            false == msg.disk_mark()) {
        logerr("logid %" PRIu64 " from %u to %u missing disk_mark", 
                msg.logid(), msg.from(), msg.to());
        return std::make_tuple(-1, nullptr, false);
    }

    assert(msg.has_disk_mark());
    assert(msg.disk_mark());
    if (msg.term() != term_) {
        logerr("logid %" PRIu64 " msg.term %" PRIu64 " term_ %" PRIu64, 
                msg.logid(), msg.term(), term_);
        return std::make_tuple(0, nullptr, false);
    }

    assert(msg.term() == term_);
    assert(msg.logid() == logid_);
    assert(msg.to() == selfid_);
	bool need_mem = false;
	auto rsp_msg_type = raft::MessageType::MsgNull;
	std::tie(rsp_msg_type, need_mem) = step(msg);

	int ret = 0;
	std::unique_ptr<raft::Message> rsp_msg;
	std::tie(ret, rsp_msg) = buildRspMsg(msg, rsp_msg_type);
	if (need_mem) {
		assert(nullptr == rsp_msg);
	}

	return std::make_tuple(ret, std::move(rsp_msg), need_mem);
}

std::tuple<raft::MessageType, bool>
RaftDiskCatchUp::step(const raft::Message& msg)
{
	assert(false == msg.one_shot_mark());
	assert(true == msg.disk_mark());
	bool need_mem = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;
    switch (msg.type())  {
    case raft::MessageType::MsgAppResp:
        {
			uint64_t next_index = 0;
			if (msg.reject()) {
				next_index = std::min(msg.index(), msg.reject_hint() + 1);
				next_index = std::max(uint64_t{1}, next_index);
			}
			else {
				next_index = msg.index() + 1;
			}

			assert(0 < next_index);
			if (next_index -1 > max_index_) {
				need_mem = true;
				break;
			}

			rsp_msg_type = raft::MessageType::MsgApp;
        }
        break;
    default:
        break;
    }

	return std::make_tuple(rsp_msg_type, need_mem);
}

std::tuple<int, std::unique_ptr<raft::Message>>
RaftDiskCatchUp::buildRspMsg(
        const raft::Message& req_msg, 
        raft::MessageType rsp_msg_type)
{
    assert(nullptr != readcb_);
    std::unique_ptr<raft::Message> rsp_msg = nullptr;
    switch (rsp_msg_type) {
    case raft::MessageType::MsgApp:
        {
            rsp_msg = cutils::make_unique<raft::Message>();
            assert(nullptr != rsp_msg);

			uint64_t next_index = 0;
			if (req_msg.reject()) {
				next_index = std::min(req_msg.index(), req_msg.reject_hint() + 1);
				next_index = std::max(uint64_t{1}, next_index);
			}
			else {
				next_index = req_msg.index() + 1;
			}

			assert(0 < next_index);
			assert(next_index - 1 <= max_index_);
			int max_size = std::min<int>(
					max_index_ + 1 - next_index, 30) + 1;
			if (req_msg.reject()) {
				max_size = 1 == next_index ? 1 : 2;
			}

			assert(0 < max_size);
			int ret = 0;
			std::unique_ptr<raft::HardState> hs;
			std::tie(ret, hs) = readcb_(
					logid_, 1 == next_index ? 1 : next_index - 1, max_size);
			if (0 != ret) {
				logerr("readcb_ logid %lu next_index %lu max_size %d", 
						logid_, next_index, max_size);
				return std::make_tuple(ret, nullptr);
			}

			assert(0 == ret);
			assert(nullptr != hs);
			assert(0 < hs->entries_size());
			int idx = 1 == next_index ? 0 : 1;
			for (; idx < hs->entries_size(); ++idx) {
				const auto& disk_entry = hs->entries(idx);
				assert(0 < disk_entry.term());
				assert(0 < disk_entry.index());
				if (1 == next_index) {
					assert(next_index + idx == disk_entry.index());
				}
				else {
					assert(1 < next_index);
					assert(next_index + idx == disk_entry.index() + 1);
				}

				auto entry = rsp_msg->add_entries();
				*entry = disk_entry;
				if (entry->index() <= disk_commit_index_) {
					rsp_msg->set_commit_index(entry->index());
					rsp_msg->set_commit_term(entry->term());
				}
			}

			rsp_msg->set_index(next_index - 1);
			rsp_msg->set_log_term(
					0 == rsp_msg->index() ? 0 : hs->entries(0).term());
			assert(disk_commit_index_ >= max_index_);
	    }
        break;
    default:
        break;
    }

    if (nullptr != rsp_msg) {
        rsp_msg->set_type(rsp_msg_type);
        rsp_msg->set_logid(req_msg.logid());
        rsp_msg->set_from(req_msg.to());
        rsp_msg->set_to(req_msg.from());
        rsp_msg->set_term(term_);
        rsp_msg->set_disk_mark(true);
    }

    return std::make_tuple(0, std::move(rsp_msg));
}

void RaftDiskCatchUp::Update(
		uint64_t term, 
		uint64_t max_catch_up_index, 
		uint64_t min_disk_index, 
		uint64_t disk_commit_index)
{
	logerr("logid %" PRIu64 " PREV: term %" PRIu64 " max_index %" PRIu64 
			" min_index %" PRIu64 " false_commit %" PRIu64, 
			logid_, term_, max_index_, min_index_, disk_commit_index_);
	logerr("logid %" PRIu64 " NEW : term %" PRIu64 " max_index %" PRIu64 
			" min_index %" PRIu64 " false commit %" PRIu64, 
			logid_, term, max_catch_up_index, min_disk_index, disk_commit_index);
	term_ = std::max(term, term_);
	max_index_ = std::max(max_index_, max_catch_up_index);
	min_index_ = std::max(min_index_, min_disk_index);
	disk_commit_index_ = std::max(disk_commit_index_, disk_commit_index);
	if (0 == min_index_) {
		assert(0 == max_index_);
	}
}

} // namespace raft


