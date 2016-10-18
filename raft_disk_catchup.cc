#include "raft_disk_catchup.h"
#include "log_utils.h"
#include "mem_utils.h"


namespace raft {


RaftDiskCatchUp::RaftDiskCatchUp(
        uint64_t logid, 
        uint32_t selfid, 
        uint32_t catch_up_id, 
        uint64_t term, 
        uint64_t max_catch_up_index, 
        uint64_t min_index, 
        ReadHandler readcb)
    : logid_(logid)
    , selfid_(selfid)
    , catch_up_id_(catch_up_id)
    , term_(term)
    , max_index_(max_catch_up_index)
    , min_index_(min_index)
    , readcb_(readcb)
{
    assert(0 < logid_);
    assert(0 < selfid_);
    assert(0 < catch_up_id_);
    assert(selfid_ != catch_up_id_);
    assert(0 < term_);
    assert(0 < max_index_);
    assert(nullptr != readcb_);
}

RaftDiskCatchUp::~RaftDiskCatchUp() = default;


std::tuple<int, std::unique_ptr<raft::Message>>
RaftDiskCatchUp::Step(const raft::Message& msg)
{
    if (false == msg.has_disk_mark() || 
            false == msg.disk_mark()) {
        logerr("logid %" PRIu64 " from %u to %u missing disk_mark", 
                msg.logid(), msg.from(), msg.to());
        return std::make_tuple(-1, nullptr);
    }

    assert(msg.has_disk_mark());
    assert(msg.disk_mark());
    if (msg.term() != term_) {
        logerr("logid %" PRIu64 " msg.term %" PRIu64 " term_ %" PRIu64, 
                msg.logid(), msg.term(), term_);
        return std::make_tuple(0, nullptr);
    }

    assert(msg.term() == term_);
    assert(msg.logid() == logid_);
    assert(msg.to() == selfid_);
    assert(msg.from() == catch_up_id_);

    auto rsp_msg_type = step(msg);

    return buildRspMsg(msg, rsp_msg_type);
}

raft::MessageType
RaftDiskCatchUp::step(const raft::Message& msg)
{
    auto rsp_msg_type = raft::MessageType::MsgNull;
    switch (msg.type())  {
    case raft::MessageType::MsgAppResp:
        {
            replicate_.UpdateReplicateState(
                    msg.from(), !msg.reject(), msg.index());
            auto next_catchup_index = 
                replicate_.NextCatchUpIndex(
                        msg.from(), min_index_, max_index_);
            if (0 != next_catchup_index) {
                rsp_msg_type = raft::MessageType::MsgApp;
                break;
            }

            assert(0 == next_catchup_index);
            assert(raft::MessageType::MsgNull == rsp_msg_type);
            auto next_explore_index = 
                replicate_.NextExploreIndex(
                        msg.from(), min_index_, max_index_);
            if (0 != next_explore_index) {
                rsp_msg_type = raft::MessageType::MsgHeartbeat;
                break;
            }

            assert(0 == next_explore_index);
            assert(raft::MessageType::MsgNull == rsp_msg_type);
            logerr("EOF: logid %" PRIu64 " msg.index %" PRIu64 " reject %d "
                    " min_index %" PRIu64 " max_index %" PRIu64 " from %u", 
                    msg.logid(), msg.index(), msg.reject(), 
                    min_index_, max_index_, msg.from());
//
//            bool update = replicate_.UpdateReplicateState(
//                    msg.from(), !msg.reject(), msg.index());
//            if (update) {
//                next_catchup_index = 
//                    replicate_.NextCatchUpIndex(
//                            msg.from(), min_index_, max_index_);
//                if (0 == next_catchup_index) {
//                    logerr("INFO: follower_id %u DiskCatchUp "
//                            "stop at %" PRIu64, 
//                            msg.from(), msg.index());
//                }
//            }
//            else {
//                rsp_msg_type = raft::MessageType::MsgApp;
//            }
//
//            if (0 == next_catchup_index && msg.reject()) {
//                assert(raft::MessageType::MsgNull == rsp_msg_type);
//                auto next_explore_index = 
//                    replicate_.NextExploreIndex(
//                            msg.from(), min_index_, max_index_);
//                if (0 == next_explore_index) {
//                    logerr("INFO: follower_id %u no need "
//                            "explore index %" PRIu64, 
//                            msg.from(), msg.index());
//                }
//            }
//            else {
//                rsp_msg_type = raft::MessageType::MsgHeartbeat;
//                logerr("INFO: follower_id %u switch CatchUp to Explore", 
//                        msg.from());
//            }
        }
        break;
    case raft::MessageType::MsgHeartbeatResp:
        {
            bool update = replicate_.UpdateReplicateState(
                    msg.from(), !msg.reject(), msg.index());

            auto next_explore_index = 
                replicate_.NextExploreIndex(
                        msg.from(), min_index_, max_index_);
            printf ( "msg.index %u next_explore_index %u\n", 
                    static_cast<uint32_t>(msg.index()), 
                    static_cast<uint32_t>(next_explore_index) );
            if (0 != next_explore_index) {
                assert(next_explore_index != msg.index());
                rsp_msg_type = raft::MessageType::MsgHeartbeat;
                break;
            }

            assert(0 == next_explore_index);
            auto next_catchup_index = 
                replicate_.NextCatchUpIndex(
                    msg.from(), min_index_, max_index_);
            if (0 != next_catchup_index) {
                assert(0 < next_catchup_index);
                rsp_msg_type = raft::MessageType::MsgApp;
                logerr("INFO: follower_id %u switch Explore to CatchUp", 
                        msg.from());
            }
        }
        break;

    default:
        break;
    }

    return rsp_msg_type;
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
            auto next_catchup_index = 
                replicate_.NextCatchUpIndex(
                        req_msg.from(), min_index_, max_index_);
            assert(0 < next_catchup_index);
            rsp_msg->set_index(next_catchup_index);
            int ret = 0;
            std::unique_ptr<raft::HardState> hs = nullptr;
            int max_size = std::min<int>(
                    max_index_ + 1 - next_catchup_index, 10) + 1;
            printf ( "max_size %d\n", max_size );
            std::tie(ret, hs) = readcb_(
                    logid_, next_catchup_index-1, max_size);
            if (0 != ret) {
                return std::make_tuple(ret, nullptr);
            }

            assert(0 == ret);
            assert(nullptr != hs);
            assert(0 < hs->entries_size());
            int idx = 1 == next_catchup_index ? 0 : 1;
            for (; idx < hs->entries_size(); ++idx) {
                const raft::Entry& disk_entry = hs->entries(idx);
                assert(0 < disk_entry.term());
                assert(0 < disk_entry.index());
                printf ( "index %u\n", (uint32_t)disk_entry.index() );
                auto entry = rsp_msg->add_entries();
                assert(nullptr != entry);
                *entry = disk_entry;
            }

            assert(0 < rsp_msg->entries_size());
            assert(rsp_msg->index() == rsp_msg->entries(0).index());
            if (1 == next_catchup_index) {
                rsp_msg->set_log_term(0);
            }
            else {
                rsp_msg->set_log_term(hs->entries(0).term());
            }
            // TODO: add commit_index && commit_term ?
        }
        break;

     case raft::MessageType::MsgHeartbeat:
        {
            rsp_msg = cutils::make_unique<raft::Message>();
            assert(nullptr != rsp_msg);
            auto next_explore_index = 
                replicate_.NextExploreIndex(
                        req_msg.from(), min_index_, max_index_);
            assert(1 < next_explore_index);
            assert(min_index_ < next_explore_index);
            assert(next_explore_index <= max_index_ + 1);
            assert(req_msg.index() != next_explore_index);
            rsp_msg->set_index(next_explore_index);

            int ret = 0;
            std::unique_ptr<raft::HardState> hs = nullptr;
            std::tie(ret, hs) = readcb_(
                    logid_, next_explore_index-1, 1);
            if (0 != ret) {
                return std::make_tuple(ret, nullptr);
            }

            assert(0 == ret);
            assert(nullptr != hs);
            assert(1 == hs->entries_size());
            assert(next_explore_index - 1 == hs->entries(0).index());
            rsp_msg->set_log_term(hs->entries(0).term());

            // TODO: commit_index && commit_term
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

} // namespace raft


