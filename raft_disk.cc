#include "raft_disk.h"


namespace raft {

RaftDisk::RaftDisk(
        uint64_t logid, 
        uint32_t selfid, 
        ReadHandler readcb)
    : logid_(logid)
    , selfid_(selfid)
    , readcb_(readcb)
{
    assert(nullptr != readcb);
    
    replicate_ = cutils::make_unique<raft::Replicate>();
    assert(nullptr != replicate_);

    replicate_follower_set_ = {1, 2, 3};
    assert(replicate_follower_set_.end() != replicate_follower_set_.find(selfid_));
    replicate_follower_set_.erase(selfid_);
}

RaftDisk::~RaftDisk() = default;

bool RaftDisk::updateTerm(uint64_t new_term)
{
    if (new_term == term_) {
        return false;
    }

    assert(new_term > term_);
    term_ = new_term;
    return true;
}

bool RaftDisk::updateRole(uint64_t new_role)
{
    if (new_role == role_) {
        return false;
    }

    assert(new_role != role_);
    role_ = new_role;
    return true;
}

// TODO
std::tuple<int, std::unique_ptr<raft::Message>>
RaftDisk::Step(
        raft::RaftRole new_role, 
        uint64_t new_term, 
        uint32_t follower_id, 
        uint64_t next_log_index, 
        bool reject, 
        raft::MessageType rsp_msg_type)
{
    assert(0 < follower_id);
    assert(0 < next_log_index);
    assert(raft::MessageType::MsgApp == rsp_msg_type || 
            raft::MessageType::MsgHeartbeat == rsp_msg_type);
    updateTerm(new_term);
    assert(new_term == term_);

    updateRole(new_role);
    assert(new_role == role_);

    if (raft::RaftRole::LEADER != role_) {
        // only leader can do the catch up!
        return std::make_tuple(-1, nullptr);
    }

    assert(raft::RaftRole::LEADER == role_);
    auto rsp_msg = cutils::make_unique<raft::Message>();
    assert(nullptr != rsp_msg);
    rsp_msg->set_type(rsp_msg_type);
    rsp_msg->set_logid(logid_);
    rsp_msg->set_from(selfid_);
    rsp_msg->set_to(follower_id);
    rsp_msg->set_term(term_);

    assert(nullptr != replicate_);
    replicate_->UpdateReplicateState(
            follower_id, !reject, next_log_index);
    if (raft::MessageType::MsgHeartbeat == rsp_msg_type) {
        return stepHearbeatMsg(follower_id);
    }

    assert(raft::MessageType::MsgApp == rsp_msg_type);
    return stepAppMsg(follower_id);
}

std::tuple<int, std::unique_ptr<raft::Message>>
RaftDisk::stepHearbeatMsg(uint32_t follower_id)
{
    assert(raft::RaftRole::LEADER == role_);
    assert(nullptr != replicate_);
    assert(nullptr != readcb_);

    auto rsp_msg = cutils::make_unique<raft::Message>();
    assert(nullptr != rsp_msg);

    auto next_explore_index = 
        replicate_->NextExploreIndex(
                follower_id, GetMinIndex(), GetMaxIndex());
    assert(1 < next_explore_index);
    assert(GetMinIndex() < next_explore_index);
    assert(next_explore_index <= GetMaxIndex() + 1);

    rsp_msg->set_index(next_explore_index);

    int ret = 0;
    std::unique_ptr<raft::HardState> hard_state;
    std::tie(ret, hard_state) = readcb_(logid_, next_explore_index - 1, 1);
    if (0 != ret) {
        logerr("readcb_ logid %" PRIu64 " "
                "next_explore_index %" PRIu64 " ret %d", 
                logid_, next_explore_index, ret);
        return std::make_tuple(ret, nullptr);
    }

    assert(0 == ret);
    assert(nullptr != hard_state);
    assert(1 == hard_state->entries_size());
    const auto& disk_entry = hard_state->entries(0);
    assert(disk_entry.index() == next_explore_index - 1);
    rsp_msg->set_log_term(disk_entry->term());
    return std::make_tuple(0, std::move(rsp_msg));
}

std::tuple<int, std::unique_ptr<raft::Message>>
RaftDisk::stepAppMsg(uint32_t follower_id)
{
    assert(raft::RaftRole::LEADER == role_);
    assert(nullptr != replicate_);
    assert(nullptr != readcb_);

    auto rsp_msg = cutils::make_unique<raft::Message>();
    assert(nullptr != rsp_msg);

    auto next_catchup_index = 
        replicate->NextCatchUpIndex(
                follower_id, GetMinIndex(), GetMaxIndex());
    assert(0 < next_catchup_index);

    rsp_msg->set_index(next_catchup_index);
    int max_size = std::min<int>(
            GetMaxIndex() + 1 - next_catchup_index, 10);
    assert(0 < max_size);
    assert(next_catchup_index + max_size <= GetMaxIndex() + 1);
    int ret = 0;
    std::unique_ptr<raft::HardState> hard_state;
    if (uint64_t{1} == next_catchup_index) {
        std::tie(ret, hard_state) = readcb_(
                logid_, next_catchup_index, max_size);
    }
    else {
        std::tie(ret, hard_state) = readcb_(
                logid_, next_catchup_index - 1, max_size + 1);
    }

    if (0 != ret) {
        logerr("readcb_ logid %" PRIu64 " next_catchup_index %" PRIu64 
                " max_size %d ret %d", 
                logid_, next_catchup_index, max_size, ret);
        return std::make_tuple(ret, nullptr);
    }

    assert(0 == ret);
    assert(nullptr != hard_state);
    assert(0 < hard_state->entries_size());

    if (uint64_t{1} == next_catchup_index) {
        rsp_msg->set_log_term(0);
        for (int idx = 0; idx < hard_state->entries_size(); ++idx) {
            const auto& disk_entry = hard_state->entries(idx);
            assert(disk_entry->index() == next_catchup_index + idx);
            auto* rsp_entry = rsp_msg->add_entries();
            *rsp_entry = disk_entry;
            if (disk_entry->index() <= commit_) {
                rsp_msg->set_commit_index(disk_entry->index());
                rsp_msg->set_commit_term(disk_entry->term());
            }
        }
    }
    else {
        assert(hard_state->entries(0).index() == next_catchup_index - 1);
        rsp_msg->set_log_term(hard_state->entries(0).term());
        for (int idx = 1; idx < hard_state->entries_size(); ++idx) {
            const auto& disk_entry = hard_state->entries(idx);
            assert(disk_entry->index() == next_catchup_index + idx);
            auto* rsp_entry = rsp_msg->add_entries();
            *rsp_entry = disk_entry;
            if (disk_entry->index() <= commit_) {
                rsp_msg->set_commit_index(disk_entry->index());
                rsp_msg->set_commit_term(disk_entry->term());
            }
        }
    }

    assert(0 < rsp_msg->entries_size());
    assert(rsp_msg->entries_size() == max_size);
    return std::make_tuple(0, std::move(rsp_msg));
}

    
} // namespace raft;


