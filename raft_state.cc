#include "raft_state.h"
#include "raft_mem.h"
#include "raft.pb.h"

namespace raft {


RaftState::RaftState(
        raft::RaftMem& raft_mem, 
        const std::unique_ptr<raft::HardState>& hard_state, 
        const std::unique_ptr<raft::SoftState>& soft_state)
    : raft_mem_(raft_mem)
    , hard_state_(hard_state)
    , soft_state_(soft_state)
{
    commit_index_ = raft_mem_.GetCommit();
    if (nullptr != hard_state_ && hard_state_->has_commit()) {
        assert(commit_index_ <= hard_state_->commit());
        commit_index_ = hard_state->commit();
    }

    assert(commit_index_ >= GetMinIndex() || uint64_t{1} == GetMinIndex());
}

raft::RaftRole RaftState::GetRole() const 
{
    if (nullptr != soft_state_ && soft_state_->has_role()) {
        return static_cast<raft::RaftRole>(soft_state_->role());
    }
    
    return raft_mem_.GetRole();
}

uint64_t RaftState::GetTerm() const 
{
    if (nullptr != hard_state_) {
        assert(hard_state_->term() >= raft_mem_.GetTerm());
        return hard_state_->term();
    }
    assert(nullptr == hard_state_);
    return raft_mem_.GetTerm();
}

uint32_t RaftState::GetVote(uint64_t msg_term) const 
{
    if (nullptr != hard_state_) {
        if (hard_state_->term() == msg_term && 
                hard_state_->has_vote()) {
            assert(0 == raft_mem_.GetVote(msg_term));
            return hard_state_->vote();
        }

        // else => 
        return 0;
    }

    assert(nullptr == hard_state_);
    return raft_mem_.GetVote(msg_term);
}

uint32_t RaftState::GetLeaderId(uint64_t msg_term) const 
{
    if (GetTerm() != msg_term) {
        return 0;
    }

    if (nullptr != soft_state_ && soft_state_->has_leader_id()) {
        return soft_state_->leader_id();
    }

    return raft_mem_.GetLeaderId(msg_term);
}

const raft::Entry* RaftState::GetLastEntry() const 
{
    if (nullptr != hard_state_) {
        if (0 < hard_state_->entries_size()) { 
            return &(hard_state_->entries(
                        hard_state_->entries_size() - 1));
        }

        assert(0 == hard_state_->entries_size());
    }

    return raft_mem_.GetLastEntry();
}

uint64_t RaftState::GetMinIndex() const 
{
    uint64_t min_index = raft_mem_.GetMinIndex();
    if (0 != min_index) {
        return min_index;
    }

    assert(0 == min_index);
    if (nullptr != hard_state_ && 0 < hard_state_->entries_size()) {
        assert(0 < hard_state_->entries(0).index());
        return hard_state_->entries(0).index();
    }

    return 0;
}

uint64_t RaftState::GetMaxIndex() const 
{
    if (nullptr != hard_state_ && 0 < hard_state_->entries_size()) {
        assert(0 < hard_state_->entries(
                    hard_state_->entries_size() - 1).index());
        return hard_state_->entries(
                hard_state_->entries_size() - 1).index();
    }

    return raft_mem_.GetMaxIndex();
}

uint64_t RaftState::GetCommit() const 
{
    if (nullptr != hard_state_ && hard_state_->has_commit()) {
        assert(hard_state_->commit() >= raft_mem_.GetCommit());
        return hard_state_->commit();
    }
    
    return raft_mem_.GetCommit();
}

const raft::Entry* RaftState::At(int mem_idx) const
{
    if (nullptr == hard_state_ || 0 == hard_state_->entries_size()) {
        return raft_mem_.At(mem_idx);
    }

    assert(nullptr != hard_state_);
    assert(0 < hard_state_->entries_size());
    uint64_t hs_min_index = hard_state_->entries(0).index();
    assert(0 < hs_min_index);

    uint64_t rmem_min_index = raft_mem_.GetMinIndex();
    if (1 == hs_min_index || rmem_min_index == hs_min_index) {
        if (mem_idx >= hard_state_->entries_size()) {
            return nullptr;
        }

        return &(hard_state_->entries(mem_idx));
    }

    assert(1 < hs_min_index);
    assert(0 < rmem_min_index);
    assert(rmem_min_index < hs_min_index);
    if (mem_idx < (hs_min_index - rmem_min_index)) {
        return raft_mem_.At(mem_idx);
    }

    // else
    int msg_idx = mem_idx - (hs_min_index - rmem_min_index);
    assert(0 <= msg_idx);
    assert(msg_idx < mem_idx);
    assert(msg_idx <= hard_state_->entries_size());
    if (msg_idx == hard_state_->entries_size()) {
        return nullptr;
    }

    return &(hard_state_->entries(msg_idx));
}

bool RaftState::CanUpdateCommit(
        uint64_t msg_commit_index, uint64_t msg_commit_term) const
{
    assert(raft::RaftRole::LEADER != GetRole());
    if (msg_commit_index <= commit_index_) {
        return false; // update nothing
    }

    assert(msg_commit_index > commit_index_);
    uint64_t max_index = GetMaxIndex();
    assert(max_index >= commit_index_);
    if (msg_commit_index > max_index) {
        // don't have enough info to update commited index
        return false; 
    }

    assert(0 < max_index);
    assert(msg_commit_index <= max_index);
    int mem_idx = msg_commit_index - GetMinIndex();
    assert(0 <= mem_idx);
    const raft::Entry* mem_entry = At(mem_idx);
    assert(nullptr != mem_entry);
    assert(mem_entry->index() == msg_commit_index);
    if (mem_entry->term() != msg_commit_term) {
        return false;
    }

    return true;
}

bool RaftState::UpdateVote(
        uint64_t vote_term, uint32_t candidate_id, bool vote_yes)
{
    return raft_mem_.UpdateVote(vote_term, candidate_id, vote_yes);
}

bool RaftState::CanWrite(int entries_size)
{
    assert(0 <= entries_size);
    // TODO
    return true;
}

bool RaftState::IsLogEmpty() const 
{
    if (nullptr != hard_state_ && 
            0 < hard_state_->entries_size()) {
        return false;
    }

    return raft_mem_.IsLogEmpty();
}

bool RaftState::IsMatch(uint64_t log_index, uint64_t log_term) const
{
    if (0 == log_index) {
        assert(0 == log_term);
        return true;
    }

    assert(0 < log_index);
    uint64_t min_index = GetMinIndex();
    if (0 == min_index) {
        assert(IsLogEmpty());
        return false;
    }

    assert(0 < min_index);
    if (log_index < min_index) {
        // must be the case !!
        assert(log_term <= GetTerm());
        return true;
    }

    assert(min_index <= log_index);
    assert(log_index <= GetMaxIndex());

    int mem_idx = log_index - min_index;
    assert(0 <= mem_idx);
    const raft::Entry* mem_entry = At(mem_idx);
    assert(nullptr != mem_entry);
    assert(mem_entry->index() == log_index);
    assert(0 < mem_entry->term());
    return mem_entry->term() == log_term;
}

const std::set<uint32_t>& RaftState::GetVoteFollowerSet() const {
    // TODO: config change ???

    return raft_mem_.GetVoteFollowerSet();
}

raft::Replicate* RaftState::GetReplicate() 
{
    return raft_mem_.GetReplicate();
}

} // namespace raft;


