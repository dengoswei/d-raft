#pragma once

#include <memory>
#include <set>
#include <stdint.h>

namespace raft {


class Entry;
class HardState;
class SoftState;
class RaftMem;
class Replicate;
class ClusterConfig;

enum class RaftRole : uint32_t;

class RaftState {

public:
    RaftState(
            raft::RaftMem& raft_mem, 
            const std::unique_ptr<raft::HardState>& hard_state, 
            const std::unique_ptr<raft::SoftState>& soft_state);

    ~RaftState() = default;

    raft::RaftRole GetRole() const;

    uint64_t GetTerm() const;

    uint32_t GetVote(uint64_t msg_term) const;

    uint32_t GetLeaderId(uint64_t msg_term) const;

    const raft::Entry* GetLastEntry() const;

    uint64_t GetMinIndex() const;

    uint64_t GetMaxIndex() const;

    uint64_t GetCommit() const;

    const raft::Entry* At(int mem_idx) const;

    bool CanUpdateCommit(
            uint64_t msg_commit_index, uint64_t msg_commit_term) const;

    bool UpdateVote(
            uint64_t vote_term, uint32_t candidate_id, bool vote_yes);

    bool CanWrite(int entries_size);

    bool IsLogEmpty() const;

    bool IsMatch(uint64_t log_index, uint64_t log_term) const;

    uint64_t GetLogTerm(uint64_t log_index) const;

    // raft::Replicate* GetReplicate();

	// uint64_t GetMinusIndexLogTerm() const;

	uint64_t GetLogId() const;

	raft::RaftMem& GetRaftMem() {
		return raft_mem_;
	}

	const raft::RaftMem& GetRaftMem() const {
		return raft_mem_;
	}

    const raft::ClusterConfig* GetConfig() const;

    bool IsMember(uint32_t peer) const;

private:
    raft::RaftMem& raft_mem_;
    const std::unique_ptr<raft::HardState>& hard_state_;
    const std::unique_ptr<raft::SoftState>& soft_state_;

    uint64_t commit_index_ = 0;
}; // class RaftState



} // namespace raft;



