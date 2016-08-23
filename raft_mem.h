#pragma once

#include <functional>
#include <memory>
#include <chrono>
#include <deque>
#include <tuple>
#include <stdint.h>
#include "raft.pb.h"

namespace raft {


enum class RaftRole : uint32_t {
    LEADER = 1, 
    CANDIDATE = 2, 
    FOLLOWER = 3, 
};

class Replicate;

class RaftMem {

private:
    using StepMessageHandler = 
        std::function<
            std::tuple<
                std::unique_ptr<raft::HardState>, 
                std::unique_ptr<raft::SoftState>, 
                bool, 
                raft::MessageType>(
                        raft::RaftMem&, 
                        const raft::Message&, 
                        std::unique_ptr<raft::HardState>, 
                        std::unique_ptr<raft::SoftState>)>;

    using TimeoutHandler = 
        std::function<
            std::tuple<
                std::unique_ptr<raft::HardState>, 
                std::unique_ptr<raft::SoftState>, 
                bool, 
                raft::MessageType>(raft::RaftMem&)>;

public:
    RaftMem();

    ~RaftMem();

    // bool for broad-cast
    std::tuple<
        std::unique_ptr<raft::HardState>, 
        std::unique_ptr<raft::SoftState>, 
        bool, raft::MessageType>
            Step(
                const raft::Message& msg, 
                std::unique_ptr<raft::HardState> hard_state, 
                std::unique_ptr<raft::SoftState> soft_state);

    // 0 ==
    void ApplyState(
            std::unique_ptr<raft::SoftState>& soft_state, 
            std::unique_ptr<raft::HardState>& hard_state);

    std::unique_ptr<raft::Message> BuildRspMsg(
            const raft::Message& msg, 
            bool mark_broadcast, raft::MessageType rsp_msg_type);

public:

    raft::RaftRole GetRole() const {
        return role_;
    }

    uint64_t GetCommit() const {
        return commit_;
    }

    uint64_t GetTerm() const {
        return term_;
    }

    uint32_t GetVote(uint64_t term) const;

    uint32_t GetLeaderId(uint64_t term) const;

    const raft::Entry* GetLastEntry() const;

    const raft::Entry* At(int mem_idx) const;

    uint64_t GetMinIndex() const;

    uint64_t GetMaxIndex() const;

    bool UpdateVote(
            uint64_t vote_term, uint32_t candidate_id, bool vote_yes);

    bool IsLogEmpty() const;

    bool HasTimeout() const ;

    void UpdateActiveTime();

    raft::Replicate* GetReplicate() {
        return replicate_.get();
    }

private:
    void setRole(uint64_t next_term, uint32_t role);

    void updateLeaderId(uint64_t next_term, uint32_t leader_id);

    void updateVote(uint64_t next_term, uint32_t vote);

    void updateTerm(uint64_t new_term);

    std::deque<std::unique_ptr<Entry>>::iterator findLogEntry(uint64_t index);

    void appendLogEntries(std::unique_ptr<raft::HardState> hard_state);

    void updateLogEntries(std::unique_ptr<raft::HardState> hard_state);

    void applyHardState(std::unique_ptr<raft::HardState> hard_state);


private:
    const uint64_t logid_ = 0ull;
    const uint32_t selfid_ = 0ull;

    raft::RaftRole role_ = RaftRole::FOLLOWER;

    TimeoutHandler timeout_handler_ = nullptr;
    StepMessageHandler step_handler_ = nullptr;

    uint32_t leader_id_ = 0; // soft state

    // hard-state
    uint64_t term_ = 0;
    uint32_t vote_ = 0;
    uint64_t commit_ = 0; // soft state ?

    std::deque<std::unique_ptr<Entry>> logs_;

    std::chrono::milliseconds election_timeout_;
    std::chrono::time_point<std::chrono::system_clock> active_time_;
    // replicate state.. TODO
    
    std::unique_ptr<raft::Replicate> replicate_;
}; // class RaftMem

} // namespace raft


