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
                raft::MessageType>(raft::RaftMem&, bool)>;

    // TODO
    using BuildRspHandler = 
        std::function<
            std::unique_ptr<raft::Message>(
                raft::RaftMem&, 
                const raft::Message&, 
                const std::unique_ptr<raft::HardState>&, 
                const std::unique_ptr<raft::SoftState>&,
                bool, 
                const raft::MessageType)>;

public:
    RaftMem(
        uint64_t logid, 
        uint32_t selfid, 
        uint32_t election_timeout_ms);

    ~RaftMem();

    int Init(const raft::HardState& hard_state);


    std::tuple<
        std::unique_ptr<raft::HardState>, 
        std::unique_ptr<raft::SoftState>, 
        bool, raft::MessageType>
            SetValue(
                    const std::vector<std::string>& vecValue, 
                    const std::vector<uint64_t>& vecRequestID);



    // bool for broad-cast
    std::tuple<
        std::unique_ptr<raft::HardState>, 
        std::unique_ptr<raft::SoftState>, 
        bool, raft::MessageType>
            Step(
                const raft::Message& msg, 
                std::unique_ptr<raft::HardState> hard_state, 
                std::unique_ptr<raft::SoftState> soft_state);

    std::tuple<
        std::unique_ptr<raft::HardState>, 
        std::unique_ptr<raft::SoftState>, 
        bool, raft::MessageType>
            CheckTimeout(bool force_timeout);

    // 0 ==
    void ApplyState(
            std::unique_ptr<raft::HardState> hard_state, 
            std::unique_ptr<raft::SoftState> soft_state);

    std::unique_ptr<raft::Message> BuildRspMsg(
            const raft::Message& req_msg, 
            const std::unique_ptr<raft::HardState>& hard_state, 
            const std::unique_ptr<raft::SoftState>& soft_state, 
            bool mark_broadcast, 
            raft::MessageType rsp_msg_type);

public:
    
    uint32_t GetSelfId() const {
        return selfid_;
    }

    uint64_t GetLogId() const {
        return logid_;
    }

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

    int UpdateVote(
            uint64_t vote_term, uint32_t candidate_id, bool vote_yes);

    bool IsLogEmpty() const;

    bool HasTimeout() const ;

    void UpdateActiveTime();

    raft::Replicate* GetReplicate() {
        return replicate_.get();
    }

    void ClearVoteMap();

    int GetVoteCount() const {
        return static_cast<int>(vote_map_.size());
    }

    bool IsMajority(int cnt) const;

    const std::set<uint32_t>& GetVoteFollowerSet() const;

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
    const uint32_t selfid_ = 0u;

    raft::RaftRole role_ = RaftRole::FOLLOWER;

    std::map<raft::RaftRole, TimeoutHandler> map_timeout_handler_;
    std::map<raft::RaftRole, StepMessageHandler> map_step_handler_;
    std::map<raft::RaftRole, BuildRspHandler> map_build_rsp_handler_;

    uint32_t leader_id_ = 0; // soft state

    // hard-state
    uint64_t term_ = 0;
    uint32_t vote_ = 0;
    uint64_t commit_ = 0; // soft state ?

    std::deque<std::unique_ptr<Entry>> logs_;

    std::chrono::milliseconds election_timeout_;
    std::chrono::time_point<std::chrono::system_clock> active_time_;
    // replicate state.. TODO
    
    std::map<uint32_t, uint64_t> vote_map_;
    std::unique_ptr<raft::Replicate> replicate_;

    std::set<uint32_t> vote_follower_set_;
}; // class RaftMem

} // namespace raft


