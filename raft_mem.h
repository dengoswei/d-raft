#pragma once

#include <functional>
#include <memory>
#include <chrono>
#include <set>
#include <deque>
#include <tuple>
#include <stdint.h>
#include "raft.pb.h"
#include "random_utils.h"

namespace raft {


enum class RaftRole : uint32_t {
    LEADER = 1, 
    CANDIDATE = 2, 
    FOLLOWER = 3, 
};

class Progress;
class RaftConfig;
class TmpEntryCache;

class RaftMem {

private:
    using StepMessageHandler = 
        std::function<
            std::tuple<
                bool, 
                raft::MessageType, 
                bool>(
                        raft::RaftMem&, 
                        const raft::Message&, 
                        std::unique_ptr<raft::HardState>&, 
                        std::unique_ptr<raft::SoftState>&)>;

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
                uint32_t, 
                const raft::MessageType, 
				bool)>;

public:
    RaftMem(
        uint64_t logid, 
        uint32_t selfid, 
        uint32_t election_tick_ms, 
		uint32_t hb_tick_ms);

    ~RaftMem();

    int Init(
			const raft::HardState& hard_state);

    std::tuple<
        std::unique_ptr<raft::Message>, 
        std::unique_ptr<raft::HardState>, 
        std::unique_ptr<raft::SoftState>>
            SetValue(
                    const std::vector<std::string>& vec_value, 
                    const std::vector<uint64_t>& vec_reqid);

    // prop_req msg, hs, sf, mk, rsp_msg_type
    std::tuple<
        std::unique_ptr<raft::Message>, 
        std::unique_ptr<raft::HardState>, 
        std::unique_ptr<raft::SoftState>>
            SetValue(const std::string& value, uint64_t reqid);

    // : 
    // servers process incoming RPC requests without consulting 
    // their current configurations.
    // bool for broad-cast
    std::tuple<
        bool, raft::MessageType, bool>
            Step(
                const raft::Message& msg, 
                std::unique_ptr<raft::HardState>& hard_state, 
                std::unique_ptr<raft::SoftState>& soft_state);

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
			uint32_t rsp_peer_id, 
            raft::MessageType rsp_msg_type, 
			bool no_null);

	std::vector<std::unique_ptr<raft::Message>>
		BuildBroadcastRspMsg(
				const raft::Message& req_msg, 
				const std::unique_ptr<raft::HardState>& hard_state, 
				const std::unique_ptr<raft::SoftState>& soft_state, 
				raft::MessageType rsp_msg_type);


    size_t CompactLog(uint64_t new_min_index);

	raft::RaftRole BecomeFollower();

	int ShrinkMemLog(size_t max_mem_log_size);

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

	void Tick();

	void RefreshElectionTimeout();

    bool HasTimeout() const;

	bool IsHeartbeatTimeout() const;

	bool IsHBSilenceTimeout() const;

    void UpdateActiveTime();

	void UpdateHeartBeatActiveTime();

	void UpdateHBSilenceTimeout();

    void ClearVoteMap();

    int GetVoteCount() const {
        return static_cast<int>(vote_map_.size());
    }

    bool IsMajority(int cnt) const;

    const std::set<uint32_t>& GetVoteFollowerSet() const;

    uint64_t GetDiskMinIndex() const;

	void RecvCatchUp();

	void MissingCatchUp();

	bool NeedCatchUp();

	void TriggerCatchUp();

	std::map<uint32_t, std::unique_ptr<raft::Progress>>& GetProgress() {
		return map_progress_;
	}

	const std::map<uint32_t, std::unique_ptr<raft::Progress>>& GetProgress() const {
		return map_progress_;
	}

	raft::Progress* GetProgress(uint32_t peer_id);

	raft::TmpEntryCache* GetTmpEntryCache() {
		return tmp_entry_cache_.get();
	}

	bool IsReplicateStall() const;

private:
    void setRole(uint64_t next_term, uint32_t role);

    void updateLeaderId(uint64_t next_term, uint32_t leader_id);

    void updateVote(uint64_t next_term, uint32_t vote);

    void updateTerm(uint64_t new_term);

    void updateCommit(uint64_t new_commit);

    void updateDiskMinIndex(uint64_t next_disk_min_index);

    void updateMetaInfo(const raft::MetaInfo& metainfo);

    std::deque<std::unique_ptr<Entry>>::iterator 
        findLogEntry(uint64_t index);

    void appendLogEntries(std::unique_ptr<raft::HardState> hard_state);

    void applyHardState(std::unique_ptr<raft::HardState> hard_state);

	void updateHBSilenceTime();

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

	const uint32_t base_election_tick_;
	cutils::RandomTimeout timeout_gen_;

	uint32_t election_tick_ = 0;
	uint32_t election_deactive_tick_ = 0;

	uint32_t hb_tick_ = 0;
	uint32_t hb_deactive_tick_ = 0;
    // replicate state.. TODO
    
    std::map<uint32_t, uint64_t> vote_map_;
	std::map<uint32_t, std::unique_ptr<raft::Progress>> map_progress_;

    uint64_t disk_min_index_ = 0;

    std::unique_ptr<raft::RaftConfig> config_;
    std::set<uint32_t> vote_follower_set_;

//	// follower
	uint32_t missing_catch_up_ = 0;
	std::unique_ptr<TmpEntryCache> tmp_entry_cache_;
}; // class RaftMem



int safe_shrink_mem_log(RaftMem& raft_mem, size_t max_mem_log_size);


class TmpEntryCache {

public:
	TmpEntryCache();

	~TmpEntryCache();

	void MayInvalid(uint64_t active_term);
		
	void Insert(const raft::Entry& entry);

	void Extract(
			uint64_t active_term, raft::HardState& hard_state);

	size_t Size() const {
		return cache_.size();
	}

	uint64_t GetMaxIndex() const; 
	uint64_t GetMinIndex() const;

private:
	uint64_t term_;
	std::map<uint64_t, std::unique_ptr<raft::Entry>> cache_;
};


} // namespace raft


