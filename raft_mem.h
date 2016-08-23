#pragma once


namespace raft {


enum class RaftRole : uint8_t;


class RaftMem {

public:
    RaftMem();

    ~RaftMem();


    // bool for broad-cast
    std::tuple<
        std::unique_ptr<raft::HardState>, 
        std::unique_ptr<raft::SoftState>, 
        bool, raft::MessageType>
            Step(const raft::Message& msg);

    // 0 ==
    void ApplyState(
            std::unique_ptr<raft::SoftState>& soft_state, 
            std::unique_ptr<raft::HardState>& hard_state);

    std::unique_ptr<raft::Message> BuildRspMsg(
            const raft::Message& msg, 
            bool mar_broadcast, raft::MessageType rsp_msg_type);

private:
    const uint64_t logid_ = 0ull;
    const uint32_t selfid_ = 0ull;

    RaftRole role_ = RaftRole::FOLLOWER;
    StepMessageHandler step_handler_ = nullptr;

    uint32_t leader_id_ = 0; // soft state

    // hard-state
    uint64_t term_ = 0;
    uint64_t vote_ = 0;
    uint64_t commited_index_ = 0; // soft state ?


    std::deque<std::unique_ptr<Entry>> logs_;

    std::chrono::time_point<std::chrono::system_clock> active_time_;
    // replicate state.. TODO


    
} // class RaftMem

} // namespace raft


