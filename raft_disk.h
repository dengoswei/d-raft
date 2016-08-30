#pragma once



namespace raft {

using ReadHandler = 
    std::function<std::tuple<
        int, std::unique_ptr<raft::HardState>(uint64_t, uint64_t, int)>>;


// catch-up use only!
class RaftDisk {

public:
    RaftDisk(
            uint64_t logid, 
            uint32_t selfid, 
            ReadHandler readcb);

    ~RaftDisk();

    std::tuple<int, std::unique_ptr<raft::Message>> 
        Step(const raft::Message& msg);

private:
    const uint64_t logid_ = 0ull;
    const uint32_t selfid_ = 0u;
    
    ReadHandler readcb_;

    raft::RaftRole role_ = raft::RaftRole::FOLLOWER;

    uint64_t term_ = 0;
    uint64_t commit_ = 0;

    std::unique_ptr<raft::Replicate> replicate_;
    std::set<uint32_t> replicate_follower_set_;
}; // class RaftDisk;


} // namespace raft



