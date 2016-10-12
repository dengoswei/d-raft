#pragma once

#include <stdint.h>
#include <memory>
#include <set>



namespace raft {

class ConfState;


// If server adopted Cnew only when they learned that Cnew
// was commited, Raft leaders would have a difficult time knowing
// when a majority of the old cluster had adopted it.
class RaftConfig {

public:
    RaftConfig();

    ~RaftConfig();

    void Apply(const raft::ConfState* new_state, bool has_commited);

    void Revert();

    const std::set<uint32_t>& GetNodeSet() const;

private:
    std::set<uint32_t> commited_;
    std::set<uint32_t> pending_;
}; // class RaftConfig


} // namespace raft


