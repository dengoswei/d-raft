#include <cassert>
#include "raft_config.h"
#include "mem_utils.h"
#include "raft.pb.h"

namespace raft {

RaftConfig::RaftConfig()
{

}

RaftConfig::~RaftConfig() = default;

void RaftConfig::Apply(
        const raft::ConfState* new_state, 
        const bool has_commited)
{
    if (nullptr == new_state) {
        if (true == has_commited) {
            assert(false == pending_.empty());
            commited_.swap(pending_);
            pending_.clear();
        }

        return ;
    }

    assert(nullptr != new_state);
    std::set<uint32_t> new_pending;
    for (int idx = 0; idx < new_state->nodes_size(); ++idx) {
        new_pending.insert(new_state->nodes(idx));
    }

    assert(false == new_pending.empty());
    if (true == has_commited) {
        commited_.swap(new_pending);
        pending_.clear();
        return ;
    }

    assert(false == has_commited);
    if (false == pending_.empty()) {
        commited_.swap(pending_);
        pending_.clear();
    }

    assert(true == pending_.empty());
    // assert check
    {
        std::vector<uint32_t> vec_diff;
        std::set_symmetric_difference(
                commited_.begin(), commited_.end(), 
                new_pending.begin(), new_pending.end(), 
                std::back_insert_iterator<
                    std::vector<uint32_t>>{vec_diff});
        assert(size_t{1} >= vec_diff.size());
    }

    pending_.swap(new_pending);
}

void RaftConfig::Revert()
{
    assert(false == commited_.empty());
    assert(false == pending_.empty());
    pending_.clear();
}

const std::set<uint32_t>& RaftConfig::GetNodeSet() const 
{
    if (false == pending_.empty()) {
        return pending_;
    }

    assert(true == pending_.empty());
    return commited_;
}

} // namespace raft




