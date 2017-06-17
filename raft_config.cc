#include <algorithm>
#include <cassert>
#include "raft_config.h"
#include "mem_utils.h"
#include "raft.pb.h"

namespace raft {

RaftConfig::RaftConfig() = default;

RaftConfig::~RaftConfig() = default;

void RaftConfig::Apply(
        const raft::SoftState& soft_state, 
        uint64_t commited_index)
{
    std::unique_ptr<raft::ClusterConfig> new_config;
    if (soft_state.has_config()) {
        new_config = cutils::make_unique<
            raft::ClusterConfig>(soft_state.config());
        assert(nullptr != new_config);
        assert(is_valid(*new_config));
    }

    if (nullptr != new_config) {
        if (new_config->index() <= commited_index) {
            config_ = std::move(new_config);
            pending_config_ = nullptr; // must be force reset anyway
        }
        else {
            // new_config.index >= commited_index;
            config_ = std::move(pending_config_);
            pending_config_ = std::move(new_config);
        }
    }

    assert(nullptr == new_config);
    assert(nullptr != config_);
    assert(config_->index() <= commited_index);
    if (nullptr != pending_config_) {
        assert(pending_config_->index() > config_->index());
        assert(pending_config_->index() > commited_index);
    }
}

const raft::ClusterConfig* RaftConfig::GetConfig() const 
{
    if (nullptr != pending_config_) {
        assert(nullptr != config_);
        assert(pending_config_->index() > config_->index());
        return pending_config_.get();
    }

    return config_.get();
}

const raft::ClusterConfig*
RaftConfig::GetCommitConfig() const 
{
    return config_.get();
}

const raft::ClusterConfig* 
RaftConfig::GetPendingConfig() const 
{
    return pending_config_.get();
}


bool is_valid(const raft::ClusterConfig& config)
{
    assert(0 < config.index());
    assert(0 < config.max_id());
    assert(3 <= config.nodes_size());

    std::set<uint32_t> unique_id;
    std::set<std::pair<uint32_t, uint32_t>> unique_endpoint;
    for (int idx = 0; idx < config.nodes_size(); ++idx) {
        const auto& node = config.nodes(idx);
        assert(unique_id.end() == unique_id.find(node.svr_id()));
        assert(unique_endpoint.end() == 
                unique_endpoint.find(
                    std::make_pair(node.svr_ip(), node.svr_port())));
        unique_id.insert(node.svr_id());
        unique_endpoint.insert(
                std::make_pair(node.svr_ip(), node.svr_port()));
        assert(node.svr_id() <= config.max_id());
    }

    return true;
}

std::tuple<bool, raft::Node> 
get(const raft::ClusterConfig& config, uint32_t peer)
{
    assert(is_valid(config));
    for (int idx = 0; idx < config.nodes_size(); ++idx) {
        const auto& node = config.nodes(idx);
        if (node.svr_id() == peer) {
            return std::make_tuple(true, node);
        }
    }

    return std::make_tuple(false, raft::Node{});
}

} // namespace raft




