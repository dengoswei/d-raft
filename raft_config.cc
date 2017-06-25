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
    {
        // check
        uint64_t index = 0;
        for (int idx = 0; idx < soft_state.configs_size(); ++idx) {
            const auto& config = soft_state.configs(idx);
            assert(index < config.index());
            index = config.index();
        }
    }

    if (soft_state.drop_pending()) {
        assert(nullptr != pending_);
        pending_ = nullptr; // drop pending
    }

    std::unique_ptr<raft::ClusterConfig> new_config;
    if (0 < soft_state.configs_size()) {
        // mark final
        int fidx = soft_state.configs_size() - 1;
        assert(0 <= fidx);
        new_config = cutils::make_unique<
            raft::ClusterConfig>(soft_state.configs(fidx));
        assert(nullptr != new_config);
        assert(is_valid(*new_config));
    }

    if (nullptr != new_config) {
        if (new_config->index() <= commited_index) {
            config_ = std::move(new_config);
            pending_ = nullptr; // must be force reset anyway
        }
        else {
            // new_config.index >= commited_index;
            if (1 < soft_state.configs_size()) {
                // more the 1..
                int pidx = soft_state.configs_size() - 2;
                assert(0 <= pidx);
                assert(config_->index() < 
                        soft_state.configs(pidx).index());
                config_ = cutils::make_unique<
                    raft::ClusterConfig>(soft_state.configs(pidx));
                assert(nullptr != config_);
                assert(config_->index() < new_config->index());
            }
            else {
                // if nullptr != pending_; config_ = pending_
                // pending_ = new_config
                assert(1 == soft_state.configs_size());
                if (nullptr != pending_) {
                    assert(pending_->index() <= commited_index);
                    assert(nullptr != config_);
                    assert(config_->index() < pending_->index());
                    config_ = std::move(pending_);
                }
                // else => nothing change;
            }

            pending_ = std::move(new_config);
            assert(nullptr != config_);
            assert(nullptr != pending_);
        }
    }

    assert(nullptr == new_config);
    assert(nullptr != config_);
    assert(config_->index() <= commited_index);
    if (nullptr != pending_) {
        assert(pending_->index() > config_->index());
        if (pending_->index() <= commited_index) {
            config_ = std::move(pending_);
        }

        assert(pending_->index() > commited_index);
    }
}

const raft::ClusterConfig* RaftConfig::GetConfig() const 
{
    if (nullptr != pending_) {
        assert(nullptr != config_);
        assert(pending_->index() > config_->index());
        return pending_.get();
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
    return pending_.get();
}

bool RaftConfig::IsMember(uint32_t peer) const 
{
    auto config = GetConfig();
    assert(nullptr != config);
    for (int idx = 0; idx < config->nodes_size(); ++idx) {
        const auto& node = config->nodes(idx);
        if (node.svr_id() == peer) {
            return true;
        }
    }

    return false;
}

raft::Node 
RaftConfig::Get(uint32_t peer, const raft::Node& def_node) const 
{
    auto config = GetConfig();
    assert(nullptr != config);
    for (int idx = 0; idx < config->nodes_size(); ++idx) {
        const auto& node = config->nodes(idx);
        if (peer == node.svr_id()) {
            return node;
        }
    }

    return def_node;
}


bool is_valid(const raft::ClusterConfig& config)
{
    assert(0 < config.index());
    assert(0 < config.max_id());
    assert(2 <= config.nodes_size());

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




