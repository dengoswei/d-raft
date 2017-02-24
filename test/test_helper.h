#pragma once

#include <deque>
#include <memory>
#include <algorithm>
#include "raft_mem.h"
#include "raft.pb.h"
#include "mem_utils.h"
#include "raft_disk.h"
#include "raft_disk_catchup.h"


std::unique_ptr<raft::RaftMem>
build_raft_mem(
        uint32_t id, 
        uint64_t term, uint64_t commit_index, raft::RaftRole role);

std::unique_ptr<raft::RaftMem> 
build_raft_mem(
        uint64_t term, uint64_t commit_index, raft::RaftRole role);

std::unique_ptr<raft::Message> 
build_to_msg(
        const raft::RaftMem& raft_mem, 
        raft::MessageType msg_type, 
        uint32_t follower_id);

std::unique_ptr<raft::Message> 
build_from_msg(
        const raft::RaftMem& raft_mem, 
        raft::MessageType msg_type, 
        uint32_t follower_id);

void add_entries(
        std::unique_ptr<raft::HardState>& hard_state, 
        uint64_t term, 
        uint64_t index);


void add_entries(
        std::unique_ptr<raft::Message>& msg, 
        uint64_t term, 
        uint64_t index);

void update_term(
        std::unique_ptr<raft::RaftMem>& raft_mem, 
        uint64_t next_term);

void update_role(
        std::unique_ptr<raft::RaftMem>& raft_mem, 
        raft::RaftRole role);

bool operator==(const raft::Entry& a, const raft::Entry& b);

bool make_leader(
        std::map<uint32_t, std::unique_ptr<raft::RaftMem>>& mapRaft, 
        uint32_t next_leader_id);

std::vector<std::unique_ptr<raft::Message>>
trigger_timeout(
        std::map<uint32_t, std::unique_ptr<raft::RaftMem>>& mapRaft,
        uint32_t id);

std::vector<std::unique_ptr<raft::Message>>
set_value(
        raft::RaftMem& raft_mem, 
        const std::string& value, uint64_t reqid);

// std::unique_ptr<raft::Message>
std::vector<std::unique_ptr<raft::Message>>
apply_msg(
        std::map<uint32_t, std::unique_ptr<raft::RaftMem>>& mapRaft, 
        const raft::Message& msg);

void loop_until(
        std::map<uint32_t, std::unique_ptr<raft::RaftMem>>& mapRaft, 
        const std::vector<std::unique_ptr<raft::Message>>& vecMsg);


class FakeDiskStorage {

public:
    FakeDiskStorage() = default;
    ~FakeDiskStorage() = default;

    int Write(const raft::HardState& hard_state)
    {
        if (0 == hard_state.entries_size()) {
            return 0;
        }

        auto min_index = getMinIndex();
        auto max_index = getMaxIndex();

        auto begin_index = hard_state.entries(0).index();
        if (begin_index > max_index + 1 || begin_index < min_index) {
            return -1;
        }

        if (begin_index <= max_index) 
        {
            for (int pop_time = 0; 
                    pop_time < max_index - begin_index + 1; ++pop_time) {
                logs_.pop_back();
            }
        }

        max_index = getMaxIndex();
        assert(begin_index == max_index + 1);

        for (int idx = 0; idx < hard_state.entries_size(); ++idx) {
            auto entry = cutils::make_unique<raft::Entry>();
            assert(nullptr != entry);
            *entry = hard_state.entries(idx);
            printf("%s idx %d index %u\n", __func__, idx, (uint32_t)entry->index());
            logs_.push_back(std::move(entry));
        }

        return 0;
    }

    std::tuple<int, std::unique_ptr<raft::HardState>>
    Read(uint64_t logid, uint64_t log_index, int entries_size)
    {
        assert(0 < log_index);
        if (logs_.empty()) {
            return std::make_tuple(-1, nullptr);
        }

        assert(false == logs_.empty());
        assert(nullptr != logs_.front());
        assert(nullptr != logs_.back());

        auto min_index = getMinIndex();
        auto max_index = getMaxIndex();
        if (log_index < min_index || log_index > max_index) {
            return std::make_tuple(-1, nullptr);
        }

        printf ( "%s log_index %u min_index %u max_index %u\n", 
                __func__, (uint32_t)log_index, (uint32_t)min_index, (uint32_t)max_index );
        int idx = log_index - min_index;
        auto hard_state = cutils::make_unique<raft::HardState>();
        assert(nullptr != hard_state);
        printf ( "entries_size %d min %d\n", 
                entries_size, (int)max_index - log_index + 1);
        entries_size = std::min<int>(entries_size, max_index - log_index + 1);
        for (; idx < (log_index - min_index) + entries_size; ++idx) {
            const auto& disk_entry = logs_[idx];
            assert(nullptr != disk_entry);

            auto new_entry = hard_state->add_entries();
            assert(nullptr != new_entry);
            *new_entry = *disk_entry;
            assert(min_index + idx == new_entry->index());
        }
        assert(0 < hard_state->entries_size());
        assert(entries_size == hard_state->entries_size());
        return std::make_tuple(0, std::move(hard_state));
    }

private:
    uint64_t getMinIndex() const {
        return logs_.empty() ? 0 : logs_.front()->index();
    }

    uint64_t getMaxIndex() const {
        return logs_.empty() ? 0 : logs_.back()->index();
    }

private:
    std::deque<std::unique_ptr<raft::Entry>> logs_;
};


//std::unique_ptr<raft::RaftDisk>
//build_raft_disk(uint32_t id, FakeDiskStorage& storage);

std::unique_ptr<raft::RaftDiskCatchUp>
build_raft_disk_c(
        uint32_t selfid, uint64_t term, 
		uint64_t max_index, uint64_t min_index, uint64_t disk_commit_index, 
        FakeDiskStorage& storage);


void set_progress_replicate(raft::RaftMem& raft_mem);
