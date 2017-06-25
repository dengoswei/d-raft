#pragma once

#include <stdint.h>
#include <memory>
#include <map>
#include "raft.pb.h"

namespace raft {

    class RaftMem;
}


bool operator==(const raft::Entry& a, const raft::Entry& b);

void add_entry(
        raft::HardState& hard_state, 
        uint64_t term, 
        uint64_t index, 
        const std::string& data);

std::unique_ptr<raft::RaftMem>
build_raft_mem(
        uint32_t id, 
        uint32_t node_cnt, 
        uint64_t term, 
        uint64_t commit_index);

std::map<uint32_t, std::unique_ptr<raft::RaftMem>>
build_raft_mem(uint32_t node_cnt, uint64_t term, uint64_t commit_index);

// 
void make_fake_leader(raft::RaftMem& raft_mem);
void make_fake_candidate(raft::RaftMem& raft_mem);
void set_progress_replicate(raft::RaftMem& raft_mem);


std::unique_ptr<raft::Message>
apply_msg(
        std::map<uint32_t, std::unique_ptr<raft::RaftMem>>& map_raft, 
        const raft::Message& req_msg);

void loop_until(
        std::map<uint32_t, std::unique_ptr<raft::RaftMem>>& map_raft, 
        std::vector<std::unique_ptr<raft::Message>>& vec_msg);

// msg helper function
void add_entry(
        raft::Message& msg, 
        uint64_t term, 
        uint64_t index, 
        const std::string& data);


raft::Message build_null_msg(
        uint64_t logid, uint64_t term, uint32_t from, uint32_t to);

raft::Message build_prop_msg(
        const raft::RaftMem& raft_mem, uint32_t entry_cnt);

raft::Message build_apprsp_msg(
        const raft::RaftMem& raft_mem, uint32_t from);

raft::Message build_votersp_msg(
        const raft::RaftMem& raft_mem, uint32_t from);

raft::Message build_vote_msg(
        const raft::RaftMem& raft_mem, uint32_t from);

raft::Message build_hb_msg(
        const raft::RaftMem& raft_mem, uint32_t from);

raft::Message build_app_msg(
        const raft::RaftMem& raft_mem, 
        uint64_t index, 
        uint32_t from, 
        uint32_t entry_cnt);

