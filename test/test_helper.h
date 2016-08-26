#pragma once

#include "raft_mem.h"
#include "raft.pb.h"
#include "mem_utils.h"


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
