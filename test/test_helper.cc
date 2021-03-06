#include <string>
#include <algorithm>
#include <cassert>
#include "tconfig_helper.h"
#include "test_helper.h"
#include "raft_mem.h"
#include "mem_utils.h"
#include "random_utils.h"
#include "progress.h"


namespace {

uint64_t get_log_term(const raft::RaftMem& raft_mem, uint64_t index)
{
    if (0 == index) {
        return 0;
    }

    assert(index >= raft_mem.GetMinIndex());
    assert(index <= raft_mem.GetMaxIndex());
    auto entry = raft_mem.At(index - raft_mem.GetMinIndex());
    assert(nullptr != entry);
    return entry->term();
}

}

bool operator==(const raft::Entry& a, const raft::Entry& b)
{
    std::string raw_a, raw_b;
    assert(a.SerializeToString(&raw_a));
    assert(b.SerializeToString(&raw_b));
    return raw_a == raw_b;
}

void add_entry(
        raft::HardState& hard_state, 
        uint64_t term, 
        uint64_t index, 
        const std::string& data)
{
    assert(0 < term);
    assert(0 < index);
    auto entry = hard_state.add_entries();
    assert(nullptr != entry);

    entry->set_type(raft::EntryType::EntryNormal);
    entry->set_term(term);
    entry->set_index(index);
    entry->set_reqid(0);
    entry->set_data(data);
}

void add_entry(
        raft::Message& msg, 
        uint64_t term, 
        uint64_t index, 
        const std::string& data)
{
    assert(0 < term);
    assert(0 < index);
    auto entry = msg.add_entries();
    assert(nullptr != entry);

    entry->set_type(raft::EntryType::EntryNormal);
    entry->set_term(term);
    entry->set_index(index);
    entry->set_reqid(0);
    entry->set_data(data);
}



std::unique_ptr<raft::RaftMem>
build_raft_mem(
        uint32_t svr_id, 
        uint32_t node_cnt, 
        uint64_t term, 
        uint64_t commit_index)
{
    assert(0 < svr_id);
    assert(3 <= node_cnt);
    assert(svr_id <= node_cnt);
    
    raft::ClusterConfig config = build_fake_config(node_cnt);

    auto hs = cutils::make_unique<raft::HardState>();
    assert(nullptr != hs);

    {
        assert(0 < term);
        assert(1 <= commit_index);
        auto meta = hs->mutable_meta();
        assert(nullptr != meta);
        meta->set_term(term);
        meta->set_vote(0);
        meta->set_commit(commit_index);
        meta->set_min_index(1);
        meta->set_max_index(commit_index);

        add_config(*hs, meta->term(), config);
        for (uint64_t idx = 2; idx <= commit_index; ++idx) {
            add_entry(*hs, term, idx, "");
        }
    }

    std::unique_ptr<raft::RaftMem> raft_mem 
        = cutils::make_unique<raft::RaftMem>(1, svr_id, default_option());
    assert(nullptr != raft_mem);
    assert(0 == raft_mem->Init(config, std::move(hs)));
    assert(commit_index == raft_mem->GetMaxIndex());
    assert(1 == raft_mem->GetMinIndex());
    assert(commit_index == raft_mem->GetCommit());
    assert(raft::RaftRole::FOLLOWER == raft_mem->GetRole());

    return std::move(raft_mem);
}

std::map<uint32_t, std::unique_ptr<raft::RaftMem>>
build_raft_mem(uint32_t node_cnt, uint64_t term, uint64_t commit_index)
{
    std::map<uint32_t, std::unique_ptr<raft::RaftMem>> map_raft;
    for (uint32_t svr_id = 1; svr_id <= node_cnt; ++svr_id) {
        auto raft_mem = build_raft_mem(
                svr_id, node_cnt, term, commit_index);
        assert(nullptr != raft_mem);
        map_raft[svr_id] = std::move(raft_mem);
    }

    return map_raft;
}

void make_fake_leader(raft::RaftMem& raft_mem)
{
    auto ss = cutils::make_unique<raft::SoftState>();
    assert(nullptr != ss);

    ss->set_role(static_cast<uint32_t>(raft::RaftRole::LEADER));
    raft_mem.ApplyState(nullptr, std::move(ss));
}

void make_fake_candidate(raft::RaftMem& raft_mem)
{
    auto ss = cutils::make_unique<raft::SoftState>();
    assert(nullptr != ss);

    ss->set_role(static_cast<uint32_t>(raft::RaftRole::CANDIDATE));
    raft_mem.ApplyState(nullptr, std::move(ss));
}

void set_progress_replicate(raft::RaftMem& raft_mem)
{
    auto& map_progress = raft_mem.GetProgress();
    assert(false == map_progress.empty());

    uint64_t max_index = raft_mem.GetMaxIndex();
    for (auto& id_progress : map_progress) {
        uint32_t peer_id = id_progress.first;
        auto progress = id_progress.second.get();
        assert(nullptr != progress);

        progress->BecomeReplicate();
        assert(raft::ProgressState::REPLICATE == progress->GetState());

        // update next
        progress->SetNext(max_index + 1);
        progress->MaybeUpdate(max_index);

        assert(max_index + 1 == progress->GetNext());
        assert(max_index == progress->GetMatched());
    }
}

std::unique_ptr<raft::Message>
apply_msg(
        std::map<uint32_t, std::unique_ptr<raft::RaftMem>>& map_raft, 
        const raft::Message& req_msg)
{
    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;
    bool need_disk_replicate = false;
    assert(0 != req_msg.to());
    if (map_raft.end() == map_raft.find(req_msg.to())) {
        printf ( "IGNORE: to %u don't exist\n", req_msg.to() );
        return nullptr;
    }

    auto& raft_mem = map_raft.at(req_msg.to());
    std::tie(broadcast, rsp_msg_type, need_disk_replicate)
        = raft_mem->Step(req_msg, hard_state, soft_state);
    raft_mem->ApplyState(
            std::move(hard_state), std::move(soft_state));
    if (false == broadcast) {
        return raft_mem->BuildRspMsg(req_msg, rsp_msg_type);
    }

    return raft_mem->BuildBroadcastRspMsg(rsp_msg_type);
}

void loop_until(
        std::map<uint32_t, std::unique_ptr<raft::RaftMem>>& map_raft, 
        std::vector<std::unique_ptr<raft::Message>>& vec_msg)
{
    if (vec_msg.empty()) {
        return ;
    }

    std::vector<std::unique_ptr<raft::Message>> vec_rsp_msg;
    for (auto& msg : vec_msg) {
        assert(nullptr != msg);
        if (0 != msg->to()) {
            auto rsp_msg = apply_msg(map_raft, *msg);
            if (nullptr != rsp_msg) {
                vec_rsp_msg.push_back(std::move(rsp_msg));
            }

            continue;
        }

        assert(0 == msg->to());
        for (int idx = 0; idx < msg->nodes_size(); ++idx) {
            const auto& node = msg->nodes(idx);
            msg->set_to(node.svr_id());
            auto rsp_msg = apply_msg(map_raft, *msg);
            if (nullptr != rsp_msg) {
                vec_rsp_msg.push_back(std::move(rsp_msg));
            }
        }
    }

    if (vec_rsp_msg.empty()) {
        return ;
    }

    loop_until(map_raft, vec_rsp_msg);
    return ;
}

raft::Message
build_null_msg(
        uint64_t logid, 
        uint64_t term, 
        uint32_t from, 
        uint32_t to)
{
    raft::Message msg;
    msg.set_type(raft::MessageType::MsgNull);
    msg.set_logid(logid);
    msg.set_term(term);
    msg.set_to(to);
    msg.set_from(from);
    return msg;
}

raft::Message
build_prop_msg(
        const raft::RaftMem& raft_mem, uint32_t entry_cnt)
{
    assert(0 < entry_cnt);
    raft::Message msg;
    msg.set_type(raft::MessageType::MsgProp);
    msg.set_logid(raft_mem.GetLogId());
    msg.set_term(raft_mem.GetTerm());
    msg.set_to(raft_mem.GetSelfId());
    msg.set_index(raft_mem.GetMaxIndex());
    msg.set_from(0);

    static cutils::RandomStrGen<10, 30> gen;
    for (uint32_t cnt = 0; cnt < entry_cnt; ++cnt) {
        add_entry(msg, msg.term(), msg.index() + 1 + cnt, gen.Next());
    }
    return msg;
}


raft::Message build_apprsp_msg(
        const raft::RaftMem& raft_mem, uint32_t from)
{
    assert(0 < from);
    raft::Message msg;
    msg.set_type(raft::MessageType::MsgAppResp);
    msg.set_logid(raft_mem.GetLogId());
    msg.set_term(raft_mem.GetTerm());
    msg.set_to(raft_mem.GetSelfId());
    msg.set_from(from);
    msg.set_index(raft_mem.GetMaxIndex());
    
    return msg;
}

raft::Message build_votersp_msg(
        const raft::RaftMem& raft_mem, uint32_t from)
{
    raft::Message msg;
    msg.set_type(raft::MessageType::MsgVoteResp);
    msg.set_logid(raft_mem.GetLogId());
    msg.set_term(raft_mem.GetTerm());
    msg.set_to(raft_mem.GetSelfId());
    msg.set_from(from);
    msg.set_reject(false);
    return msg;
}


raft::Message build_vote_msg(
        const raft::RaftMem& raft_mem, uint32_t from)
{
    raft::Message msg;
    msg.set_type(raft::MessageType::MsgVote);
    msg.set_logid(raft_mem.GetLogId());
    msg.set_term(raft_mem.GetTerm());
    msg.set_to(raft_mem.GetSelfId());
    msg.set_from(from);
    msg.set_index(raft_mem.GetMaxIndex());
    if (0 == raft_mem.GetMaxIndex()) {
        msg.set_log_term(0);
    }
    else {
        auto entry = raft_mem.At(
                raft_mem.GetMaxIndex() - raft_mem.GetMinIndex());
        assert(nullptr != entry);
        assert(entry->index() == raft_mem.GetMaxIndex());
        msg.set_log_term(entry->term());
    }

    return msg;
}

raft::Message build_hb_msg(
        const raft::RaftMem& raft_mem, uint32_t from)
{
    raft::Message msg;
    msg.set_type(raft::MessageType::MsgHeartbeat);
    msg.set_term(raft_mem.GetTerm());
    msg.set_logid(raft_mem.GetLogId());
    msg.set_from(from);    
    msg.set_to(raft_mem.GetSelfId());

    msg.set_index(raft_mem.GetMaxIndex());
    msg.set_log_term(get_log_term(raft_mem, msg.index()));
    return msg;
}

raft::Message build_app_msg(
        const raft::RaftMem& raft_mem, 
        uint64_t index, 
        uint32_t from, 
        uint32_t entry_cnt)
{
    raft::Message msg;
    msg.set_type(raft::MessageType::MsgApp);
    msg.set_logid(raft_mem.GetLogId());
    msg.set_term(raft_mem.GetTerm());
    msg.set_to(raft_mem.GetSelfId());
    msg.set_from(from);
    
    msg.set_index(index);
    msg.set_log_term(get_log_term(raft_mem, msg.index()));
    printf ( "msg index %lu log_term %lu\n", 
            msg.index(), msg.log_term() );
    cutils::RandomStrGen<10, 30> gen;
    for (uint32_t cnt = 0; cnt < entry_cnt; ++cnt) {
        add_entry(msg, raft_mem.GetTerm(), index + cnt + 1, gen.Next());
    }

    return msg;
}
