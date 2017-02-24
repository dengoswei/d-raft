#include "test_helper.h"
#include "log_utils.h"
#include "progress.h"

std::unique_ptr<raft::RaftMem>
build_raft_mem(
        uint32_t id, 
        uint64_t term, uint64_t commit_index, raft::RaftRole role)
{
    std::unique_ptr<raft::RaftMem> raft_mem = 
        cutils::make_unique<raft::RaftMem>(1, id, 100, 15);
    assert(nullptr != raft_mem);

    std::unique_ptr<raft::HardState> hard_state = 
        cutils::make_unique<raft::HardState>();
    assert(nullptr != hard_state);

    {
        auto meta = hard_state->mutable_meta();
        assert(nullptr != meta);
        meta->set_term(term);
        meta->set_vote(0);
        meta->set_commit(commit_index);
    }
    
    if (0 != commit_index) {
        raft::Entry* entry = hard_state->add_entries();
        assert(nullptr != entry);
        entry->set_type(raft::EntryType::EntryNormal);
        entry->set_term(term);
        entry->set_index(commit_index);
        entry->set_reqid(0);
    }

	assert(0 == raft_mem->Init(*hard_state));

    std::unique_ptr<raft::SoftState> soft_state = 
        cutils::make_unique<raft::SoftState>();
    assert(nullptr != soft_state);
    soft_state->set_role(static_cast<uint32_t>(role));

    raft_mem->ApplyState(nullptr, std::move(soft_state));
    assert(raft_mem->GetRole() == role);
    assert(raft_mem->GetTerm() == term);
    assert(raft_mem->GetCommit() == commit_index);
    assert(raft_mem->GetMaxIndex() == commit_index);
    assert(raft_mem->GetMinIndex() == commit_index);
    return raft_mem;
}

std::unique_ptr<raft::RaftMem> 
    build_raft_mem(
            uint64_t term, uint64_t commit_index, raft::RaftRole role)
{
    return build_raft_mem(1, term, commit_index, role);
}


std::unique_ptr<raft::Message> 
build_to_msg(
        const raft::RaftMem& raft_mem, 
        raft::MessageType msg_type, 
        uint32_t follower_id)
{
    std::unique_ptr<raft::Message> msg =
        cutils::make_unique<raft::Message>();
    assert(nullptr != msg);

    msg->set_type(msg_type);
    msg->set_logid(raft_mem.GetLogId());
    msg->set_term(raft_mem.GetTerm());
    msg->set_to(follower_id);
    msg->set_from(raft_mem.GetSelfId());
    return msg;
}

std::unique_ptr<raft::Message> 
build_from_msg(
        const raft::RaftMem& raft_mem, 
        raft::MessageType msg_type, 
        uint32_t follower_id)
{
    auto msg = cutils::make_unique<raft::Message>();
    assert(nullptr != msg);

    msg->set_type(msg_type);
    msg->set_logid(raft_mem.GetLogId());
    msg->set_term(raft_mem.GetTerm());
    msg->set_to(raft_mem.GetSelfId());
    msg->set_from(follower_id);
    return msg;
}

void add_entries(
        std::unique_ptr<raft::HardState>& hard_state, 
        uint64_t term, 
        uint64_t index)
{
    assert(nullptr != hard_state);
    auto entry = hard_state->add_entries();
    assert(nullptr != entry);
    entry->set_type(raft::EntryType::EntryNormal);
    entry->set_term(term);
    entry->set_index(index);
    entry->set_reqid(0);
}

void add_entries(
        std::unique_ptr<raft::Message>& msg, 
        uint64_t term, 
        uint64_t index)
{
    assert(nullptr != msg);
    auto entry = msg->add_entries();
    assert(nullptr != entry);
    entry->set_type(raft::EntryType::EntryNormal);
    entry->set_term(term);
    entry->set_index(index);
    entry->set_reqid(0);
}

void update_term(
        std::unique_ptr<raft::RaftMem>& raft_mem, 
        uint64_t next_term)
{
    assert(nullptr != raft_mem);
    assert(raft_mem->GetTerm() < next_term);

    std::unique_ptr<raft::HardState>
        hard_state = cutils::make_unique<raft::HardState>();
    assert(nullptr != hard_state);

    {
        auto meta = hard_state->mutable_meta();
        assert(nullptr != meta);
        meta->set_term(next_term);
    }

    raft_mem->ApplyState(std::move(hard_state), nullptr);
}


void update_role(
        std::unique_ptr<raft::RaftMem>& raft_mem, 
        raft::RaftRole role)
{
    assert(nullptr != raft_mem);
    auto soft_state = cutils::make_unique<raft::SoftState>();
    assert(nullptr != soft_state);

    soft_state->set_role(static_cast<uint32_t>(role));

    raft_mem->ApplyState(nullptr, std::move(soft_state));
    assert(raft_mem->GetRole() == role);
}

bool operator==(const raft::Entry&a, const raft::Entry& b)
{
    assert(a.type() == b.type());
    assert(a.term() == b.term());
    assert(a.index() == b.index());
    assert(a.reqid() == b.reqid());
    assert(a.data() == b.data());
    return true;
}

std::vector<std::unique_ptr<raft::Message>>
trigger_timeout(
        std::map<uint32_t, std::unique_ptr<raft::RaftMem>>& mapRaft, 
        uint32_t id)
{
    assert(mapRaft.end() != mapRaft.find(id));

    auto& raft_mem = mapRaft.at(id);
    assert(nullptr != raft_mem);

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;

    std::tie(hard_state, 
            soft_state, mark_broadcast, rsp_msg_type) = raft_mem->CheckTimeout(true);

	assert(mark_broadcast);
    raft::Message fake_msg;
    fake_msg.set_type(raft::MessageType::MsgNull);
    fake_msg.set_logid(raft_mem->GetLogId());
    fake_msg.set_to(raft_mem->GetSelfId());
    fake_msg.set_from(0);
    fake_msg.set_index(1);
    auto vec_rsp_msg = raft_mem->BuildBroadcastRspMsg(
            fake_msg, hard_state, soft_state, rsp_msg_type);
    raft_mem->ApplyState(std::move(hard_state), std::move(soft_state));
	return vec_rsp_msg;
}


// std::unique_ptr<raft::Message>
std::vector<std::unique_ptr<raft::Message>>
apply_msg(
        std::map<uint32_t, std::unique_ptr<raft::RaftMem>>& mapRaft, 
        const raft::Message& msg)
{
    if (mapRaft.end() == mapRaft.find(msg.to()) || 
            nullptr == mapRaft.at(msg.to())) {
        return {};
    }

    assert(mapRaft.end() != mapRaft.find(msg.to()));

    auto& raft_mem = mapRaft.at(msg.to());

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;
    bool need_disk_replicate = false;

    std::tie(hard_state, 
            soft_state, mark_broadcast, rsp_msg_type, 
            need_disk_replicate) = raft_mem->Step(msg, nullptr, nullptr);

    assert(false == need_disk_replicate);
	if (false == mark_broadcast) {
		auto rsp_msg = raft_mem->BuildRspMsg(
				msg, hard_state, soft_state, msg.from(), rsp_msg_type);
		raft_mem->ApplyState(std::move(hard_state), std::move(soft_state));

		std::vector<std::unique_ptr<raft::Message>> vec_msg;
		if (nullptr != rsp_msg) {
			vec_msg.push_back(std::move(rsp_msg));
		}
		return vec_msg;
	}

	assert(mark_broadcast);
	auto vec_msg = raft_mem->BuildBroadcastRspMsg(
			msg, hard_state, soft_state, rsp_msg_type);
	assert(false == vec_msg.empty());

	raft_mem->ApplyState(std::move(hard_state), std::move(soft_state));
	return vec_msg;
}

void loop_until(
        std::map<uint32_t, std::unique_ptr<raft::RaftMem>>& mapRaft, 
        const std::vector<std::unique_ptr<raft::Message>>& vecMsg)
{
    if (vecMsg.empty()) {
        return ;
    }

    std::vector<std::unique_ptr<raft::Message>> vecRspMsg;
    for (const auto& msg : vecMsg) {
        assert(nullptr != msg);

        logdebug("MSG type %d from %u to %u index %d", 
                static_cast<int>(msg->type()), 
                msg->from(), msg->to(), static_cast<int>(msg->index()));
		assert(0 != msg->to());
        auto vec_msg = apply_msg(mapRaft, *msg);
		for (auto& rsp_msg : vec_msg) {
			assert(nullptr != rsp_msg);
			assert(0 != rsp_msg->to());
			vecRspMsg.push_back(std::move(rsp_msg));
		}
    }

    return loop_until(mapRaft, vecRspMsg);
}


bool make_leader(
        std::map<uint32_t, std::unique_ptr<raft::RaftMem>>& mapRaft, 
        uint32_t next_leader_id)
{
    assert(mapRaft.end() != mapRaft.find(next_leader_id));
    assert(nullptr != mapRaft.at(next_leader_id));

    std::vector<std::unique_ptr<raft::Message>> 
		vecMsg = trigger_timeout(mapRaft, next_leader_id);
	assert(size_t{2} == vecMsg.size());

    loop_until(mapRaft, vecMsg);
    return raft::RaftRole::LEADER == mapRaft.at(next_leader_id)->GetRole();
}


std::vector<std::unique_ptr<raft::Message>>
set_value(
        raft::RaftMem& raft_mem, 
        const std::string& value, uint64_t reqid)
{
    assert(raft::RaftRole::LEADER == raft_mem.GetRole());
    std::unique_ptr<raft::Message> prop_msg;
    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;

    std::tie(prop_msg, 
            hard_state, soft_state, mark_broadcast, rsp_msg_type)
        = raft_mem.SetValue(value, reqid);
    assert(nullptr != prop_msg);
    assert(nullptr != hard_state);
    assert(nullptr == soft_state);
    assert(true == mark_broadcast);
    assert(raft::MessageType::MsgApp == rsp_msg_type);

    auto vec_app_msg = raft_mem.BuildBroadcastRspMsg(
            *prop_msg, hard_state, soft_state, rsp_msg_type);
	assert(false == vec_app_msg.empty());
    raft_mem.ApplyState(std::move(hard_state), std::move(soft_state));
    return vec_app_msg;
}


//std::unique_ptr<raft::RaftDisk>
//build_raft_disk(uint32_t id, FakeDiskStorage& storage)
//{
//    auto raft_disk = cutils::make_unique<raft::RaftDisk>(1, id, 
//            [&](uint64_t logid, uint64_t log_index, int entries_size)
//                -> std::tuple<int, std::unique_ptr<raft::HardState>> {
//                return storage.Read(logid, log_index, entries_size);
//            });
//
//    assert(nullptr != raft_disk);
//    return raft_disk;
//}


std::unique_ptr<raft::RaftDiskCatchUp>
build_raft_disk_c(
        uint32_t selfid, uint64_t term, 
		uint64_t max_index, uint64_t min_index, uint64_t disk_commit_index, 
        FakeDiskStorage& storage)
{
	assert(max_index >= disk_commit_index);
    auto raft_disk_c = 
        cutils::make_unique<raft::RaftDiskCatchUp>(
                1, selfid, term, max_index, min_index, disk_commit_index, 
                [&](uint64_t logid, uint64_t log_index, int entries_size)
                    -> std::tuple<int, std::unique_ptr<raft::HardState>> {
                    return storage.Read(logid, log_index, entries_size);
                });
    assert(nullptr != raft_disk_c);
    return raft_disk_c;
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
		// update matched
		progress->MaybeUpdate(max_index);

		assert(max_index + 1 == progress->GetNext());
		assert(max_index == progress->GetMatched());
	}
}
