#include <algorithm>
#include "mem_utils.h"
#include "log_utils.h"
#include "time_utils.h"
#include "id_utils.h"
#include "raft_mem.h"
#include "raft_state.h"
#include "raft_config.h"
#include "progress.h"
#include "hassert.h"


namespace {

enum {
	SWITCH_TO_EXPLORE = 50, 
	SWITCH_TO_APP = 51, 
	SWITCH_TO_DISK = 52, 
	SILENCE = 53, 

	KEEP_EXPLORE = 60, 
};


using namespace raft;

void process_entry(
        const raft::Entry& entry, 
        std::unique_ptr<raft::SoftState>& soft_state)
{
    if (raft::EntryType::EntryConfChange != entry.type()) {
        return ;
    }

    // only deal with EntryConfChange
    if (nullptr == soft_state) {
        soft_state = cutils::make_unique<raft::SoftState>();
    }

    assert(nullptr != soft_state);
    if (0 < soft_state->configs_size()) {
        int fidx = soft_state->configs_size() - 1;
        assert(0 <= fidx);
        assert(entry.index() > soft_state->configs(fidx).index());
    }

    auto new_config = soft_state->add_configs();
    assert(nullptr != new_config);
    assert(new_config->ParseFromString(entry.data()));
    assert(new_config->index() == entry.index());
}


void assert_check(
		const raft::Entry& a, const raft::Entry& b)
{
	assert(a.ByteSize() == b.ByteSize());
	std::string sa;
	std::string sb;

	assert(a.SerializeToString(&sa));
	assert(b.SerializeToString(&sb));
	assert(sa == sb);
}

void setVote(
        std::unique_ptr<raft::HardState>& hard_state, 
        uint64_t term, 
        uint32_t vote)
{
    if (nullptr == hard_state) {
        hard_state = cutils::make_unique<raft::HardState>();
        assert(nullptr != hard_state);
    }

    assert(nullptr != hard_state);
    auto meta = hard_state->mutable_meta();
    if (meta->has_term()) {
        if (meta->term() == term) {
            assert(0 == meta->vote());
            meta->set_vote(vote);
        }
        else {
            assert(meta->term() < term);
            meta->set_term(term);
            meta->set_vote(vote);
        }
    }
    else {
        assert(false == meta->has_vote());
        meta->set_term(term);
        meta->set_vote(vote);
    }
}

void updateCommit(
        std::unique_ptr<raft::HardState>& hard_state, 
		uint64_t new_commit, uint64_t max_index)
{
	assert(new_commit <= max_index);
    if (nullptr == hard_state) {
        hard_state = cutils::make_unique<raft::HardState>();
        assert(nullptr != hard_state);
    }

    assert(nullptr != hard_state);
    auto meta = hard_state->mutable_meta();
    assert(nullptr != meta);

    if (meta->commit() < new_commit) {
        meta->set_commit(new_commit);
    }

	meta->set_max_index(max_index);
	assert(meta->commit() <= meta->max_index());
}

void updateDiskMinIndex(
		std::unique_ptr<raft::HardState>& hard_state, uint64_t disk_min_index)
{
	if (nullptr == hard_state) {
		hard_state = cutils::make_unique<raft::HardState>();
		assert(nullptr != hard_state);
	}

	assert(nullptr != hard_state);
	auto meta = hard_state->mutable_meta();
	assert(nullptr != meta);
	assert(meta->min_index() <= disk_min_index);
	meta->set_min_index(disk_min_index);
}

void resetMaxIndex(
        std::unique_ptr<raft::HardState>& hard_state, uint64_t new_max_index)
{
    if (nullptr == hard_state) {
        hard_state = cutils::make_unique<raft::HardState>();
    }

    assert(nullptr != hard_state);
    auto meta = hard_state->mutable_meta();
    assert(nullptr != meta);
    meta->set_max_index(new_max_index);
}

//void updateVoteFollowerSet(
//        uint32_t selfid, 
//        const raft::RaftConfig& config, 
//        std::set<uint32_t>& vote_follower_set)
//{
//    std::set<uint32_t> new_vote_follower_set = config.GetNodeSet();
//    new_vote_follower_set.erase(selfid);
//    vote_follower_set.swap(new_vote_follower_set);
//}

int calculateMajorYesCount(
        uint64_t vote_term, 
        const std::map<uint32_t, uint64_t>& vote_map)
{
    int major_yes_cnt = 0;
    for (const auto& vote_pair : vote_map) {
        if (vote_pair.second == vote_term) {
            ++major_yes_cnt;
        }
        else {
            assert(0 == vote_pair.second);
        }
    }

    return major_yes_cnt;
}

const raft::Entry* getLogEntry(
        const raft::RaftState& raft_state, uint64_t log_index)
{
    if (0 == log_index) {
        return nullptr;
    }

    assert(0 < log_index);
    assert(log_index <= raft_state.GetMaxIndex());
    uint64_t min_index = raft_state.GetMinIndex();
    assert(0 < min_index);
	if (log_index < min_index) {
		assert(log_index + 1 == min_index);
		return nullptr;
	}
    assert(log_index >= min_index);
    int mem_idx = log_index - min_index;
    assert(0 <= mem_idx);
    const raft::Entry* mem_entry = raft_state.At(mem_idx);
    assert(nullptr != mem_entry);
    assert(mem_entry->index() == log_index);
    assert(0 < mem_entry->term());
    return mem_entry;
}

uint64_t getLogTerm(const raft::RaftState& raft_state, uint64_t log_index)
{
    const raft::Entry* mem_entry = getLogEntry(raft_state, log_index);
    if (nullptr == mem_entry) {
		assert(0 == log_index);
		return 0;
    }

    assert(mem_entry->index() == log_index);
    assert(0 < mem_entry->term());
    return mem_entry->term();
}


// :
// there will be a period of time(while it is committing Cnew) when a
// leader can manage a cluster taht does not include itself; it replicates
// log entries but does not count itself in majorities. 
uint64_t calculateCommit(const raft::RaftState& raft_state)
{
	const auto& raft_mem = raft_state.GetRaftMem();
	assert(raft::RaftRole::LEADER == raft_mem.GetRole());

	const auto& map_progress = raft_mem.GetProgress();
	assert(false == map_progress.empty());

	std::vector<uint64_t> vec_matched;
	vec_matched.reserve(map_progress.size());
	for (const auto& id_progress : map_progress) {
		uint32_t peer_id = id_progress.first;
		const auto& progress = id_progress.second;
		assert(nullptr != progress);
		if (false == progress->CanVote()) {
			continue;
		}

		if (raft_mem.GetSelfId() == peer_id) {
			vec_matched.emplace_back(raft_state.GetMaxIndex());
		}
		else {
			vec_matched.emplace_back(progress->GetMatched());
		}
	}

	assert(false == vec_matched.empty());
	assert(vec_matched.size() <= map_progress.size());
	std::sort(vec_matched.begin(), vec_matched.end());

    // 
    // raft paper:
    // 3.6.2 committing entries from previous terms
    // Raft never commits log entries from previous terms by counting
    // replicas. Only log entries from the leader's current term 
    // are commited by counting replicate; once an entry from the 
    // current term has been commited in this way, the all prior 
    // entries are commited indirectly because of the Log 
    // Matching Property.
	const uint64_t major_matched = vec_matched[vec_matched.size() / 2];
	assert(major_matched >= vec_matched.front());
	assert(major_matched <= vec_matched.back());

	if (major_matched >= raft_state.GetMinIndex() && 
			raft_state.GetTerm() == raft_state.GetLogTerm(major_matched)) {
		return major_matched;
	}

	assert(major_matched < raft_state.GetMinIndex() || 
			raft_state.GetLogTerm(major_matched) < raft_state.GetTerm());
	return 0;
}

bool canVoteYes(
        raft::RaftState& raft_state, 
        uint64_t msg_term, 
        uint64_t vote_index, 
        uint64_t vote_term)
{
    assert(raft_state.GetTerm() == msg_term);
    // raft paper 
    //  5.4.1 Election restriction
    //  raft determines which of two logs is more up-to-date by
    //  comparing the index and term of the last entries in the logs.
    //  - If the logs have last entries with different terms, then the log
    //    with the later term is more up-to-date;
    //  - If the logs end with the same term, then whichever log is longer
    //    is more up-to-date.
    if (raft_state.IsLogEmpty()) {
        return true;
    }

    const raft::Entry* last_entry = raft_state.GetLastEntry();
    assert(nullptr != last_entry);
    assert(0 < last_entry->index());
    if (last_entry->term() != vote_term) {
        return vote_term > last_entry->term();
    }

    assert(last_entry->term() == vote_term);
    return vote_index >= last_entry->index();
}


namespace follower {


int resolveEntries(
        raft::RaftState& raft_state, 
        const raft::Message& msg)
{
    if (0 == msg.entries_size()) {
        return -4;
    }

    assert(0 < msg.entries_size());
    const uint64_t min_index = raft_state.GetMinIndex();
    const uint64_t max_index = raft_state.GetMaxIndex();
    assert(min_index <= max_index);

    // msg.index() => current index;
    // msg.log_term(); => prev_log_term; => current_index - 1;
    if (0 == min_index) {
        assert(0 == max_index);
		if (0 != msg.index()) {
            logerr("INFO: raft_mem.logs_ empty msg.index %" PRIu64, 
                    msg.index());
            return -1;
        }
        assert(uint64_t{0} == msg.index());
        assert(uint64_t{0} == msg.log_term());
        return 0;
    }

    assert(0 < min_index);
    uint64_t check_index = msg.index();
    uint64_t msg_max_index = msg.entries(msg.entries_size() - 1).index();
		
    hassert(check_index < msg_max_index, "check_index %lu msg_max_index %lu msg.entries_size %d", 
			check_index, msg_max_index, msg.entries_size());
	if (0 == check_index && (0 == min_index || 1 == min_index)) {
		assert(0 == msg.log_term());
	}
	else {
		if (check_index < min_index || check_index > max_index) {
			logerr("INFO: logid %lu from %u index %lu raft_mem %" PRIu64 " %" PRIu64 " "
					" msg.index %" PRIu64 " %" PRIu64, 
					msg.logid(), msg.from(), msg.index(), 
					min_index, max_index, check_index, msg_max_index);
			return -2;
		}

		assert(0 < check_index);
		assert(min_index <= check_index);
		assert(max_index >= check_index);
		int mem_idx = check_index - min_index;
		assert(0 <= mem_idx);
		const raft::Entry* mem_entry = raft_state.At(mem_idx);
		assert(nullptr != mem_entry);
		assert(check_index == mem_entry->index());
		if (msg.log_term() != mem_entry->term()) {
			// reject case
			// must be the case !
			logerr("logid %lu check_index %" PRIu64 " msg.log_term %" PRIu64 " "
					"mem_entry->term %" PRIu64, 
					msg.logid(), 
					check_index, msg.log_term(), mem_entry->term());
			if (mem_entry->index() <= raft_state.GetCommit()) {
				printf ( "logid %lu mem_entry index %lu term %lu | commit %lu msg.index %lu log_term %lu \n", 
						msg.logid(), mem_entry->index(), mem_entry->term(), 
						raft_state.GetCommit(), msg.index(), msg.log_term() );
			}
			assert(mem_entry->index() > raft_state.GetCommit());
			// oss attr inc
			return -3;
		}
		assert(msg.log_term() == mem_entry->term());
	}

    // pass msg.index && msg.log_term check
    int idx = 0;
    for (; idx < msg.entries_size(); ++idx) { 
        const raft::Entry& msg_entry = msg.entries(idx);
        int mem_idx = msg_entry.index() - min_index;
        assert(0 <= mem_idx);
        const raft::Entry* mem_entry = raft_state.At(mem_idx);
        if (nullptr == mem_entry) {
            assert(msg_entry.index() > max_index);
            break;
        }

        assert(nullptr != mem_entry);
        assert(mem_entry->index() == msg_entry.index());
        logerr ( "msg_entry.term %d mem_entry->term %d\n", 
                static_cast<int>(msg_entry.term()), 
                static_cast<int>(mem_entry->term()) );
        if (msg_entry.term() != mem_entry->term()) {
            // must be the case
            assert(mem_entry->index() > raft_state.GetCommit());
            logerr("IMPORTANT: index %" PRIu64 " find inconsist term "
                    "msg_entry.term %" PRIu64 " mem_entry->term %" PRIu64, 
                    msg_entry.index(), msg_entry.term(), 
                    mem_entry->term());
            // truncate local log
            break;
        }

        // assert check
        assert(msg_entry.term() == mem_entry->term());
        assert(msg_entry.type() == mem_entry->type());
        assert(msg_entry.reqid() == mem_entry->reqid());
        assert(msg_entry.data() == mem_entry->data());
    }

    assert(0 <= idx); 
    assert(idx <= msg.entries_size());
    return idx; // resolve
}

// follower
std::tuple<
    std::unique_ptr<raft::HardState>, 
    std::unique_ptr<raft::SoftState>, 
    bool, 
    raft::MessageType>
onTimeout(raft::RaftMem& raft_mem, bool force_timeout)
{
    // follower => timeout => become candidate;
    assert(raft::RaftRole::FOLLOWER == raft_mem.GetRole());
    if (false == raft_mem.IsMember(raft_mem.GetSelfId())) {
        return std::make_tuple(
                nullptr, nullptr, false, raft::MessageType::MsgNull);
    }

    if (false == force_timeout && false == raft_mem.HasTimeout()) {
        return std::make_tuple(
                nullptr, nullptr, false, raft::MessageType::MsgNull);
    }

	logerr("TEST: FOLLOWER timeout term %lu", raft_mem.GetTerm());
    // timeout =>
    std::unique_ptr<raft::HardState> 
        hard_state = cutils::make_unique<raft::HardState>();
    assert(nullptr != hard_state);

    {
        auto meta = hard_state->mutable_meta();
        assert(nullptr != meta);
        meta->set_term(raft_mem.GetTerm() + 1);
        meta->set_vote(0);
    }

    std::unique_ptr<raft::SoftState>
        soft_state = cutils::make_unique<raft::SoftState>();
    assert(nullptr != soft_state);

    soft_state->set_role(static_cast<uint32_t>(raft::RaftRole::CANDIDATE));

    raft_mem.ClearVoteMap();
    raft_mem.UpdateActiveTime();
    return std::make_tuple(
            std::move(hard_state), std::move(soft_state), 
            true, raft::MessageType::MsgVote);
}

// follower
std::tuple<
    bool, 
    raft::MessageType, 
    bool>
onStepMessage(
        raft::RaftMem& raft_mem, 
        const raft::Message& msg, 
        std::unique_ptr<raft::HardState>& hard_state, 
        std::unique_ptr<raft::SoftState>& soft_state)
{
    raft::RaftState raft_state(raft_mem, hard_state, soft_state);
    assert(raft::RaftRole::FOLLOWER == raft_state.GetRole());

    if (false == raft_state.IsMember(msg.from())) {
        logerr("CONFIG peer %u is not a memmber", 
                msg.from());
    }

    bool mark_update_active_time = false;
    auto term = raft_state.GetTerm();
    if (msg.term() < term) {
        // ignore;
        return std::make_tuple(
                false, raft::MessageType::MsgInvalidTerm, false);
    }

    assert(msg.term() >= term);
    // TODO :
    // Disruptive servers
    // Raft's solution uses heartbeats to determine when a valid leader
    // exists. 
    // =>
    // if a server receives a RequestVote request within the minimum election
    // timeout of hearing from a current leader, it does not update its term
    // or grant its vote.
    if (msg.term() > term) {
		// raft_mem.UpdateNextExpectedIndex(0, "msg.term > term");
		if (nullptr == hard_state) {
            hard_state = cutils::make_unique<raft::HardState>();
            assert(nullptr != hard_state);
        }

        assert(nullptr != hard_state);
        auto meta = hard_state->mutable_meta();
        assert(nullptr != meta);
        if (meta->has_term()) {
            assert(meta->term() < msg.term());
            meta->set_vote(0); // reset
        }

        meta->set_term(msg.term());
        assert(0 == meta->vote());
        meta->set_vote(0);

        mark_update_active_time = true;
        term = raft_state.GetTerm();
    }

	auto tmp_entry_cache = raft_mem.GetTmpEntryCache();
	assert(nullptr != tmp_entry_cache);
	tmp_entry_cache->MayInvalid(msg.term());

    assert(msg.term() == term);
    auto rsp_msg_type = raft::MessageType::MsgNull;
    switch (msg.type()) {

    case raft::MessageType::MsgVote:
        {
            mark_update_active_time = true;
            if (false == canVoteYes(
                        raft_state, msg.term(), msg.index(), msg.log_term())) {
                break;
            }

            rsp_msg_type = raft::MessageType::MsgVoteResp;
            uint32_t vote = raft_state.GetVote(msg.term());
            uint32_t leader_id = raft_state.GetLeaderId(msg.term());
			if (0 != leader_id) {
				rsp_msg_type = raft::MessageType::MsgNull;
				logerr("INFO: logid %lu term %lu has_leader %u from %u", 
						msg.logid(), msg.term(), leader_id, msg.from());
				break;
			}

			if (0 != vote && msg.from() != vote) {
				// reject;
				logerr("INFO: logid %lu term %lu has_vote %u from %u", 
						msg.logid(), msg.term(), vote, msg.from());
				break;
			}

            assert(0 == leader_id);
            assert(0 == vote || msg.from() == vote);
            if (0 == vote) {
                if (nullptr == hard_state) {
                    hard_state = cutils::make_unique<raft::HardState>();
                    assert(nullptr != hard_state);
                }
                
                auto meta = hard_state->mutable_meta();
                if (meta->has_term()) {
                    assert(meta->term() == msg.term());
                }

                assert(0 == meta->vote());
                meta->set_vote(msg.from());
                meta->set_term(msg.term());
            }
        }
        break;

    case raft::MessageType::MsgHeartbeat:
        {
            mark_update_active_time = true;
            // leader heart beat msg
            rsp_msg_type = raft::MessageType::MsgHeartbeatResp;
            uint32_t leader_id = raft_state.GetLeaderId(msg.term());
            if (0 == leader_id) {
                if (nullptr == soft_state) {
                    soft_state = cutils::make_unique<raft::SoftState>();
                    assert(nullptr != soft_state);
                }
                assert(nullptr != soft_state);
                soft_state->set_leader_id(msg.from());
            }
            else {
				assert(raft_state.GetTerm() == msg.term());
                hassert(leader_id == msg.from(), 
						"logid %lu leader_id %u msg.from %u msg.term %lu term %lu", 
						msg.logid(), leader_id, msg.from(), msg.term(), raft_state.GetTerm());
            }

			if (msg.index() != raft_state.GetMaxIndex()) {
				raft_mem.MissingCatchUp();
			}

			if (raft_mem.NeedCatchUp()) {
				// MsgAppResp trigger catch-up;
				rsp_msg_type = raft::MessageType::MsgAppResp;
			}
        }
        break;

    case raft::MessageType::MsgApp:
        {
            mark_update_active_time = true;
			raft_mem.RecvCatchUp();

            uint32_t leader_id = raft_state.GetLeaderId(msg.term());
            if (0 == leader_id) {
                if (nullptr == soft_state) {
                    soft_state = cutils::make_unique<raft::SoftState>();
                    assert(nullptr != soft_state);
                }

                soft_state->set_leader_id(msg.from());
            }
			else {
				assert(leader_id == msg.from());
			}

            rsp_msg_type = raft::MessageType::MsgAppResp;

            const uint64_t prev_max_index = raft_state.GetMaxIndex();
            int app_idx = resolveEntries(raft_state, msg);
            logerr("TEST: logid %lu from %u msg.index %" PRIu64 
                    " msg.log_term %" PRIu64 " entries_size %d app_idx %d "
                    "local min_index %" PRIu64 " max_index %" PRIu64, 
					msg.logid(), 
                    msg.from(), msg.index(), msg.log_term(), 
                    msg.entries_size(), app_idx, 
                    raft_state.GetMinIndex(), raft_state.GetMaxIndex());
            if (0 <= app_idx) {
                if (app_idx < 
                        msg.entries_size() && nullptr == hard_state) { 
                    hard_state = cutils::make_unique<raft::HardState>();
                    assert(nullptr != hard_state);
                    const auto& entry = msg.entries(app_idx);
                    const auto pending_config = raft_mem.GetPendingConfig();
                    if (nullptr != pending_config && 
                            pending_config->index() > entry.index()) {
                        if (nullptr == soft_state) {
                            soft_state = 
                                cutils::make_unique<raft::SoftState>();
                        }
                        assert(nullptr != soft_state);
                        soft_state->set_drop_pending(true);
                    }
                }

                for (int idx = app_idx; idx < msg.entries_size(); ++idx) {
                    const auto& entry = msg.entries(idx);
                    assert(0 < entry.term());
                    assert(0 < entry.index());

                    auto* new_entry = hard_state->add_entries();
                    assert(nullptr != new_entry);
                    *new_entry = entry;
                    process_entry(*new_entry, soft_state);
                }

                // TODO: tmp_entry_cache: extract: deal-with config change!!
				// may do nothing;
				// if (nullptr != hard_state) {
				// 	int prev_entries_size = hard_state->entries_size();
				// 	assert(nullptr != tmp_entry_cache);
				// 	tmp_entry_cache->Extract(msg.term(), *hard_state);
				// 	assert(prev_entries_size <= hard_state->entries_size());
				// 	if (prev_entries_size < hard_state->entries_size()) {
				// 		logerr("IMPORTANT: use tmp_entry_cache %d", 
				// 				hard_state->entries_size() - prev_entries_size);
				// 	}
				// }

				assert(raft_state.IsMatch(msg.index(), msg.log_term()));
            }
			else {
				if (-2 == app_idx && 
						msg.log_term() == msg.term() &&
						msg.log_term() == raft_state.GetTerm() && 
						nullptr != raft_state.GetLastEntry() && 
						msg.log_term() == raft_state.GetLastEntry()->term()) {
					assert(msg.log_term() == raft_state.GetTerm());
					// try insert int tmp_entry_cache;
					assert(0 < msg.entries_size());
					if (msg.index() > raft_state.GetMaxIndex() && 
							msg.index() <= raft_state.GetMaxIndex() + 1000) {
						assert(nullptr != tmp_entry_cache);
						for (int idx = 0; idx < msg.entries_size(); ++idx) {
							const auto& msg_entry = msg.entries(idx);
							assert(msg_entry.term() == raft_state.GetTerm());
							tmp_entry_cache->Insert(msg_entry);
						}

						// silicent
						rsp_msg_type = raft::MessageType::MsgNull;
					}
				}
			}

            if (prev_max_index > raft_state.GetMaxIndex()) {
                // need truncate;
                ::resetMaxIndex(hard_state, raft_state.GetMaxIndex());
            }

            if (raft_state.CanUpdateCommit(
                        msg.commit_index(), msg.commit_term())) {
                assert(msg.commit_index() > raft_state.GetCommit());
                ::updateCommit(hard_state, 
						msg.commit_index(), raft_state.GetMaxIndex());
            }
        }
        break;

    default:
        logerr("IGNORE: recv msg type %d", static_cast<int>(msg.type()));
        break;
    }

    if (mark_update_active_time) {
        raft_mem.UpdateActiveTime();
    }

    return std::make_tuple(false, rsp_msg_type, false);
}

// follower
std::unique_ptr<raft::Message>
onNewBuildRsp(
        raft::RaftMem& raft_mem, 
        const raft::Message& req_msg, 
        const raft::MessageType rsp_msg_type)
{
    std::unique_ptr<raft::HardState> hard_state; // nullptr;
    std::unique_ptr<raft::SoftState> soft_state;
    raft::RaftState raft_state(raft_mem, hard_state, soft_state);

    auto rsp_msg = cutils::make_unique<raft::Message>();
    assert(nullptr != rsp_msg);
    switch (rsp_msg_type) {

    case raft::MessageType::MsgInvalidTerm:
        {
            assert(req_msg.term() < raft_state.GetTerm());
            rsp_msg->set_term(raft_mem.GetTerm());
        }
        break;

    case raft::MessageType::MsgVoteResp:
        {
            // ONLY VOTE_YES => MsgVoteResp
            assert(nullptr != rsp_msg);
            assert(req_msg.term() == raft_state.GetTerm());
            // assert check
            assert(canVoteYes(raft_state, 
                        req_msg.term(), 
                        req_msg.index(), req_msg.log_term()));

			uint32_t leader_id = raft_state.GetLeaderId(req_msg.term());
			assert(0 == leader_id);
            if (req_msg.from() == raft_state.GetVote(req_msg.term())) {
                rsp_msg->set_reject(false);
            }
            else {
                rsp_msg->set_reject(true);
            }
        }
        break;

    case raft::MessageType::MsgHeartbeatResp:
		{
			if (raft_state.GetTerm() == 
					raft_state.GetLogTerm(raft_state.GetMaxIndex())) {
				rsp_msg->set_index(raft_state.GetMaxIndex());
			}
			else {
				rsp_msg->set_index(raft_state.GetCommit());
			}
		}
		break;

    case raft::MessageType::MsgAppResp:
        {
			if (raft::MessageType::MsgHeartbeat == req_msg.type()) {
				assert(raft_mem.NeedCatchUp());
				rsp_msg->set_reject(false);
				if (raft_state.GetTerm() == 
						raft_state.GetLogTerm(raft_state.GetMaxIndex())) {
					rsp_msg->set_index(raft_state.GetMaxIndex());
				}
				else {
					rsp_msg->set_index(raft_state.GetCommit());
				}

				raft_mem.TriggerCatchUp();
				break;
			}

			assert(raft::MessageType::MsgApp == req_msg.type());

			int req_entries_size = req_msg.entries_size();
			uint64_t req_max_index = 0 == req_entries_size ? 
				req_msg.index() : 
                req_msg.entries(req_entries_size-1).index();
			rsp_msg->set_index(req_max_index); // !!!

            if (raft_state.IsMatch(req_msg.index(), req_msg.log_term())) {
                rsp_msg->set_reject(false); 
                assert(0 == raft_state.GetCommit() || 
                    raft_state.GetMinIndex() <= raft_state.GetCommit());
                rsp_msg->set_index(raft_state.GetMaxIndex());
            }
            else {
				assert(0 < req_msg.index());
                rsp_msg->set_reject(true);

				assert(raft_state.GetCommit() <= 
                        raft_state.GetMaxIndex());
				// check max index term
				uint64_t reject_hint = raft_state.GetCommit();
				if (raft_state.GetTerm() == 
						raft_state.GetLogTerm(raft_state.GetMaxIndex())) {
					reject_hint = raft_state.GetMaxIndex();
				}

				// reject in req_msg.log_index();
				assert(0 < req_msg.index());
				reject_hint = std::min(reject_hint, req_msg.index() - 1);
				rsp_msg->set_reject_hint(reject_hint);
            }

            if (req_msg.has_disk_mark() && req_msg.disk_mark()) {
                rsp_msg->set_disk_mark(true);
            }
        }
        break;
    
    default: 
        rsp_msg = nullptr;
        logerr("IGNORE: req_msg.type %d rsp_msg_type %d", 
                static_cast<int>(req_msg.type()), 
                static_cast<int>(rsp_msg_type));
        break;
    }

    if (nullptr != rsp_msg) {
        rsp_msg->set_type(rsp_msg_type);
        rsp_msg->set_logid(req_msg.logid());
        rsp_msg->set_to(req_msg.from());
        rsp_msg->set_from(raft_mem.GetSelfId());
        rsp_msg->set_term(raft_state.GetTerm());
        if (req_msg.disk_mark()) {
            rsp_msg->set_disk_mark(true);
        }

        if (req_msg.one_shot_mark()) {
            rsp_msg->set_one_shot_mark(true);
        }
    }

    return rsp_msg;
}

// follower

} // namespace follower


namespace candidate {


// :
// a server that is not part of its own latest configuration should still
// start new elections, as it might still be needed until the Cnew entry 
// is committed!
// => it does not count its own vote in elections unless it is part of its
// latest configuration.!
std::tuple<
    std::unique_ptr<raft::HardState>, 
    std::unique_ptr<raft::SoftState>, 
    bool, 
    raft::MessageType>
onTimeout(raft::RaftMem& raft_mem, bool force_timeout)
{
    assert(raft::RaftRole::CANDIDATE == raft_mem.GetRole());
    assert(raft_mem.IsMember(raft_mem.GetSelfId()));
    if (false == force_timeout && false == raft_mem.HasTimeout()) {
        return std::make_tuple(
                nullptr, nullptr, false, raft::MessageType::MsgNull);
    }

	if (raft_mem.HasTimeout()) {
		raft_mem.RefreshElectionTimeout();
	}

    // timeout
    int vote_cnt = raft_mem.GetVoteCount();
	logerr("TEST CANDIDATE logid %lu timeout term %lu vote_cnt %d vote %u", 
			raft_mem.GetLogId(), raft_mem.GetTerm(), vote_cnt, raft_mem.GetVote(raft_mem.GetTerm()));
    if (false == raft_mem.IsMajority(vote_cnt)) {
        raft_mem.UpdateActiveTime();
        return std::make_tuple(
                nullptr, nullptr, true, raft::MessageType::MsgVote);
    }

    std::unique_ptr<raft::HardState>
        hard_state = cutils::make_unique<raft::HardState>();
    assert(nullptr != hard_state);

    {
        auto meta = hard_state->mutable_meta();
        assert(nullptr != meta);
        meta->set_term(raft_mem.GetTerm() + 1);
        meta->set_vote(0);
    }

    raft_mem.ClearVoteMap();
    raft_mem.UpdateActiveTime();
    return std::make_tuple(
            std::move(hard_state), nullptr, 
            true, raft::MessageType::MsgVote);
}

// candicate
std::tuple<
    bool, 
    raft::MessageType, 
    bool>
onStepMessage(
        raft::RaftMem& raft_mem, 
        const raft::Message& msg, 
        std::unique_ptr<raft::HardState>& hard_state, 
        std::unique_ptr<raft::SoftState>& soft_state)
{
    raft::RaftState raft_state(raft_mem, hard_state, soft_state);
    assert(raft::RaftRole::CANDIDATE == raft_state.GetRole());

    uint64_t term = raft_state.GetTerm();
    if (msg.term() < term) {
        return std::make_tuple(
                false, raft::MessageType::MsgVote, false);
    }

    assert(msg.term() >= term);
    if (msg.term() > term ||
            (msg.term() == term && 
             (raft::MessageType::MsgHeartbeat == msg.type() || 
			  raft::MessageType::MsgApp == msg.type()))) {
        // fall back to follower state;
        if (nullptr == soft_state) {
            soft_state = cutils::make_unique<raft::SoftState>();
            assert(nullptr != soft_state);
        }

        assert(nullptr != soft_state);
        soft_state->set_role(
                static_cast<uint32_t>(raft::RaftRole::FOLLOWER));
        return follower::onStepMessage(
                raft_mem, msg, hard_state, soft_state);
    }

    assert(msg.term() == term);
    if (false == raft_state.IsMember(msg.from())) {
        return std::make_tuple(false, raft::MessageType::MsgNull, false);
    }

    bool mark_update_active_time = false;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;
    switch (msg.type()) {

    case raft::MessageType::MsgVoteResp:
        {
            mark_update_active_time = true;
            int major_yes_cnt = 
                raft_mem.UpdateVote(
                        msg.term(), msg.from(), !msg.reject());
            if (false == raft_mem.IsMajority(major_yes_cnt)) {
                // check local vote
                if (0 >= major_yes_cnt || 
                        0 != raft_state.GetVote(msg.term()) ||
                        false == raft_mem.IsMajority(major_yes_cnt + 1)) {
                    break; // 
                }

                assert(0 < major_yes_cnt);
                assert(0 == raft_state.GetVote(msg.term()));
                assert(true == raft_mem.IsMajority(major_yes_cnt + 1));

                ::setVote(hard_state, 
                        raft_state.GetTerm(), raft_mem.GetSelfId());
            }

            // => UpdateVote => yes => reach major
            // => new leader may issue an no-op to commited all the previous
            //    log-term log entries;
            if (nullptr == soft_state) {
                soft_state = cutils::make_unique<raft::SoftState>();
                assert(nullptr != soft_state);
            }

            assert(nullptr != soft_state);
            soft_state->set_role(
                    static_cast<uint32_t>(raft::RaftRole::LEADER));
            // assert msg.to() == raft_mem.GetSelfID();
            soft_state->set_leader_id(raft_mem.GetSelfId());

			// TODO: reset progress;
			for (auto& id_progress : raft_mem.GetProgress()) {
				uint32_t peer_id = id_progress.first;
				auto& progress = id_progress.second;
				assert(nullptr != progress);
				if (raft_mem.GetSelfId() == peer_id) {
					continue;
				}

				progress->Reset(
						raft_state.GetMaxIndex() + 1, progress->CanVote());
			}

            // broad-cast: i am the new leader now!!
            mark_broadcast = true;

			rsp_msg_type = raft::MessageType::MsgApp;
			if (raft_state.GetCommit() < raft_state.GetMaxIndex()) {
				assert(raft_state.CanWrite(1));

				uint64_t max_index = raft_state.GetMaxIndex();
				if (nullptr == hard_state) {
					hard_state = cutils::make_unique<raft::HardState>();
				}

				assert(nullptr != hard_state);
				auto* new_entry = hard_state->add_entries();
				assert(nullptr != new_entry);
				new_entry->set_type(raft::EntryType::EntryNoop);
				new_entry->set_term(raft_state.GetTerm());
				new_entry->set_index(max_index + 1);
				new_entry->set_reqid(0);

				assert(max_index + 1 == raft_state.GetMaxIndex());
			}

			logerr("IMPORTANT logid %lu become leader rsp_msg_type %d", 
					msg.logid(), rsp_msg_type);
        }
        break;

	case raft::MessageType::MsgVote:
		{
			auto can_vote_yes = canVoteYes(
					raft_state, msg.term(), msg.index(), msg.log_term());
			logerr("IMPORTANT logid %lu from %u can_vote_yes %d [%lu %lu %u %d]", 
					msg.logid(), msg.from(), can_vote_yes, 
					raft_state.GetMinIndex(), raft_state.GetMaxIndex(), 
					raft_state.GetVote(raft_state.GetTerm()), 
					raft_mem.GetVoteCount());
			if (false == can_vote_yes) {
				break; // ignore is msg;
			}

			rsp_msg_type = raft::MessageType::MsgVoteResp;
			uint32_t vote = raft_state.GetVote(msg.term());
			// must be the case;
			assert(0 == raft_state.GetLeaderId(msg.term()));
			if (0 != vote && msg.from() != vote) {
				logerr("INFO: logid %" PRIu64" already vote %u term %" PRIu64 " vote %u from %u", 
						msg.logid(), vote, msg.term(), vote, msg.from());
				break;
			}

			assert(0 == vote || msg.from() == vote);
			if (0 == vote) {
                if (nullptr == hard_state) {
                    hard_state = cutils::make_unique<raft::HardState>();
                    assert(nullptr != hard_state);
                }
                
                auto meta = hard_state->mutable_meta();
                if (meta->has_term()) {
                    assert(meta->term() == msg.term());
                }

                assert(0 == meta->vote());
                meta->set_vote(msg.from());
                meta->set_term(msg.term());
			}
		}

    default:
        assert(raft::MessageType::MsgHeartbeat != msg.type());
        logerr("IGNORE: logid %lu recv msg type %d", msg.logid(), msg.type());
        break;
    }

    if (mark_update_active_time) {
        raft_mem.UpdateActiveTime();
    }

    return std::make_tuple(
            mark_broadcast, rsp_msg_type, false);
}

// candidate
std::unique_ptr<raft::Message>
onNewBuildRsp(
        raft::RaftMem& raft_mem, 
        const raft::Message& req_msg, 
        const raft::MessageType rsp_msg_type)
{
    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    raft::RaftState raft_state(raft_mem, hard_state, soft_state);

    auto rsp_msg = cutils::make_unique<raft::Message>();
    assert(nullptr != rsp_msg);

    switch (rsp_msg_type) {

    case raft::MessageType::MsgVote:
        {
            rsp_msg->set_term(raft_state.GetTerm());
            rsp_msg->set_index(raft_state.GetMaxIndex());
            // assert(0 < rsp_msg->index());
            rsp_msg->set_log_term(
                    raft_state.GetLogTerm(rsp_msg->index()));
        }
        break;

	case raft::MessageType::MsgVoteResp:
		{
            // ONLY VOTE_YES => MsgVoteResp
            assert(nullptr != rsp_msg);
            assert(req_msg.term() == raft_state.GetTerm());
            // assert check
            assert(canVoteYes(raft_state, 
                        req_msg.term(), 
                        req_msg.index(), req_msg.log_term()));

			uint32_t leader_id = raft_state.GetLeaderId(req_msg.term());
			assert(0 == leader_id);
            if (req_msg.from() == raft_state.GetVote(req_msg.term())) {
                rsp_msg->set_reject(false);
            }
            else {
                rsp_msg->set_reject(true);
            }

			rsp_msg->set_term(raft_state.GetTerm());
		}
		break;

    default:
        rsp_msg = nullptr;
        logerr("IGNORE: req_msg.type %d rsp_msg_type %d", 
                static_cast<int>(req_msg.type()), 
                static_cast<int>(rsp_msg_type));
        break;
    }

    if (nullptr != rsp_msg) {
        rsp_msg->set_type(rsp_msg_type);
        rsp_msg->set_logid(req_msg.logid());
		rsp_msg->set_to(req_msg.from());
        rsp_msg->set_from(raft_mem.GetSelfId());
        assert(rsp_msg->has_term());
    }

    return std::move(rsp_msg);
}

// candidate
} // namespace candidate


namespace leader {

std::tuple<
    std::unique_ptr<raft::HardState>, 
    std::unique_ptr<raft::SoftState>, 
    bool, 
    raft::MessageType>
onTimeout(raft::RaftMem& raft_mem, bool force_timeout)
{
    assert(raft::RaftRole::LEADER == raft_mem.GetRole());
    std::unique_ptr<raft::HardState> hard_state;
    
//	logerr("TEST LEADER timeout term %lu", raft_mem.GetTerm());
    // TODO: for now
    raft::RaftState raft_state(raft_mem, nullptr, nullptr);
    uint64_t replicate_commit = calculateCommit(raft_state);
    if (raft_mem.GetCommit() < replicate_commit) {
        ::updateCommit(hard_state, replicate_commit, raft_mem.GetMaxIndex());
    }

    raft_mem.UpdateActiveTime();
	for (auto& id_progress : raft_mem.GetProgress()) {
		uint32_t peer_id = id_progress.first;
		auto progress = id_progress.second.get();
		assert(nullptr != progress);
		if (raft_mem.GetSelfId() == peer_id) {
			continue;
		}

		progress->Tick(); // else;
	}

	bool mark_broadcast = false;
	auto rsp_msg_type = raft::MessageType::MsgNull;
	if (force_timeout || raft_mem.IsHeartbeatTimeout()) {
		mark_broadcast = true;
		rsp_msg_type = raft::MessageType::MsgHeartbeat;
		raft_mem.UpdateHeartBeatActiveTime();
		// timeout => deactive progress state;
	}

	return std::make_tuple(
			std::move(hard_state), nullptr, mark_broadcast, rsp_msg_type);
}

// leader
std::tuple<
    bool, 
    raft::MessageType, 
    bool>
onStepMessage(
        raft::RaftMem& raft_mem, 
        const raft::Message& msg, 
        std::unique_ptr<raft::HardState>& hard_state, 
        std::unique_ptr<raft::SoftState>& soft_state)
{
    bool need_disk_replicate = false;
    raft::RaftState raft_state(raft_mem, hard_state, soft_state);
    assert(raft::RaftRole::LEADER == raft_state.GetRole());

    uint64_t term = raft_state.GetTerm();
    if (msg.term() < term) {
        return std::make_tuple(
                false, raft::MessageType::MsgHeartbeat, need_disk_replicate);
    }

    assert(msg.term() >= term);
    if (msg.term() > term) {
        // revert back to follower
        if (nullptr == soft_state) {
            soft_state = cutils::make_unique<raft::SoftState>();
            assert(nullptr != soft_state);
        }

        assert(nullptr != soft_state);
        soft_state->set_role(
                static_cast<uint32_t>(raft::RaftRole::FOLLOWER));
        return follower::onStepMessage(
                raft_mem, msg, hard_state, soft_state);
    }

    assert(msg.term() == term);
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;
    switch (msg.type()) {

    case raft::MessageType::MsgProp:
        {
			assert(msg.index() == raft_state.GetMaxIndex());
            if (0 == msg.entries_size()) {
                break;  // do nothing
            }

            // must be in this state; => 
            assert(raft_state.CanWrite(msg.entries_size()));

            if (nullptr == hard_state) {
                hard_state = cutils::make_unique<raft::HardState>();
                assert(nullptr != hard_state);
            }
            assert(nullptr != hard_state);

            uint64_t max_index = raft_state.GetMaxIndex();
            for (int idx = 0; idx < msg.entries_size(); ++idx) {
                const raft::Entry& msg_entry = msg.entries(idx);

				assert(msg_entry.term() == raft_state.GetTerm());
				assert(msg_entry.index() == max_index + idx + 1);
                auto* new_entry = hard_state->add_entries();
                assert(nullptr != new_entry);
                *new_entry = msg_entry;
            }
            assert(hard_state->entries_size() >= msg.entries_size());
            mark_broadcast = true;
            rsp_msg_type = MessageType::MsgApp;
        }
        break;

    case raft::MessageType::MsgAppResp:
        {
			rsp_msg_type = raft::MessageType::MsgApp;
			auto progress = raft_mem.GetProgress(msg.from());
			assert(nullptr != progress);
			if (progress->GetNext() > raft_state.GetMaxIndex() + 1) {
				printf ( "logid %lu next %lu matched %lu min %lu max %lu \n", 
						msg.logid(), 
                        progress->GetNext(), progress->GetMatched(), 
						raft_state.GetMinIndex(), raft_state.GetMaxIndex() );
			}

			assert(progress->GetNext() <= raft_state.GetMaxIndex() + 1);
			if (msg.reject()) {
				if (progress->MaybeDecreaseNext(
							msg.index(), msg.reject_hint())) {
					// next has been decreased;
					if (ProgressState::REPLICATE == progress->GetState()) {
						progress->BecomeProbe();
					}
				}
				else {
					// stale reject, rsp nothing;
					rsp_msg_type = raft::MessageType::MsgNull;
				}

				assert(progress->GetNext() <= raft_state.GetMaxIndex() + 1);
			}
			else {
				// no rejected;
				if (progress->MaybeUpdate(msg.index())) {
					// update matched_;
					if (ProgressState::PROBE == progress->GetState()) {
						progress->BecomeReplicate();
					}

                    // remove: rsp with new msg_app: (replicate entries or new commited);
				//	if (progress->GetNext() == raft_state.GetMaxIndex() + 1 && 
				//			0 < progress->GetMatched()) {
				//		rsp_msg_type = raft::MessageType::MsgNull;
				//	}
				}
				else {
					if (progress->NeedResetByHBAppRsp()) {
						progress->ResetByHBAppRsp();	
						assert(false == progress->IsPause());
						logerr("RESET: logid %lu msg.from %u matched %lu next_ %lu max %lu", 
								msg.logid(), msg.from(), 
								progress->GetMatched(), 
								progress->GetNext(), 
								raft_state.GetMaxIndex());
					}
					else {
						rsp_msg_type = raft::MessageType::MsgNull;
					}
					// rsp nothing;
				}
				assert(progress->GetNext() <= raft_state.GetMaxIndex() + 1);
			}

			if (progress->IsPause() && 
					raft::ProgressState::PROBE == progress->GetState()) {
				rsp_msg_type = raft::MessageType::MsgNull;
			}

            uint64_t replicate_commit = calculateCommit(raft_state);
            if (raft_state.GetCommit() < replicate_commit) {
                // => update commit
                ::updateCommit(hard_state, 
						replicate_commit, raft_state.GetMaxIndex());
            }

			if (0 < raft_state.GetMinIndex() &&
					progress->GetNext() - 1 < raft_state.GetMinIndex()) {
				// app: get log term failed
				need_disk_replicate = true;
				rsp_msg_type = raft::MessageType::MsgNull;
			}

			assert(progress->GetNext() <= raft_state.GetMaxIndex() + 1);
        }
        break;

    case raft::MessageType::MsgHeartbeatResp:
        {
			auto progress = raft_mem.GetProgress(msg.from());
			assert(nullptr != progress);
			progress->PostActive(true);
			break;
        }
        break;

    default:
        logerr("IGNORE: recv msg type %d", static_cast<int>(msg.type()));
        break;
    }

    return std::make_tuple(
            mark_broadcast, rsp_msg_type, need_disk_replicate);
}

// leader
std::unique_ptr<raft::Message>
onNewBuildRsp(
        raft::RaftMem& raft_mem, 
        const raft::Message& req_msg, 
        const raft::MessageType rsp_msg_type)
{
    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    raft::RaftState raft_state(raft_mem, hard_state, soft_state);
    assert(raft::RaftRole::LEADER == raft_state.GetRole());

    std::unique_ptr<raft::Message> rsp_msg;
    switch (rsp_msg_type) {
    
    case raft::MessageType::MsgApp:
        {
			auto progress = raft_mem.GetProgress(req_msg.from());
			assert(nullptr != progress);

			if (progress->IsPause()) {
				assert(
                    raft::ProgressState::PROBE == progress->GetState());
				rsp_msg = nullptr; // discard;
				break;
			}

			uint64_t next_index = progress->GetNext();
			assert(1 <= next_index);
			if (1 < raft_state.GetMinIndex() && 
					next_index - 1 < raft_state.GetMinIndex()) {
				// need disk_replicate_;
				logerr("TEST: broadcast: need disk "
                        "replicate logid %lu min %lu next %lu", 
						req_msg.logid(), 
                        raft_state.GetMinIndex(), next_index);
				break;
			}

			rsp_msg = cutils::make_unique<raft::Message>();
			assert(nullptr != rsp_msg);

			rsp_msg->set_index(next_index - 1);
			// TODO: fix bug ????
			if (0 != rsp_msg->index() && rsp_msg->index() < raft_state.GetMinIndex()) {
				printf ( "rsp_msg: index %lu min %lu max %lu next_index %lu matched %lu\n", 
						rsp_msg->index(), raft_state.GetMinIndex(), 
						raft_state.GetMaxIndex(), next_index, progress->GetMatched() );
			}
			assert(0 == rsp_msg->index() || 
					rsp_msg->index() >= raft_state.GetMinIndex());
			assert(rsp_msg->index() <= raft_state.GetMaxIndex());

			rsp_msg->set_log_term(raft_state.GetLogTerm(rsp_msg->index()));
			assert(0 <= raft_state.GetMaxIndex() - rsp_msg->index());

			size_t max_size = std::min<int>(
					raft_state.GetMaxIndex() - rsp_msg->index(), 60);
			if (raft::ProgressState::PROBE == progress->GetState() && 0 < max_size) {
				max_size = std::min<size_t>(max_size, 2);
			}

			if (0 < max_size) {
				assert(next_index <= raft_state.GetMaxIndex());
				assert(1 == next_index || next_index > raft_state.GetMinIndex());
				for (auto index = 
						next_index; index < next_index + max_size; ++index) {
					assert(1 == index || index > raft_state.GetMinIndex());
					assert(index <= raft_state.GetMaxIndex());
					const auto mem_entry = 
						raft_state.At(index - raft_state.GetMinIndex());
					assert(nullptr != mem_entry);
					assert(0 < mem_entry->term());
					assert(mem_entry->index() == index);

					auto rsp_entry = rsp_msg->add_entries();
					*rsp_entry = *mem_entry;
				}

				assert(next_index + max_size <= raft_state.GetMaxIndex() + 1);
				progress->UpdateNext(next_index + max_size);
				assert(progress->GetNext() <= raft_state.GetMaxIndex() + 1);
				logerr("logid %lu raft_mem.GetMaxIndex %lu raft_state.GetMaxIndex %lu progress->Next %lu", 
						req_msg.logid(), raft_mem.GetMaxIndex(), 
						raft_state.GetMaxIndex(), progress->GetNext());
			}

			uint64_t commit_index = std::min(
					raft_state.GetCommit(), next_index + max_size - 1);
			rsp_msg->set_commit_index(commit_index);
			rsp_msg->set_commit_term(raft_state.GetLogTerm(commit_index));
			if (raft::ProgressState::PROBE == progress->GetState()) {
				progress->Pause();
			}
        }
        break;

    case raft::MessageType::MsgHeartbeat:
        {
			rsp_msg = cutils::make_unique<raft::Message>();
			assert(nullptr != rsp_msg);

			rsp_msg->set_index(raft_state.GetMaxIndex());
			rsp_msg->set_log_term(raft_state.GetLogTerm(rsp_msg->index()));
        }
        break;

    default:
        rsp_msg = nullptr;
        logerr("IGNORE: req_msg.type %d rsp_msg_type %d", 
                static_cast<int>(req_msg.type()), 
                static_cast<int>(rsp_msg_type));
        break;
    }

    if (nullptr != rsp_msg) {
        rsp_msg->set_type(rsp_msg_type);
        rsp_msg->set_logid(req_msg.logid());
        rsp_msg->set_from(raft_mem.GetSelfId());
		rsp_msg->set_to(req_msg.from());
        rsp_msg->set_term(raft_state.GetTerm());
    }

    return std::move(rsp_msg);
}

// leader
} // namespace leader


void assert_check(const raft::RaftConfig& config, 
        const std::map<uint32_t, 
            std::unique_ptr<raft::Progress>>& map_progress)
{
    auto cluster_config = config.GetConfig();
    assert(nullptr != cluster_config);
    assert(map_progress.size() == 
            static_cast<size_t>(cluster_config->nodes_size()));
    for (int idx = 0; idx < cluster_config->nodes_size(); ++idx) {
        const auto& node = cluster_config->nodes(idx);
        assert(map_progress.end() != map_progress.find(node.svr_id()));
        assert(nullptr != map_progress.at(node.svr_id()));
    }
}

void updateMapProgress(
        uint64_t logid, 
        uint64_t max_index, 
        const raft::RaftConfig& config, 
        std::map<uint32_t,
            std::unique_ptr<raft::Progress>>& map_progress)
{
    auto cluster_config = config.GetConfig();
    assert(nullptr != cluster_config);

    // del
    {
        std::set<uint32_t> valid_nodes;
        for (int idx = 0; 
                idx < cluster_config->nodes_size(); ++idx) {
            valid_nodes.insert(cluster_config->nodes(idx).svr_id());
        }

        std::set<uint32_t> del_nodes;
        for (const auto& item : map_progress) {
            if (valid_nodes.end() == valid_nodes.find(item.first)) {
                del_nodes.insert(item.first);
            }
        }

        for (auto node : del_nodes) {
            map_progress.erase(node);
        }
    }

    // add
    for (int idx = 0; idx < cluster_config->nodes_size(); ++idx) {
        const auto& node = cluster_config->nodes(idx);
        if (map_progress.end() == map_progress.find(node.svr_id())) {
            map_progress[node.svr_id()] = 
                cutils::make_unique<raft::Progress>(logid, max_index, true);
            assert(nullptr != map_progress.at(node.svr_id()));
        }
    }
}


} // namespace


namespace raft {

RaftMem::RaftMem(
        uint64_t logid, 
        uint32_t selfid, 
        const raft::RaftOption& option)
    : logid_(logid)
    , selfid_(selfid)
	, base_election_tick_(option.election_tick)
	, timeout_gen_(0, option.election_tick / 2)
	, election_tick_(0)
	, election_deactive_tick_(0)
	, hb_tick_(option.hb_tick)
	, hb_deactive_tick_(0)
    , config_(nullptr)
{
	assert(0 < selfid_);
	assert(0 < option.election_tick);
	assert(0 < option.hb_tick);
    assert(option.hb_tick <= option.election_tick);
	
	election_tick_ = base_election_tick_ + timeout_gen_();
    map_timeout_handler_[
        raft::RaftRole::FOLLOWER] = follower::onTimeout;
    map_timeout_handler_[
        raft::RaftRole::CANDIDATE] = candidate::onTimeout;
    map_timeout_handler_[
        raft::RaftRole::LEADER] = leader::onTimeout;

    map_step_handler_[
        raft::RaftRole::FOLLOWER] = follower::onStepMessage;
    map_step_handler_[
        raft::RaftRole::CANDIDATE] = candidate::onStepMessage;
    map_step_handler_[
        raft::RaftRole::LEADER] = leader::onStepMessage;

    map_new_build_rsp_handler_[
        raft::RaftRole::FOLLOWER] = follower::onNewBuildRsp;
    map_new_build_rsp_handler_[
        raft::RaftRole::CANDIDATE] = candidate::onNewBuildRsp;
    map_new_build_rsp_handler_[
        raft::RaftRole::LEADER] = leader::onNewBuildRsp;

    tmp_entry_cache_ = cutils::make_unique<raft::TmpEntryCache>();
    assert(nullptr != tmp_entry_cache_);
}


RaftMem::~RaftMem() = default;

std::tuple<
    bool, 
    raft::MessageType, 
    bool>
RaftMem::Step(
        const raft::Message& msg, 
        std::unique_ptr<raft::HardState>& hard_state, 
        std::unique_ptr<raft::SoftState>& soft_state)
{
    assert(msg.logid() == logid_);
    assert(msg.to() == selfid_);

    auto role = raft::RaftRole::FOLLOWER;
    {
        raft::RaftState raft_state(*this, hard_state, soft_state);
        role = raft_state.GetRole();
    }
    assert(map_step_handler_.end() != map_step_handler_.find(role));
    assert(nullptr != map_step_handler_.at(role));
    return map_step_handler_.at(role)(*this, msg, hard_state, soft_state);
}

std::tuple<
    std::unique_ptr<raft::HardState>, 
    std::unique_ptr<raft::SoftState>, 
    bool, 
    raft::MessageType>
RaftMem::CheckTimeout(bool force_timeout)
{
    assert(map_timeout_handler_.end() != map_timeout_handler_.find(role_));
    assert(nullptr != map_timeout_handler_.at(role_));
	Tick();
    return map_timeout_handler_.at(role_)(*this, force_timeout);
}

void RaftMem::setRole(uint64_t next_term, uint32_t role)
{
    logerr("INFO: term_ %" PRIu64 " next_term %" PRIu64 
            " role_ %u role %u", 
            term_, next_term, 
            static_cast<uint32_t>(role_), role);
    assert(next_term == term_);
    role_ = static_cast<raft::RaftRole>(role);
}

void RaftMem::updateLeaderId(uint64_t next_term, uint32_t leader_id)
{
    logerr("INFO: term_ %" PRIu64 " next_term %" PRIu64 
            " leader_id_ %u leader_id %u", 
            term_, next_term, 
            leader_id_, leader_id);
    assert(next_term == term_);
    leader_id_ = leader_id;
}

void RaftMem::updateTerm(uint64_t new_term)
{
    assert(term_ <= new_term);
    if (term_ == new_term)
    {
        return ;
    }

    logerr("INFO: logid %lu term_ %" PRIu64 " vote_ %u new_term %" PRIu64, 
            logid_, term_, vote_, new_term);
    term_ = new_term;
    vote_ = 0;
    leader_id_ = 0;
}

void RaftMem::updateVote(uint64_t vote_term, uint32_t vote)
{
    assert(term_ == vote_term);
    logerr ( "logid %lu vote_term %d vote %u vote_ %u\n", 
			logid_, static_cast<int>(vote_term), vote, vote_);
    assert(0 == vote_);
    assert(0 == leader_id_);
    logerr("INFO: logid %lu term_ %" PRIu64 " vote %u", logid_, term_, vote);
    vote_ = vote;
}

std::deque<std::unique_ptr<Entry>>::iterator 
RaftMem::findLogEntry(uint64_t index)
{
    auto ans = std::lower_bound(
            logs_.begin(), logs_.end(), index, 
            [=](const std::unique_ptr<Entry>& entry, uint64_t index) {
                assert(nullptr != entry);
                return entry->index() < index;
            });
    if (ans != logs_.end()) {
        const std::unique_ptr<Entry>& entry = *ans;
        assert(nullptr != entry);
        assert(entry->index() >= index);
    }

    return ans;
}

void RaftMem::appendLogEntries(std::unique_ptr<HardState> hard_state)
{
    assert(nullptr != hard_state);
    assert(0 < hard_state->entries_size());

    uint64_t index = hard_state->entries(0).index();
    assert(0 < index);

    // assert check
    for (int idx = 0; idx < hard_state->entries_size(); ++idx) {
        const auto& entry = hard_state->entries(idx);
        assert(term_ >= entry.term());
        assert(index + idx == entry.index());
    }

    auto inmem_iter = findLogEntry(index);
    // shrink
    if (logs_.end() != inmem_iter) {
        // assert: must be conflict;
        const auto& entry = hard_state->entries(0);
        assert(nullptr != *inmem_iter);
        const auto& conflict_entry = *(*(inmem_iter));
        assert(index == entry.index());
        assert(index == conflict_entry.index());
        assert(entry.term() != conflict_entry.term());
        logerr("INFO: logs_.erase %d", 
                static_cast<int>(std::distance(inmem_iter, logs_.end())));
        logs_.erase(inmem_iter, logs_.end());
    }

    // append
    // TODO: config ?
    for (int idx = 0; idx < hard_state->entries_size(); ++idx) {
        auto entry = hard_state->mutable_entries(idx);
        assert(nullptr != entry);

        std::unique_ptr<raft::Entry> 
            new_entry = cutils::make_unique<raft::Entry>();
        assert(nullptr != new_entry);
        new_entry->Swap(entry);
        assert(0 < new_entry->term());
        assert(0 < new_entry->index());
        logs_.emplace_back(std::move(new_entry));
    }

    assert(false == logs_.empty());
    // assert check
    assert(nullptr != logs_[0]);
    uint64_t log_index = logs_[0]->index();
    for (size_t idx = 0; idx < logs_.size(); ++idx) {
        const auto& entry = logs_[idx];
        assert(nullptr != entry);
        assert(term_ >= entry->term());
        assert(0 < entry->term());
        assert(log_index + idx == entry->index());
    }

    return ;
}

void RaftMem::updateCommit(uint64_t new_commit)
{
    logerr("INFO: term_ %" PRIu64 " commit_ %" PRIu64 
            " new_commit %" PRIu64, 
            term_, commit_, new_commit);
    assert(new_commit >= commit_);
    commit_ = new_commit;
}

void RaftMem::updateDiskMinIndex(uint64_t next_disk_min_index)
{
    logerr("INFO: term_ %" PRIu64 " disk_min_index_ %" PRIu64
            " next_disk_min_index %" PRIu64, 
            term_, disk_min_index_, next_disk_min_index);
    assert(next_disk_min_index >= disk_min_index_);
    disk_min_index_ = next_disk_min_index;
}

void RaftMem::updateMetaInfo(
        const raft::MetaInfo& metainfo)
{
    if (metainfo.has_term()) {
        assert(metainfo.term() >= GetTerm());
        updateTerm(metainfo.term());
    }

    if (metainfo.has_vote()) {
        updateVote(GetTerm(), metainfo.vote());
    }

    if (metainfo.has_commit()) {
        updateCommit(metainfo.commit());
    }

    if (metainfo.has_min_index()) {
        updateDiskMinIndex(metainfo.min_index());
    }
}

void RaftMem::applyHardState(
        std::unique_ptr<raft::HardState> hard_state)
{
    if (nullptr == hard_state) {
        return ;
    }

    assert(nullptr != hard_state);
    if (hard_state->has_meta()) {
        updateMetaInfo(hard_state->meta());
    }

    if (0 < hard_state->entries_size()) {
        // update log entry;
		uint64_t begin_time = cutils::get_curr_ms();
		int entries_size = hard_state->entries_size();
        appendLogEntries(std::move(hard_state));
		uint64_t cost_time = cutils::get_curr_ms() - begin_time;
		if (5 <= cost_time) {
			logerr("PERFORMANCE cost_time %lu entries_size %d", 
					cost_time, entries_size);
		}

        assert(nullptr == hard_state);
    }
}

void RaftMem::ApplyState(
        std::unique_ptr<raft::HardState> hard_state, 
        std::unique_ptr<raft::SoftState> soft_state)
{
    bool config_change = 
        nullptr != soft_state && 0 < soft_state->configs_size();
    if (nullptr != hard_state) {
		// need_update_expected = 0 != hard_state->entries_size();
        applyHardState(std::move(hard_state));
        assert(nullptr == hard_state);
    }

    auto next_term = GetTerm();
    if (nullptr != soft_state) {
        if (soft_state->has_role()) {
            setRole(next_term, soft_state->role());
        }

        if (soft_state->has_leader_id()) {
            updateLeaderId(next_term, soft_state->leader_id());
        }
    }

    // map_progress
    if (config_change) {
        updateMapProgress(
                logid_, GetMaxIndex(), *config_, map_progress_);
    }

    return ;
}

std::unique_ptr<raft::Message>
RaftMem::BuildRspMsg(
        const raft::Message& req_msg, 
        const raft::MessageType rsp_msg_type)
{
    if (raft::MessageType::MsgNull == rsp_msg_type) {
        return nullptr;
    }

    auto handler = map_new_build_rsp_handler_.at(GetRole());
    assert(nullptr != handler);

    auto rsp_msg = handler(*this, req_msg, rsp_msg_type);
    if (nullptr == rsp_msg) {
        return nullptr;
    }

    assert(nullptr != rsp_msg);
    // fill nodes;
    if (0 != rsp_msg->to()) {
        auto node = rsp_msg->add_nodes();
        assert(nullptr != node);
        *node = config_->Get(rsp_msg->to(), req_msg.from_node());
        return rsp_msg;
    }

    assert(0 == rsp_msg->to());
    for (const auto& node : GetConfigNodes()) {
        if (GetSelfId() == node.svr_id()) {
            continue;
        }

        auto new_node = rsp_msg->add_nodes();
        assert(nullptr != new_node);
        *new_node = node;
    }

    return rsp_msg;
}

std::unique_ptr<raft::Message> 
RaftMem::BuildBroadcastRspMsg(raft::MessageType rsp_msg_type)
{
    if (raft::MessageType::MsgVote != rsp_msg_type &&
            raft::MessageType::MsgHeartbeat != rsp_msg_type) {
        return nullptr;
    }

    auto handler = map_new_build_rsp_handler_.at(GetRole());
    assert(nullptr != handler);

    raft::Message fake;
    fake.set_logid(GetLogId());
    fake.set_term(GetTerm());
    fake.set_from(0);
    fake.set_to(GetSelfId());
    fake.set_index(GetMaxIndex());
    auto rsp_msg = BuildRspMsg(fake, rsp_msg_type);
    assert(nullptr != rsp_msg);
    assert(0 < rsp_msg->nodes_size());
    assert(rsp_msg_type == rsp_msg->type());
    return rsp_msg;
}


std::vector<std::unique_ptr<raft::Message>>
RaftMem::BuildAppMsg()
{
    assert(raft::RaftRole::LEADER == GetRole());

    std::vector<std::unique_ptr<raft::Message>> vec_rsp_msg;

    raft::Message fake;
    fake.set_logid(GetLogId());
    fake.set_term(GetTerm());
    fake.set_to(GetSelfId());
    fake.set_index(GetMaxIndex());
    for (const auto& node : GetConfigNodes()) {
        if (GetSelfId() == node.svr_id()) {
            continue;
        }

        fake.set_from(node.svr_id());
        auto rsp_msg = BuildRspMsg(fake, raft::MessageType::MsgApp);
        if (nullptr != rsp_msg) {
            vec_rsp_msg.push_back(std::move(rsp_msg));
        }
    }

    return vec_rsp_msg;
}

uint64_t RaftMem::GetMinIndex() const
{
    if (true == logs_.empty()) {
        return 0;
    }

    const auto& entry = logs_.front();
    assert(nullptr != entry);
    assert(0 < entry->index());
    return entry->index();
}

uint64_t RaftMem::GetMaxIndex() const 
{
    if (true == logs_.empty()) {
        return 0;
    }

    const auto& entry = logs_.back();
    assert(nullptr != entry);
    assert(0 < entry->index());
    return entry->index();
}

const raft::Entry* RaftMem::At(int mem_idx) const 
{
    assert(0 <= mem_idx);
    if (mem_idx >= logs_.size()) {
        return nullptr;
    }

    assert(nullptr != logs_.at(mem_idx));
    return logs_.at(mem_idx).get();
}

void RaftMem::UpdateActiveTime()
{
	election_deactive_tick_ = 0; // reset;
//    auto now = std::chrono::system_clock::now();
////	logerr("INFO: active_time_ %zu now %zu", 
////			cutils::calculate_ms(active_time_), cutils::calculate_ms(now));
//    active_time_ = now;
}

void RaftMem::RefreshElectionTimeout()
{
	election_tick_ = base_election_tick_ + timeout_gen_();
}

bool RaftMem::HasTimeout() const
{
	return election_deactive_tick_ >= election_tick_;

//    auto now = std::chrono::system_clock::now();
//	bool has_timeout = active_time_ + election_tick_ < now;
//	if (has_timeout) {
//		election_tick_ = 
//			base_election_tick_ + std::chrono::milliseconds{timeout_gen_()};
//		logerr("logid %lu new election_tick_ %zu", logid_, election_tick_.count());
//		logerr("INFO: active_time_ %zu now %zu election_tick_ %zu has_timeout %d", 
//				cutils::calculate_ms(active_time_), cutils::calculate_ms(now), 
//				election_tick_.count(), has_timeout);
//	}
//
//	return has_timeout;
}

bool RaftMem::IsHeartbeatTimeout() const 
{
	return hb_deactive_tick_ >= hb_tick_;
//	auto now = std::chrono::system_clock::now();
//	bool has_timeout = hb_active_time_ + hb_tick_ < now;
//	return has_timeout;
}

void RaftMem::UpdateHeartBeatActiveTime() 
{
	hb_deactive_tick_ = 0;
	// hb_active_time_ = std::chrono::system_clock::now();
}

void RaftMem::Tick()
{
	++election_deactive_tick_;
	++hb_deactive_tick_;
}

uint32_t RaftMem::GetVote(uint64_t term) const 
{
    if (term_ != term) {
        return 0;
    }

    assert(term_ == term);
    return vote_;
}

uint32_t RaftMem::GetLeaderId(uint64_t term) const
{
    if (term_ != term) {
        return 0;
    }

    assert(term_ == term);
    return leader_id_;
}

const raft::Entry* RaftMem::GetLastEntry() const
{
    if (logs_.empty()) {
        return nullptr;
    }

    assert(false == logs_.empty());
    assert(nullptr != logs_.back());
    return logs_.back().get();
}


int RaftMem::UpdateVote(
        uint64_t vote_term, uint32_t candidate_id, bool vote_yes)
{
    assert(nullptr != GetConfig());
    assert(IsMember(candidate_id));
    if (vote_map_.end() != vote_map_.find(candidate_id)) {
        if (vote_yes) {
            assert(vote_term == vote_map_.at(candidate_id));
        }
        else {
            assert(0 == vote_map_.at(candidate_id));
        }

        return calculateMajorYesCount(vote_term, vote_map_);
    }

    // else
    vote_map_[candidate_id] = vote_yes ? vote_term : 0;
	logerr("IMPORTANT: logid %lu candidate_id %u vote_yes %d vote_term %lu", 
			logid_, candidate_id, vote_yes, vote_term);
    return calculateMajorYesCount(vote_term, vote_map_);
}

bool RaftMem::IsLogEmpty() const
{
    return logs_.empty();
}

void RaftMem::ClearVoteMap()
{
    vote_map_.clear();
    logerr ( "TEST: vote_map_.size %zu\n", vote_map_.size() );
}


bool RaftMem::IsMajority(int cnt) const
{
    assert(nullptr != config_);
    auto cluster_config = config_->GetConfig();
    assert(nullptr != cluster_config);

    assert(3 <= cluster_config->nodes_size());
    const int major_cnt = (cluster_config->nodes_size() / 2) + 
        ((0 == cluster_config->nodes_size() % 2) ? 0 : 1) + 1;

//    // TODO
//    // assume 3 node
//    const int major_cnt = (
//            vote_follower_set_.size() / 2) + 
//        ((0 == vote_follower_set_.size() % 2) ? 0 : 1) + 1;
    logerr ( "%d %d cnt %d other %d\n", 
            cluster_config->nodes_size(), 
            cluster_config->nodes_size() / 2, 
            cnt, major_cnt );
    return cnt >= major_cnt;
}

int RaftMem::Init(
        const raft::ClusterConfig& commit_config, 
        std::unique_ptr<raft::HardState> hard_state)
{
    assert(0 == leader_id_);
    assert(0 == term_);
    assert(0 == vote_);
    assert(0 == commit_);
    assert(true == logs_.empty());
    assert(true == vote_map_.empty());
    assert(true == map_progress_.empty());
    assert(nullptr == config_);

    assert(is_valid(commit_config));
    assert(nullptr != hard_state);

    config_ = cutils::make_unique<raft::RaftConfig>();
    assert(nullptr != config_);
    {
        auto soft_state = cutils::make_unique<raft::SoftState>();
        assert(nullptr != soft_state);
        auto new_config = soft_state->add_configs();
        assert(nullptr != new_config);
        *new_config = commit_config;
        config_->Apply(*soft_state, commit_config.index());
    }

    std::unique_ptr<raft::SoftState> soft_state;
    for (int idx = 0; idx < hard_state->entries_size(); ++idx) {
        const auto& entry = hard_state->entries(idx);
        assert(0 < entry.term());
        assert(0 < entry.index());
        if (raft::EntryType::EntryConfChange == entry.type()) {
            auto cluster_config = config_->GetConfig();
            assert(nullptr != cluster_config);
            assert(commit_config.index() == cluster_config->index());
            if (cluster_config->index() < entry.index()) {
                process_entry(entry, soft_state);
            }
        }
    }

    ApplyState(std::move(hard_state), std::move(soft_state));
    // update map progress anyway
    updateMapProgress(
            logid_, GetMaxIndex(), *config_, map_progress_);
    assert(raft::RaftRole::FOLLOWER == GetRole());
    UpdateActiveTime();
    assert(nullptr != config_->GetConfig());
    assert(nullptr != config_->GetCommitConfig());
    assert(config_->GetCommitConfig()->index() <= GetCommit());
    assert(GetMinIndex() <= GetCommit());
    assert(GetCommit() <= GetMaxIndex());
    if (nullptr != config_->GetPendingConfig()) {
        assert(config_->GetPendingConfig()->index() > GetCommit());
        assert(config_->GetPendingConfig()->index() <= GetMaxIndex());
    }

    // check map_progress_
    assert_check(*config_, map_progress_);
    return 0;
}

//int RaftMem::Init(
//        const raft::HardState& hard_state)
//{
//    assert(0 == leader_id_);
//    assert(0 == term_);
//    assert(0 == vote_);
//    assert(0 == commit_);
//    assert(true == logs_.empty());
//    assert(true == vote_map_.empty());
//    assert(false == vote_follower_set_.empty());
//
//    auto new_hard_state = cutils::make_unique<raft::HardState>();
//    *new_hard_state = hard_state;
//    ApplyState(std::move(new_hard_state), nullptr);
//    assert(raft::RaftRole::FOLLOWER == GetRole());
//
//	// prev_entry_ = std::move(prev_entry);
//    UpdateActiveTime(); 
//
////	if (1 < GetMinIndex()) {
////		assert(nullptr != prev_entry_);
////		assert(prev_entry_->index() + 1 == GetMinIndex());
////	}
//
//	if (0 < GetMinIndex()) {
//		assert(0 < GetMaxIndex());
//		if (0 == disk_min_index_) {
//			disk_min_index_ = 1;
//		}
//	}
//
//	assert(GetCommit() >= GetMinIndex() || uint64_t{1} == GetMinIndex());
//	tmp_entry_cache_ = cutils::make_unique<TmpEntryCache>();
//    return 0;
//}


std::tuple<
	std::unique_ptr<raft::Message>, 
	std::unique_ptr<raft::HardState>, 
	std::unique_ptr<raft::SoftState>>
RaftMem::SetValue(
		const std::vector<std::string>& vec_value, 
		const std::vector<uint64_t>& vec_reqid)
{
    assert(raft::RaftRole::LEADER == role_);
	assert(false == vec_value.empty());
    assert(vec_value.size() == vec_reqid.size());

    auto prop_msg = cutils::make_unique<raft::Message>();
    assert(nullptr != prop_msg);
    prop_msg->set_type(raft::MessageType::MsgProp);
    prop_msg->set_logid(logid_);
    prop_msg->set_to(selfid_);
    prop_msg->set_from(0);
    prop_msg->set_term(term_);
    uint64_t max_index = GetMaxIndex();
    prop_msg->set_index(max_index);
    prop_msg->set_log_term(
            logs_.empty() ? 0 : logs_.back()->term());
    for (size_t idx = 0; idx < vec_value.size(); ++idx) {
        auto new_entry = prop_msg->add_entries();
        assert(nullptr != new_entry);
        new_entry->set_type(raft::EntryType::EntryNormal);
        new_entry->set_term(prop_msg->term());
        new_entry->set_index(prop_msg->index() + idx + 1);
        new_entry->set_reqid(vec_reqid[idx]);
        new_entry->set_data(vec_value[idx].data(), vec_value[idx].size());
    }

    assert(static_cast<
            size_t>(prop_msg->entries_size()) == vec_value.size());

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;
    bool need_disk_replicate = false;
    std::tie(
			mark_broadcast, rsp_msg_type, 
            need_disk_replicate) = Step(*prop_msg, hard_state, soft_state);
    assert(nullptr != hard_state);
    assert(nullptr == soft_state);
    assert(true == mark_broadcast);
    assert(raft::MessageType::MsgApp == rsp_msg_type);
    assert(false == need_disk_replicate);
    return std::make_tuple(
            std::move(prop_msg), std::move(hard_state), std::move(soft_state));
}


std::tuple<
	std::unique_ptr<raft::Message>, 
	std::unique_ptr<raft::HardState>, 
	std::unique_ptr<raft::SoftState>>
RaftMem::SetValue(
		const std::string& value, uint64_t reqid)
{
	assert(raft::RaftRole::LEADER == role_);

    auto prop_msg = cutils::make_unique<raft::Message>();
    assert(nullptr != prop_msg);

    prop_msg->set_type(raft::MessageType::MsgProp);
    prop_msg->set_logid(logid_);
    prop_msg->set_to(selfid_);
    prop_msg->set_from(0);
    prop_msg->set_term(term_);

    auto max_index = GetMaxIndex();
    prop_msg->set_index(max_index);
    prop_msg->set_log_term(
            logs_.empty() ? 0 : logs_.back()->term());
    {
        auto new_entry = prop_msg->add_entries();
        assert(nullptr != new_entry);
        new_entry->set_type(raft::EntryType::EntryNormal);
        new_entry->set_term(prop_msg->term());
        new_entry->set_index(prop_msg->index() + 1);
        new_entry->set_reqid(reqid);
        new_entry->set_data(value.data(), value.size());
    }
    assert(1 == prop_msg->entries_size());

    std::unique_ptr<raft::HardState> hard_state;
    std::unique_ptr<raft::SoftState> soft_state;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;
    bool need_disk_replicate = false;
    std::tie(mark_broadcast, rsp_msg_type, 
            need_disk_replicate) = Step(*prop_msg, hard_state, soft_state);
    assert(nullptr != hard_state);
    assert(nullptr == soft_state);
    assert(true == mark_broadcast);
    assert(raft::MessageType::MsgApp == rsp_msg_type);
    assert(false == need_disk_replicate);
    return std::make_tuple(
            std::move(prop_msg), std::move(hard_state), std::move(soft_state));
}

size_t RaftMem::CompactLog(uint64_t new_min_index)
{
    auto min_index = GetMinIndex();
    if (new_min_index <= min_index) {
        return 0;
    }

    if (new_min_index >= commit_) {
        return 0;
    }

    int mem_idx = new_min_index - min_index;
    assert(0 < mem_idx);
    logs_.erase(logs_.begin(), logs_.begin() + mem_idx);
    return mem_idx;
}

raft::RaftRole RaftMem::BecomeFollower()
{
	auto prev_role = GetRole();
	if (raft::RaftRole::FOLLOWER != prev_role) {
		setRole(GetTerm(), static_cast<uint32_t>(raft::RaftRole::FOLLOWER));
	}

	UpdateActiveTime();
	election_tick_ = election_tick_ + election_tick_ / 2;
	logerr("INFO: term %lu election_tick_ %u", GetTerm(), election_tick_);
	return prev_role;
}


int RaftMem::ShrinkMemLog(size_t max_mem_log_size)
{
	if (logs_.size() <= max_mem_log_size) {
		return 0;
	}

	assert(false == logs_.empty());
	assert(logs_.size() > max_mem_log_size);
	auto role = GetRole();
	if (raft::RaftRole::CANDIDATE == role) {
		return -1; // don't truncate in candidate state;
	}

	// SAME:
	// TODO: for now: most simple case

	int erase_size = std::min<int>(
			logs_.size() - max_mem_log_size, GetCommit() - GetMinIndex());
	assert(0 <= erase_size);
	if (0 < erase_size) {
		auto erase_util = logs_.begin() + erase_size;
		logerr("role %u shrink index %" PRIu64 " to %" PRIu64, 
				static_cast<uint32_t>(role), 
				logs_.front()->index(), (*erase_util)->index());

		logs_.erase(logs_.begin(), erase_util);
	}

	if (0 == erase_size) {
		return -2;
	}

	return 0;
}

void RaftMem::RecvCatchUp()
{
	missing_catch_up_ = 0;
}

void RaftMem::MissingCatchUp()
{
	++missing_catch_up_;
}

bool RaftMem::NeedCatchUp()
{
	return missing_catch_up_ > 5; // TODO
}

void RaftMem::TriggerCatchUp()
{
	missing_catch_up_ = missing_catch_up_ >> 1;
}


raft::Progress* RaftMem::GetProgress(uint32_t peer_id)
{
	auto iter = map_progress_.find(peer_id);
	assert(map_progress_.end() != iter);
	assert(nullptr != iter->second);
	return iter->second.get();
}

uint64_t RaftMem::GetDiskMinIndex() const 
{
	if (0 < GetMaxIndex()) {
		return 0 == disk_min_index_ ? 1 : disk_min_index_;
	}

	assert(0 == disk_min_index_);
	return disk_min_index_;
}

bool RaftMem::IsReplicateStall() const
{
	bool is_stall = true;
	for (const auto& id_progress : map_progress_) {
		uint32_t peer_id = id_progress.first;
		if (selfid_ == peer_id) {
			continue;
		}

		const auto& progress = id_progress.second.get();
		assert(nullptr != progress);
		if (progress->CanVote() && 
				false == progress->IsPause() && 
				(progress->GetNext() - 1 >= GetMinIndex())) {
			is_stall = false;
			break;
		}
	}
	
	return is_stall;
}

const raft::ClusterConfig* RaftMem::GetConfig() const 
{
    if (nullptr == config_) {
        return nullptr;
    }

    return config_->GetConfig();
}

std::vector<raft::Node> RaftMem::GetConfigNodes() const 
{
    auto cluster_config = GetConfig();
    assert(nullptr != cluster_config);
    std::vector<raft::Node> nodes;
    nodes.reserve(cluster_config->nodes_size());
    for (int idx = 0; idx < cluster_config->nodes_size(); ++idx) {
        nodes.push_back(cluster_config->nodes(idx));
    }

    return nodes;
}

const raft::ClusterConfig* RaftMem::GetPendingConfig() const 
{
    if (nullptr == config_) {
        return nullptr;
    }

    return config_->GetPendingConfig();
}

bool RaftMem::IsMember(uint32_t peer) const 
{
    assert(nullptr != config_);
    return config_->IsMember(peer);
}

TmpEntryCache::TmpEntryCache()
	: term_(0)
{

}

TmpEntryCache::~TmpEntryCache() = default;

void TmpEntryCache::MayInvalid(uint64_t active_term)
{
	if (active_term != term_) {
		assert(term_ < active_term);
		cache_.clear();
		term_ = active_term;
	}
}

void TmpEntryCache::Insert(const raft::Entry& entry)
{
	assert(0 < entry.index());
	assert(0 < entry.term());
	auto active_term = entry.term();
	if (active_term != term_) {
		assert(term_ < active_term);
		cache_.clear();
		term_ = active_term;
	}

	assert(active_term == term_);
	auto iter = cache_.find(entry.index());
	if (cache_.end() != iter) {
		assert(nullptr != iter->second);

		assert_check(entry, *(iter->second));
		return ;
	}

	auto new_entry = cutils::make_unique<raft::Entry>(entry);
	assert(nullptr != new_entry);
	assert(new_entry->index() == entry.index());
	cache_[entry.index()] = std::move(new_entry);
	assert(nullptr == new_entry);
}

void TmpEntryCache::Extract(
		uint64_t active_term, 
		raft::HardState& hard_state)
{
	assert(0 < active_term);
	if (active_term != term_) {
		assert(term_ < active_term);
		cache_.clear();
		term_ = active_term;
	}

	assert(active_term == term_);
	if (0 == hard_state.entries_size()) {
		return ; // do nothing;
	}

	assert(0 < hard_state.entries_size());
	uint64_t max_index = 
		hard_state.entries(hard_state.entries_size()-1).index();
	assert(0 < max_index);

	int prev_entry = hard_state.entries_size();
	auto index = max_index + 1;
	for (; true; ++index) {
		auto iter = cache_.lower_bound(index);
		if (cache_.end() == iter || 
				iter->first > index) {
			break;
		}

		assert(cache_.end() != iter);
		assert(iter->first == index);
		assert(nullptr != iter->second);
		auto new_entry = hard_state.add_entries();
		assert(nullptr != new_entry);

		*new_entry = *(iter->second);
		assert(new_entry->index() == index);
		assert(new_entry->term() == active_term);
	}

	if (false == cache_.empty()) {
		logerr("try to erase until index %lu", index);
		cache_.erase(cache_.begin(), cache_.lower_bound(index));
	}

	if (prev_entry < hard_state.entries_size()) {
		logerr("IMPORTANT: extra from cache.size %zu prev_entry %d now %d", 
				cache_.size(), prev_entry, hard_state.entries_size());
	}

	return ;
}

uint64_t TmpEntryCache::GetMaxIndex() const {
	return cache_.empty() ? 0 : cache_.rbegin()->first;
}

uint64_t TmpEntryCache::GetMinIndex() const {
	return cache_.empty() ? 0 : cache_.begin()->first;
}


} // namespace raft

