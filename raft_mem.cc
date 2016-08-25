#include "mem_utils.h"
#include "log_utils.h"
#include "time_utils.h"
#include "replicate.h"
#include "raft_mem.h"
#include "raft_state.h"


namespace {

using namespace raft;


void updateHardState(
        raft::RaftMem& raft_mem, 
        std::unique_ptr<raft::HardState>& hard_state)
{
    if (nullptr == hard_state) {
        return ;
    }

    assert(nullptr != hard_state);
    if (false == hard_state->has_term()) {
        hard_state->set_term(raft_mem.GetTerm());
    }

    if (false == hard_state->has_vote()) {
        hard_state->set_vote(raft_mem.GetVote(hard_state->term()));
    }

    if (false == hard_state->has_commit()) {
        hard_state->set_commit(raft_mem.GetCommit());
    }
}

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

uint64_t nextExploreIndex(
        const raft::RaftState& raft_state, 
        const raft::Replicate* replicate, 
        const raft::Message& req_msg)
{
    assert(nullptr != replicate);
    if (req_msg.reject()) {
        uint64_t lower_index = 
            std::max(
                    replicate->GetAcceptedIndex(req_msg.from()), 
                    raft_state.GetMinIndex());
        assert(0 < lower_index);
        assert(lower_index < req_msg.index() - 1);
        return lower_index + (req_msg.index() - lower_index) / 2;
    }

    assert(false == req_msg.reject());
    uint64_t rejected_index = 
        replicate->GetRejectedIndex(req_msg.from());
    assert(0 < rejected_index);
    assert(rejected_index <= raft_state.GetMaxIndex() + 1);
    assert(rejected_index > req_msg.index());
    return req_msg.index() + (rejected_index = req_msg.index()) / 2;
}

uint64_t calculateCommit(
        const std::set<uint32_t>& vote_follower_set, 
        const raft::Replicate* replicate)
{
    assert(nullptr != replicate);
    assert(size_t{2} <= vote_follower_set.size()); // exclude self;

    const size_t major_cnt = vote_follower_set.size() / 2 + 
        0 == vote_follower_set.size() % 2 ? 0 : 1;

    auto accepted_distribution = 
        replicate->GetAcceptedDistributionOn(vote_follower_set);
    size_t cnt = 0;
    uint64_t commit_index = 0;
    for (const auto& idx_cnt_pair : accepted_distribution) {
        assert(0 < idx_cnt_pair.second);
        if (cnt + idx_cnt_pair.second >= major_cnt) {
            return commit_index;
        }

        cnt += idx_cnt_pair.second;
        assert(cnt < major_cnt);
        assert(commit_index < idx_cnt_pair.first);
        commit_index = idx_cnt_pair.first;
    }

    // must be the case !!!
    return 0;
}

namespace follower {

bool canVoteYes(
        raft::RaftState& raft_state, 
        uint64_t msg_term, 
        uint64_t vote_index, 
        uint64_t vote_term)
{
    assert(raft_state.GetTerm() == msg_term);
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

bool canVote(
        raft::RaftState& raft_state, 
        uint64_t msg_term, 
        uint64_t vote_index, 
        uint64_t vote_term)
{
    const auto term = raft_state.GetTerm();
    if (term != msg_term) {
        return term > msg_term;
    }

    assert(term == msg_term);
    uint32_t vote = raft_state.GetVote(msg_term);
    uint32_t leader_id = raft_state.GetLeaderId(msg_term);
    if (0 != vote || 0 != leader_id) {
        logerr("INFO: already vote term %" PRIu64 " vote %u leader_id %u", 
                msg_term, vote, leader_id);
        return false;
    }

    assert(0 == vote);
    assert(0 == leader_id);
    // raft paper 
    //  5.4.1 Election restriction
    //  raft determines which of two logs is more up-to-date by
    //  comparing the index and term of the last entries in the logs.
    //  - If the logs have last entries with different terms, then the log
    //    with the later term is more up-to-date;
    //  - If the logs end with the same term, then whichever log is longer
    //    is more up-to-date.
    return canVoteYes(raft_state, msg_term, vote_index, vote_term);
}

int resolveEntries(
        raft::RaftState& raft_state, 
        const raft::Message& msg)
{
    assert(0 < msg.index());
    const uint64_t min_index = raft_state.GetMinIndex();
    const uint64_t max_index = raft_state.GetMaxIndex();
    assert(min_index <= max_index);

    // msg.index() => current index;
    // msg.log_term(); => prev_log_term; => current_index - 1;
    if (0 == min_index) {
        assert(0 == max_index);
        if (uint64_t{1} != msg.index()) {
            logerr("INFO: raft_mem.logs_ empty msg.index %" PRIu64, 
                    msg.index());
            return -1;
        }
        assert(uint64_t{1} == msg.index());
        assert(uint64_t{0} == msg.log_term());
        return 0;
    }

    assert(0 < min_index);
    if (msg.index() <= min_index || msg.index() > (max_index + 1)) {
        logerr("INFO: raft_mem %" PRIu64 " %" PRIu64 " "
                " msg.index %" PRIu64, 
                min_index, max_index, msg.index());
        return -2;
    }

    assert(msg.index() > min_index && msg.index() <= (max_index + 1));
    // else => intersect
    //      => check prev_index && prev_logterm
    {
        assert(uint64_t{1} <= msg.index());
        uint64_t check_index = msg.index() - 1;
        if (0 == check_index) {
            assert(0 == msg.log_term());
        }
        else {
            assert(0 < check_index);
            assert(min_index <= check_index);
            assert(max_index >= check_index);
            int mem_idx = check_index - min_index;
            assert(0 <= mem_idx);
            const raft::Entry* mem_entry = raft_state.At(mem_idx);
            assert(nullptr != mem_entry);
            assert(check_index == mem_entry->index());
            if (msg.log_term() != mem_entry->term()) {
                logerr("check_index %" PRIu64 " msg.log_term %" PRIu64 " "
                        "mem_entry->term %" PRIu64, 
                        check_index, msg.log_term(), mem_entry->term());
                return -3;
            }
            assert(msg.log_term() == mem_entry->term());
        }
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
        if (msg_entry.term() != mem_entry->term()) {
            logerr("IMPORTANT: index %" PRIu64 " find inconsist term "
                    "msg_entry.term %" PRIu64 " mem_entry->term %" PRIu64, 
                    msg_entry.index(), msg_entry.term(), mem_entry->term());
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
    if (false == force_timeout && false == raft_mem.HasTimeout()) {
        return std::make_tuple(
                nullptr, nullptr, false, raft::MessageType::MsgNull);
    }

    // timeout =>
    std::unique_ptr<raft::HardState> 
        hard_state = cutils::make_unique<raft::HardState>();
    assert(nullptr != hard_state);

    std::unique_ptr<raft::SoftState>
        soft_state = cutils::make_unique<raft::SoftState>();
    assert(nullptr != soft_state);

    soft_state->set_role(static_cast<uint32_t>(raft::RaftRole::CANDIDATE));
    soft_state->set_leader_id(0);

    hard_state->set_term(raft_mem.GetTerm() + 1);
    hard_state->set_vote(0);
    updateHardState(raft_mem, hard_state);

    raft_mem.ClearVoteMap();
    raft_mem.UpdateActiveTime();
    return std::make_tuple(
            std::move(hard_state), std::move(soft_state), 
            true, raft::MessageType::MsgVote);
}

// follower
std::tuple<
    std::unique_ptr<raft::HardState>, 
    std::unique_ptr<raft::SoftState>, 
    bool, 
    raft::MessageType>
onStepMessage(
        raft::RaftMem& raft_mem, 
        const raft::Message& msg, 
        std::unique_ptr<raft::HardState> hard_state, 
        std::unique_ptr<raft::SoftState> soft_state)
{
    raft::RaftState raft_state(raft_mem, hard_state, soft_state);
    assert(raft::RaftRole::FOLLOWER == raft_state.GetRole());

    bool mark_update_active_time = false;
    auto term = raft_state.GetTerm();
    if (msg.term() < term) {
        // ignore;
        return std::make_tuple(
                std::move(hard_state), std::move(soft_state), 
                false, raft::MessageType::MsgInvalidTerm);
    }

    assert(msg.term() >= term);
    if (msg.term() > term) {
       if (nullptr == hard_state) {
            hard_state = cutils::make_unique<raft::HardState>();
            assert(nullptr != hard_state);
        }

        assert(nullptr != hard_state);
        hard_state->set_term(msg.term());
        if (hard_state->has_vote()) {
            hard_state->set_vote(0); // reset;
        }

        mark_update_active_time = true;
        term = raft_state.GetTerm();
    }

    assert(msg.term() == term);
    auto rsp_msg_type = raft::MessageType::MsgNull;
    switch (msg.type()) {

    case raft::MessageType::MsgVote:
        {
            mark_update_active_time = true;
            assert(0 < msg.index());
            if (false == canVote(
                        raft_state, 
                        msg.term(), msg.index() - 1, msg.log_term())) {
                break;
            }

            if (nullptr == hard_state) {
                hard_state = cutils::make_unique<raft::HardState>();
                assert(nullptr != hard_state);
            }

            hard_state->set_term(msg.term());
            hard_state->set_vote(msg.from());

            rsp_msg_type = raft::MessageType::MsgVoteResp;
        }
        break;

    case raft::MessageType::MsgHeartbeat:
        {
            mark_update_active_time = true;
            // leader heart beat msg
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
                assert(leader_id == msg.from());
            }

            if (raft_state.CanUpdateCommit(
                        msg.commit_index(), msg.commit_term())) {
                assert(msg.commit_index() > raft_state.GetCommit());
                if (nullptr == hard_state) {
                    hard_state = cutils::make_unique<raft::HardState>();
                    assert(nullptr != hard_state);
                }

                hard_state->set_commit(msg.commit_index());
            }

            rsp_msg_type = raft::MessageType::MsgHeartbeatResp;
        }
        break;

    case raft::MessageType::MsgApp:
        {
            mark_update_active_time = true;
            uint32_t leader_id = raft_state.GetLeaderId(msg.term());
            if (0 == leader_id) {
                if (nullptr == soft_state) {
                    soft_state = cutils::make_unique<raft::SoftState>();
                    assert(nullptr != soft_state);
                }

                soft_state->set_leader_id(msg.from());
            }

            int app_idx = resolveEntries(raft_state, msg);
            if (0 <= app_idx) {
                if (app_idx < msg.entries_size() && 
                        nullptr == hard_state) { 
                    hard_state = cutils::make_unique<raft::HardState>();
                    assert(nullptr != hard_state);
                }

                for (int idx = app_idx; idx < msg.entries_size(); ++idx) {
                    const auto& entry = msg.entries(idx);
                    assert(0 < entry.term());
                    assert(0 < entry.index());

                    auto* new_entry = hard_state->add_entries();
                    assert(nullptr != new_entry);
                    *new_entry = entry;
                }
            }

            if (raft_state.CanUpdateCommit(
                        msg.commit_index(), msg.commit_term())) {
                assert(msg.commit_index() > raft_state.GetCommit());
                if (nullptr == hard_state) {
                    hard_state = cutils::make_unique<raft::HardState>();
                    assert(nullptr != hard_state);
                }

                hard_state->set_commit(msg.commit_index());
            }

            rsp_msg_type = raft::MessageType::MsgAppResp;
        }
        break;

    default:
        logerr("IGNORE: recv msg type %d", static_cast<int>(msg.type()));
        break;
    }

    if (nullptr != hard_state) {
        updateHardState(raft_mem, hard_state);
    }

    if (mark_update_active_time) {
        raft_mem.UpdateActiveTime();
    }

    return std::make_tuple(
            std::move(hard_state), std::move(soft_state), 
            false, rsp_msg_type);
}


// follower
std::unique_ptr<raft::Message>
onBuildRsp(
        raft::RaftMem& raft_mem, 
        const raft::Message& req_msg, 
        const std::unique_ptr<raft::HardState>& hard_state, 
        const std::unique_ptr<raft::SoftState>& soft_state, 
        bool mark_broadcast, 
        const raft::MessageType rsp_msg_type)
{
    assert(false == mark_broadcast);
    raft::RaftState raft_state(raft_mem, hard_state, soft_state);    
    assert(raft::RaftRole::FOLLOWER == raft_state.GetRole());

    std::unique_ptr<raft::Message> 
        rsp_msg = cutils::make_unique<raft::Message>();
    assert(nullptr != rsp_msg);
    switch (rsp_msg_type) {

    case raft::MessageType::MsgInvalidTerm:
        {
            assert(req_msg.term() < raft_state.GetTerm());
            rsp_msg->set_term(raft_state.GetTerm());
        }
        break;

    case raft::MessageType::MsgVoteResp:
        {
            // ONLY VOTE_YES => MsgVoteResp
            assert(nullptr != rsp_msg);
            assert(req_msg.term() == raft_state.GetTerm());
            // assert check
            assert(canVoteYes(raft_state, 
                        req_msg.term(), req_msg.index() - 1, 
                        req_msg.log_term()));

            assert(req_msg.from() == raft_state.GetVote(req_msg.term()));
            rsp_msg->set_reject(false);
        }
        break;

    case raft::MessageType::MsgAppResp:
    case raft::MessageType::MsgHeartbeatResp:
        {
            if (raft_state.IsMatch(
                        req_msg.index() - 1, req_msg.log_term())) {
                rsp_msg->set_reject(false); 
                rsp_msg->set_index(raft_state.GetMaxIndex() + 1);
            }
            else {
                rsp_msg->set_reject(true);
                rsp_msg->set_index(req_msg.index());
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
        rsp_msg->set_from(req_msg.to());
        if (false == rsp_msg->has_term()) {
            rsp_msg->set_term(req_msg.term());
        }
    }

    return std::move(rsp_msg);
}

} // namespace follower


namespace candidate {

std::tuple<
    std::unique_ptr<raft::HardState>, 
    std::unique_ptr<raft::SoftState>, 
    bool, 
    raft::MessageType>
onTimeout(raft::RaftMem& raft_mem, bool force_timeout)
{
    assert(raft::RaftRole::CANDIDATE == raft_mem.GetRole());
    if (false == force_timeout && false == raft_mem.HasTimeout()) {
        return std::make_tuple(
                nullptr, nullptr, false, raft::MessageType::MsgNull);
    }

    // timeout
    int vote_cnt = raft_mem.GetVoteCount();
    if (false == raft_mem.IsMajority(vote_cnt)) {
        raft_mem.UpdateActiveTime();
        return std::make_tuple(
                nullptr, nullptr, true, raft::MessageType::MsgVote);
    }

    std::unique_ptr<raft::HardState>
        hard_state = cutils::make_unique<raft::HardState>();
    assert(nullptr != hard_state);

    hard_state->set_term(raft_mem.GetTerm() + 1);
    hard_state->set_vote(0);
    updateHardState(raft_mem, hard_state);

    raft_mem.ClearVoteMap();
    raft_mem.UpdateActiveTime();
    return std::make_tuple(
            std::move(hard_state), nullptr, 
            true, raft::MessageType::MsgVote);
}

// candicate
std::tuple<
    std::unique_ptr<raft::HardState>, 
    std::unique_ptr<raft::SoftState>, 
    bool, 
    raft::MessageType>
onStepMessage(
        raft::RaftMem& raft_mem, 
        const raft::Message& msg, 
        std::unique_ptr<raft::HardState> hard_state, 
        std::unique_ptr<raft::SoftState> soft_state)
{
    raft::RaftState raft_state(raft_mem, hard_state, soft_state);
    assert(raft::RaftRole::CANDIDATE == raft_state.GetRole());

    uint64_t term = raft_state.GetTerm();
    if (msg.term() < term) {
        return std::make_tuple(
                std::move(hard_state), std::move(soft_state), 
                false, raft::MessageType::MsgVote);
    }

    assert(msg.term() >= term);
    if (msg.term() > term ||
            (msg.term() == term && 
             raft::MessageType::MsgHeartbeat == msg.type())) {
        // fall back to follower state;
        return follower::onStepMessage(
                raft_mem, msg, 
                std::move(hard_state), std::move(soft_state));
    }

    assert(msg.term() == term);
    bool mark_update_active_time = false;
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;
    switch (msg.type()) {

    case raft::MessageType::MsgVoteResp:
        {
            mark_update_active_time = true;
            int major_yes_cnt = 
                raft_mem.UpdateVote(msg.term(), msg.from(), !msg.reject());
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

                if (nullptr == hard_state) {
                    hard_state = cutils::make_unique<raft::HardState>();
                    assert(nullptr != hard_state);
                }

                // 
                hard_state->set_vote(raft_mem.GetSelfId());
            }

            // => UpdateVote => yes => reach major
            if (nullptr == soft_state) {
                soft_state = cutils::make_unique<raft::SoftState>();
                assert(nullptr != soft_state);
            }

            assert(nullptr != soft_state);
            soft_state->set_role(
                    static_cast<uint32_t>(raft::RaftRole::LEADER));
            // assert msg.to() == raft_mem.GetSelfID();
            soft_state->set_leader_id(raft_mem.GetSelfId());

            raft::Replicate* replicate = raft_mem.GetReplicate();
            assert(nullptr != replicate);
            replicate->Reset(raft_state.GetCommit());

            // broad-cast: i am the new leader now!!
            mark_broadcast = true;
            rsp_msg_type = raft::MessageType::MsgHeartbeat;
        }
        break;

    default:
        logerr("IGNORE: recv msg type %d", static_cast<int>(msg.type()));
        break;
    }

    if (nullptr != hard_state) {
        updateHardState(raft_mem, hard_state);
    }

    if (mark_update_active_time) {
        raft_mem.UpdateActiveTime();
    }

    return std::make_tuple(
            std::move(hard_state), std::move(soft_state), 
            mark_broadcast, rsp_msg_type);
}

std::unique_ptr<raft::Message>
onBuildRsp(
        raft::RaftMem& raft_mem, 
        const raft::Message& req_msg, 
        const std::unique_ptr<raft::HardState>& hard_state, 
        const std::unique_ptr<raft::SoftState>& soft_state, 
        bool mark_broadcast, 
        const raft::MessageType rsp_msg_type)
{
    raft::RaftState raft_state(raft_mem, hard_state, soft_state);
    assert(raft::RaftRole::CANDIDATE == raft_state.GetRole());

    std::unique_ptr<raft::Message>
        rsp_msg = cutils::make_unique<raft::Message>();
    assert(nullptr != rsp_msg);

    switch (rsp_msg_type) {

    case raft::MessageType::MsgVote:
        {
            rsp_msg->set_term(raft_state.GetTerm());
            rsp_msg->set_index(raft_state.GetMaxIndex() + 1);
            assert(0 < rsp_msg->index());
            rsp_msg->set_log_term(
                    getLogTerm(raft_state, rsp_msg->index() - 1));
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
        rsp_msg->set_from(req_msg.to());
        if (mark_broadcast) {
            rsp_msg->set_to(0);
        }
        else {
            rsp_msg->set_to(req_msg.from());
        }
        assert(rsp_msg->has_term());
    }

    return std::move(rsp_msg);
}

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
    
    raft::Replicate* replicate = raft_mem.GetReplicate();
    assert(nullptr != replicate);
    // TODO: for now
    uint64_t replicate_commit = calculateCommit(
            raft_mem.GetVoteFollowerSet(), replicate);
    if (raft_mem.GetCommit() < replicate_commit) {
        hard_state = cutils::make_unique<raft::HardState>();
        assert(nullptr != hard_state);
        hard_state->set_commit(replicate_commit);

        updateHardState(raft_mem, hard_state);
    }

    raft_mem.UpdateActiveTime();
    return std::make_tuple(
            std::move(hard_state), nullptr, 
            true, raft::MessageType::MsgHeartbeat);
}

// leader
std::tuple<
    std::unique_ptr<raft::HardState>, 
    std::unique_ptr<raft::SoftState>, 
    bool, 
    raft::MessageType>
onStepMessage(
        raft::RaftMem& raft_mem, 
        const raft::Message& msg, 
        std::unique_ptr<raft::HardState> hard_state, 
        std::unique_ptr<raft::SoftState> soft_state)
{
    raft::RaftState raft_state(raft_mem, hard_state, soft_state);
    assert(raft::RaftRole::LEADER == raft_state.GetRole());

    uint64_t term = raft_state.GetTerm();
    if (msg.term() < term) {
        return std::make_tuple(
                std::move(hard_state), std::move(soft_state), 
                false, raft::MessageType::MsgHeartbeat);
    }

    assert(msg.term() >= term);
    if (msg.term() > term) {
        // revert back to follower
        return follower::onStepMessage(
                raft_mem, msg, 
                std::move(hard_state), std::move(soft_state));
    }

    assert(msg.term() == term);
    bool mark_broadcast = false;
    auto rsp_msg_type = raft::MessageType::MsgNull;
    switch (msg.type()) {

    case raft::MessageType::MsgProp:
        {
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

                auto* new_entry = hard_state->add_entries();
                assert(nullptr != new_entry);
                *new_entry = msg_entry;
                new_entry->set_term(raft_mem.GetTerm());
                new_entry->set_index(max_index + idx + 1);
            }
            assert(hard_state->entries_size() >= msg.entries_size());
            mark_broadcast = true;
            rsp_msg_type = MessageType::MsgApp;
        }
        break;

    case raft::MessageType::MsgAppResp:
        {
            raft::Replicate* 
                replicate = raft_mem.GetReplicate();
            assert(nullptr != replicate);

            if (replicate->UpdateReplicateState(
                        msg.from(), msg.reject(), msg.index())) {
                mark_broadcast = false;
                rsp_msg_type = raft::MessageType::MsgAppResp;
            }

            if (msg.reject()) {
                // switch to MsgHeartbeat explore
                assert(false == mark_broadcast);
                rsp_msg_type = raft::MessageType::MsgHeartbeat;
            }

            uint64_t replicate_commit = calculateCommit(
                    raft_state.GetVoteFollowerSet(), replicate);
            if (raft_state.GetCommit() < replicate_commit) {
                // => update commit
                if (nullptr == hard_state) {
                    hard_state = cutils::make_unique<raft::HardState>();
                    assert(nullptr != hard_state);
                }

                assert(nullptr != hard_state);
                hard_state->set_commit(replicate_commit);
                if (raft::MessageType::MsgNull == rsp_msg_type) {
                    mark_broadcast = true;
                    rsp_msg_type = raft::MessageType::MsgHeartbeat;
                }
            }
        }
        break;

    case raft::MessageType::MsgHeartbeatResp:
        {
            raft::Replicate* 
                replicate = raft_mem.GetReplicate();
            assert(nullptr != replicate);

            if (replicate->UpdateReplicateState(
                        msg.from(), msg.reject(), msg.index())) {
                mark_broadcast = false;
                rsp_msg_type = raft::MessageType::MsgHeartbeat;
            }
            else {
                // msg heart-beat => update nothing
                uint64_t accepted_index = 
                    replicate->GetAcceptedIndex(msg.from());
                uint64_t rejected_index = 
                    replicate->GetRejectedIndex(msg.from());
                // switch to MsgApp Catch-Up
                if (accepted_index + 1 == rejected_index &&
                        accepted_index < raft_state.GetMaxIndex()) {
                    mark_broadcast = false;
                    rsp_msg_type = raft::MessageType::MsgApp;
                }
            }
        }
        break;

    default:
        logerr("IGNORE: recv msg type %d", static_cast<int>(msg.type()));
        break;
    }

    if (nullptr != hard_state) {
        updateHardState(raft_mem, hard_state);
    }

    return std::make_tuple(
            std::move(hard_state), std::move(soft_state), 
            mark_broadcast, rsp_msg_type);
}

// leader
std::unique_ptr<raft::Message>
onBuildRsp(
        raft::RaftMem& raft_mem, 
        const raft::Message& req_msg, 
        const std::unique_ptr<raft::HardState>& hard_state, 
        const std::unique_ptr<raft::SoftState>& soft_state, 
        bool mark_broadcast, 
        const raft::MessageType rsp_msg_type)
{
    raft::RaftState raft_state(raft_mem, hard_state, soft_state);
    assert(raft::RaftRole::LEADER == raft_state.GetRole());

    std::unique_ptr<raft::Message>
        rsp_msg = cutils::make_unique<raft::Message>();
    assert(nullptr != rsp_msg);

    switch (rsp_msg_type) {
    
    case raft::MessageType::MsgApp:
        {
            if (mark_broadcast) {
                assert(raft::MessageType::MsgProp == req_msg.type());
                assert(0 < req_msg.entries_size());
                {
                    const raft::Entry& msg_entry = 
                        req_msg.entries(req_msg.entries_size() - 1);
                    assert(0 < msg_entry.index());
                    assert(msg_entry.index() == raft_state.GetMaxIndex());
                }

                for (int idx = 0; idx < req_msg.entries_size(); ++idx) {
                    raft::Entry* rsp_entry = rsp_msg->add_entries();
                    assert(nullptr != rsp_entry);
                    *rsp_entry = req_msg.entries(idx);
                    assert(rsp_entry->term() == raft_state.GetTerm());
                }

                rsp_msg->set_index(req_msg.entries(0).index());
                assert(0 < rsp_msg->index());
                rsp_msg->set_log_term(
                        getLogTerm(raft_state, rsp_msg->index() - 1));
                rsp_msg->set_commit_index(raft_state.GetCommit());
                rsp_msg->set_commit_term(
                        getLogTerm(raft_state, rsp_msg->commit_index()));
                break;
            }

            assert(false == mark_broadcast);
            raft::Replicate* replicate = raft_mem.GetReplicate();
            assert(nullptr != replicate);
            assert(0 < req_msg.from());

            if (req_msg.reject()) {
                uint64_t next_explore_index = 
                    nextExploreIndex(raft_state, replicate, req_msg);
                assert(0 < next_explore_index);
                assert(next_explore_index != req_msg.index());
                rsp_msg->set_index(next_explore_index);
                const raft::Entry* mem_entry = 
                    getLogEntry(raft_state, rsp_msg->index() - 1);
                if (nullptr != mem_entry) {
                    assert(nullptr != mem_entry);
                    assert(mem_entry->index() + 1 == rsp_msg->index());
                    raft::Entry* rsp_entry = rsp_msg->add_entries();
                    assert(nullptr != rsp_entry);
                    *rsp_entry = *mem_entry;
                    assert(1 == rsp_msg->entries_size());
                }
            }
            else {
                // assert false == reject
                assert(req_msg.index() <= raft_state.GetMaxIndex() + 1);
                rsp_msg->set_index(req_msg.index());
                int max_size = std::min<int>(
                        raft_state.GetMaxIndex() + 1 - req_msg.index(), 
                        10); // TODO: for now
                assert(0 <= max_size);
                assert(req_msg.index() + max_size <= 
                        raft_state.GetMaxIndex() + 1);
                for (int idx = 0; idx < max_size; ++idx) {
                    const raft::Entry* mem_entry = 
                        getLogEntry(raft_state, rsp_msg->index() + idx);
                    assert(nullptr != mem_entry);
                    assert(mem_entry->index() == rsp_msg->index() + idx);
                    raft::Entry* rsp_entry = rsp_msg->add_entries();
                    *rsp_entry = *mem_entry;
                }
                assert(rsp_msg->entries_size() == max_size);
            }

            uint64_t rsp_max_index = rsp_msg->index() - 1;
            if (0 < rsp_msg->entries_size()) {
                rsp_max_index = rsp_msg->entries(
                        rsp_msg->entries_size() - 1).index();
                assert(0 < rsp_max_index);
            }
            assert(rsp_max_index >= rsp_msg->index() - 1);
            rsp_msg->set_commit_index(
                    std::min(rsp_max_index, raft_state.GetCommit()));
            rsp_msg->set_commit_term(rsp_msg->commit_index());
        }
        break;

    case raft::MessageType::MsgHeartbeat:
        {
            if (mark_broadcast) {
                // 
                rsp_msg->set_index(raft_state.GetMaxIndex() + 1);
                assert(0 < rsp_msg->index());
                rsp_msg->set_log_term(
                        getLogTerm(raft_state, rsp_msg->index() - 1));

                rsp_msg->set_commit_index(raft_state.GetCommit());
                rsp_msg->set_commit_term(
                        getLogTerm(raft_state, rsp_msg->commit_index()));
                break;
            }

            assert(false == mark_broadcast);
            assert(raft::MessageType::MsgHeartbeatResp == req_msg.type());
            raft::Replicate* replicate = raft_mem.GetReplicate();
            assert(nullptr != replicate);
            assert(0 < req_msg.from());

            uint64_t next_explore_index = 
                nextExploreIndex(raft_state, replicate, req_msg);
            assert(0 < next_explore_index);
            assert(req_msg.index() != next_explore_index);
            rsp_msg->set_index(next_explore_index);
            assert(0 < rsp_msg->index());
            rsp_msg->set_log_term(
                    getLogTerm(raft_state, rsp_msg->index()));
            if (rsp_msg->index() - 1 <= raft_state.GetCommit()) {
                rsp_msg->set_commit_index(rsp_msg->index() - 1);
                rsp_msg->set_commit_term(rsp_msg->log_term());
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
        rsp_msg->set_from(req_msg.to());
        if (mark_broadcast) {
            rsp_msg->set_to(0);
        }
        else {
            rsp_msg->set_to(req_msg.from());
        }

        rsp_msg->set_term(raft_state.GetTerm());
    }

    return std::move(rsp_msg);
}

} // namespace leader

} // namespace


namespace raft {

RaftMem::RaftMem(
        uint64_t logid, 
        uint32_t selfid, 
        uint32_t election_timeout_ms)
    : logid_(logid)
    , selfid_(selfid)
    , election_timeout_(election_timeout_ms)
    , active_time_(std::chrono::system_clock::now())
{
    map_timeout_handler_[raft::RaftRole::FOLLOWER] = follower::onTimeout;
    map_timeout_handler_[raft::RaftRole::CANDIDATE] = candidate::onTimeout;
    map_timeout_handler_[raft::RaftRole::LEADER] = leader::onTimeout;

    map_step_handler_[raft::RaftRole::FOLLOWER] = follower::onStepMessage;
    map_step_handler_[raft::RaftRole::CANDIDATE] = candidate::onStepMessage;
    map_step_handler_[raft::RaftRole::LEADER] = leader::onStepMessage;

    map_build_rsp_handler_[raft::RaftRole::FOLLOWER] = follower::onBuildRsp;
    map_build_rsp_handler_[raft::RaftRole::CANDIDATE] = candidate::onBuildRsp;
    map_build_rsp_handler_[raft::RaftRole::LEADER] = leader::onBuildRsp;

    replicate_ = cutils::make_unique<raft::Replicate>();
    vote_follower_set_ = {1, 2, 3};
    assert(vote_follower_set_.end() != vote_follower_set_.find(selfid_));
    vote_follower_set_.erase(selfid_);
}

RaftMem::~RaftMem() = default;

std::tuple<
    std::unique_ptr<raft::HardState>, 
    std::unique_ptr<raft::SoftState>, 
    bool, 
    raft::MessageType>
RaftMem::Step(
        const raft::Message& msg, 
        std::unique_ptr<raft::HardState> hard_state, 
        std::unique_ptr<raft::SoftState> soft_state)
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
    return map_step_handler_.at(role)(
            *this, msg, std::move(hard_state), std::move(soft_state));
    // assert(nullptr != step_handler_);
    // return step_handler_(*this, msg, std::move(hard_state), std::move(soft_state));
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

    logerr("INFO: term_ %" PRIu64 " vote_ %u new_term %" PRIu64, 
            term_, vote_, new_term);
    term_ = new_term;
    vote_ = 0;
    leader_id_ = 0;
}

void RaftMem::updateVote(uint64_t vote_term, uint32_t vote)
{
    assert(term_ == vote_term);
    assert(0 == vote_);
    assert(0 == leader_id_);
    logerr("INFO: term_ %" PRIu64 " vote %u", term_, vote);
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

void RaftMem::updateLogEntries(std::unique_ptr<HardState> hard_state)
{
    assert(nullptr != hard_state);
    if (hard_state->has_commit()) {
        logerr("INFO: term_ %" PRIu64 " commit_ %" PRIu64 
                " commit %" PRIu64, 
                term_, commit_, hard_state->commit());
        commit_ = hard_state->commit();
    }

    if (0 < hard_state->entries_size()) {
        // update log entry;
        appendLogEntries(std::move(hard_state));
        assert(nullptr == hard_state);
    }
}

void RaftMem::applyHardState(
        std::unique_ptr<raft::HardState> hard_state)
{
    if (nullptr == hard_state) {
        return ;
    }

    assert(nullptr != hard_state);
    if (hard_state->has_term()) {
        updateTerm(hard_state->term());
    }

    if (hard_state->has_vote()) {
        updateVote(hard_state->term(), hard_state->vote());
    }

    updateLogEntries(std::move(hard_state));
    assert(nullptr == hard_state);
}

void RaftMem::ApplyState(
        std::unique_ptr<raft::HardState> hard_state, 
        std::unique_ptr<raft::SoftState> soft_state)
{
    uint64_t next_term = 
        nullptr == hard_state ? term_ : hard_state->term();
    if (nullptr != hard_state) {
        if (hard_state->has_term()) {
            updateTerm(hard_state->term());
        }
    }

    if (nullptr != soft_state) {
        if (soft_state->has_role()) {
            setRole(next_term, soft_state->role());
        }
    }

    if (nullptr != hard_state) {
        applyHardState(std::move(hard_state));
        assert(nullptr == hard_state);
    }

    if (nullptr != soft_state) {
        if (soft_state->has_leader_id()) {
            updateLeaderId(next_term, soft_state->leader_id());
        }
    }

    return ;
}


std::unique_ptr<raft::Message> 
RaftMem::BuildRspMsg(
        const raft::Message& msg, 
        const std::unique_ptr<raft::HardState>& hard_state, 
        const std::unique_ptr<raft::SoftState>& soft_state, 
        bool mark_broadcast, 
        const raft::MessageType rsp_msg_type)
{
    if (raft::MessageType::MsgNull == rsp_msg_type) {
        return nullptr; // nothing
    }

    auto role = raft::RaftRole::FOLLOWER;
    {
        raft::RaftState raft_state(*this, hard_state, soft_state);
        role = raft_state.GetRole();
    }

    assert(map_build_rsp_handler_.end() != map_build_rsp_handler_.find(role));
    assert(nullptr != map_build_rsp_handler_.at(role));
    return map_build_rsp_handler_.at(role)(
            *this, msg, hard_state, soft_state, mark_broadcast, rsp_msg_type);
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
    auto now = std::chrono::system_clock::now();
    {
        auto at_str = cutils::format_time(active_time_);
        auto now_str = cutils::format_time(now);
        logerr("INFO: active_time_ %s now %s", at_str.c_str(), now_str.c_str());
    }
    active_time_ = now;
}

bool RaftMem::HasTimeout() const 
{
    auto now = std::chrono::system_clock::now();
    return active_time_ + election_timeout_ < now;
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
    return calculateMajorYesCount(vote_term, vote_map_);
}

bool RaftMem::IsLogEmpty() const
{
    return logs_.empty();
}

void RaftMem::ClearVoteMap()
{
    vote_map_.clear();
    printf ( "TEST: vote_map_.size %zu\n", vote_map_.size() );
}


bool RaftMem::IsMajority(int cnt) const
{
    // TODO
    // assume 3 node
    const int major_cnt = (
            vote_follower_set_.size() / 2) + 
        ((0 == vote_follower_set_.size() % 2) ? 0 : 1) + 1;
    printf ( "%zu %zu cnt %d other %d\n", 
            vote_follower_set_.size(), vote_follower_set_.size() / 2, 
            cnt, major_cnt );
    return cnt >= major_cnt;
}

const std::set<uint32_t>& RaftMem::GetVoteFollowerSet() const
{
    assert(false == vote_follower_set_.empty());
    assert(size_t{2} <= vote_follower_set_.size());
    assert(vote_follower_set_.end() == vote_follower_set_.find(selfid_));
    return vote_follower_set_;
}

} // namespace raft

