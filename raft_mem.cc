#include "raft_mem.h"


namespace {

using namespace raft;


class RaftState {

public:
    RaftState(
            raft::RaftMem& raft_mem, 
            std::unique_ptr<raft::HardState>& hard_state, 
            std::unique_ptr<raft::SoftState>& soft_state)
        : raft_mem_(raft_mem)
        , hard_state_(hard_state)
        , soft_state_(soft_state)
    {
        commit_index_ = raft_mem_.GetCommit();
        if (nullptr != hard_state_ && hard_state_->has_commit()) {
            assert(commit_index_ <= hard_state_->commit());
            commit_index_ = hard_state->commit();
        }
    }

    raft::RaftRole GetRole() const {
        return nullptr == soft_state ? 
            raft_mem_.GetRole() : soft_state->role();
    }

    uint64_t GetTerm() const {
        if (nullptr != hard_state_) {
            assert(hard_state_->term() >= raft_mem_.GetTerm());
            return hard_state_->term();
        }
        assert(nullptr == hard_state_);
        return raft_mem_.GetTerm();
    }

    uint32_t GetVote(uint64_t msg_term) const {
        if (nullptr != hard_state_) {
            if (hard_state_->term() == msg_term && 
                    hard_state_->has_vote()) {
                assert(0 == raft_mem_->GetVote(msg_term));
                return hard_state_->vote();
            }

            // else => 
            return 0;
        }

        assert(nullptr == hard_state_);
        return raft_mem_->GetVote(msg_term);
    }

    uint32_t GetLeaderId(uint64_t msg_term) const {
        if (nullptr != hard_state_) {
            if (hard_state_->term() != msg_term) {
                return 0;
            }

            assert(hard_state_->term() == msg_term);
            return nullptr == soft_state ? 0 : soft_state->leader_id();
        }

        assert(nullptr == hard_state_);
        return raft_mem_.GetLeaderId(msg_term);
    }

    const raft::Entry* GetLastEntry() const {
        if (nullptr != hard_state_) {
            if (0 < hard_state_->entries_size()) { 
                return &(hard_state_->entries(
                            hard_state_->entries_size() - 1));
            }

            assert(0 == hard_state_->entries_size());
        }

        return raft_mem_.GetLastEntry();
    }

    uint64_t GetMinIndex() const {
        uint64_t min_index = raft_mem_.GetMinIndex();
        if (0 != min_index) {
            return min_index;
        }

        assert(0 == min_index);
        if (nullptr != hard_state_ && 0 < hard_state_->entries_size()) {
            assert(0 < hard_state_->entries(0).index());
            return hard_state_->entries(0).index();
        }

        return 0;
    }

    uint64_t GetMaxIndex() const {
        if (nullptr != hard_state_ && 0 < hard_state_->entries_size()) {
            assert(0 < hard_state_->entries(
                        hard_state_->entries_size() - 1).index());
            return hard_state_->entries(
                    hard_state_->entries_size() - 1).index();
        }

        return raft_mem_.GetMaxIndex();
    }

    uint64_t GetCommit() const {
        if (nullptr != hard_state_ && hard_state_->has_commit()) {
            assert(hard_state_->commit() >= raft_mem_.GetCommit());
            return hard_state_->commit();
        }
        
        return raft_mem_.GetCommit();
    }

    const raft::Entry* At(int mem_idx) const
    {
        if (nullptr == hard_state_ || 0 == hard_state_->entries_size()) {
            return raft_mem_.At(mem_idx);
        }

        assert(nullptr != hard_state_);
        assert(0 < hard_state_->entries_size());
        uint64_t hs_min_index = hard_state_->entries(0).index();
        assert(0 < hs_min_index);

        uint64_t rmem_min_index = raft_mem_.GetMinIndex();
        if (1 == hs_min_index || rmem_min_index == hs_min_index) {
            if (mem_idx >= hard_state_->entries_size()) {
                return nullptr;
            }

            return &(hard_state_->entries(mem_idx));
        }

        assert(1 < hs_min_index);
        uint64_t rmem_min_index = raft_mem_.GetMinIndex();
        assert(0 < rmem_min_index);
        assert(rmem_min_index < hs_min_index);
        if (mem_idx < (hs_min_index - rmem_min_index)) {
            return raft_mem_.At(mem_idx);
        }

        // else
        int msg_idx = mem_idx - (hs_min_index - rmem_min_index);
        assert(0 <= msg_idx);
        assert(msg_idx < mem_idx);
        assert(msg_idx <= hard_state_->entries_size());
        if (msg_idx == hard_state_->entries_size()) {
            return nullptr;
        }

        return &(hard_state_->entries(msg_idx));
    }

    uint64_t GetCommit() const {
        if (nullptr != hard_state_ && hard_state_->has_commit()) {
            assert(hard_state_->commit() >= raft_mem_.GetCommit());
            return hard_state_->commit();
        }

        return raft_mem_.GetCommit();
    }

    bool CanUpdateCommit(
            uint64_t msg_commit_index, uint64_t msg_commit_term) const
    {
        if (msg_commit_index <= commit_index_) {
            return false; // update nothing
        }

        assert(msg_commit_index > commit_index_);
        uint64_t max_index = GetMaxIndex();
        assert(max_index >= commit_index_);
        if (msg_commit_index > max_index) {
            // don't have enough info to update commited index
            return false; 
        }

        assert(0 < max_index);
        assert(msg_commit_index <= max_index);
        int mem_idx = msg_commit_index - GetMinIndex();
        assert(0 <= mem_idx);
        const raft::Entry* mem_entry = At(mem_idx);
        assert(nullptr != mem_entry);
        assert(mem_entry->index() == msg_commit_index);
        if (msg_entry->term() != msg_commit_term) {
            return false;
        }

        return true;
    }

    bool UpdateVote(uint32_t candidate_id, bool vote_yes)
    {
        return raft_mem_.UpdateVote(candidate_id, vote_yes);
    }

    bool CanWrite(int entries_size)
    {
        assert(0 <= entries_size);
        // TODO
        return true;
    }

private:
    raft::RaftMem& raft_mem_;
    std::unique_ptr<raft::HardState>& hard_state_;
    std::unique_ptr<raft::SoftState>& soft_state_;

    uint64_t commit_index_ = 0;
}; // class RaftState

void updateHardState(
        raft::RaftMem& raft_mem, 
        std::unique_ptr<raft::HardState>& hard_state)
{
    if (nullptr == hard_state) {
        return ;
    }

    assert(nullptr != hard_state);
    if (false == hard_state.has_term()) {
        hard_state->set_term(raft_mem.GetTerm());
    }

    if (false == hard_state.has_vote()) {
        hard_state->set_vote(raft_mem.GetVote(hard_state->term()));
    }

    if (false == hard_state.has_commit()) {
        hard_state->set_commit(raft_mem.GetCommit());
    }
}


namespace follower {

bool canVote(
        RaftState& raft_state, 
        uint64_t msg_term, 
        uint64_t vote_term, 
        uint63_t vote_index)
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
    if (raft_state.IsLogEmpty()) {
        return true;
    }

    // => else
    const raft::Entry* last_entry = raft_state.GetLastEntry();
    assert(nullptr != last_entry);
    assert(0 < last_entry->index());
    if (last_entry->term() != vote_term) {
        return vote_term > last_entry->term();
    }

    assert(last_entry->term() == vote_term);
    return vote_index >= last_entry->index();
}

int resolveEntries(
        RaftState& raft_state, 
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

std::tuple<
    std::unique_ptr<raft::HardState>, 
    std::unique_ptr<raft::SoftState>, 
    bool, 
    raft::MessageType>
onTimeout(raft::RaftMem& raft_mem)
{
    // follower => timeout => become candidate;
    assert(raft::RaftRole::FOLLOWER == raft_mem.GetRole());
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

    raft_mem.UpdateActiveTime();
    return std::make_tuple(
            std::move(hard_state), std::move(soft_state), 
            true, raft::MessageType::MsgVote);
}

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
    RaftState raft_state(raft_mem, hard_state, soft_state);
    assert(raft::RaftRole::FOLLOWER == raft_state.GetRole());

    auto term = raft_state.GetTerm();
    assert(msg.term() >= term);
    if (msg.term() > term) {
        if (nullptr == soft_state) {
            soft_state = cutils::make_unique<raft::SoftState>();
            assert(nullptr != soft_state);
        }

        assert(nullptr != soft_state);
        soft_state->set_role(
                static_cast<uint32_t>(raft::RaftRole::FOLLOWER));

        if (nullptr == hard_state) {
            hard_state = cutils::make_unique<raft::HardState>();
            asesrt(nullptr != hard_state);
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
            if (false == canVote(
                        raft_state, 
                        msg.term(), msg.log_term(), msg.index())) {
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
                    soft_state->set_role(
                            static_cast<uint32_t>(
                                raft::RaftRole::FOLLOWER));
                }
                assert(nullptr != soft_state);
                assert(soft_state->role() == 
                        static_cast<uint32_t>(raft::RaftRole::FOLLOWER));
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

                    auto* new_entry = hard_state.add_entries();
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

} // namespace follower


namespace candidate {

std::tuple<
    std::unique_ptr<raft::HardState>, 
    std::unique_ptr<raft::SoftState>, 
    bool, 
    raft::MessageType>
onTimeout(raft::RaftMem& raft_mem)
{
    assert(raft::RaftRole::CANDIDATE == raft_mem.GetRole());
    std::unique_ptr<raft::HardState>
        hard_state = cutils::make_unique<raft::HardState>();
    assert(nullptr != hard_state);

    hard_state->set_term(raft_mem.GetTerm() + 1);
    hard_state->set_vote(0);
    updateHardState(raft_mem, hard_state);

    raft_mem.UpdateActiveTime();
    return std::make_tuple(
            std::move(hard_state), nullptr, true, raft::MessageType::MsgVote);
}

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
    RaftState raft_state(raft_mem, hard_state, soft_state);
    assert(raft::RaftRole::CANDIDATE == raft_state.GetRole());

    uint64_t term = raft_state.GetTerm();
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
            if (false == raft_state.UpdateVote(
                        msg.from(), !msg.reject())) {
                break;
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
            soft_state->set_leader_id(msg.to());

            // broad-cast: i am the new leader now!!
            mark_broadcast = true;
            rsp_msg_type = raft::MessageType::MsgHeartbeat;
        }
        break;

    default:
        logerr("IGNORE: recv msg type %d", static_cast<int>(msg.type()));
        break;
    }

    if (mark_update_active_time) {
        raft_mem.UpdateActiveTime();
    }

    return std::make_tuple(
            nullptr, std::move(soft_state), mark_broadcast, rsp_msg_type);
}

} // namespace candidate


namespace leader {

std::tuple<
    std::unique_ptr<raft::HardState>, 
    std::unique_ptr<raft::SoftState>, 
    bool, 
    raft::MessageType>
onTimeout(raft::RaftMem& raft_mem)
{
    assert(raft::RaftRole::LEADER == raft_mem.GetRole());
    std::unique_ptr<raft::HardState>
        hard_state = cutils::make_unique<raft::HardState>();
    assert(nullptr != hard_state);

    hard_state->set_commit(raft_mem.GetCommit());
    updateHardState(raft_mem, hard_state);

    raft_mem.UpdateActiveTime();
    return std::make_tuple(
            std::move(hard_state), nullptr, 
            true, raft::MessageType::MsgHeartbeat);
}


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
    RaftState raft_state(raft_mem, hard_state, soft_state);
    assert(raft::RaftRole::LEADER == raft_state.GetRole());

    uint64_t term = raft_state.GetTerm();
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
            raft::ReplicateTracker* 
                replicate = raft_mem.GetReplicateTracker();
            assert(nullptr != replicate);

            if (replicate->UpdateReplicateState(
                        msg.from(), msg.reject(), msg.index())) {
                mark_broadcast = false;
                rsp_msg_type = raft::MessageType::MsgHeartbeat;
            }

            if (raft_state.GetCommit() != replicate->GetCommit()) {
                // => update commit
                if (nullptr == hard_state) {
                    hard_state = cutils::make_unique<raft::HardState>();
                    assert(nullptr != hard_state);
                }

                assert(nullptr != hard_state);
                hard_state->set_commit(replicate->GetCommit());
                if (raft::MessageType::MsgNull == rsp_msg_type) {
                    mark_broadcast = true;
                    rsp_msg_type = raft::MessageType::MsgHeartbeat;
                }
            }
        }
        break;

    case raft::MessageType::MsgHeartbeatResp:
        {
            raft::ReplicateTracker* 
                replicate = raft_mem.GetReplicateTracker();
            assert(nullptr != replicate);

            if (replicate->UpdateReplicateState(
                        msg.from(), msg.reject(), msg.index())) {
                mark_broadcast = false;
                rsp_msg_type = raft::MessageType::MsgHeartbeat;
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

} // namespace leader

} // namespace


namespace raft {


std::tuple<
    int, 
    std::unique_ptr<raft::HardState>, 
    std::unique_ptr<raft::SoftState>, 
    std::unique_ptr<std::map<uint32_t, uint64_t>>, 
    std::unique_ptr<raft::Message>>
RaftMem::stepInvalidTerm(const raft::Message& msg)
{
    assert(msg.term() != term_); 

    if (msg.term() > term_) {
        // todo => become follower;
    }

    assert(msg.term() < term_);
    // todo: rsp with valid term;
}

std::tuple<
    std::unique_ptr<raft::HardState>, 
    std::unique_ptr<raft::SoftState>, 
    bool, 
    raft::MessageType>
RaftMem::stepInvalidTerm(const raft::Message& msg)
{
    assert(msg.term() != term_);
    logerr("INFO: term_ %" PRIu64 " msg.term %" PRIu64, 
            term_, msg.term());
    if (msg.term() < term_) {
        return std::make_tuple(
                nullptr, nullptr, false, 
                raft::MessageType::MsgInvalidTerm);
    }

    assert(msg.term() > term_);
    return follower::onStepMessage(*this, msg);
}

std::tuple<
    std::unique_ptr<raft::HardState>, 
    std::unique_ptr<raft::SoftState>, 
    bool, 
    raft::MessageType>
RaftMem::Step(const raft::Message& msg)
{
    assert(msg.logid() == logid_);
    assert(msg.to() == selfid_);

    // check term
    if (msg.term() != term_) {
        return stepInvalidTerm();
    }

    assert(msg.term() == term_);
    return step_handler_(*this, msg);
}


void RaftMem::setRole(uint64_t next_term, uint32_t role)
{
    logerr("INFO: term_ %" PRIu64 " next_term %" PRIu64 
            " role_ %u role %u", 
            term_, next_term, 
            static_cast<uint32_t>(role_), role);
    role_ = role;
    // TODO: update handler ...
}

void RaftMem::updateLeaderId(uint64_t next_term, uint32_t leader_id)
{
    logerr("INFO: term_ %" PRIu64 " next_term %" PRIu64 
            " leader_id_ %u leader_id %u", 
            term_, next_term, 
            leader_id_, leader_id);
    leader_id_ = leader_id;
}

void RaftMem::updateTerm(uint64_t new_term)
{
    assert(term_ < new_term);
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
            logs_.begin(), logs_.end(), 
            [](std::deque<std::unique_ptr<Entry>>::iterator iter, 
                uint64_t index) {
                const std::unique_ptr<Entry>& entry = *iter;
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

    uint64_t index = hard_state->entries(0)->index();
    assert(0 < index);

    // assert check
    for (int idx = 0; idx < hard_state->entries_size(); ++idx) {
        const auto& entry = hard_state->entries(idx);
        assert(term_ >= entry->term());
        assert(index + idx == entry->index());
    }

    auto inmem_iter = findLogEntry(index);
    // shrink
    if (logs_.end() != inmem_iter) {
        // assert: must be conflict;
        const auto& entry = hard_state->entries(0);
        assert(nullptr != *inmem_iter);
        const auto& conflict_entry = *(*(inmem_iter));
        assert(index == entry->index());
        assert(index == conflict_entry->index());
        assert(entry->term() != conflict_entry.term());
        logerr("INFO: logs_.erase %d", 
                static_cast<int>(std::distance(inmem_iter, logs_.end())));
        logs_.erase(inmem_iter, logs_.end());
    }

    // append
    // TODO: config ?
    for (int idx = 0; idx < hard_state->entries_size(); ++idx) {
        auto entry = mutable_entries(idx);
        asesrt(nullptr != entry);

        std::unique_ptr<raft::Entry> 
            new_entry = cutils::make_unique<raft::Entry>();
        assert(nullptr != new_entry);
        new_entry->Swap(entry);
        assert(0 < new_entry->term());
        assert(0 < new_entry->index());
        logs_.append(std::move(new_entry));
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
        std::unique_ptr<HardState> hard_state)
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
        std::unique_ptr<raft::SoftState>& soft_state, 
        std::unique_ptr<raft::HardState>& hard_state)
{
    uint64_t next_term = 
        nullptr == hard_state ? term_ : hard_state->term();
    if (nullptr != soft_state) {
        if (soft_state->has_role()) {
            setRole(next_term, soft_state->role());
        }
        assert(nullptr == soft_state);
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
        const raft::MessageType rsp_msg_type)
{
    if (raft::MessageType::MsgNull == rsp_msg_type) {
        return nullptr; // nothing
    }

    assert(raft::MessageType::MsgNull != rsp_msg_type);
    // TODO
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

} // namespace raft


