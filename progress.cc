#include <algorithm>
#include <cassert>
#include "progress.h"
#include "log_utils.h"
#include "hassert.h"


namespace raft {

bool Progress::MaybeDecreaseNext(
		uint64_t rejected_index, uint64_t rejected_hint)
{
	if (ProgressState::REPLICATE == state_) {
		if (rejected_index <= matched_) {
			return false;
		}

		assert(rejected_index > matched_);
		next_ = matched_ + 1;
		curr_max_size_ = std::max<uint32_t>(1, curr_max_size_ >> 1);
		logerr("TEST: logid %lu next_ %lu matched_ %lu curr_max_size_ %u", 
				logid_, next_, matched_, curr_max_size_);
		return true;
	}

	// no need expential proble.. 
	assert(ProgressState::PROBE == state_);
	// what if rejected_index from follower missing ??
//	printf ( "rejected_index %lu rejected_hint %lu next_ %lu matched_ %lu\n", 
//			rejected_index, rejected_hint, next_, matched_ );
	if (rejected_index != next_ - 1) {
		return false; // stale reject msg;
	}

	assert(rejected_index == next_ - 1);
	// may lead to slow probe !!!
	// : rejected_index, rejected_index-1, rejected_index-2...
	next_ = std::min(rejected_index, rejected_hint + 1);
	next_ = std::max(uint64_t{1}, next_);
	assert(0 < next_);
	next_ = std::max(next_, matched_+1); // never below matched !!
	hassert(matched_ + 1 <= next_, "logid %lu matched_ %lu next_ %lu rejected_index %lu rejected_hint %lu", 
			logid_, matched_, next_, rejected_index, rejected_hint);
	pause_ = false; // resume
	logerr("TEST: logid %lu next_ %lu matched_ %lu", 
			logid_, next_, matched_);
	return true;
}

void Progress::Tick()
{
	++no_progress_tick_;
}

bool Progress::NeedResetByHBAppRsp()
{
	if (matched_ + 1 == next_) {
		no_progress_tick_ = 0;
		return false;
	}

	hassert(matched_ + 1 < next_, "logid %lu matched_ %lu next_ %lu no_progress_tick_ %u", 
			logid_, matched_, next_, no_progress_tick_);
	return no_progress_tick_ >= 5;
}

void Progress::ResetByHBAppRsp()
{
	assert(matched_ + 1 < next_);
	BecomeProbe();	
	pause_ = false;
	no_progress_tick_ = 0;
}


bool Progress::MaybeUpdate(uint64_t new_matched_index)
{
	bool update = false;
	if (new_matched_index > matched_) {
		matched_ = new_matched_index;
		update = true;
		pause_ = false;
		no_progress_tick_ = 0;
		curr_max_size_ = 
			std::min(max_max_size_, curr_max_size_ << 1);
	}

	if (new_matched_index + 1 > next_) {
		next_ = new_matched_index + 1;
		logerr("TEST: logid %lu next_ %lu matched_ %lu", logid_, next_, matched_);
	}

	return update;
}

void Progress::UpdateNext(uint64_t new_next)
{
	assert(0 < new_next);
	assert(next_ < new_next);

	logerr("TEST: logid %lu next_ %lu matched_ %lu new_next %lu", logid_, next_, matched_, new_next);
	next_ = new_next;
}

void Progress::BecomeReplicate()
{
	if (ProgressState::PROBE == state_) {
		state_ = ProgressState::REPLICATE;
		next_ = matched_ + 1;
		logerr("TEST: logid %lu next_ %lu matched_ %lu", logid_, next_, matched_);
		pause_ = false;
	}
	assert(false == pause_);
	assert(ProgressState::REPLICATE == state_);
}

void Progress::BecomeProbe()
{
	if (ProgressState::REPLICATE == state_) {
		state_ = ProgressState::PROBE;
		next_ = matched_ + 1;
		logerr("TEST: logid %lu next_ %lu matched_ %lu", logid_, next_, matched_);
		pause_ = false;
	}

	assert(ProgressState::PROBE == state_);
}

void Progress::PostActive(bool active)
{
	active_ = active;
}

void Progress::Reset(uint64_t new_next, bool can_vote)
{
	assert(0 < new_next);
	state_ = ProgressState::REPLICATE;
	next_ = new_next;
	matched_ = 0;
	if (1 < next_) {
		state_ = ProgressState::PROBE;
	}

	can_vote_ = can_vote;
	pause_ = false;
	active_ = true;
	logerr("TEST: logid %lu next_ %lu matched_ %lu", logid_, next_, matched_);
}

void Progress::Pause()
{
	pause_ = true;
}

// test helper function
void Progress::SetNext(uint64_t new_next)
{
	next_ = new_next;
	logerr("TEST: logid %lu next_ %lu matched_ %lu", logid_, next_, matched_);
}

} // namespace raft;


