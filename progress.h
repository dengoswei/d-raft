#pragma once


#include <stdint.h>

namespace raft {


enum class ProgressState : uint32_t {
	
	PROBE = 1, 
	REPLICATE = 2, 

};


class Progress {

public: 
	Progress(uint64_t logid, uint64_t new_next, bool can_vote)
		: logid_(logid)
		, next_(new_next)
		, can_vote_(can_vote)
	{
		if (1 < next_) {
			state_ = ProgressState::PROBE;
		}
	}

	void Reset(uint64_t new_next, bool can_vote);

	bool MaybeUpdate(uint64_t new_matched_index);

	bool MaybeDecreaseNext(uint64_t rejected_index, uint64_t rejected_hint);

	bool CanVote() const {
		return can_vote_;
	}

	void UpdateNext(uint64_t new_next);

	void BecomeProbe();

	void BecomeReplicate();

	void PostActive(bool active);

	uint64_t GetMatched() const {
		return matched_;
	}

	uint64_t GetNext() const {
		return next_;
	}

	ProgressState GetState() const {
		return state_;
	}

	void Pause();

	bool IsPause() const {
		return pause_;
	}

	void Tick();

	bool NeedResetByHBAppRsp();

	void ResetByHBAppRsp();

	size_t GetCurrMaxSize() const {
		return curr_max_size_;
	}

	// test helper function
	void SetNext(uint64_t new_next);

private:
	const uint64_t logid_;
	const uint32_t max_max_size_ = 30;
	ProgressState state_ = ProgressState::REPLICATE;
	uint64_t next_ = 1;
	uint64_t matched_ = 0;

	uint32_t no_progress_tick_ = 0;
	uint32_t curr_max_size_ = 30;

	bool can_vote_ = true;
	bool pause_ = false;
	bool active_ = true;
}; 

} // namespace raft


