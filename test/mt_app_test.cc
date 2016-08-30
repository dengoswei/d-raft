#include "gtest/gtest.h"
#include "raft_mem.h"
#include "raft.pb.h"
#include "hassert.h"
#include "mem_utils.h"
#include "test_helper.h"


TEST(MTAppTest, PerfectApp)
{
    std::map<uint32_t, std::unique_ptr<raft::RaftMem>> mapRaft;
    for (auto id : {1, 2, 3}) {
        mapRaft[id] = build_raft_mem(id, 1, 0, raft::RaftRole::FOLLOWER);
        assert(nullptr != mapRaft[id]);
        assert(id == mapRaft[id]->GetSelfId());
    }

    assert(true == make_leader(mapRaft, 1));


}



