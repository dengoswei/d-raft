#include "gtest/gtest.h"
#include "raft_mem.h"
#include "raft.pb.h"
#include "mem_utils.h"
#include "test_helper.h"
#include "replicate.h"


TEST(ReplicateTest, SimpleConstruct)
{
    raft::Replicate replicate;
    assert(0 == replicate.GetAcceptedIndex(1));
    assert(0 == replicate.GetRejectedIndex(1));

    std::set<uint32_t> follower_set = {2, 3};
    auto distr_map = replicate.GetAcceptedDistributionOn(follower_set);
    assert(distr_map.empty());
}

TEST(ReplicateTest, UpdateRejected)
{
    raft::Replicate replicate;
    assert(0 == replicate.GetRejectedIndex(1));

    assert(true == replicate.UpdateReplicateState(1, false, 100));
    assert(100 - 1 == replicate.GetRejectedIndex(1));

    assert(true == replicate.UpdateReplicateState(1, false, 50));
    assert(50 - 1 == replicate.GetRejectedIndex(1));

    assert(false == replicate.UpdateReplicateState(1, false, 50));
    assert(false == replicate.UpdateReplicateState(1, false, 60));
    assert(50 - 1 == replicate.GetRejectedIndex(1));

    assert(0 == replicate.GetAcceptedIndex(1));
}

TEST(ReplicateTest, UpdateAccepted)
{
    raft::Replicate replicate;

    assert(0 == replicate.GetAcceptedIndex(1));
    
    assert(true == replicate.UpdateReplicateState(1, true, 3));
    assert(3 - 1 == replicate.GetAcceptedIndex(1));

    assert(false == replicate.UpdateReplicateState(1, true, 3));
    assert(true == replicate.UpdateReplicateState(1, true, 50));
    assert(50 - 1 == replicate.GetAcceptedIndex(1));
    assert(0 == replicate.GetRejectedIndex(1));

    replicate.Reset(3);
    assert(3 == replicate.GetAcceptedIndex(1));
}


TEST(ReplicateTest, NextExploreIndex)
{
    {
        raft::Replicate replicate;
        
        assert(11 == replicate.NextExploreIndex(1, 1, 10));
        assert(21 == replicate.NextExploreIndex(1, 1, 20));

        assert(true == replicate.UpdateReplicateState(1, true, 3));
        assert(2 == replicate.GetAcceptedIndex(1));
        assert(0 == replicate.NextExploreIndex(1, 1, 20));

        assert(true == replicate.UpdateReplicateState(1, false, 21));
        assert(20 == replicate.GetRejectedIndex(1));
        assert(11 + 1 == replicate.NextExploreIndex(1, 1, 20));
        assert(15 + 1 == replicate.NextExploreIndex(1, 10, 20));

        assert(true == replicate.UpdateReplicateState(1, false, 4));
        assert(3 == replicate.GetRejectedIndex(1));
        assert(0 == replicate.NextExploreIndex(1, 1, 20));
    }
}

TEST(ReplicateTest, NextCatchUpIndex)
{
    {
        raft::Replicate replicate;
        
        assert(0 == replicate.GetAcceptedIndex(1));
        assert(0 == replicate.NextCatchUpIndex(1, 1, 20));
        assert(0 == replicate.NextCatchUpIndex(1, 10, 20));

        assert(true == replicate.UpdateReplicateState(1, true, 3));
        printf ( "next catchup %d\n", 
                static_cast<int>(replicate.NextCatchUpIndex(1, 1, 20)) );
        assert(3 == replicate.NextCatchUpIndex(1, 1, 20));
        assert(0 == replicate.NextCatchUpIndex(1, 10, 20));

        assert(true == replicate.UpdateReplicateState(1, false, 15));
        assert(14 == replicate.GetRejectedIndex(1));
        assert(0 == replicate.NextCatchUpIndex(1, 1, 20));
        assert(0 == replicate.NextCatchUpIndex(1, 10, 20));

        assert(true == replicate.UpdateReplicateState(1, true, 14));
        assert(13 == replicate.GetAcceptedIndex(1));
        assert(0 == replicate.NextExploreIndex(1, 1, 20));
        assert(0 == replicate.NextExploreIndex(1, 10, 20));
        assert(14 == replicate.NextCatchUpIndex(1, 1, 20));
        assert(14 == replicate.NextCatchUpIndex(1, 10, 20));
    }
}
