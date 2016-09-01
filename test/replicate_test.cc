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

    replicate.Fix(3);
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

    {
        raft::Replicate replicate;
        assert(0 == replicate.NextExploreIndex(1, 2, 2));
        assert(2 == replicate.NextCatchUpIndex(1, 2, 2));
    }
}

TEST(ReplicateTest, ResetCommit)
{
    raft::Replicate replicate;

    assert(true == replicate.UpdateReplicateState(2, true, 10));
    assert(true == replicate.UpdateReplicateState(3, true, 3));
    assert(true == replicate.UpdateReplicateState(4, false, 3));
    assert(9 == replicate.GetAcceptedIndex(2));
    assert(2 == replicate.GetAcceptedIndex(3));
    assert(2 == replicate.GetRejectedIndex(4));

    replicate.Fix(5);
    assert(5 == replicate.GetAcceptedIndex(2));
    assert(2 == replicate.GetAcceptedIndex(3));
    assert(2 == replicate.GetRejectedIndex(4));
}


TEST(ReplicateTest, GetAcceptedDistributionOn)
{
    raft::Replicate replicate;

    assert(true == replicate.UpdateReplicateState(1, true, 10));
    assert(true == replicate.UpdateReplicateState(2, false, 2));
    assert(true == replicate.UpdateReplicateState(3, true, 2));
    assert(true == replicate.UpdateReplicateState(4, true, 2));

    // case 1
    {
        std::set<uint32_t> follower_set = {1, 2};
        auto ans = replicate.GetAcceptedDistributionOn(follower_set);
        assert(false == ans.empty());
        assert(size_t{1} == ans.size());
        assert(ans.end() != ans.find(9));
        assert(1 == ans.at(9));
    }

    // case 2
    {
        std::set<uint32_t> follower_set = {1, 2, 3};
        auto ans = replicate.GetAcceptedDistributionOn(follower_set);
        assert(false == ans.empty());
        assert(size_t{2} == ans.size());
        assert(ans.end() != ans.find(9));
        assert(ans.end() != ans.find(1));
        assert(1 == ans.at(9));
        assert(1 == ans.at(1));
    }

    // case 3
    {
        std::set<uint32_t> follower_set = {1, 2, 3, 4};
        auto ans = replicate.GetAcceptedDistributionOn(follower_set);
        assert(false == ans.empty());
        assert(size_t{2} == ans.size());
        assert(ans.end() != ans.find(9));
        assert(ans.end() != ans.find(1));
        assert(1 == ans.at(9));
        assert(2 == ans.at(1));
    }

    // case 4
    {
        std::set<uint32_t> follower_set = {2, 5};
        auto ans = replicate.GetAcceptedDistributionOn(follower_set);
        assert(true == ans.empty());
    }
}

