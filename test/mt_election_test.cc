#include "gtest/gtest.h"
#include "raft_mem.h"
#include "raft.pb.h"
#include "hassert.h"
#include "mem_utils.h"
#include "test_helper.h"


TEST(MTElectionTest, EmptyState)
{
    std::map<uint32_t, std::unique_ptr<raft::RaftMem>> mapRaft;
    for (auto id : {1, 2, 3}) {
        mapRaft[id] = build_raft_mem(id, 1, 0, raft::RaftRole::FOLLOWER);
        assert(nullptr != mapRaft[id]);
        assert(id == mapRaft[id]->GetSelfId());
    }

    uint64_t term = 1;
    for (int testime = 0; testime < 100; ++testime) {
        auto testid = rand() % 3 + 1;
        assert(mapRaft.end() != mapRaft.find(testid));
        bool already_leader = 
            raft::RaftRole::LEADER == mapRaft.at(testid)->GetRole();

        std::vector<std::unique_ptr<raft::Message>> vecMsg;
        vecMsg.push_back(nullptr);
        vecMsg[0] = trigger_timeout(mapRaft, testid);
        assert(nullptr != vecMsg[0]);

        loop_until(mapRaft, vecMsg);
        if (true == already_leader) {
            assert(raft::RaftRole::LEADER == mapRaft.at(testid)->GetRole());
            for (const auto& idpair : mapRaft) {
                printf ( "id %u term %u expected_term %u\n", 
                        idpair.first, 
                        static_cast<uint32_t>(idpair.second->GetTerm()), 
                        static_cast<uint32_t>(term) );
                assert(idpair.second->GetTerm() == term);
            }
        }
        else {
            bool has_leader = false;
            for (const auto& idpair : mapRaft) {
                assert(idpair.second->GetTerm() == term+1);
                if (raft::RaftRole::LEADER == idpair.second->GetRole()) {
                    assert(false == has_leader);
                    has_leader = true;
                    printf ( "\n" );
                }
            }
            assert(true == has_leader);
            ++term;
        }
    }
}

