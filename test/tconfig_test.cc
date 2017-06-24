#include <string>
#include <memory>
#include <cassert>
#include "gtest/gtest.h"
#include "mem_utils.h"
#include "raft.pb.h"
#include "tconfig_helper.h"


TEST(TConfigTest, tconfig)
{
    auto config = build_fake_config(3);
    std::string raw_data;
    assert(config.SerializeToString(&raw_data));
    assert(false == raw_data.empty());
}

TEST(TConfigTest, tadd_config)
{
    auto hs = cutils::make_unique<raft::HardState>();
    assert(nullptr != hs);

    auto config = build_fake_config(3);
    add_config(*hs, 1, config);
}

