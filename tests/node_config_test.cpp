#include "common/node_config.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

// ── Helpers ───────────────────────────────────────────────────────────────────

// Build a fake argv array from a vector of strings.
// The returned pointers are valid as long as `args` is alive.
static std::vector<char*> make_argv(std::vector<std::string>& args) {
    std::vector<char*> argv;
    argv.reserve(args.size());
    for (auto& s : args) {
        argv.push_back(s.data());
    }
    return argv;
}

// ── Fixture ───────────────────────────────────────────────────────────────────

class NodeConfigTest : public ::testing::Test {
protected:
    // Minimal valid args for a 3-node cluster (node 1).
    std::vector<std::string> valid_args_{
        "kv-server",
        "--id", "1",
        "--client-port", "6379",
        "--raft-port",   "7001",
        "--peers",       "2:127.0.0.1:7002,3:127.0.0.1:7003",
        "--data-dir",    "./data/node1",
    };
};

// ── Valid configuration ────────────────────────────────────────────────────────

TEST_F(NodeConfigTest, ParsesMinimalValidConfig) {
    auto argv = make_argv(valid_args_);
    auto cfg  = kv::parse_config(static_cast<int>(argv.size()), argv.data());

    EXPECT_EQ(cfg.id,                1u);
    EXPECT_EQ(cfg.host,              "0.0.0.0");  // default
    EXPECT_EQ(cfg.client_port,       6379u);
    EXPECT_EQ(cfg.raft_port,         7001u);
    EXPECT_EQ(cfg.data_dir,          "./data/node1");
    EXPECT_EQ(cfg.snapshot_interval, 1000u);      // default
    EXPECT_EQ(cfg.log_level,         "info");     // default
    ASSERT_EQ(cfg.peers.size(),      2u);
}

TEST_F(NodeConfigTest, ParsesPeersCorrectly) {
    auto argv = make_argv(valid_args_);
    auto cfg  = kv::parse_config(static_cast<int>(argv.size()), argv.data());

    ASSERT_EQ(cfg.peers.size(), 2u);

    EXPECT_EQ(cfg.peers[0].id,        2u);
    EXPECT_EQ(cfg.peers[0].host,      "127.0.0.1");
    EXPECT_EQ(cfg.peers[0].raft_port, 7002u);

    EXPECT_EQ(cfg.peers[1].id,        3u);
    EXPECT_EQ(cfg.peers[1].host,      "127.0.0.1");
    EXPECT_EQ(cfg.peers[1].raft_port, 7003u);
}

TEST_F(NodeConfigTest, ParsesCustomHost) {
    valid_args_.insert(valid_args_.end(), {"--host", "192.168.1.10"});
    auto argv = make_argv(valid_args_);
    auto cfg  = kv::parse_config(static_cast<int>(argv.size()), argv.data());
    EXPECT_EQ(cfg.host, "192.168.1.10");
}

TEST_F(NodeConfigTest, ParsesCustomSnapshotInterval) {
    valid_args_.insert(valid_args_.end(), {"--snapshot-interval", "500"});
    auto argv = make_argv(valid_args_);
    auto cfg  = kv::parse_config(static_cast<int>(argv.size()), argv.data());
    EXPECT_EQ(cfg.snapshot_interval, 500u);
}

TEST_F(NodeConfigTest, ParsesCustomLogLevel) {
    valid_args_.insert(valid_args_.end(), {"--log-level", "debug"});
    auto argv = make_argv(valid_args_);
    auto cfg  = kv::parse_config(static_cast<int>(argv.size()), argv.data());
    EXPECT_EQ(cfg.log_level, "debug");
}

TEST_F(NodeConfigTest, ParsesThreePeers) {
    // 4-node cluster: this node is 1, peers are 2, 3, 4.
    valid_args_.back() = "2:127.0.0.1:7002,3:127.0.0.1:7003,4:127.0.0.1:7004";
    // (replace the peers value that was last arg; add --peers key is already there)
    // Rebuild properly:
    std::vector<std::string> args{
        "kv-server",
        "--id",    "1",
        "--peers", "2:127.0.0.1:7002,3:127.0.0.1:7003,4:127.0.0.1:7004",
        "--data-dir", "./data",
    };
    auto argv = make_argv(args);
    auto cfg  = kv::parse_config(static_cast<int>(argv.size()), argv.data());
    EXPECT_EQ(cfg.peers.size(), 3u);
}

// ── Validation errors ─────────────────────────────────────────────────────────

TEST_F(NodeConfigTest, RejectsIdZero) {
    std::vector<std::string> args{
        "kv-server",
        "--id", "0",
        "--peers", "2:127.0.0.1:7002,3:127.0.0.1:7003",
        "--data-dir", "./data",
    };
    auto argv = make_argv(args);
    EXPECT_THROW(kv::parse_config(static_cast<int>(argv.size()), argv.data()),
                 std::runtime_error);
}

TEST_F(NodeConfigTest, RejectsMissingId) {
    std::vector<std::string> args{
        "kv-server",
        "--peers", "2:127.0.0.1:7002,3:127.0.0.1:7003",
        "--data-dir", "./data",
    };
    auto argv = make_argv(args);
    EXPECT_THROW(kv::parse_config(static_cast<int>(argv.size()), argv.data()),
                 std::runtime_error);
}

TEST_F(NodeConfigTest, RejectsMissingPeers) {
    std::vector<std::string> args{
        "kv-server",
        "--id", "1",
        "--data-dir", "./data",
    };
    auto argv = make_argv(args);
    EXPECT_THROW(kv::parse_config(static_cast<int>(argv.size()), argv.data()),
                 std::runtime_error);
}

TEST_F(NodeConfigTest, RejectsFewerThanTwoPeers) {
    std::vector<std::string> args{
        "kv-server",
        "--id",    "1",
        "--peers", "2:127.0.0.1:7002",   // only 1 peer
        "--data-dir", "./data",
    };
    auto argv = make_argv(args);
    EXPECT_THROW(kv::parse_config(static_cast<int>(argv.size()), argv.data()),
                 std::runtime_error);
}

TEST_F(NodeConfigTest, RejectsPeerWithSameIdAsNode) {
    std::vector<std::string> args{
        "kv-server",
        "--id",    "1",
        "--peers", "1:127.0.0.1:7001,2:127.0.0.1:7002",  // peer id == node id
        "--data-dir", "./data",
    };
    auto argv = make_argv(args);
    EXPECT_THROW(kv::parse_config(static_cast<int>(argv.size()), argv.data()),
                 std::runtime_error);
}

TEST_F(NodeConfigTest, RejectsDuplicatePeerIds) {
    std::vector<std::string> args{
        "kv-server",
        "--id",    "1",
        "--peers", "2:127.0.0.1:7002,2:127.0.0.1:7003",  // id 2 duplicated
        "--data-dir", "./data",
    };
    auto argv = make_argv(args);
    EXPECT_THROW(kv::parse_config(static_cast<int>(argv.size()), argv.data()),
                 std::runtime_error);
}

TEST_F(NodeConfigTest, RejectsPortZeroForClientPort) {
    std::vector<std::string> args{
        "kv-server",
        "--id",          "1",
        "--client-port", "0",
        "--peers",       "2:127.0.0.1:7002,3:127.0.0.1:7003",
        "--data-dir",    "./data",
    };
    auto argv = make_argv(args);
    EXPECT_THROW(kv::parse_config(static_cast<int>(argv.size()), argv.data()),
                 std::runtime_error);
}

TEST_F(NodeConfigTest, RejectsPortZeroForRaftPort) {
    std::vector<std::string> args{
        "kv-server",
        "--id",        "1",
        "--raft-port", "0",
        "--peers",     "2:127.0.0.1:7002,3:127.0.0.1:7003",
        "--data-dir",  "./data",
    };
    auto argv = make_argv(args);
    EXPECT_THROW(kv::parse_config(static_cast<int>(argv.size()), argv.data()),
                 std::runtime_error);
}

TEST_F(NodeConfigTest, RejectsMalformedPeerEntry) {
    std::vector<std::string> args{
        "kv-server",
        "--id",    "1",
        "--peers", "2-127.0.0.1-7002,3:127.0.0.1:7003",  // '-' instead of ':'
        "--data-dir", "./data",
    };
    auto argv = make_argv(args);
    EXPECT_THROW(kv::parse_config(static_cast<int>(argv.size()), argv.data()),
                 std::runtime_error);
}

TEST_F(NodeConfigTest, RejectsPeerIdZero) {
    std::vector<std::string> args{
        "kv-server",
        "--id",    "1",
        "--peers", "0:127.0.0.1:7002,3:127.0.0.1:7003",  // peer id == 0
        "--data-dir", "./data",
    };
    auto argv = make_argv(args);
    EXPECT_THROW(kv::parse_config(static_cast<int>(argv.size()), argv.data()),
                 std::runtime_error);
}

TEST_F(NodeConfigTest, RejectsSnapshotIntervalZero) {
    std::vector<std::string> args{
        "kv-server",
        "--id",                 "1",
        "--peers",              "2:127.0.0.1:7002,3:127.0.0.1:7003",
        "--data-dir",           "./data",
        "--snapshot-interval",  "0",
    };
    auto argv = make_argv(args);
    EXPECT_THROW(kv::parse_config(static_cast<int>(argv.size()), argv.data()),
                 std::runtime_error);
}
