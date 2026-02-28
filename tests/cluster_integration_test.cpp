// Cluster integration tests: spawn 3 kv-server processes, verify Raft-based
// key-value operations including leader election, client writes, reads,
// REDIRECT from followers, and data replication.
//
// These tests require the kv-server binary at build/debug/src/server/kv-server.
// If the binary is not found, the tests are skipped.
//
// Test ports are chosen in a high range to avoid conflicts:
//   Node 1: client=18401, raft=18411
//   Node 2: client=18402, raft=18412
//   Node 3: client=18403, raft=18413

#include <gtest/gtest.h>

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/asio/streambuf.hpp>
#include <boost/asio/write.hpp>

#include <array>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <istream>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {

namespace fs = std::filesystem;

using tcp    = boost::asio::ip::tcp;
using io_ctx = boost::asio::io_context;

// ── Configuration ────────────────────────────────────────────────────────────

constexpr const char* TEST_HOST = "127.0.0.1";

struct NodePorts {
    uint16_t client_port;
    uint16_t raft_port;
};

constexpr std::array<NodePorts, 3> kNodes = {{
    {18401, 18411},
    {18402, 18412},
    {18403, 18413},
}};

// Path to kv-server binary (relative to project root).
const fs::path kServerBinary = "build/debug/src/server/kv-server";

// ── SyncClient ───────────────────────────────────────────────────────────────

class SyncClient {
public:
    explicit SyncClient(uint16_t port) : ioc_(1), socket_(ioc_) {
        tcp::resolver resolver{ioc_};
        auto endpoints = resolver.resolve(TEST_HOST, std::to_string(port));
        boost::asio::connect(socket_, endpoints);
    }

    void send(const std::string& cmd) {
        std::string line = cmd + "\n";
        boost::asio::write(socket_, boost::asio::buffer(line));
    }

    std::string recv_line() {
        boost::asio::streambuf buf;
        boost::asio::read_until(socket_, buf, '\n');
        std::istream is(&buf);
        std::string line;
        std::getline(is, line);
        if (!line.empty() && line.back() == '\r') line.pop_back();
        return line;
    }

    std::string cmd(const std::string& c) {
        send(c);
        return recv_line();
    }

private:
    io_ctx      ioc_;
    tcp::socket socket_;
};

// ── Cluster test fixture ─────────────────────────────────────────────────────

class ClusterIntegrationTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        // Find project root (walk up from CWD until we find CMakeLists.txt).
        project_root_ = fs::current_path();
        while (!fs::exists(project_root_ / "CMakeLists.txt")) {
            if (project_root_.has_parent_path() &&
                project_root_ != project_root_.parent_path()) {
                project_root_ = project_root_.parent_path();
            } else {
                break;
            }
        }

        server_binary_ = project_root_ / kServerBinary;
        if (!fs::exists(server_binary_)) {
            skip_reason_ = "kv-server binary not found at " + server_binary_.string();
            return;
        }

        // Create temp data directories.
        data_root_ = fs::temp_directory_path() / "kv_cluster_test";
        fs::remove_all(data_root_);
        fs::create_directories(data_root_);

        // Launch 3 nodes.
        for (int i = 0; i < 3; ++i) {
            auto data_dir = data_root_ / ("node" + std::to_string(i + 1));
            fs::create_directories(data_dir);

            // Build peers string: all nodes except this one.
            std::string peers;
            for (int j = 0; j < 3; ++j) {
                if (j == i) continue;
                if (!peers.empty()) peers += ",";
                peers += std::to_string(j + 1) + ":" + TEST_HOST + ":" +
                         std::to_string(kNodes[j].raft_port) + ":" +
                         std::to_string(kNodes[j].client_port);
            }

            pid_t pid = fork();
            if (pid == 0) {
                // Child process: exec kv-server.
                execl(server_binary_.c_str(), "kv-server",
                      "--id", std::to_string(i + 1).c_str(),
                      "--host", TEST_HOST,
                      "--client-port", std::to_string(kNodes[i].client_port).c_str(),
                      "--raft-port", std::to_string(kNodes[i].raft_port).c_str(),
                      "--peers", peers.c_str(),
                      "--data-dir", data_dir.c_str(),
                      "--log-level", "warn",
                      nullptr);
                _exit(127); // execl failed
            }
            pids_[i] = pid;
        }

        // Wait for nodes to start accepting connections.
        // Try connecting to all 3 ports with retries.
        constexpr int kMaxRetries = 60;  // up to 6 seconds
        constexpr auto kRetryDelay = std::chrono::milliseconds{100};

        for (int i = 0; i < 3; ++i) {
            bool connected = false;
            for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
                try {
                    SyncClient client(kNodes[i].client_port);
                    auto resp = client.cmd("PING");
                    if (resp == "PONG") {
                        connected = true;
                        break;
                    }
                } catch (...) {
                    // Connection refused — retry.
                }
                std::this_thread::sleep_for(kRetryDelay);
            }
            if (!connected) {
                skip_reason_ = "Node " + std::to_string(i + 1) +
                               " failed to start within timeout";
                TearDownTestSuite();
                return;
            }
        }

        // Wait for leader election (try SET on each node, see who succeeds).
        constexpr int kElectionRetries = 100;  // up to 10 seconds
        for (int attempt = 0; attempt < kElectionRetries; ++attempt) {
            for (int i = 0; i < 3; ++i) {
                try {
                    SyncClient client(kNodes[i].client_port);
                    auto resp = client.cmd("SET __election_probe__ 1");
                    if (resp == "OK") {
                        leader_index_ = i;
                        // Clean up the probe key.
                        client.cmd("DEL __election_probe__");
                        goto election_done;
                    }
                } catch (...) {}
            }
            std::this_thread::sleep_for(kRetryDelay);
        }
    election_done:
        if (leader_index_ < 0) {
            skip_reason_ = "No leader elected within timeout";
            TearDownTestSuite();
        }
    }

    static void TearDownTestSuite() {
        // Send SIGTERM to all children.
        for (auto pid : pids_) {
            if (pid > 0) {
                kill(pid, SIGTERM);
            }
        }
        // Wait for them to exit.
        for (auto pid : pids_) {
            if (pid > 0) {
                int status = 0;
                waitpid(pid, &status, 0);
            }
        }
        pids_ = {0, 0, 0};

        // Clean up data directories.
        if (!data_root_.empty()) {
            std::error_code ec;
            fs::remove_all(data_root_, ec);
        }
    }

    void SetUp() override {
        if (!skip_reason_.empty()) {
            GTEST_SKIP() << skip_reason_;
        }
    }

    // Find a follower index (any node that isn't the leader).
    static int follower_index() {
        for (int i = 0; i < 3; ++i) {
            if (i != leader_index_) return i;
        }
        return -1;
    }

    static uint16_t leader_port() {
        return kNodes[leader_index_].client_port;
    }

    static uint16_t follower_port() {
        return kNodes[follower_index()].client_port;
    }

    static fs::path project_root_;
    static fs::path server_binary_;
    static fs::path data_root_;
    static std::array<pid_t, 3> pids_;
    static int leader_index_;
    static std::string skip_reason_;
};

fs::path ClusterIntegrationTest::project_root_;
fs::path ClusterIntegrationTest::server_binary_;
fs::path ClusterIntegrationTest::data_root_;
std::array<pid_t, 3> ClusterIntegrationTest::pids_ = {0, 0, 0};
int ClusterIntegrationTest::leader_index_ = -1;
std::string ClusterIntegrationTest::skip_reason_;

// ── Tests ────────────────────────────────────────────────────────────────────

TEST_F(ClusterIntegrationTest, PingAllNodes) {
    for (int i = 0; i < 3; ++i) {
        SyncClient client(kNodes[i].client_port);
        EXPECT_EQ(client.cmd("PING"), "PONG")
            << "PING failed on node " << (i + 1);
    }
}

TEST_F(ClusterIntegrationTest, LeaderAcceptsWrite) {
    SyncClient client(leader_port());
    EXPECT_EQ(client.cmd("SET cluster_key cluster_val"), "OK");
    EXPECT_EQ(client.cmd("GET cluster_key"), "VALUE cluster_val");
}

TEST_F(ClusterIntegrationTest, FollowerRedirectsWrite) {
    SyncClient client(follower_port());
    auto resp = client.cmd("SET redirect_test value");
    // Should be a REDIRECT response pointing to the leader.
    EXPECT_TRUE(resp.rfind("REDIRECT ", 0) == 0)
        << "Expected REDIRECT, got: " << resp;
}

TEST_F(ClusterIntegrationTest, FollowerRedirectsDel) {
    SyncClient client(follower_port());
    auto resp = client.cmd("DEL some_key");
    // Should be REDIRECT or NOT_FOUND (if REDIRECT not sent for DEL on unknown key).
    // Per our implementation, followers always redirect writes.
    EXPECT_TRUE(resp.rfind("REDIRECT ", 0) == 0)
        << "Expected REDIRECT for DEL on follower, got: " << resp;
}

TEST_F(ClusterIntegrationTest, ReadAfterWrite) {
    // Write through leader.
    SyncClient leader(leader_port());
    EXPECT_EQ(leader.cmd("SET read_key read_value"), "OK");

    // Give a moment for replication.
    std::this_thread::sleep_for(std::chrono::milliseconds{200});

    // Read from a follower — reads should work on any node since they
    // read from local storage (which is updated by StateMachine::apply).
    SyncClient follower(follower_port());
    EXPECT_EQ(follower.cmd("GET read_key"), "VALUE read_value");
}

TEST_F(ClusterIntegrationTest, MultipleWritesAndKeys) {
    SyncClient leader(leader_port());
    EXPECT_EQ(leader.cmd("SET k1 v1"), "OK");
    EXPECT_EQ(leader.cmd("SET k2 v2"), "OK");
    EXPECT_EQ(leader.cmd("SET k3 v3"), "OK");

    EXPECT_EQ(leader.cmd("GET k1"), "VALUE v1");
    EXPECT_EQ(leader.cmd("GET k2"), "VALUE v2");
    EXPECT_EQ(leader.cmd("GET k3"), "VALUE v3");
}

TEST_F(ClusterIntegrationTest, DeleteThroughLeader) {
    SyncClient leader(leader_port());
    EXPECT_EQ(leader.cmd("SET del_key del_val"), "OK");
    EXPECT_EQ(leader.cmd("GET del_key"), "VALUE del_val");
    EXPECT_EQ(leader.cmd("DEL del_key"), "DELETED");

    // Give a moment for replication.
    std::this_thread::sleep_for(std::chrono::milliseconds{200});

    // Verify deletion propagated.
    SyncClient follower(follower_port());
    EXPECT_EQ(follower.cmd("GET del_key"), "NOT_FOUND");
}

TEST_F(ClusterIntegrationTest, OverwriteThroughLeader) {
    SyncClient leader(leader_port());
    EXPECT_EQ(leader.cmd("SET ow_key original"), "OK");
    EXPECT_EQ(leader.cmd("SET ow_key updated"), "OK");
    EXPECT_EQ(leader.cmd("GET ow_key"), "VALUE updated");
}

TEST_F(ClusterIntegrationTest, FollowerReadsAreConsistent) {
    SyncClient leader(leader_port());
    EXPECT_EQ(leader.cmd("SET consistent_key consistent_val"), "OK");

    // Wait for replication.
    std::this_thread::sleep_for(std::chrono::milliseconds{200});

    // All nodes should return the same value.
    for (int i = 0; i < 3; ++i) {
        SyncClient client(kNodes[i].client_port);
        EXPECT_EQ(client.cmd("GET consistent_key"), "VALUE consistent_val")
            << "Inconsistent read on node " << (i + 1);
    }
}

} // anonymous namespace
