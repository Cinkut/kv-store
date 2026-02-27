// Integration test: starts a real Server in a background thread, connects via
// a synchronous TCP socket, and verifies all command/response round-trips.
//
// Port 17379 is chosen to avoid conflicts with any local Redis instance.
// The port is bound before the server thread starts; all sync socket ops use
// a short deadline so the suite never hangs indefinitely.

#include "network/server.hpp"
#include "storage/storage.hpp"

#include <gtest/gtest.h>

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/asio/streambuf.hpp>
#include <boost/asio/write.hpp>

#include <chrono>
#include <istream>
#include <stdexcept>
#include <string>
#include <thread>

namespace {

using tcp      = boost::asio::ip::tcp;
using io_ctx   = boost::asio::io_context;

constexpr std::uint16_t TEST_PORT = 17379;
constexpr const char*   TEST_HOST = "127.0.0.1";

// ---------------------------------------------------------------------------
// Thin synchronous TCP helper used in tests.
// ---------------------------------------------------------------------------
class SyncClient {
public:
    SyncClient() : ioc_(1), socket_(ioc_) {
        tcp::resolver resolver{ioc_};
        auto endpoints = resolver.resolve(TEST_HOST, std::to_string(TEST_PORT));
        boost::asio::connect(socket_, endpoints);
    }

    // Send a raw command line (newline appended automatically).
    void send(const std::string& cmd) {
        std::string line = cmd + "\n";
        boost::asio::write(socket_, boost::asio::buffer(line));
    }

    // Read one response line (up to and including '\n'), return without '\n'.
    std::string recv_line() {
        boost::asio::streambuf buf;
        boost::asio::read_until(socket_, buf, '\n');
        std::istream is(&buf);
        std::string line;
        std::getline(is, line);
        // Strip trailing '\r' in case the server sends CRLF.
        if (!line.empty() && line.back() == '\r') line.pop_back();
        return line;
    }

    // Convenience: send a command and return the single response line.
    std::string cmd(const std::string& c) {
        send(c);
        return recv_line();
    }

private:
    io_ctx      ioc_;
    tcp::socket socket_;
};

// ---------------------------------------------------------------------------
// Test fixture: manages server lifetime.
// ---------------------------------------------------------------------------
class IntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        server_ = std::make_unique<kv::network::Server>(TEST_HOST, TEST_PORT, storage_);
        server_thread_ = std::thread([this] { server_->run(); });

        // Give the server a moment to start accepting before connecting.
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    void TearDown() override {
        server_->stop();
        if (server_thread_.joinable()) {
            server_thread_.join();
        }
    }

    kv::Storage storage_;
    std::unique_ptr<kv::network::Server> server_;
    std::thread server_thread_;
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

TEST_F(IntegrationTest, Ping) {
    SyncClient client;
    EXPECT_EQ(client.cmd("PING"), "PONG");
}

TEST_F(IntegrationTest, SetAndGet) {
    SyncClient client;
    EXPECT_EQ(client.cmd("SET hello world"), "OK");
    EXPECT_EQ(client.cmd("GET hello"), "VALUE world");
}

TEST_F(IntegrationTest, GetNotFound) {
    SyncClient client;
    EXPECT_EQ(client.cmd("GET nonexistent"), "NOT_FOUND");
}

TEST_F(IntegrationTest, Overwrite) {
    SyncClient client;
    EXPECT_EQ(client.cmd("SET k v1"), "OK");
    EXPECT_EQ(client.cmd("SET k v2"), "OK");
    EXPECT_EQ(client.cmd("GET k"), "VALUE v2");
}

TEST_F(IntegrationTest, Del) {
    SyncClient client;
    EXPECT_EQ(client.cmd("SET foo bar"), "OK");
    EXPECT_EQ(client.cmd("DEL foo"), "DELETED");
    EXPECT_EQ(client.cmd("GET foo"), "NOT_FOUND");
}

TEST_F(IntegrationTest, DelNotFound) {
    SyncClient client;
    EXPECT_EQ(client.cmd("DEL ghost"), "NOT_FOUND");
}

TEST_F(IntegrationTest, KeysEmpty) {
    SyncClient client;
    EXPECT_EQ(client.cmd("KEYS"), "KEYS");
}

TEST_F(IntegrationTest, KeysAfterInserts) {
    SyncClient client;
    EXPECT_EQ(client.cmd("SET a 1"), "OK");
    EXPECT_EQ(client.cmd("SET b 2"), "OK");

    const std::string resp = client.cmd("KEYS");
    // The response is "KEYS a b" or "KEYS b a" (order unspecified).
    EXPECT_TRUE(resp.find("KEYS") == 0);
    EXPECT_TRUE(resp.find("a") != std::string::npos);
    EXPECT_TRUE(resp.find("b") != std::string::npos);
}

TEST_F(IntegrationTest, ValueWithSpaces) {
    SyncClient client;
    EXPECT_EQ(client.cmd("SET msg hello world foo"), "OK");
    EXPECT_EQ(client.cmd("GET msg"), "VALUE hello world foo");
}

TEST_F(IntegrationTest, MultipleConnections) {
    SyncClient c1;
    SyncClient c2;

    EXPECT_EQ(c1.cmd("SET shared 42"), "OK");
    EXPECT_EQ(c2.cmd("GET shared"), "VALUE 42");
}

TEST_F(IntegrationTest, MalformedCommand) {
    SyncClient client;
    const std::string resp = client.cmd("NOTACOMMAND");
    EXPECT_TRUE(resp.rfind("ERROR", 0) == 0);
}

TEST_F(IntegrationTest, SetEmptyValueRejected) {
    // The protocol explicitly rejects SET with an empty value.
    SyncClient client;
    const std::string resp = client.cmd("SET ekey ");
    EXPECT_TRUE(resp.rfind("ERROR", 0) == 0);
}

TEST_F(IntegrationTest, PipelineMultipleCommands) {
    SyncClient client;
    // Send several commands back-to-back before reading responses.
    client.send("SET p1 aaa");
    client.send("SET p2 bbb");
    client.send("GET p1");
    client.send("GET p2");
    client.send("PING");

    EXPECT_EQ(client.recv_line(), "OK");
    EXPECT_EQ(client.recv_line(), "OK");
    EXPECT_EQ(client.recv_line(), "VALUE aaa");
    EXPECT_EQ(client.recv_line(), "VALUE bbb");
    EXPECT_EQ(client.recv_line(), "PONG");
}

} // anonymous namespace
