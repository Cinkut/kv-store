// Integration test: starts a real Server in a background thread, connects via
// a synchronous TCP socket, and verifies all command/response round-trips.
//
// Port 17379 is chosen to avoid conflicts with any local Redis instance.
// The port is bound before the server thread starts; all sync socket ops use
// a short deadline so the suite never hangs indefinitely.

#include "network/binary_protocol.hpp"
#include "network/server.hpp"
#include "storage/storage.hpp"

#include <gtest/gtest.h>

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/asio/streambuf.hpp>
#include <boost/asio/write.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <istream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

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
    explicit SyncClient(std::uint16_t port = TEST_PORT) : ioc_(1), socket_(ioc_) {
        tcp::resolver resolver{ioc_};
        auto endpoints = resolver.resolve(TEST_HOST, std::to_string(port));
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

// ===========================================================================
// Binary protocol integration tests
// ===========================================================================

using namespace kv;
using namespace kv::network;

constexpr std::uint16_t BINARY_TEST_PORT = 17380;

// ---------------------------------------------------------------------------
// Synchronous TCP client that speaks the binary protocol.
// Uses serialize_binary_request() to frame commands and
// parse_binary_response() to decode responses — exercising the full
// TCP → Session auto-detect → binary parse → dispatch → serialize → TCP path.
// ---------------------------------------------------------------------------
class BinarySyncClient {
public:
    BinarySyncClient() : ioc_(1), socket_(ioc_) {
        tcp::resolver resolver{ioc_};
        auto endpoints = resolver.resolve(TEST_HOST, std::to_string(BINARY_TEST_PORT));
        boost::asio::connect(socket_, endpoints);
    }

    // Send a binary-framed request for the given Command.
    void send(const Command& cmd) {
        auto wire = serialize_binary_request(cmd);
        boost::asio::write(socket_, boost::asio::buffer(wire));
    }

    // Read one binary-framed response and return the parsed Response.
    Response recv() {
        // Read the 5-byte header.
        std::vector<uint8_t> header(binary::kHeaderSize);
        boost::asio::read(socket_, boost::asio::buffer(header));

        uint8_t status = 0;
        uint32_t payload_len = 0;
        EXPECT_TRUE(read_binary_header(header, status, payload_len));

        // Read the payload.
        std::vector<uint8_t> payload(payload_len);
        if (payload_len > 0) {
            boost::asio::read(socket_, boost::asio::buffer(payload));
        }

        auto result = parse_binary_response(status, payload);
        // If parsing itself produced an outer ErrorResp, wrap it.
        if (std::holds_alternative<ErrorResp>(result)) {
            return std::get<ErrorResp>(result);
        }
        return std::get<Response>(result);
    }

    // Convenience: send + recv in one call.
    Response cmd(const Command& c) {
        send(c);
        return recv();
    }

    // Send raw bytes (for malformed-message tests).
    void send_raw(const std::vector<uint8_t>& data) {
        boost::asio::write(socket_, boost::asio::buffer(data));
    }

private:
    io_ctx      ioc_;
    tcp::socket socket_;
};

// ---------------------------------------------------------------------------
// Test fixture: manages server lifetime on a different port to avoid
// conflicts with the text-protocol fixture running in the same process.
// ---------------------------------------------------------------------------

class BinaryIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        server_ = std::make_unique<kv::network::Server>(
            TEST_HOST, BINARY_TEST_PORT, storage_);
        server_thread_ = std::thread([this] { server_->run(); });
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
// Response type assertions (helpers).
// ---------------------------------------------------------------------------

template <typename T>
bool holds(const Response& r) {
    return std::holds_alternative<T>(r);
}

template <typename T>
const T& get(const Response& r) {
    return std::get<T>(r);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

TEST_F(BinaryIntegrationTest, Ping) {
    BinarySyncClient client;
    auto resp = client.cmd(PingCmd{});
    EXPECT_TRUE(holds<PongResp>(resp));
}

TEST_F(BinaryIntegrationTest, SetAndGet) {
    BinarySyncClient client;

    auto set_resp = client.cmd(SetCmd{"hello", "world"});
    ASSERT_TRUE(holds<OkResp>(set_resp));

    auto get_resp = client.cmd(GetCmd{"hello"});
    ASSERT_TRUE(holds<ValueResp>(get_resp));
    EXPECT_EQ(get<ValueResp>(get_resp).value, "world");
}

TEST_F(BinaryIntegrationTest, GetNotFound) {
    BinarySyncClient client;
    auto resp = client.cmd(GetCmd{"nonexistent"});
    EXPECT_TRUE(holds<NotFoundResp>(resp));
}

TEST_F(BinaryIntegrationTest, Overwrite) {
    BinarySyncClient client;
    EXPECT_TRUE(holds<OkResp>(client.cmd(SetCmd{"k", "v1"})));
    EXPECT_TRUE(holds<OkResp>(client.cmd(SetCmd{"k", "v2"})));

    auto resp = client.cmd(GetCmd{"k"});
    ASSERT_TRUE(holds<ValueResp>(resp));
    EXPECT_EQ(get<ValueResp>(resp).value, "v2");
}

TEST_F(BinaryIntegrationTest, Del) {
    BinarySyncClient client;
    EXPECT_TRUE(holds<OkResp>(client.cmd(SetCmd{"foo", "bar"})));

    auto del_resp = client.cmd(DelCmd{"foo"});
    EXPECT_TRUE(holds<DeletedResp>(del_resp));

    auto get_resp = client.cmd(GetCmd{"foo"});
    EXPECT_TRUE(holds<NotFoundResp>(get_resp));
}

TEST_F(BinaryIntegrationTest, DelNotFound) {
    BinarySyncClient client;
    auto resp = client.cmd(DelCmd{"ghost"});
    EXPECT_TRUE(holds<NotFoundResp>(resp));
}

TEST_F(BinaryIntegrationTest, KeysEmpty) {
    BinarySyncClient client;
    auto resp = client.cmd(KeysCmd{});
    ASSERT_TRUE(holds<KeysResp>(resp));
    EXPECT_TRUE(get<KeysResp>(resp).keys.empty());
}

TEST_F(BinaryIntegrationTest, KeysAfterInserts) {
    BinarySyncClient client;
    EXPECT_TRUE(holds<OkResp>(client.cmd(SetCmd{"a", "1"})));
    EXPECT_TRUE(holds<OkResp>(client.cmd(SetCmd{"b", "2"})));

    auto resp = client.cmd(KeysCmd{});
    ASSERT_TRUE(holds<KeysResp>(resp));
    auto keys = get<KeysResp>(resp).keys;
    std::sort(keys.begin(), keys.end());
    ASSERT_EQ(keys.size(), 2u);
    EXPECT_EQ(keys[0], "a");
    EXPECT_EQ(keys[1], "b");
}

TEST_F(BinaryIntegrationTest, ValueWithBinaryData) {
    // Binary protocol should handle arbitrary bytes (including null bytes).
    BinarySyncClient client;
    std::string value("hello\x00world", 11); // 11 bytes: "hello" + \0 + "world"
    EXPECT_TRUE(holds<OkResp>(client.cmd(SetCmd{"binkey", value})));

    auto resp = client.cmd(GetCmd{"binkey"});
    ASSERT_TRUE(holds<ValueResp>(resp));
    EXPECT_EQ(get<ValueResp>(resp).value, value);
}

TEST_F(BinaryIntegrationTest, LargeValue) {
    // Test with a value larger than typical TCP buffer sizes.
    BinarySyncClient client;
    std::string big(100'000, 'X');
    EXPECT_TRUE(holds<OkResp>(client.cmd(SetCmd{"bigkey", big})));

    auto resp = client.cmd(GetCmd{"bigkey"});
    ASSERT_TRUE(holds<ValueResp>(resp));
    EXPECT_EQ(get<ValueResp>(resp).value, big);
}

TEST_F(BinaryIntegrationTest, MultipleConnections) {
    BinarySyncClient c1;
    BinarySyncClient c2;

    EXPECT_TRUE(holds<OkResp>(c1.cmd(SetCmd{"shared", "42"})));

    auto resp = c2.cmd(GetCmd{"shared"});
    ASSERT_TRUE(holds<ValueResp>(resp));
    EXPECT_EQ(get<ValueResp>(resp).value, "42");
}

TEST_F(BinaryIntegrationTest, UnknownMessageType) {
    BinarySyncClient client;
    // Send a binary request with unknown msg_type 0x0F and empty payload.
    std::vector<uint8_t> raw = {0x0F, 0x00, 0x00, 0x00, 0x00};
    client.send_raw(raw);

    auto resp = client.recv();
    ASSERT_TRUE(holds<ErrorResp>(resp));
    EXPECT_NE(get<ErrorResp>(resp).message.find("unknown"), std::string::npos);
}

TEST_F(BinaryIntegrationTest, PipelineMultipleCommands) {
    BinarySyncClient client;
    // Send several commands back-to-back before reading responses.
    client.send(SetCmd{"p1", "aaa"});
    client.send(SetCmd{"p2", "bbb"});
    client.send(GetCmd{"p1"});
    client.send(GetCmd{"p2"});
    client.send(PingCmd{});

    EXPECT_TRUE(holds<OkResp>(client.recv()));
    EXPECT_TRUE(holds<OkResp>(client.recv()));

    auto r3 = client.recv();
    ASSERT_TRUE(holds<ValueResp>(r3));
    EXPECT_EQ(get<ValueResp>(r3).value, "aaa");

    auto r4 = client.recv();
    ASSERT_TRUE(holds<ValueResp>(r4));
    EXPECT_EQ(get<ValueResp>(r4).value, "bbb");

    EXPECT_TRUE(holds<PongResp>(client.recv()));
}

TEST_F(BinaryIntegrationTest, AutoDetectionTextAfterBinary) {
    // One client uses binary, another uses text — same server, same port.
    BinarySyncClient bin_client;
    EXPECT_TRUE(holds<OkResp>(bin_client.cmd(SetCmd{"mixed", "frombinary"})));

    // Text client on the same port (BINARY_TEST_PORT).
    SyncClient text_client(BINARY_TEST_PORT);
    EXPECT_EQ(text_client.cmd("GET mixed"), "VALUE frombinary");
}

TEST_F(BinaryIntegrationTest, EmptyKey) {
    // The session should handle requests that the parser rejects
    // (empty key produces an ErrorResp from parse_binary_request).
    BinarySyncClient client;
    // Craft a GET request with key_len = 0.
    std::vector<uint8_t> raw;
    raw.push_back(binary::kMsgGet); // msg_type
    // payload_length = 2 (key_len field only, key_len=0)
    raw.push_back(0x00); raw.push_back(0x00);
    raw.push_back(0x00); raw.push_back(0x02);
    // key_len = 0
    raw.push_back(0x00); raw.push_back(0x00);
    client.send_raw(raw);

    auto resp = client.recv();
    ASSERT_TRUE(holds<ErrorResp>(resp));
    EXPECT_NE(get<ErrorResp>(resp).message.find("empty key"), std::string::npos);
}

// ===========================================================================
// RESP protocol integration tests
// ===========================================================================

class RespSyncClient {
public:
    explicit RespSyncClient(std::uint16_t port = TEST_PORT) : ioc_(1), socket_(ioc_) {
        tcp::resolver resolver{ioc_};
        auto endpoints = resolver.resolve(TEST_HOST, std::to_string(port));
        boost::asio::connect(socket_, endpoints);
    }

    // Send a raw RESP request string.
    void send_raw(const std::string& data) {
        boost::asio::write(socket_, boost::asio::buffer(data));
    }

    // Send a RESP array command (e.g., {"SET", "key", "value"}).
    void send(const std::vector<std::string>& args) {
        std::string wire = "*" + std::to_string(args.size()) + "\r\n";
        for (const auto& a : args) {
            wire += "$" + std::to_string(a.size()) + "\r\n" + a + "\r\n";
        }
        send_raw(wire);
    }

    // Read one CRLF-terminated line from the persistent buffer + socket.
    std::string recv_line() {
        for (;;) {
            // Check if we already have a complete line in the buffer.
            auto pos = buf_.find("\r\n");
            if (pos != std::string::npos) {
                std::string line = buf_.substr(0, pos);
                buf_.erase(0, pos + 2);
                return line;
            }
            // Read more data.
            char tmp[256];
            auto n = socket_.read_some(boost::asio::buffer(tmp));
            buf_.append(tmp, n);
        }
    }

    // Simple RESP command: send array, read one response line.
    std::string cmd(const std::vector<std::string>& args) {
        send(args);
        return recv_line();
    }

    // Read a bulk string response ($len\r\ndata\r\n).
    std::string recv_bulk() {
        std::string header = recv_line();
        if (header.empty() || header[0] != '$') return header;
        int len = std::stoi(header.substr(1));
        if (len < 0) return "$-1"; // null bulk
        // Need exactly len + 2 bytes (data + \r\n) from the buffer.
        std::size_t need = static_cast<std::size_t>(len) + 2;
        while (buf_.size() < need) {
            char tmp[256];
            auto n = socket_.read_some(boost::asio::buffer(tmp));
            buf_.append(tmp, n);
        }
        std::string data = buf_.substr(0, static_cast<std::size_t>(len));
        buf_.erase(0, need);
        return data;
    }

    // Send command, expect bulk string response.
    std::string cmd_bulk(const std::vector<std::string>& args) {
        send(args);
        return recv_bulk();
    }

private:
    io_ctx      ioc_;
    tcp::socket socket_;
    std::string buf_;  // persistent read buffer
};

class RespIntegrationTest : public ::testing::Test {
protected:
    static constexpr std::uint16_t kRespPort = 17383;

    void SetUp() override {
        server_ = std::make_unique<kv::network::Server>(TEST_HOST, kRespPort, storage_);
        server_thread_ = std::thread([this] { server_->run(); });
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

TEST_F(RespIntegrationTest, Ping) {
    RespSyncClient client(kRespPort);
    EXPECT_EQ(client.cmd({"PING"}), "+PONG");
}

TEST_F(RespIntegrationTest, SetAndGet) {
    RespSyncClient client(kRespPort);
    EXPECT_EQ(client.cmd({"SET", "hello", "world"}), "+OK");
    EXPECT_EQ(client.cmd_bulk({"GET", "hello"}), "world");
}

TEST_F(RespIntegrationTest, GetNotFound) {
    RespSyncClient client(kRespPort);
    auto resp = client.cmd_bulk({"GET", "nonexistent"});
    EXPECT_EQ(resp, "$-1");
}

TEST_F(RespIntegrationTest, Del) {
    RespSyncClient client(kRespPort);
    EXPECT_EQ(client.cmd({"SET", "foo", "bar"}), "+OK");
    EXPECT_EQ(client.cmd({"DEL", "foo"}), ":1");
    auto resp = client.cmd_bulk({"GET", "foo"});
    EXPECT_EQ(resp, "$-1");
}

TEST_F(RespIntegrationTest, UnknownCommand) {
    RespSyncClient client(kRespPort);
    auto resp = client.cmd({"FOOBAR"});
    EXPECT_TRUE(resp.starts_with("-ERR"));
    EXPECT_TRUE(resp.find("unknown") != std::string::npos);
}

TEST_F(RespIntegrationTest, CaseInsensitive) {
    RespSyncClient client(kRespPort);
    EXPECT_EQ(client.cmd({"ping"}), "+PONG");
    EXPECT_EQ(client.cmd({"set", "k", "v"}), "+OK");
    EXPECT_EQ(client.cmd_bulk({"get", "k"}), "v");
}

TEST_F(RespIntegrationTest, MultipleCommandsOneConnection) {
    RespSyncClient client(kRespPort);
    EXPECT_EQ(client.cmd({"SET", "a", "1"}), "+OK");
    EXPECT_EQ(client.cmd({"SET", "b", "2"}), "+OK");
    EXPECT_EQ(client.cmd_bulk({"GET", "a"}), "1");
    EXPECT_EQ(client.cmd_bulk({"GET", "b"}), "2");
}

} // anonymous namespace
