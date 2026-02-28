#include "network/resp_protocol.hpp"
#include "network/binary_protocol.hpp"
#include "network/protocol.hpp"
#include "common/logger.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/asio/write.hpp>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include <string>
#include <variant>
#include <vector>

namespace asio = boost::asio;
using tcp = asio::ip::tcp;

namespace {

constexpr auto use_awaitable = asio::as_tuple(asio::use_awaitable);

// ── Serializer tests (pure functions, no I/O) ────────────────────────────────

class RespSerializerTest : public ::testing::Test {
protected:
    void SetUp() override {
        spdlog::drop("kv");
        kv::init_default_logger();
    }
};

TEST_F(RespSerializerTest, OkResp) {
    kv::Response r = kv::OkResp{};
    EXPECT_EQ(kv::network::serialize_resp_response(r), "+OK\r\n");
}

TEST_F(RespSerializerTest, PongResp) {
    kv::Response r = kv::PongResp{};
    EXPECT_EQ(kv::network::serialize_resp_response(r), "+PONG\r\n");
}

TEST_F(RespSerializerTest, ValueResp) {
    kv::Response r = kv::ValueResp{"hello"};
    EXPECT_EQ(kv::network::serialize_resp_response(r), "$5\r\nhello\r\n");
}

TEST_F(RespSerializerTest, ValueRespEmpty) {
    kv::Response r = kv::ValueResp{""};
    EXPECT_EQ(kv::network::serialize_resp_response(r), "$0\r\n\r\n");
}

TEST_F(RespSerializerTest, NotFoundResp) {
    kv::Response r = kv::NotFoundResp{};
    EXPECT_EQ(kv::network::serialize_resp_response(r), "$-1\r\n");
}

TEST_F(RespSerializerTest, DeletedResp) {
    kv::Response r = kv::DeletedResp{};
    EXPECT_EQ(kv::network::serialize_resp_response(r), ":1\r\n");
}

TEST_F(RespSerializerTest, KeysRespEmpty) {
    kv::Response r = kv::KeysResp{{}};
    EXPECT_EQ(kv::network::serialize_resp_response(r), "*0\r\n");
}

TEST_F(RespSerializerTest, KeysRespMultiple) {
    kv::Response r = kv::KeysResp{{"a", "bb", "ccc"}};
    EXPECT_EQ(kv::network::serialize_resp_response(r),
              "*3\r\n$1\r\na\r\n$2\r\nbb\r\n$3\r\nccc\r\n");
}

TEST_F(RespSerializerTest, ErrorResp) {
    kv::Response r = kv::ErrorResp{"something went wrong"};
    EXPECT_EQ(kv::network::serialize_resp_response(r),
              "-ERR something went wrong\r\n");
}

TEST_F(RespSerializerTest, RedirectResp) {
    kv::Response r = kv::RedirectResp{"127.0.0.1:6380"};
    EXPECT_EQ(kv::network::serialize_resp_response(r),
              "-MOVED 0 127.0.0.1:6380\r\n");
}

// ── Request serializer tests ────────────────────────────────────────────────

TEST_F(RespSerializerTest, SerializePing) {
    kv::Command cmd = kv::PingCmd{};
    EXPECT_EQ(kv::network::serialize_resp_request(cmd), "*1\r\n$4\r\nPING\r\n");
}

TEST_F(RespSerializerTest, SerializeGet) {
    kv::Command cmd = kv::GetCmd{"mykey"};
    EXPECT_EQ(kv::network::serialize_resp_request(cmd),
              "*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n");
}

TEST_F(RespSerializerTest, SerializeSet) {
    kv::Command cmd = kv::SetCmd{"k", "v"};
    EXPECT_EQ(kv::network::serialize_resp_request(cmd),
              "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");
}

TEST_F(RespSerializerTest, SerializeDel) {
    kv::Command cmd = kv::DelCmd{"foo"};
    EXPECT_EQ(kv::network::serialize_resp_request(cmd),
              "*2\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n");
}

TEST_F(RespSerializerTest, SerializeKeys) {
    kv::Command cmd = kv::KeysCmd{};
    EXPECT_EQ(kv::network::serialize_resp_request(cmd),
              "*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n");
}

// ── Auto-detection tests ────────────────────────────────────────────────────

TEST_F(RespSerializerTest, IsRespProtocol) {
    EXPECT_TRUE(kv::network::is_resp_protocol('*'));
    EXPECT_TRUE(kv::network::is_resp_protocol('+'));
    EXPECT_TRUE(kv::network::is_resp_protocol('-'));
    EXPECT_TRUE(kv::network::is_resp_protocol('$'));
    EXPECT_TRUE(kv::network::is_resp_protocol(':'));

    // Text chars should NOT be RESP.
    EXPECT_FALSE(kv::network::is_resp_protocol('S'));
    EXPECT_FALSE(kv::network::is_resp_protocol('G'));
    EXPECT_FALSE(kv::network::is_resp_protocol('P'));
    EXPECT_FALSE(kv::network::is_resp_protocol(' '));

    // Binary bytes should NOT be RESP.
    EXPECT_FALSE(kv::network::is_resp_protocol(0x00));
    EXPECT_FALSE(kv::network::is_resp_protocol(0x01));
    EXPECT_FALSE(kv::network::is_resp_protocol(0x1F));
}

// ── Round-trip tests (parser + serializer over loopback TCP) ─────────────────

class RespRoundTripTest : public ::testing::Test {
protected:
    void SetUp() override {
        spdlog::drop("kv");
        kv::init_default_logger();
    }

    // Helper: start a server that reads one RESP request, parses it,
    // echoes back the Command variant index as a simple string, then closes.
    void run_parse_test(const std::string& wire_input,
                        bool expect_success,
                        std::function<void(const std::variant<kv::Command, kv::ErrorResp>&)> checker) {
        asio::io_context ioc;

        tcp::acceptor acceptor(ioc, tcp::endpoint(tcp::v4(), 0));
        auto port = acceptor.local_endpoint().port();

        // Server side: accept, parse RESP, call checker.
        asio::co_spawn(ioc, [&]() -> asio::awaitable<void> {
            auto [ec, socket] = co_await acceptor.async_accept(use_awaitable);
            if (ec) co_return;

            // The first byte is already consumed by auto-detect in real code.
            // Here we read it ourselves.
            uint8_t first_byte = 0;
            auto [rec, rn] = co_await asio::async_read(
                socket, asio::buffer(&first_byte, 1), use_awaitable);
            if (rec) co_return;

            std::string parse_buf;
            auto result = co_await kv::network::parse_resp_request(socket, first_byte, parse_buf);
            checker(result);
        }, asio::detached);

        // Client side: connect, send wire_input, close.
        asio::co_spawn(ioc, [&]() -> asio::awaitable<void> {
            tcp::socket socket(ioc);
            auto [ec] = co_await socket.async_connect(
                tcp::endpoint(tcp::v4(), port), use_awaitable);
            if (ec) co_return;

            auto [wec, _] = co_await asio::async_write(
                socket, asio::buffer(wire_input), use_awaitable);
            // Graceful shutdown so server sees EOF after our data.
            boost::system::error_code sec;
            socket.shutdown(tcp::socket::shutdown_send, sec);
        }, asio::detached);

        ioc.run();
    }
};

TEST_F(RespRoundTripTest, ParsePing) {
    run_parse_test("*1\r\n$4\r\nPING\r\n", true, [](const auto& result) {
        ASSERT_TRUE(std::holds_alternative<kv::Command>(result));
        const auto& cmd = std::get<kv::Command>(result);
        EXPECT_TRUE(std::holds_alternative<kv::PingCmd>(cmd));
    });
}

TEST_F(RespRoundTripTest, ParseGet) {
    run_parse_test("*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n", true, [](const auto& result) {
        ASSERT_TRUE(std::holds_alternative<kv::Command>(result));
        const auto& cmd = std::get<kv::Command>(result);
        ASSERT_TRUE(std::holds_alternative<kv::GetCmd>(cmd));
        EXPECT_EQ(std::get<kv::GetCmd>(cmd).key, "mykey");
    });
}

TEST_F(RespRoundTripTest, ParseSet) {
    run_parse_test("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", true,
                   [](const auto& result) {
        ASSERT_TRUE(std::holds_alternative<kv::Command>(result));
        const auto& cmd = std::get<kv::Command>(result);
        ASSERT_TRUE(std::holds_alternative<kv::SetCmd>(cmd));
        EXPECT_EQ(std::get<kv::SetCmd>(cmd).key, "foo");
        EXPECT_EQ(std::get<kv::SetCmd>(cmd).value, "bar");
    });
}

TEST_F(RespRoundTripTest, ParseDel) {
    run_parse_test("*2\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n", true, [](const auto& result) {
        ASSERT_TRUE(std::holds_alternative<kv::Command>(result));
        const auto& cmd = std::get<kv::Command>(result);
        ASSERT_TRUE(std::holds_alternative<kv::DelCmd>(cmd));
        EXPECT_EQ(std::get<kv::DelCmd>(cmd).key, "foo");
    });
}

TEST_F(RespRoundTripTest, ParseKeys) {
    run_parse_test("*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n", true, [](const auto& result) {
        ASSERT_TRUE(std::holds_alternative<kv::Command>(result));
        const auto& cmd = std::get<kv::Command>(result);
        EXPECT_TRUE(std::holds_alternative<kv::KeysCmd>(cmd));
    });
}

TEST_F(RespRoundTripTest, ParseCaseInsensitive) {
    run_parse_test("*1\r\n$4\r\nping\r\n", true, [](const auto& result) {
        ASSERT_TRUE(std::holds_alternative<kv::Command>(result));
        const auto& cmd = std::get<kv::Command>(result);
        EXPECT_TRUE(std::holds_alternative<kv::PingCmd>(cmd));
    });
}

TEST_F(RespRoundTripTest, ParseUnknownCommand) {
    run_parse_test("*1\r\n$7\r\nUNKNOWN\r\n", true, [](const auto& result) {
        ASSERT_TRUE(std::holds_alternative<kv::ErrorResp>(result));
        EXPECT_TRUE(std::get<kv::ErrorResp>(result).message.find("unknown") != std::string::npos);
    });
}

TEST_F(RespRoundTripTest, ParseSetWrongArgs) {
    // SET with only 1 arg (key, no value).
    run_parse_test("*2\r\n$3\r\nSET\r\n$3\r\nfoo\r\n", true, [](const auto& result) {
        ASSERT_TRUE(std::holds_alternative<kv::ErrorResp>(result));
    });
}

// ── Client-side response reading ─────────────────────────────────────────────

class RespClientTest : public ::testing::Test {
protected:
    void SetUp() override {
        spdlog::drop("kv");
        kv::init_default_logger();
    }

    void run_response_test(const std::string& wire_response,
                           std::function<void(const std::variant<kv::Response, kv::ErrorResp>&)> checker) {
        asio::io_context ioc;

        tcp::acceptor acceptor(ioc, tcp::endpoint(tcp::v4(), 0));
        auto port = acceptor.local_endpoint().port();

        // Server side: accept, send wire_response, close.
        asio::co_spawn(ioc, [&]() -> asio::awaitable<void> {
            auto [ec, socket] = co_await acceptor.async_accept(use_awaitable);
            if (ec) co_return;

            auto [wec, _] = co_await asio::async_write(
                socket, asio::buffer(wire_response), use_awaitable);

            boost::system::error_code sec;
            socket.shutdown(tcp::socket::shutdown_send, sec);
        }, asio::detached);

        // Client side: connect, read RESP response, call checker.
        asio::co_spawn(ioc, [&]() -> asio::awaitable<void> {
            tcp::socket socket(ioc);
            auto [ec] = co_await socket.async_connect(
                tcp::endpoint(tcp::v4(), port), use_awaitable);
            if (ec) co_return;

            auto result = co_await kv::network::read_resp_response(socket);
            checker(result);
        }, asio::detached);

        ioc.run();
    }
};

TEST_F(RespClientTest, ReadOk) {
    run_response_test("+OK\r\n", [](const auto& result) {
        ASSERT_TRUE(std::holds_alternative<kv::Response>(result));
        const auto& resp = std::get<kv::Response>(result);
        EXPECT_TRUE(std::holds_alternative<kv::OkResp>(resp));
    });
}

TEST_F(RespClientTest, ReadPong) {
    run_response_test("+PONG\r\n", [](const auto& result) {
        ASSERT_TRUE(std::holds_alternative<kv::Response>(result));
        const auto& resp = std::get<kv::Response>(result);
        EXPECT_TRUE(std::holds_alternative<kv::PongResp>(resp));
    });
}

TEST_F(RespClientTest, ReadBulkString) {
    run_response_test("$5\r\nhello\r\n", [](const auto& result) {
        ASSERT_TRUE(std::holds_alternative<kv::Response>(result));
        const auto& resp = std::get<kv::Response>(result);
        ASSERT_TRUE(std::holds_alternative<kv::ValueResp>(resp));
        EXPECT_EQ(std::get<kv::ValueResp>(resp).value, "hello");
    });
}

TEST_F(RespClientTest, ReadNullBulk) {
    run_response_test("$-1\r\n", [](const auto& result) {
        ASSERT_TRUE(std::holds_alternative<kv::Response>(result));
        const auto& resp = std::get<kv::Response>(result);
        EXPECT_TRUE(std::holds_alternative<kv::NotFoundResp>(resp));
    });
}

TEST_F(RespClientTest, ReadInteger) {
    run_response_test(":1\r\n", [](const auto& result) {
        ASSERT_TRUE(std::holds_alternative<kv::Response>(result));
        const auto& resp = std::get<kv::Response>(result);
        EXPECT_TRUE(std::holds_alternative<kv::DeletedResp>(resp));
    });
}

TEST_F(RespClientTest, ReadError) {
    run_response_test("-ERR something bad\r\n", [](const auto& result) {
        ASSERT_TRUE(std::holds_alternative<kv::Response>(result));
        const auto& resp = std::get<kv::Response>(result);
        ASSERT_TRUE(std::holds_alternative<kv::ErrorResp>(resp));
        EXPECT_EQ(std::get<kv::ErrorResp>(resp).message, "something bad");
    });
}

TEST_F(RespClientTest, ReadMoved) {
    run_response_test("-MOVED 0 127.0.0.1:6380\r\n", [](const auto& result) {
        ASSERT_TRUE(std::holds_alternative<kv::Response>(result));
        const auto& resp = std::get<kv::Response>(result);
        ASSERT_TRUE(std::holds_alternative<kv::RedirectResp>(resp));
        EXPECT_EQ(std::get<kv::RedirectResp>(resp).address, "127.0.0.1:6380");
    });
}

TEST_F(RespClientTest, ReadArray) {
    run_response_test("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", [](const auto& result) {
        ASSERT_TRUE(std::holds_alternative<kv::Response>(result));
        const auto& resp = std::get<kv::Response>(result);
        ASSERT_TRUE(std::holds_alternative<kv::KeysResp>(resp));
        const auto& keys = std::get<kv::KeysResp>(resp).keys;
        ASSERT_EQ(keys.size(), 2u);
        EXPECT_EQ(keys[0], "foo");
        EXPECT_EQ(keys[1], "bar");
    });
}

TEST_F(RespClientTest, ReadEmptyArray) {
    run_response_test("*0\r\n", [](const auto& result) {
        ASSERT_TRUE(std::holds_alternative<kv::Response>(result));
        const auto& resp = std::get<kv::Response>(result);
        ASSERT_TRUE(std::holds_alternative<kv::KeysResp>(resp));
        EXPECT_TRUE(std::get<kv::KeysResp>(resp).keys.empty());
    });
}

} // anonymous namespace
