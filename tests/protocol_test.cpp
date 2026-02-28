#include "network/protocol.hpp"

#include <algorithm>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace kv::network {

// ── Helpers ───────────────────────────────────────────────────────────────────

// Unwrap a parse result that is expected to be a Command.
static Command expect_command(std::variant<Command, ErrorResp> result) {
    EXPECT_TRUE(std::holds_alternative<Command>(result))
        << "Expected Command but got ErrorResp: "
        << (std::holds_alternative<ErrorResp>(result)
                ? std::get<ErrorResp>(result).message
                : "");
    return std::get<Command>(result);
}

// Unwrap a parse result that is expected to be an ErrorResp.
static ErrorResp expect_error(std::variant<Command, ErrorResp> result) {
    EXPECT_TRUE(std::holds_alternative<ErrorResp>(result))
        << "Expected ErrorResp but got a Command";
    return std::get<ErrorResp>(result);
}

// ── parse_command: PING ───────────────────────────────────────────────────────

TEST(ParseCommand, PingNoArgs) {
    auto cmd = expect_command(parse_command("PING"));
    EXPECT_TRUE(std::holds_alternative<PingCmd>(cmd));
}

TEST(ParseCommand, PingWithArgIsError) {
    auto err = expect_error(parse_command("PING extra"));
    EXPECT_FALSE(err.message.empty());
}

TEST(ParseCommand, PingCRLF) {
    // The parser must strip trailing \r.
    auto cmd = expect_command(parse_command("PING\r"));
    EXPECT_TRUE(std::holds_alternative<PingCmd>(cmd));
}

// ── parse_command: KEYS ───────────────────────────────────────────────────────

TEST(ParseCommand, KeysNoArgs) {
    auto cmd = expect_command(parse_command("KEYS"));
    EXPECT_TRUE(std::holds_alternative<KeysCmd>(cmd));
}

TEST(ParseCommand, KeysWithArgIsError) {
    auto err = expect_error(parse_command("KEYS extra"));
    EXPECT_FALSE(err.message.empty());
}

// ── parse_command: GET ────────────────────────────────────────────────────────

TEST(ParseCommand, GetValidKey) {
    auto cmd = expect_command(parse_command("GET mykey"));
    ASSERT_TRUE(std::holds_alternative<GetCmd>(cmd));
    EXPECT_EQ(std::get<GetCmd>(cmd).key, "mykey");
}

TEST(ParseCommand, GetMissingKeyIsError) {
    auto err = expect_error(parse_command("GET"));
    EXPECT_FALSE(err.message.empty());
}

TEST(ParseCommand, GetTwoTokensIsError) {
    auto err = expect_error(parse_command("GET key extra"));
    EXPECT_FALSE(err.message.empty());
}

TEST(ParseCommand, GetEmptyKeyAfterSpaceIsError) {
    // "GET " – one space, but empty key
    // split_once("") returns {"", ""} → key is empty but rest is also empty
    // so the "rest.empty()" guard fires first.
    auto err = expect_error(parse_command("GET "));
    EXPECT_FALSE(err.message.empty());
}

// ── parse_command: DEL ────────────────────────────────────────────────────────

TEST(ParseCommand, DelValidKey) {
    auto cmd = expect_command(parse_command("DEL target"));
    ASSERT_TRUE(std::holds_alternative<DelCmd>(cmd));
    EXPECT_EQ(std::get<DelCmd>(cmd).key, "target");
}

TEST(ParseCommand, DelMissingKeyIsError) {
    auto err = expect_error(parse_command("DEL"));
    EXPECT_FALSE(err.message.empty());
}

TEST(ParseCommand, DelTwoTokensIsError) {
    auto err = expect_error(parse_command("DEL key extra"));
    EXPECT_FALSE(err.message.empty());
}

// ── parse_command: SET ────────────────────────────────────────────────────────

TEST(ParseCommand, SetKeyValue) {
    auto cmd = expect_command(parse_command("SET foo bar"));
    ASSERT_TRUE(std::holds_alternative<SetCmd>(cmd));
    const auto& s = std::get<SetCmd>(cmd);
    EXPECT_EQ(s.key, "foo");
    EXPECT_EQ(s.value, "bar");
}

TEST(ParseCommand, SetValueWithSpaces) {
    auto cmd = expect_command(parse_command("SET greeting hello world foo"));
    ASSERT_TRUE(std::holds_alternative<SetCmd>(cmd));
    const auto& s = std::get<SetCmd>(cmd);
    EXPECT_EQ(s.key, "greeting");
    EXPECT_EQ(s.value, "hello world foo");
}

TEST(ParseCommand, SetMissingKeyIsError) {
    auto err = expect_error(parse_command("SET"));
    EXPECT_FALSE(err.message.empty());
}

TEST(ParseCommand, SetMissingValueIsError) {
    auto err = expect_error(parse_command("SET key"));
    EXPECT_FALSE(err.message.empty());
}

// ── parse_command: unknown verb ───────────────────────────────────────────────

TEST(ParseCommand, UnknownVerbIsError) {
    auto err = expect_error(parse_command("FOOBAR"));
    EXPECT_NE(err.message.find("unknown command"), std::string::npos);
}

TEST(ParseCommand, EmptyLineIsError) {
    auto err = expect_error(parse_command(""));
    EXPECT_FALSE(err.message.empty());
}

TEST(ParseCommand, LowercaseVerbIsError) {
    // Protocol is case-sensitive: verbs must be uppercase.
    auto err = expect_error(parse_command("get mykey"));
    EXPECT_FALSE(err.message.empty());
}

// ── serialize_response ────────────────────────────────────────────────────────

TEST(SerializeResponse, Ok) {
    EXPECT_EQ(serialize_response(OkResp{}), "OK\n");
}

TEST(SerializeResponse, Pong) {
    EXPECT_EQ(serialize_response(PongResp{}), "PONG\n");
}

TEST(SerializeResponse, NotFound) {
    EXPECT_EQ(serialize_response(NotFoundResp{}), "NOT_FOUND\n");
}

TEST(SerializeResponse, Deleted) {
    EXPECT_EQ(serialize_response(DeletedResp{}), "DELETED\n");
}

TEST(SerializeResponse, Value) {
    EXPECT_EQ(serialize_response(ValueResp{"hello"}), "VALUE hello\n");
}

TEST(SerializeResponse, ValueWithSpaces) {
    EXPECT_EQ(serialize_response(ValueResp{"hello world"}), "VALUE hello world\n");
}

TEST(SerializeResponse, KeysEmpty) {
    EXPECT_EQ(serialize_response(KeysResp{{}}), "KEYS\n");
}

TEST(SerializeResponse, KeysMultiple) {
    // Exact order depends on the caller – test that all keys appear.
    auto result = serialize_response(KeysResp{{"alpha", "beta", "gamma"}});
    EXPECT_EQ(result.back(), '\n');
    EXPECT_NE(result.find("alpha"), std::string::npos);
    EXPECT_NE(result.find("beta"), std::string::npos);
    EXPECT_NE(result.find("gamma"), std::string::npos);
    // Starts with "KEYS "
    EXPECT_EQ(result.substr(0, 5), "KEYS ");
}

TEST(SerializeResponse, Error) {
    EXPECT_EQ(serialize_response(ErrorResp{"bad input"}), "ERROR bad input\n");
}

TEST(SerializeResponse, ErrorAlwaysEndsWithNewline) {
    auto r = serialize_response(ErrorResp{"x"});
    EXPECT_EQ(r.back(), '\n');
}

// ── serialize_response: REDIRECT ─────────────────────────────────────────────

TEST(SerializeResponse, Redirect) {
    EXPECT_EQ(serialize_response(RedirectResp{"127.0.0.1:6001"}),
              "REDIRECT 127.0.0.1:6001\n");
}

TEST(SerializeResponse, RedirectAlwaysEndsWithNewline) {
    auto r = serialize_response(RedirectResp{"10.0.0.1:8080"});
    EXPECT_EQ(r.back(), '\n');
}

TEST(SerializeResponse, RedirectWithHostname) {
    EXPECT_EQ(serialize_response(RedirectResp{"node1.example.com:7001"}),
              "REDIRECT node1.example.com:7001\n");
}

// ── Round-trip: parse then serialize ─────────────────────────────────────────

TEST(RoundTrip, PingProducesPong) {
    auto parsed = parse_command("PING");
    ASSERT_TRUE(std::holds_alternative<Command>(parsed));
    ASSERT_TRUE(std::holds_alternative<PingCmd>(std::get<Command>(parsed)));
    EXPECT_EQ(serialize_response(PongResp{}), "PONG\n");
}

TEST(RoundTrip, SetProducesOk) {
    auto parsed = parse_command("SET k v");
    ASSERT_TRUE(std::holds_alternative<Command>(parsed));
    const auto& s = std::get<SetCmd>(std::get<Command>(parsed));
    EXPECT_EQ(s.key, "k");
    EXPECT_EQ(s.value, "v");
    EXPECT_EQ(serialize_response(OkResp{}), "OK\n");
}

} // namespace kv::network
