#include "network/binary_protocol.hpp"

#include <cstdint>
#include <span>
#include <string>
#include <variant>
#include <vector>

#include <gtest/gtest.h>

namespace kv::network {

// ── Helpers ───────────────────────────────────────────────────────────────────

// Unwrap a parse result expected to be a Command.
static Command expect_command(std::variant<Command, ErrorResp> result) {
    EXPECT_TRUE(std::holds_alternative<Command>(result))
        << "Expected Command but got ErrorResp: "
        << (std::holds_alternative<ErrorResp>(result)
                ? std::get<ErrorResp>(result).message
                : "");
    return std::get<Command>(result);
}

// Unwrap a parse result expected to be an ErrorResp.
static ErrorResp expect_parse_error(std::variant<Command, ErrorResp> result) {
    EXPECT_TRUE(std::holds_alternative<ErrorResp>(result))
        << "Expected ErrorResp but got a Command";
    return std::get<ErrorResp>(result);
}

// Unwrap a response parse result expected to be a Response.
static Response expect_response(std::variant<Response, ErrorResp> result) {
    EXPECT_TRUE(std::holds_alternative<Response>(result))
        << "Expected Response but got ErrorResp: "
        << (std::holds_alternative<ErrorResp>(result)
                ? std::get<ErrorResp>(result).message
                : "");
    return std::get<Response>(result);
}

// Unwrap a response parse result expected to be an ErrorResp.
static ErrorResp expect_response_error(std::variant<Response, ErrorResp> result) {
    EXPECT_TRUE(std::holds_alternative<ErrorResp>(result))
        << "Expected ErrorResp but got a Response";
    return std::get<ErrorResp>(result);
}

// ── is_binary_protocol ───────────────────────────────────────────────────────

TEST(BinaryProtocol, AutoDetectBinaryBytes) {
    // Bytes 0x00–0x1F should be detected as binary.
    for (uint8_t b = 0x00; b <= 0x1F; ++b) {
        EXPECT_TRUE(is_binary_protocol(b)) << "byte=" << static_cast<int>(b);
    }
}

TEST(BinaryProtocol, AutoDetectTextBytes) {
    // Bytes 0x20–0x7F should be detected as text.
    for (uint8_t b = 0x20; b <= 0x7F; ++b) {
        EXPECT_FALSE(is_binary_protocol(b)) << "byte=" << static_cast<int>(b);
    }
}

// ── read_binary_header ───────────────────────────────────────────────────────

TEST(BinaryProtocol, ReadHeaderValid) {
    // msg_type=0x01, payload_length=256 (0x00000100)
    std::vector<uint8_t> data = {0x01, 0x00, 0x00, 0x01, 0x00};
    uint8_t msg_type = 0;
    uint32_t payload_len = 0;
    EXPECT_TRUE(read_binary_header(data, msg_type, payload_len));
    EXPECT_EQ(msg_type, 0x01);
    EXPECT_EQ(payload_len, 256u);
}

TEST(BinaryProtocol, ReadHeaderTooShort) {
    std::vector<uint8_t> data = {0x01, 0x00, 0x00};
    uint8_t msg_type = 0;
    uint32_t payload_len = 0;
    EXPECT_FALSE(read_binary_header(data, msg_type, payload_len));
}

TEST(BinaryProtocol, ReadHeaderEmpty) {
    std::span<const uint8_t> data;
    uint8_t msg_type = 0;
    uint32_t payload_len = 0;
    EXPECT_FALSE(read_binary_header(data, msg_type, payload_len));
}

TEST(BinaryProtocol, ReadHeaderZeroPayload) {
    std::vector<uint8_t> data = {0x05, 0x00, 0x00, 0x00, 0x00};
    uint8_t msg_type = 0;
    uint32_t payload_len = 0;
    EXPECT_TRUE(read_binary_header(data, msg_type, payload_len));
    EXPECT_EQ(msg_type, 0x05);
    EXPECT_EQ(payload_len, 0u);
}

// ── parse_binary_request: PING ───────────────────────────────────────────────

TEST(BinaryParseRequest, PingEmptyPayload) {
    auto cmd = expect_command(
        parse_binary_request(binary::kMsgPing, {}));
    EXPECT_TRUE(std::holds_alternative<PingCmd>(cmd));
}

// ── parse_binary_request: KEYS ───────────────────────────────────────────────

TEST(BinaryParseRequest, KeysEmptyPayload) {
    auto cmd = expect_command(
        parse_binary_request(binary::kMsgKeys, {}));
    EXPECT_TRUE(std::holds_alternative<KeysCmd>(cmd));
}

// ── parse_binary_request: GET ────────────────────────────────────────────────

TEST(BinaryParseRequest, GetValidKey) {
    // key_len=3 (BE), key="foo"
    std::vector<uint8_t> payload = {0x00, 0x03, 'f', 'o', 'o'};
    auto cmd = expect_command(
        parse_binary_request(binary::kMsgGet, payload));
    ASSERT_TRUE(std::holds_alternative<GetCmd>(cmd));
    EXPECT_EQ(std::get<GetCmd>(cmd).key, "foo");
}

TEST(BinaryParseRequest, GetEmptyKeyIsError) {
    // key_len=0
    std::vector<uint8_t> payload = {0x00, 0x00};
    auto err = expect_parse_error(
        parse_binary_request(binary::kMsgGet, payload));
    EXPECT_FALSE(err.message.empty());
}

TEST(BinaryParseRequest, GetTruncatedKeyLenIsError) {
    // Only 1 byte, need 2 for key_len.
    std::vector<uint8_t> payload = {0x00};
    auto err = expect_parse_error(
        parse_binary_request(binary::kMsgGet, payload));
    EXPECT_FALSE(err.message.empty());
}

TEST(BinaryParseRequest, GetTruncatedKeyIsError) {
    // key_len=5 but only 2 bytes of key data.
    std::vector<uint8_t> payload = {0x00, 0x05, 'a', 'b'};
    auto err = expect_parse_error(
        parse_binary_request(binary::kMsgGet, payload));
    EXPECT_FALSE(err.message.empty());
}

// ── parse_binary_request: DEL ────────────────────────────────────────────────

TEST(BinaryParseRequest, DelValidKey) {
    std::vector<uint8_t> payload = {0x00, 0x04, 't', 'e', 's', 't'};
    auto cmd = expect_command(
        parse_binary_request(binary::kMsgDel, payload));
    ASSERT_TRUE(std::holds_alternative<DelCmd>(cmd));
    EXPECT_EQ(std::get<DelCmd>(cmd).key, "test");
}

TEST(BinaryParseRequest, DelEmptyKeyIsError) {
    std::vector<uint8_t> payload = {0x00, 0x00};
    auto err = expect_parse_error(
        parse_binary_request(binary::kMsgDel, payload));
    EXPECT_FALSE(err.message.empty());
}

// ── parse_binary_request: SET ────────────────────────────────────────────────

TEST(BinaryParseRequest, SetKeyValue) {
    // key_len=3 "abc", value_len=5 "hello"
    std::vector<uint8_t> payload = {
        0x00, 0x03, 'a', 'b', 'c',                 // key
        0x00, 0x00, 0x00, 0x05, 'h', 'e', 'l', 'l', 'o'  // value
    };
    auto cmd = expect_command(
        parse_binary_request(binary::kMsgSet, payload));
    ASSERT_TRUE(std::holds_alternative<SetCmd>(cmd));
    EXPECT_EQ(std::get<SetCmd>(cmd).key, "abc");
    EXPECT_EQ(std::get<SetCmd>(cmd).value, "hello");
}

TEST(BinaryParseRequest, SetEmptyValue) {
    // key_len=1 "k", value_len=0 ""
    std::vector<uint8_t> payload = {
        0x00, 0x01, 'k',
        0x00, 0x00, 0x00, 0x00
    };
    auto cmd = expect_command(
        parse_binary_request(binary::kMsgSet, payload));
    ASSERT_TRUE(std::holds_alternative<SetCmd>(cmd));
    EXPECT_EQ(std::get<SetCmd>(cmd).key, "k");
    EXPECT_EQ(std::get<SetCmd>(cmd).value, "");
}

TEST(BinaryParseRequest, SetEmptyKeyIsError) {
    std::vector<uint8_t> payload = {
        0x00, 0x00,                       // key_len=0
        0x00, 0x00, 0x00, 0x01, 'v'       // value
    };
    auto err = expect_parse_error(
        parse_binary_request(binary::kMsgSet, payload));
    EXPECT_FALSE(err.message.empty());
}

TEST(BinaryParseRequest, SetTruncatedValueLenIsError) {
    // key_len=1 "k" but then only 2 bytes for value_len (need 4).
    std::vector<uint8_t> payload = {
        0x00, 0x01, 'k',
        0x00, 0x00
    };
    auto err = expect_parse_error(
        parse_binary_request(binary::kMsgSet, payload));
    EXPECT_FALSE(err.message.empty());
}

TEST(BinaryParseRequest, SetTruncatedValueIsError) {
    // key_len=1 "k", value_len=10 but only 3 bytes of value data.
    std::vector<uint8_t> payload = {
        0x00, 0x01, 'k',
        0x00, 0x00, 0x00, 0x0A, 'a', 'b', 'c'
    };
    auto err = expect_parse_error(
        parse_binary_request(binary::kMsgSet, payload));
    EXPECT_FALSE(err.message.empty());
}

// ── parse_binary_request: unknown msg_type ───────────────────────────────────

TEST(BinaryParseRequest, UnknownMsgTypeIsError) {
    auto err = expect_parse_error(
        parse_binary_request(0xFF, {}));
    EXPECT_NE(err.message.find("ff"), std::string::npos);
}

// ── serialize_binary_response ────────────────────────────────────────────────

TEST(BinarySerializeResponse, OkResponse) {
    auto buf = serialize_binary_response(OkResp{});
    ASSERT_EQ(buf.size(), binary::kHeaderSize);
    EXPECT_EQ(buf[0], binary::kStatusOk);
    // Payload length = 0
    EXPECT_EQ(buf[1], 0x00);
    EXPECT_EQ(buf[2], 0x00);
    EXPECT_EQ(buf[3], 0x00);
    EXPECT_EQ(buf[4], 0x00);
}

TEST(BinarySerializeResponse, PongResponse) {
    auto buf = serialize_binary_response(PongResp{});
    ASSERT_EQ(buf.size(), binary::kHeaderSize);
    EXPECT_EQ(buf[0], binary::kStatusPong);
}

TEST(BinarySerializeResponse, NotFoundResponse) {
    auto buf = serialize_binary_response(NotFoundResp{});
    ASSERT_EQ(buf.size(), binary::kHeaderSize);
    EXPECT_EQ(buf[0], binary::kStatusNotFound);
}

TEST(BinarySerializeResponse, DeletedResponse) {
    auto buf = serialize_binary_response(DeletedResp{});
    ASSERT_EQ(buf.size(), binary::kHeaderSize);
    EXPECT_EQ(buf[0], binary::kStatusDeleted);
}

TEST(BinarySerializeResponse, ValueResponse) {
    auto buf = serialize_binary_response(ValueResp{"data"});
    // header(5) + value_len(4) + "data"(4) = 13
    ASSERT_EQ(buf.size(), 13u);
    EXPECT_EQ(buf[0], binary::kStatusValue);
    // payload_length = 8 (4 for value_len + 4 for value)
    EXPECT_EQ(buf[1], 0x00);
    EXPECT_EQ(buf[2], 0x00);
    EXPECT_EQ(buf[3], 0x00);
    EXPECT_EQ(buf[4], 0x08);
    // value_len = 4
    EXPECT_EQ(buf[5], 0x00);
    EXPECT_EQ(buf[6], 0x00);
    EXPECT_EQ(buf[7], 0x00);
    EXPECT_EQ(buf[8], 0x04);
    // value
    EXPECT_EQ(buf[9], 'd');
    EXPECT_EQ(buf[10], 'a');
    EXPECT_EQ(buf[11], 't');
    EXPECT_EQ(buf[12], 'a');
}

TEST(BinarySerializeResponse, KeysResponseEmpty) {
    auto buf = serialize_binary_response(KeysResp{{}});
    // header(5) + count(4) = 9
    ASSERT_EQ(buf.size(), 9u);
    EXPECT_EQ(buf[0], binary::kStatusKeys);
    // count = 0
    EXPECT_EQ(buf[5], 0x00);
    EXPECT_EQ(buf[6], 0x00);
    EXPECT_EQ(buf[7], 0x00);
    EXPECT_EQ(buf[8], 0x00);
}

TEST(BinarySerializeResponse, KeysResponseMultiple) {
    auto buf = serialize_binary_response(KeysResp{{"a", "bb"}});
    // header(5) + count(4) + key_len(2)+"a"(1) + key_len(2)+"bb"(2) = 16
    ASSERT_EQ(buf.size(), 16u);
    EXPECT_EQ(buf[0], binary::kStatusKeys);
    // count = 2
    EXPECT_EQ(buf[8], 0x02);
    // first key: len=1 "a"
    EXPECT_EQ(buf[9], 0x00);
    EXPECT_EQ(buf[10], 0x01);
    EXPECT_EQ(buf[11], 'a');
    // second key: len=2 "bb"
    EXPECT_EQ(buf[12], 0x00);
    EXPECT_EQ(buf[13], 0x02);
    EXPECT_EQ(buf[14], 'b');
    EXPECT_EQ(buf[15], 'b');
}

TEST(BinarySerializeResponse, ErrorResponse) {
    auto buf = serialize_binary_response(ErrorResp{"fail"});
    // header(5) + msg_len(2) + "fail"(4) = 11
    ASSERT_EQ(buf.size(), 11u);
    EXPECT_EQ(buf[0], binary::kStatusError);
}

TEST(BinarySerializeResponse, RedirectResponse) {
    auto buf = serialize_binary_response(RedirectResp{"127.0.0.1:5000"});
    // header(5) + addr_len(2) + "127.0.0.1:5000"(14) = 21
    ASSERT_EQ(buf.size(), 21u);
    EXPECT_EQ(buf[0], binary::kStatusRedirect);
}

// ── serialize_binary_request ─────────────────────────────────────────────────

TEST(BinarySerializeRequest, PingRequest) {
    auto buf = serialize_binary_request(PingCmd{});
    ASSERT_EQ(buf.size(), binary::kHeaderSize);
    EXPECT_EQ(buf[0], binary::kMsgPing);
    // payload_length = 0
    EXPECT_EQ(buf[4], 0x00);
}

TEST(BinarySerializeRequest, KeysRequest) {
    auto buf = serialize_binary_request(KeysCmd{});
    ASSERT_EQ(buf.size(), binary::kHeaderSize);
    EXPECT_EQ(buf[0], binary::kMsgKeys);
}

TEST(BinarySerializeRequest, GetRequest) {
    auto buf = serialize_binary_request(GetCmd{"mykey"});
    // header(5) + key_len(2) + "mykey"(5) = 12
    ASSERT_EQ(buf.size(), 12u);
    EXPECT_EQ(buf[0], binary::kMsgGet);
    // payload_length = 7
    EXPECT_EQ(buf[4], 0x07);
    // key_len = 5
    EXPECT_EQ(buf[5], 0x00);
    EXPECT_EQ(buf[6], 0x05);
}

TEST(BinarySerializeRequest, DelRequest) {
    auto buf = serialize_binary_request(DelCmd{"x"});
    // header(5) + key_len(2) + "x"(1) = 8
    ASSERT_EQ(buf.size(), 8u);
    EXPECT_EQ(buf[0], binary::kMsgDel);
}

TEST(BinarySerializeRequest, SetRequest) {
    auto buf = serialize_binary_request(SetCmd{"k", "v"});
    // header(5) + key_len(2) + "k"(1) + value_len(4) + "v"(1) = 13
    ASSERT_EQ(buf.size(), 13u);
    EXPECT_EQ(buf[0], binary::kMsgSet);
    // payload_length = 8  (2+1+4+1)
    EXPECT_EQ(buf[4], 0x08);
}

// ── parse_binary_response ────────────────────────────────────────────────────

TEST(BinaryParseResponse, OkResponse) {
    auto resp = expect_response(
        parse_binary_response(binary::kStatusOk, {}));
    EXPECT_TRUE(std::holds_alternative<OkResp>(resp));
}

TEST(BinaryParseResponse, PongResponse) {
    auto resp = expect_response(
        parse_binary_response(binary::kStatusPong, {}));
    EXPECT_TRUE(std::holds_alternative<PongResp>(resp));
}

TEST(BinaryParseResponse, NotFoundResponse) {
    auto resp = expect_response(
        parse_binary_response(binary::kStatusNotFound, {}));
    EXPECT_TRUE(std::holds_alternative<NotFoundResp>(resp));
}

TEST(BinaryParseResponse, DeletedResponse) {
    auto resp = expect_response(
        parse_binary_response(binary::kStatusDeleted, {}));
    EXPECT_TRUE(std::holds_alternative<DeletedResp>(resp));
}

TEST(BinaryParseResponse, ValueResponse) {
    // value_len=3 "abc"
    std::vector<uint8_t> payload = {0x00, 0x00, 0x00, 0x03, 'a', 'b', 'c'};
    auto resp = expect_response(
        parse_binary_response(binary::kStatusValue, payload));
    ASSERT_TRUE(std::holds_alternative<ValueResp>(resp));
    EXPECT_EQ(std::get<ValueResp>(resp).value, "abc");
}

TEST(BinaryParseResponse, ValueTruncatedIsError) {
    // value_len=10 but only 2 bytes of data.
    std::vector<uint8_t> payload = {0x00, 0x00, 0x00, 0x0A, 'a', 'b'};
    auto err = expect_response_error(
        parse_binary_response(binary::kStatusValue, payload));
    EXPECT_FALSE(err.message.empty());
}

TEST(BinaryParseResponse, KeysResponse) {
    // count=2, key1="x", key2="yy"
    std::vector<uint8_t> payload = {
        0x00, 0x00, 0x00, 0x02,  // count=2
        0x00, 0x01, 'x',         // key1
        0x00, 0x02, 'y', 'y'     // key2
    };
    auto resp = expect_response(
        parse_binary_response(binary::kStatusKeys, payload));
    ASSERT_TRUE(std::holds_alternative<KeysResp>(resp));
    const auto& keys = std::get<KeysResp>(resp).keys;
    ASSERT_EQ(keys.size(), 2u);
    EXPECT_EQ(keys[0], "x");
    EXPECT_EQ(keys[1], "yy");
}

TEST(BinaryParseResponse, KeysTruncatedIsError) {
    // count=1 but no key data.
    std::vector<uint8_t> payload = {0x00, 0x00, 0x00, 0x01};
    auto err = expect_response_error(
        parse_binary_response(binary::kStatusKeys, payload));
    EXPECT_FALSE(err.message.empty());
}

TEST(BinaryParseResponse, ErrorResponse) {
    // msg_len=3 "err"
    std::vector<uint8_t> payload = {0x00, 0x03, 'e', 'r', 'r'};
    auto resp = expect_response(
        parse_binary_response(binary::kStatusError, payload));
    ASSERT_TRUE(std::holds_alternative<ErrorResp>(resp));
    EXPECT_EQ(std::get<ErrorResp>(resp).message, "err");
}

TEST(BinaryParseResponse, RedirectResponse) {
    // addr_len=9 "host:1234"
    std::vector<uint8_t> payload = {
        0x00, 0x09, 'h', 'o', 's', 't', ':', '1', '2', '3', '4'
    };
    auto resp = expect_response(
        parse_binary_response(binary::kStatusRedirect, payload));
    ASSERT_TRUE(std::holds_alternative<RedirectResp>(resp));
    EXPECT_EQ(std::get<RedirectResp>(resp).address, "host:1234");
}

TEST(BinaryParseResponse, UnknownStatusIsError) {
    auto err = expect_response_error(
        parse_binary_response(0xAB, {}));
    EXPECT_NE(err.message.find("ab"), std::string::npos);
}

// ── Round-trip tests (serialize → header → parse) ────────────────────────────

TEST(BinaryRoundTrip, PingPong) {
    // Serialize PING request, parse it back.
    auto buf = serialize_binary_request(PingCmd{});
    uint8_t msg_type = 0;
    uint32_t payload_len = 0;
    ASSERT_TRUE(read_binary_header(buf, msg_type, payload_len));
    EXPECT_EQ(msg_type, binary::kMsgPing);
    EXPECT_EQ(payload_len, 0u);

    auto cmd = expect_command(parse_binary_request(
        msg_type, std::span<const uint8_t>(buf).subspan(binary::kHeaderSize)));
    EXPECT_TRUE(std::holds_alternative<PingCmd>(cmd));

    // Serialize PONG response, parse it back.
    auto resp_buf = serialize_binary_response(PongResp{});
    uint8_t status = 0;
    uint32_t resp_payload_len = 0;
    ASSERT_TRUE(read_binary_header(resp_buf, status, resp_payload_len));
    auto resp = expect_response(parse_binary_response(
        status, std::span<const uint8_t>(resp_buf).subspan(binary::kHeaderSize)));
    EXPECT_TRUE(std::holds_alternative<PongResp>(resp));
}

TEST(BinaryRoundTrip, SetAndOk) {
    // Serialize SET request.
    auto buf = serialize_binary_request(SetCmd{"mykey", "myvalue"});
    uint8_t msg_type = 0;
    uint32_t payload_len = 0;
    ASSERT_TRUE(read_binary_header(buf, msg_type, payload_len));
    EXPECT_EQ(msg_type, binary::kMsgSet);

    auto cmd = expect_command(parse_binary_request(
        msg_type, std::span<const uint8_t>(buf).subspan(binary::kHeaderSize, payload_len)));
    ASSERT_TRUE(std::holds_alternative<SetCmd>(cmd));
    EXPECT_EQ(std::get<SetCmd>(cmd).key, "mykey");
    EXPECT_EQ(std::get<SetCmd>(cmd).value, "myvalue");

    // Serialize OK response.
    auto resp_buf = serialize_binary_response(OkResp{});
    uint8_t status = 0;
    uint32_t resp_payload_len = 0;
    ASSERT_TRUE(read_binary_header(resp_buf, status, resp_payload_len));
    auto resp = expect_response(parse_binary_response(
        status, std::span<const uint8_t>(resp_buf).subspan(binary::kHeaderSize)));
    EXPECT_TRUE(std::holds_alternative<OkResp>(resp));
}

TEST(BinaryRoundTrip, GetAndValue) {
    // Serialize GET request.
    auto buf = serialize_binary_request(GetCmd{"thekey"});
    uint8_t msg_type = 0;
    uint32_t payload_len = 0;
    ASSERT_TRUE(read_binary_header(buf, msg_type, payload_len));

    auto cmd = expect_command(parse_binary_request(
        msg_type, std::span<const uint8_t>(buf).subspan(binary::kHeaderSize, payload_len)));
    ASSERT_TRUE(std::holds_alternative<GetCmd>(cmd));
    EXPECT_EQ(std::get<GetCmd>(cmd).key, "thekey");

    // Serialize VALUE response.
    auto resp_buf = serialize_binary_response(ValueResp{"thevalue"});
    uint8_t status = 0;
    uint32_t resp_payload_len = 0;
    ASSERT_TRUE(read_binary_header(resp_buf, status, resp_payload_len));
    auto resp = expect_response(parse_binary_response(
        status, std::span<const uint8_t>(resp_buf).subspan(binary::kHeaderSize, resp_payload_len)));
    ASSERT_TRUE(std::holds_alternative<ValueResp>(resp));
    EXPECT_EQ(std::get<ValueResp>(resp).value, "thevalue");
}

TEST(BinaryRoundTrip, KeysRoundTrip) {
    // Serialize KEYS request.
    auto buf = serialize_binary_request(KeysCmd{});
    uint8_t msg_type = 0;
    uint32_t payload_len = 0;
    ASSERT_TRUE(read_binary_header(buf, msg_type, payload_len));
    auto cmd = expect_command(parse_binary_request(
        msg_type, std::span<const uint8_t>(buf).subspan(binary::kHeaderSize)));
    EXPECT_TRUE(std::holds_alternative<KeysCmd>(cmd));

    // Serialize KEYS response with multiple keys.
    auto resp_buf = serialize_binary_response(KeysResp{{"alpha", "beta", "gamma"}});
    uint8_t status = 0;
    uint32_t resp_payload_len = 0;
    ASSERT_TRUE(read_binary_header(resp_buf, status, resp_payload_len));
    auto resp = expect_response(parse_binary_response(
        status, std::span<const uint8_t>(resp_buf).subspan(binary::kHeaderSize, resp_payload_len)));
    ASSERT_TRUE(std::holds_alternative<KeysResp>(resp));
    const auto& keys = std::get<KeysResp>(resp).keys;
    ASSERT_EQ(keys.size(), 3u);
    EXPECT_EQ(keys[0], "alpha");
    EXPECT_EQ(keys[1], "beta");
    EXPECT_EQ(keys[2], "gamma");
}

TEST(BinaryRoundTrip, DelAndDeleted) {
    auto buf = serialize_binary_request(DelCmd{"gone"});
    uint8_t msg_type = 0;
    uint32_t payload_len = 0;
    ASSERT_TRUE(read_binary_header(buf, msg_type, payload_len));

    auto cmd = expect_command(parse_binary_request(
        msg_type, std::span<const uint8_t>(buf).subspan(binary::kHeaderSize, payload_len)));
    ASSERT_TRUE(std::holds_alternative<DelCmd>(cmd));
    EXPECT_EQ(std::get<DelCmd>(cmd).key, "gone");

    auto resp_buf = serialize_binary_response(DeletedResp{});
    uint8_t status = 0;
    uint32_t resp_payload_len = 0;
    ASSERT_TRUE(read_binary_header(resp_buf, status, resp_payload_len));
    auto resp = expect_response(parse_binary_response(
        status, std::span<const uint8_t>(resp_buf).subspan(binary::kHeaderSize)));
    EXPECT_TRUE(std::holds_alternative<DeletedResp>(resp));
}

TEST(BinaryRoundTrip, ErrorRoundTrip) {
    auto resp_buf = serialize_binary_response(ErrorResp{"something went wrong"});
    uint8_t status = 0;
    uint32_t resp_payload_len = 0;
    ASSERT_TRUE(read_binary_header(resp_buf, status, resp_payload_len));
    auto resp = expect_response(parse_binary_response(
        status, std::span<const uint8_t>(resp_buf).subspan(binary::kHeaderSize, resp_payload_len)));
    ASSERT_TRUE(std::holds_alternative<ErrorResp>(resp));
    EXPECT_EQ(std::get<ErrorResp>(resp).message, "something went wrong");
}

TEST(BinaryRoundTrip, RedirectRoundTrip) {
    auto resp_buf = serialize_binary_response(RedirectResp{"10.0.0.1:7000"});
    uint8_t status = 0;
    uint32_t resp_payload_len = 0;
    ASSERT_TRUE(read_binary_header(resp_buf, status, resp_payload_len));
    auto resp = expect_response(parse_binary_response(
        status, std::span<const uint8_t>(resp_buf).subspan(binary::kHeaderSize, resp_payload_len)));
    ASSERT_TRUE(std::holds_alternative<RedirectResp>(resp));
    EXPECT_EQ(std::get<RedirectResp>(resp).address, "10.0.0.1:7000");
}

} // namespace kv::network
