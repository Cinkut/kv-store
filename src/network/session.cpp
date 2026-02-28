#include "network/session.hpp"
#include "network/binary_protocol.hpp"
#include "network/protocol.hpp"
#include "network/resp_protocol.hpp"

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/write.hpp>
#include <boost/system/error_code.hpp>

#include <spdlog/spdlog.h>

#include <cstdint>
#include <string>
#include <vector>

namespace kv::network {

namespace {
constexpr auto use_awaitable = boost::asio::as_tuple(boost::asio::use_awaitable);
} // namespace

Session::Session(boost::asio::ip::tcp::socket socket, StorageEngine& storage)
    : socket_(std::move(socket)), storage_(storage) {}

Session::Session(boost::asio::ip::tcp::socket socket, StorageEngine& storage,
                 ClusterContext& cluster_ctx)
    : socket_(std::move(socket)), storage_(storage), cluster_ctx_(&cluster_ctx) {}

boost::asio::awaitable<void> Session::run() {
    const auto remote = [&]() -> std::string {
        boost::system::error_code ec;
        const auto ep = socket_.remote_endpoint(ec);
        return ec ? "<unknown>" : ep.address().to_string() + ":" + std::to_string(ep.port());
    }();

    spdlog::debug("Session::run() - client connected from {}", remote);

    // Read the first byte to determine protocol.
    uint8_t first_byte = 0;
    auto [ec, n] = co_await boost::asio::async_read(
        socket_, boost::asio::buffer(&first_byte, 1), use_awaitable);

    if (ec || n == 0) {
        spdlog::debug("Session::run() - client disconnected before first byte: {}", remote);
        co_return;
    }

    if (is_binary_protocol(first_byte)) {
        spdlog::debug("Session {}: detected binary protocol (first byte 0x{:02x})",
                       remote, first_byte);

        // Read the remaining 4 bytes of the first header.
        std::vector<uint8_t> first_header(binary::kHeaderSize);
        first_header[0] = first_byte;

        auto [hec, hn] = co_await boost::asio::async_read(
            socket_,
            boost::asio::buffer(first_header.data() + 1, binary::kHeaderSize - 1),
            use_awaitable);

        if (hec) {
            spdlog::debug("Session {}: binary header read error: {}", remote, hec.message());
            co_return;
        }

        co_await run_binary(remote, std::move(first_header));
    } else if (is_resp_protocol(first_byte)) {
        spdlog::debug("Session {}: detected RESP protocol (first byte 0x{:02x})",
                       remote, first_byte);
        co_await run_resp(remote, first_byte);
    } else {
        spdlog::debug("Session {}: detected text protocol (first byte 0x{:02x})",
                       remote, first_byte);

        // Seed the text buffer with the first byte already consumed.
        std::string seed(1, static_cast<char>(first_byte));
        co_await run_text(remote, std::move(seed));
    }

    spdlog::debug("Session::run() - client disconnected: {}", remote);
}

boost::asio::awaitable<void> Session::run_text(const std::string& remote,
                                                std::string seed) {
    std::string buf = std::move(seed);
    buf.reserve(256);

    for (;;) {
        // Read one newline-delimited line.
        auto [ec, n] = co_await boost::asio::async_read_until(
            socket_, boost::asio::dynamic_buffer(buf), '\n', use_awaitable);

        if (ec) {
            if (ec != boost::asio::error::eof &&
                ec != boost::asio::error::connection_reset) {
                spdlog::warn("Session {}: read error: {}", remote, ec.message());
            }
            break;
        }

        // Extract the line (everything up to and including '\n').
        std::string line = buf.substr(0, n - 1); // strip the '\n'
        buf.erase(0, n);

        // Strip trailing '\r' (CRLF tolerance).
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }

        spdlog::debug("Session {}: recv '{}'", remote, line);

        // Parse.
        auto parse_result = parse_command(line);

        Response response = co_await process_request(parse_result);

        // Serialize and send.
        const std::string wire = serialize_response(response);
        spdlog::debug("Session {}: send '{}'", remote, wire.substr(0, wire.size() - 1));

        auto [wec, _] = co_await boost::asio::async_write(
            socket_, boost::asio::buffer(wire), use_awaitable);

        if (wec) {
            spdlog::warn("Session {}: write error: {}", remote, wec.message());
            break;
        }
    }
}

boost::asio::awaitable<void> Session::run_binary(const std::string& remote,
                                                  std::vector<uint8_t> first_header) {
    std::vector<uint8_t> header_buf(binary::kHeaderSize);
    bool have_first_header = true;

    for (;;) {
        if (have_first_header) {
            header_buf = std::move(first_header);
            have_first_header = false;
        } else {
            // Read the 5-byte header.
            auto [ec, n] = co_await boost::asio::async_read(
                socket_, boost::asio::buffer(header_buf), use_awaitable);

            if (ec) {
                if (ec != boost::asio::error::eof &&
                    ec != boost::asio::error::connection_reset) {
                    spdlog::warn("Session {}: binary read header error: {}",
                                 remote, ec.message());
                }
                break;
            }
        }

        uint8_t msg_type = 0;
        uint32_t payload_len = 0;
        read_binary_header(header_buf, msg_type, payload_len);

        // Read the payload.
        std::vector<uint8_t> payload(payload_len);
        if (payload_len > 0) {
            auto [pec, pn] = co_await boost::asio::async_read(
                socket_, boost::asio::buffer(payload), use_awaitable);

            if (pec) {
                spdlog::warn("Session {}: binary read payload error: {}",
                             remote, pec.message());
                break;
            }
        }

        spdlog::debug("Session {}: binary recv msg_type=0x{:02x} payload_len={}",
                       remote, msg_type, payload_len);

        // Parse the request.
        auto parse_result = parse_binary_request(msg_type, payload);

        Response response = co_await process_request(parse_result);

        // Serialize and send.
        auto wire = serialize_binary_response(response);
        spdlog::debug("Session {}: binary send status=0x{:02x} payload_len={}",
                       remote, wire[0], wire.size() - binary::kHeaderSize);

        auto [wec, _] = co_await boost::asio::async_write(
            socket_, boost::asio::buffer(wire), use_awaitable);

        if (wec) {
            spdlog::warn("Session {}: binary write error: {}", remote, wec.message());
            break;
        }
    }
}

boost::asio::awaitable<Response>
Session::process_request(std::variant<Command, ErrorResp>& parse_result) {
    if (std::holds_alternative<ErrorResp>(parse_result)) {
        co_return std::get<ErrorResp>(parse_result);
    }
    co_return co_await dispatch(std::get<Command>(parse_result));
}

boost::asio::awaitable<void> Session::run_resp(const std::string& remote,
                                                uint8_t first_byte) {
    std::string buf;
    buf.reserve(256);

    for (;;) {
        // Parse one RESP request.
        auto parse_result = co_await parse_resp_request(socket_, first_byte, buf);

        Response response = co_await process_request(parse_result);

        // Serialize and send RESP response.
        const std::string wire = serialize_resp_response(response);

        auto [wec, _] = co_await boost::asio::async_write(
            socket_, boost::asio::buffer(wire), use_awaitable);

        if (wec) {
            spdlog::warn("Session {}: RESP write error: {}", remote, wec.message());
            break;
        }

        // For subsequent requests, read the first byte of the next command.
        // But first check if the buf already has data from a previous read_until.
        if (!buf.empty()) {
            first_byte = static_cast<uint8_t>(buf[0]);
            buf.erase(0, 1);
        } else {
            auto [ec, n] = co_await boost::asio::async_read(
                socket_, boost::asio::buffer(&first_byte, 1), use_awaitable);

            if (ec) {
                if (ec != boost::asio::error::eof &&
                    ec != boost::asio::error::connection_reset) {
                    spdlog::warn("Session {}: RESP read error: {}", remote, ec.message());
                }
                break;
            }
        }
    }
}

boost::asio::awaitable<Response> Session::dispatch(const Command& cmd) {
    co_return co_await std::visit(
        [&](const auto& c) -> boost::asio::awaitable<Response> {
            using T = std::decay_t<decltype(c)>;

            if constexpr (std::is_same_v<T, PingCmd>) {
                co_return PongResp{};

            } else if constexpr (std::is_same_v<T, GetCmd>) {
                // In cluster mode, only the leader with a valid lease serves reads.
                if (cluster_ctx_) {
                    if (!cluster_ctx_->is_leader()) {
                        auto addr = cluster_ctx_->leader_address();
                        if (addr) {
                            co_return RedirectResp{std::move(*addr)};
                        }
                        co_return ErrorResp{"no leader available"};
                    }
                    if (!cluster_ctx_->has_read_lease()) {
                        co_return ErrorResp{"no read lease"};
                    }
                }
                auto val = storage_.get(c.key);
                if (!val.has_value()) {
                    co_return NotFoundResp{};
                }
                co_return ValueResp{std::move(*val)};

            } else if constexpr (std::is_same_v<T, SetCmd>) {
                // In cluster mode, only the leader processes writes.
                if (cluster_ctx_ && !cluster_ctx_->is_leader()) {
                    auto addr = cluster_ctx_->leader_address();
                    if (addr) {
                        co_return RedirectResp{std::move(*addr)};
                    }
                    co_return ErrorResp{"no leader available"};
                }
                // In cluster mode (leader), submit through Raft.
                if (cluster_ctx_) {
                    bool ok = co_await cluster_ctx_->submit_write(c.key, c.value, 1);
                    if (!ok) {
                        co_return ErrorResp{"commit failed or timed out"};
                    }
                    co_return OkResp{};
                }
                // Standalone mode — write directly.
                storage_.set(c.key, c.value);
                co_return OkResp{};

            } else if constexpr (std::is_same_v<T, DelCmd>) {
                // In cluster mode, only the leader processes writes.
                if (cluster_ctx_ && !cluster_ctx_->is_leader()) {
                    auto addr = cluster_ctx_->leader_address();
                    if (addr) {
                        co_return RedirectResp{std::move(*addr)};
                    }
                    co_return ErrorResp{"no leader available"};
                }
                // In cluster mode (leader), submit through Raft.
                if (cluster_ctx_) {
                    bool ok = co_await cluster_ctx_->submit_write(c.key, "", 2);
                    if (!ok) {
                        co_return ErrorResp{"commit failed or timed out"};
                    }
                    // For DEL via Raft, we always return DELETED (the entry was committed).
                    // The actual key existence check happens at apply time.
                    co_return DeletedResp{};
                }
                // Standalone mode — delete directly.
                const bool removed = storage_.del(c.key);
                if (!removed) {
                    co_return NotFoundResp{};
                }
                co_return DeletedResp{};

            } else if constexpr (std::is_same_v<T, KeysCmd>) {
                // In cluster mode, only the leader with a valid lease serves reads.
                if (cluster_ctx_) {
                    if (!cluster_ctx_->is_leader()) {
                        auto addr = cluster_ctx_->leader_address();
                        if (addr) {
                            co_return RedirectResp{std::move(*addr)};
                        }
                        co_return ErrorResp{"no leader available"};
                    }
                    if (!cluster_ctx_->has_read_lease()) {
                        co_return ErrorResp{"no read lease"};
                    }
                }
                co_return KeysResp{storage_.keys()};

            } else if constexpr (std::is_same_v<T, AddServerCmd>) {
                // Admin command: add a server to the cluster.
                if (!cluster_ctx_) {
                    co_return ErrorResp{"ADDSERVER not available in standalone mode"};
                }
                if (!cluster_ctx_->is_leader()) {
                    auto addr = cluster_ctx_->leader_address();
                    if (addr) {
                        co_return RedirectResp{std::move(*addr)};
                    }
                    co_return ErrorResp{"no leader available"};
                }
                bool ok = cluster_ctx_->submit_config_change({c}, {});
                if (!ok) {
                    co_return ErrorResp{"config change failed (another in progress?)"};
                }
                co_return OkResp{};

            } else if constexpr (std::is_same_v<T, RemoveServerCmd>) {
                // Admin command: remove a server from the cluster.
                if (!cluster_ctx_) {
                    co_return ErrorResp{"REMOVESERVER not available in standalone mode"};
                }
                if (!cluster_ctx_->is_leader()) {
                    auto addr = cluster_ctx_->leader_address();
                    if (addr) {
                        co_return RedirectResp{std::move(*addr)};
                    }
                    co_return ErrorResp{"no leader available"};
                }
                bool ok = cluster_ctx_->submit_config_change({}, {c.node_id});
                if (!ok) {
                    co_return ErrorResp{"config change failed (another in progress?)"};
                }
                co_return OkResp{};
            }
        },
        cmd);
}

} // namespace kv::network
