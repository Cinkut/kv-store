#include "network/session.hpp"
#include "network/protocol.hpp"

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/write.hpp>
#include <boost/system/error_code.hpp>

#include <spdlog/spdlog.h>

#include <string>

namespace kv::network {

namespace {
constexpr auto use_awaitable = boost::asio::as_tuple(boost::asio::use_awaitable);
} // namespace

Session::Session(boost::asio::ip::tcp::socket socket, Storage& storage)
    : socket_(std::move(socket)), storage_(storage) {}

Session::Session(boost::asio::ip::tcp::socket socket, Storage& storage,
                 ClusterContext& cluster_ctx)
    : socket_(std::move(socket)), storage_(storage), cluster_ctx_(&cluster_ctx) {}

boost::asio::awaitable<void> Session::run() {
    const auto remote = [&]() -> std::string {
        boost::system::error_code ec;
        const auto ep = socket_.remote_endpoint(ec);
        return ec ? "<unknown>" : ep.address().to_string() + ":" + std::to_string(ep.port());
    }();

    spdlog::debug("Session::run() - client connected from {}", remote);

    std::string buf;
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

        Response response;
        if (std::holds_alternative<ErrorResp>(parse_result)) {
            response = std::get<ErrorResp>(parse_result);
        } else {
            response = dispatch(std::get<Command>(parse_result));
        }

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

    spdlog::debug("Session::run() - client disconnected: {}", remote);
}

Response Session::dispatch(const Command& cmd) {
    return std::visit(
        [&](const auto& c) -> Response {
            using T = std::decay_t<decltype(c)>;

            if constexpr (std::is_same_v<T, PingCmd>) {
                return PongResp{};

            } else if constexpr (std::is_same_v<T, GetCmd>) {
                auto val = storage_.get(c.key);
                if (!val.has_value()) {
                    return NotFoundResp{};
                }
                return ValueResp{std::move(*val)};

            } else if constexpr (std::is_same_v<T, SetCmd>) {
                // In cluster mode, only the leader processes writes.
                if (cluster_ctx_ && !cluster_ctx_->is_leader()) {
                    auto addr = cluster_ctx_->leader_address();
                    if (addr) {
                        return RedirectResp{std::move(*addr)};
                    }
                    return ErrorResp{"no leader available"};
                }
                storage_.set(c.key, c.value);
                return OkResp{};

            } else if constexpr (std::is_same_v<T, DelCmd>) {
                // In cluster mode, only the leader processes writes.
                if (cluster_ctx_ && !cluster_ctx_->is_leader()) {
                    auto addr = cluster_ctx_->leader_address();
                    if (addr) {
                        return RedirectResp{std::move(*addr)};
                    }
                    return ErrorResp{"no leader available"};
                }
                const bool removed = storage_.del(c.key);
                if (!removed) {
                    return NotFoundResp{};
                }
                return DeletedResp{};

            } else if constexpr (std::is_same_v<T, KeysCmd>) {
                return KeysResp{storage_.keys()};
            }
        },
        cmd);
}

} // namespace kv::network
