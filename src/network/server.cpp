#include "network/server.hpp"
#include "network/session.hpp"
#include "network/protocol.hpp"

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/error_code.hpp>

#include <spdlog/spdlog.h>

#include <memory>
#include <thread>
#include <vector>

namespace kv::network {

Server::Server(std::string host, std::uint16_t port, Storage& storage)
    : host_(std::move(host)),
      port_(port),
      storage_(storage),
      ioc_(static_cast<int>(std::thread::hardware_concurrency())),
      acceptor_(ioc_) {
    const auto address = boost::asio::ip::make_address(host_);
    const boost::asio::ip::tcp::endpoint endpoint{address, port_};

    acceptor_.open(endpoint.protocol());
    acceptor_.set_option(boost::asio::ip::tcp::acceptor::reuse_address(true));
    acceptor_.bind(endpoint);
    acceptor_.listen();

    spdlog::info("Server listening on {}:{}", host_, port_);
}

Server::Server(std::string host, std::uint16_t port, Storage& storage,
               ClusterContext& cluster_ctx)
    : host_(std::move(host)),
      port_(port),
      storage_(storage),
      cluster_ctx_(&cluster_ctx),
      ioc_(static_cast<int>(std::thread::hardware_concurrency())),
      acceptor_(ioc_) {
    const auto address = boost::asio::ip::make_address(host_);
    const boost::asio::ip::tcp::endpoint endpoint{address, port_};

    acceptor_.open(endpoint.protocol());
    acceptor_.set_option(boost::asio::ip::tcp::acceptor::reuse_address(true));
    acceptor_.bind(endpoint);
    acceptor_.listen();

    spdlog::info("Server listening on {}:{} (cluster mode)", host_, port_);
}

void Server::run() {
    // Install SIGINT / SIGTERM handler for graceful shutdown.
    boost::asio::signal_set signals(ioc_, SIGINT, SIGTERM);
    signals.async_wait([this](const boost::system::error_code& ec, int signo) {
        if (!ec) {
            spdlog::info("Server: received signal {}, shutting down", signo);
            stop();
        }
    });

    // Start the accept loop.
    boost::asio::co_spawn(ioc_, accept_loop(), boost::asio::detached);

    // Run the io_context across a thread pool.
    const unsigned int nthreads = std::max(1u, std::thread::hardware_concurrency());
    std::vector<std::thread> pool;
    pool.reserve(nthreads - 1);
    for (unsigned int i = 1; i < nthreads; ++i) {
        pool.emplace_back([this] { ioc_.run(); });
    }

    ioc_.run(); // Run on the calling thread as well.

    for (auto& t : pool) {
        t.join();
    }

    spdlog::info("Server: io_context stopped, all threads joined");
}

void Server::stop() {
    acceptor_.close();
    ioc_.stop();
}

boost::asio::awaitable<void> Server::accept_loop() {
    spdlog::info("Server: accept loop started");

    constexpr auto use_awaitable = boost::asio::as_tuple(boost::asio::use_awaitable);

    for (;;) {
        auto [ec, socket] = co_await acceptor_.async_accept(use_awaitable);

        if (ec) {
            if (ec != boost::asio::error::operation_aborted) {
                spdlog::warn("Server: accept error: {}", ec.message());
            }
            break; // Acceptor was closed – time to stop.
        }

        // Disable Nagle – send responses immediately.
        socket.set_option(boost::asio::ip::tcp::no_delay(true));

        // Spawn a detached coroutine for this session.
        std::shared_ptr<Session> session_ptr;
        if (cluster_ctx_) {
            session_ptr = std::make_shared<Session>(
                std::move(socket), storage_, *cluster_ctx_);
        } else {
            session_ptr = std::make_shared<Session>(
                std::move(socket), storage_);
        }
        boost::asio::co_spawn(
            ioc_,
            [sp = std::move(session_ptr)]() -> boost::asio::awaitable<void> {
                co_await sp->run();
            },
            boost::asio::detached);
    }

    spdlog::info("Server: accept loop exited");
}

} // namespace kv::network
