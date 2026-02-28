// Throughput benchmark: compares text vs binary protocol performance.
//
// Spins up a kv::network::Server in a background thread, then runs N SET+GET
// cycles over (1) text protocol and (2) binary protocol on the same server
// (auto-detection picks the right mode per connection).
//
// Prints: total ops, elapsed time, ops/sec, and latency percentiles (p50,
// p90, p99, p999) for each protocol.

#include "network/binary_protocol.hpp"
#include "network/protocol.hpp"
#include "network/server.hpp"
#include "storage/storage.hpp"

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/asio/streambuf.hpp>
#include <boost/asio/write.hpp>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <istream>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

namespace {

using tcp    = boost::asio::ip::tcp;
using io_ctx = boost::asio::io_context;
using clock  = std::chrono::high_resolution_clock;
using ns     = std::chrono::nanoseconds;

constexpr const char*   kHost = "127.0.0.1";
constexpr std::uint16_t kPort = 17381;

// ── Text protocol client ─────────────────────────────────────────────────────

class TextClient {
public:
    TextClient() : ioc_(1), socket_(ioc_) {
        tcp::resolver resolver{ioc_};
        auto endpoints = resolver.resolve(kHost, std::to_string(kPort));
        boost::asio::connect(socket_, endpoints);
        socket_.set_option(tcp::no_delay(true));
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

// ── Binary protocol client ───────────────────────────────────────────────────

class BinaryClient {
public:
    BinaryClient() : ioc_(1), socket_(ioc_) {
        tcp::resolver resolver{ioc_};
        auto endpoints = resolver.resolve(kHost, std::to_string(kPort));
        boost::asio::connect(socket_, endpoints);
        socket_.set_option(tcp::no_delay(true));
    }

    void send(const kv::Command& cmd) {
        auto wire = kv::network::serialize_binary_request(cmd);
        boost::asio::write(socket_, boost::asio::buffer(wire));
    }

    kv::Response recv() {
        std::vector<uint8_t> header(kv::network::binary::kHeaderSize);
        boost::asio::read(socket_, boost::asio::buffer(header));

        uint8_t status = 0;
        uint32_t payload_len = 0;
        [[maybe_unused]] bool ok = kv::network::read_binary_header(header, status, payload_len);

        std::vector<uint8_t> payload(payload_len);
        if (payload_len > 0) {
            boost::asio::read(socket_, boost::asio::buffer(payload));
        }

        auto result = kv::network::parse_binary_response(status, payload);
        if (std::holds_alternative<kv::ErrorResp>(result)) {
            return std::get<kv::ErrorResp>(result);
        }
        return std::get<kv::Response>(result);
    }

    kv::Response cmd(const kv::Command& c) {
        send(c);
        return recv();
    }

private:
    io_ctx      ioc_;
    tcp::socket socket_;
};

// ── Stats helpers ────────────────────────────────────────────────────────────

struct BenchResult {
    std::size_t total_ops{};
    double elapsed_sec{};
    double ops_per_sec{};
    double p50_us{};
    double p90_us{};
    double p99_us{};
    double p999_us{};
    double avg_us{};
};

BenchResult compute_stats(std::vector<int64_t>& latencies_ns) {
    BenchResult r;
    r.total_ops = latencies_ns.size();

    if (latencies_ns.empty()) return r;

    std::sort(latencies_ns.begin(), latencies_ns.end());

    auto total_ns = std::accumulate(latencies_ns.begin(), latencies_ns.end(), int64_t{0});
    r.elapsed_sec = static_cast<double>(total_ns) / 1e9;
    r.ops_per_sec = static_cast<double>(r.total_ops) / r.elapsed_sec;
    r.avg_us      = static_cast<double>(total_ns) / static_cast<double>(r.total_ops) / 1000.0;

    auto percentile = [&](double p) -> double {
        auto idx = static_cast<std::size_t>(p * static_cast<double>(latencies_ns.size() - 1));
        return static_cast<double>(latencies_ns[idx]) / 1000.0; // ns → µs
    };

    r.p50_us  = percentile(0.50);
    r.p90_us  = percentile(0.90);
    r.p99_us  = percentile(0.99);
    r.p999_us = percentile(0.999);

    return r;
}

void print_result(const char* label, const BenchResult& r) {
    fprintf(stdout,
        "\n── %s ──\n"
        "  Total ops:    %zu\n"
        "  Elapsed:      %.3f s\n"
        "  Throughput:   %.0f ops/sec\n"
        "  Avg latency:  %.1f µs\n"
        "  p50:          %.1f µs\n"
        "  p90:          %.1f µs\n"
        "  p99:          %.1f µs\n"
        "  p99.9:        %.1f µs\n",
        label, r.total_ops, r.elapsed_sec, r.ops_per_sec,
        r.avg_us, r.p50_us, r.p90_us, r.p99_us, r.p999_us);
}

// ── Benchmark runners ────────────────────────────────────────────────────────

BenchResult bench_text(std::size_t num_cycles) {
    TextClient client;
    std::vector<int64_t> latencies;
    latencies.reserve(num_cycles * 2); // SET + GET per cycle

    for (std::size_t i = 0; i < num_cycles; ++i) {
        std::string key = "key" + std::to_string(i);
        std::string val = "val" + std::to_string(i);

        // SET
        {
            auto t0 = clock::now();
            client.cmd("SET " + key + " " + val);
            auto t1 = clock::now();
            latencies.push_back(std::chrono::duration_cast<ns>(t1 - t0).count());
        }

        // GET
        {
            auto t0 = clock::now();
            client.cmd("GET " + key);
            auto t1 = clock::now();
            latencies.push_back(std::chrono::duration_cast<ns>(t1 - t0).count());
        }
    }

    return compute_stats(latencies);
}

BenchResult bench_binary(std::size_t num_cycles) {
    BinaryClient client;
    std::vector<int64_t> latencies;
    latencies.reserve(num_cycles * 2);

    for (std::size_t i = 0; i < num_cycles; ++i) {
        std::string key = "bkey" + std::to_string(i);
        std::string val = "bval" + std::to_string(i);

        // SET
        {
            auto t0 = clock::now();
            client.cmd(kv::SetCmd{key, val});
            auto t1 = clock::now();
            latencies.push_back(std::chrono::duration_cast<ns>(t1 - t0).count());
        }

        // GET
        {
            auto t0 = clock::now();
            client.cmd(kv::GetCmd{key});
            auto t1 = clock::now();
            latencies.push_back(std::chrono::duration_cast<ns>(t1 - t0).count());
        }
    }

    return compute_stats(latencies);
}

} // anonymous namespace

int main(int argc, char* argv[]) {
    // Suppress server logs during benchmark.
    spdlog::set_level(spdlog::level::warn);

    std::size_t num_cycles = 10'000;
    if (argc > 1) {
        num_cycles = static_cast<std::size_t>(std::atol(argv[1]));
        if (num_cycles == 0) num_cycles = 10'000;
    }

    fprintf(stdout,
        "KV Store Protocol Benchmark\n"
        "===========================\n"
        "Cycles:   %zu (each cycle = 1 SET + 1 GET = 2 ops)\n"
        "Server:   %s:%u\n",
        num_cycles, kHost, kPort);

    // Start server.
    kv::Storage storage;
    kv::network::Server server{kHost, kPort, storage};
    std::thread server_thread{[&] { server.run(); }};
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Warm up (small batch to prime TCP paths / allocator).
    {
        TextClient tc;
        for (int i = 0; i < 100; ++i) {
            tc.cmd("SET warmup" + std::to_string(i) + " x");
        }
        BinaryClient bc;
        for (int i = 0; i < 100; ++i) {
            bc.cmd(kv::SetCmd{"bwarmup" + std::to_string(i), "x"});
        }
    }

    // Run benchmarks.
    auto text_result   = bench_text(num_cycles);
    auto binary_result = bench_binary(num_cycles);

    print_result("Text Protocol", text_result);
    print_result("Binary Protocol", binary_result);

    // Comparison.
    if (text_result.ops_per_sec > 0 && binary_result.ops_per_sec > 0) {
        double speedup = binary_result.ops_per_sec / text_result.ops_per_sec;
        fprintf(stdout,
            "\n── Comparison ──\n"
            "  Binary / Text throughput ratio: %.2fx\n"
            "  Binary avg latency savings:     %.1f%%\n",
            speedup,
            (1.0 - binary_result.avg_us / text_result.avg_us) * 100.0);
    }

    fprintf(stdout, "\n");

    server.stop();
    if (server_thread.joinable()) {
        server_thread.join();
    }

    return 0;
}
