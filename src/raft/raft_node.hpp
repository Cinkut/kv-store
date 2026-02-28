#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <vector>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/steady_timer.hpp>

#include <spdlog/spdlog.h>

#include "common/logger.hpp"
#include "raft/raft_log.hpp"
#include "raft.pb.h"

namespace kv::raft {

// ── Transport abstraction ────────────────────────────────────────────────────
//
// RaftNode sends RPCs through this interface.  Production code uses a real
// TCP transport; tests inject a mock that operates in-memory.

class Transport {
public:
    virtual ~Transport() = default;

    // Send a RaftMessage to a peer.  Returns true if the send succeeds.
    virtual boost::asio::awaitable<bool>
    send(uint32_t peer_id, RaftMessage msg) = 0;
};

// ── TimerFactory abstraction ─────────────────────────────────────────────────
//
// Allows tests to inject deterministic timers (manually advanceable) instead
// of real wall-clock steady_timers.

class Timer {
public:
    virtual ~Timer() = default;

    // Wait until the timer fires or is cancelled.
    // Returns false if cancelled.
    virtual boost::asio::awaitable<bool> async_wait() = 0;

    // Reset the timer to fire after `duration`.
    virtual void expires_after(std::chrono::milliseconds duration) = 0;

    // Cancel the timer.
    virtual void cancel() = 0;
};

class TimerFactory {
public:
    virtual ~TimerFactory() = default;

    // Create a new timer.
    virtual std::unique_ptr<Timer> create_timer() = 0;
};

// ── RaftNode ─────────────────────────────────────────────────────────────────
//
// Core Raft consensus state machine.
//
// ALL state is accessed on a single strand.  Not thread-safe.
// Use dispatch/post to the strand for external interaction.

class RaftNode {
public:
    // Timer constants from spec.
    static constexpr auto kElectionTimeoutMin = std::chrono::milliseconds{150};
    static constexpr auto kElectionTimeoutMax = std::chrono::milliseconds{300};
    static constexpr auto kHeartbeatInterval  = std::chrono::milliseconds{50};

    // Callback invoked when a log entry is committed and should be applied
    // to the state machine.
    using ApplyCallback = std::function<void(const LogEntry&)>;

    // Constructor.
    //   ioc          – io_context (strand will be created from its executor)
    //   node_id      – this node's ID
    //   peer_ids     – IDs of all other nodes in the cluster
    //   transport    – RPC transport (owned externally; must outlive RaftNode)
    //   timer_factory – timer creation (owned externally; must outlive RaftNode)
    //   logger       – per-node spdlog logger
    //   on_apply     – callback for committed entries
    RaftNode(boost::asio::io_context& ioc,
             uint32_t node_id,
             std::vector<uint32_t> peer_ids,
             Transport& transport,
             TimerFactory& timer_factory,
             std::shared_ptr<spdlog::logger> logger,
             ApplyCallback on_apply = {});

    ~RaftNode() = default;

    // Non-copyable, non-movable.
    RaftNode(const RaftNode&) = delete;
    RaftNode& operator=(const RaftNode&) = delete;

    // ── Lifecycle ────────────────────────────────────────────────────────────

    // Start the Raft node (begins as Follower, starts election timer).
    void start();

    // Stop the node (cancel all timers and pending operations).
    void stop();

    // ── RPC handlers (called by the transport layer) ─────────────────────────

    // Handle an incoming RequestVote request.  Returns the response.
    boost::asio::awaitable<RequestVoteResponse>
    handle_request_vote(const RequestVoteRequest& req);

    // Handle an incoming AppendEntries request.  Returns the response.
    boost::asio::awaitable<AppendEntriesResponse>
    handle_append_entries(const AppendEntriesRequest& req);

    // Handle an incoming RequestVote response (from a peer we solicited).
    void handle_vote_response(uint32_t from_peer,
                              const RequestVoteResponse& resp);

    // Handle an incoming AppendEntries response (from a peer we replicated to).
    void handle_append_entries_response(uint32_t from_peer,
                                        const AppendEntriesResponse& resp);

    // Dispatch an incoming RaftMessage from a peer to the appropriate handler.
    // Returns the response message to send back (if any), or nullopt for
    // response-type messages (vote resp, AE resp) which are handled internally.
    boost::asio::awaitable<std::optional<RaftMessage>>
    handle_message(uint32_t from_peer, const RaftMessage& msg);

    // ── Client interaction ───────────────────────────────────────────────────

    // Submit a command from a client.  Only succeeds if this node is Leader.
    // Returns true if the command was appended to the log.
    // The on_apply callback will be invoked when it is committed.
    [[nodiscard]] bool submit(Command cmd);

    // ── State queries ────────────────────────────────────────────────────────

    [[nodiscard]] NodeState state() const noexcept { return state_; }
    [[nodiscard]] uint64_t current_term() const noexcept { return current_term_; }
    [[nodiscard]] uint32_t node_id() const noexcept { return node_id_; }
    [[nodiscard]] std::optional<uint32_t> leader_id() const noexcept { return leader_id_; }
    [[nodiscard]] std::optional<uint32_t> voted_for() const noexcept { return voted_for_; }
    [[nodiscard]] uint64_t commit_index() const noexcept { return commit_index_; }
    [[nodiscard]] uint64_t last_applied() const noexcept { return last_applied_; }
    [[nodiscard]] const RaftLog& log() const noexcept { return log_; }

    // ── Strand accessor (for external dispatch) ──────────────────────────────
    [[nodiscard]] boost::asio::strand<boost::asio::io_context::executor_type>&
    strand() noexcept { return strand_; }

private:
    // ── Internal state transitions ───────────────────────────────────────────

    void become_follower(uint64_t term);
    void become_candidate();
    void become_leader();

    // ── Timer coroutines ─────────────────────────────────────────────────────

    // Election timer loop.
    boost::asio::awaitable<void> election_timer_loop();

    // Heartbeat timer loop (leader only).
    boost::asio::awaitable<void> heartbeat_timer_loop();

    // ── Election ─────────────────────────────────────────────────────────────

    // Start an election: increment term, vote for self, send RequestVote.
    boost::asio::awaitable<void> start_election();

    // Send RequestVote to a single peer, tally result.
    boost::asio::awaitable<void> request_vote_from(uint32_t peer_id);

    // ── Log replication ──────────────────────────────────────────────────────

    // Send AppendEntries (or heartbeat) to all peers.
    boost::asio::awaitable<void> send_append_entries_to_all();

    // Send AppendEntries to a single peer.
    boost::asio::awaitable<void> send_append_entries_to(uint32_t peer_id);

    // Try to advance commitIndex based on matchIndex majority.
    void try_advance_commit();

    // Apply committed entries to the state machine.
    void apply_committed_entries();

    // ── Helper ───────────────────────────────────────────────────────────────

    // Step down to follower if the incoming term is higher.
    // Returns true if term was updated (stepped down).
    bool maybe_step_down(uint64_t incoming_term);

    // Random election timeout in [kElectionTimeoutMin, kElectionTimeoutMax].
    std::chrono::milliseconds random_election_timeout();

    // Reset the election timer to a new random timeout.
    void reset_election_timer();

    // ── Data members ─────────────────────────────────────────────────────────

    boost::asio::strand<boost::asio::io_context::executor_type> strand_;

    const uint32_t node_id_;
    const std::vector<uint32_t> peer_ids_;

    Transport& transport_;
    TimerFactory& timer_factory_;
    std::shared_ptr<spdlog::logger> logger_;
    ApplyCallback on_apply_;

    // Persistent state (Raft §5.2).
    uint64_t current_term_ = 0;
    std::optional<uint32_t> voted_for_;
    RaftLog log_;

    // Volatile state (all servers).
    NodeState state_ = NodeState::Follower;
    uint64_t commit_index_ = 0;
    uint64_t last_applied_ = 0;
    std::optional<uint32_t> leader_id_;

    // Volatile state (leader only).
    std::map<uint32_t, uint64_t> next_index_;
    std::map<uint32_t, uint64_t> match_index_;

    // Election state.
    uint32_t votes_received_ = 0;

    // Timers.
    std::unique_ptr<Timer> election_timer_;
    std::unique_ptr<Timer> heartbeat_timer_;

    // RNG for election timeout.
    std::mt19937 rng_{std::random_device{}()};

    // Shutdown flag.
    bool stopped_ = false;
};

} // namespace kv::raft
