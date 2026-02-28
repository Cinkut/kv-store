#include "raft/raft_node.hpp"

#include <algorithm>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/use_awaitable.hpp>

namespace kv::raft {

namespace asio = boost::asio;

// ── Constructor ──────────────────────────────────────────────────────────────

RaftNode::RaftNode(asio::io_context& ioc,
                   uint32_t node_id,
                   std::vector<uint32_t> peer_ids,
                   Transport& transport,
                   TimerFactory& timer_factory,
                   std::shared_ptr<spdlog::logger> logger,
                   ApplyCallback on_apply)
    : strand_(asio::make_strand(ioc))
    , node_id_(node_id)
    , peer_ids_(std::move(peer_ids))
    , transport_(transport)
    , timer_factory_(timer_factory)
    , logger_(std::move(logger))
    , on_apply_(std::move(on_apply))
{
}

// ── Lifecycle ────────────────────────────────────────────────────────────────

void RaftNode::start() {
    logger_->info("[term={}] Starting as Follower", current_term_);
    state_ = NodeState::Follower;

    election_timer_ = timer_factory_.create_timer();
    heartbeat_timer_ = timer_factory_.create_timer();

    asio::co_spawn(strand_, election_timer_loop(), asio::detached);
}

void RaftNode::stop() {
    stopped_ = true;
    if (election_timer_) election_timer_->cancel();
    if (heartbeat_timer_) heartbeat_timer_->cancel();
}

// ── RPC handlers ─────────────────────────────────────────────────────────────

asio::awaitable<RequestVoteResponse>
RaftNode::handle_request_vote(const RequestVoteRequest& req) {
    RequestVoteResponse resp;
    resp.set_term(current_term_);
    resp.set_vote_granted(false);

    // Rule: step down if incoming term is higher.
    if (req.term() > current_term_) {
        become_follower(req.term());
        resp.set_term(current_term_);
    }

    // Reject if stale term.
    if (req.term() < current_term_) {
        logger_->debug("[term={}] Rejecting vote for node {} (stale term {})",
                       current_term_, req.candidate_id(), req.term());
        co_return resp;
    }

    // Vote if: (a) haven't voted yet this term OR already voted for this candidate
    //          AND (b) candidate's log is at least as up-to-date.
    bool can_vote = !voted_for_ || *voted_for_ == req.candidate_id();
    if (!can_vote) {
        logger_->debug("[term={}] Rejecting vote for node {} (already voted for {})",
                       current_term_, req.candidate_id(), *voted_for_);
        co_return resp;
    }

    // Log up-to-date check (Raft §5.4.1):
    //   candidate's log is at least as up-to-date if:
    //   - last log term > our last log term, OR
    //   - last log term == our last log term AND last log index >= our last log index
    uint64_t our_last_term = log_.last_term();
    uint64_t our_last_index = log_.last_index();

    bool log_ok = (req.last_log_term() > our_last_term) ||
                  (req.last_log_term() == our_last_term &&
                   req.last_log_index() >= our_last_index);

    if (!log_ok) {
        logger_->debug("[term={}] Rejecting vote for node {} (log not up-to-date)",
                       current_term_, req.candidate_id());
        co_return resp;
    }

    // Grant vote.
    voted_for_ = req.candidate_id();
    resp.set_vote_granted(true);
    reset_election_timer();
    logger_->info("[term={}] Voted for node {}", current_term_, req.candidate_id());

    co_return resp;
}

asio::awaitable<AppendEntriesResponse>
RaftNode::handle_append_entries(const AppendEntriesRequest& req) {
    AppendEntriesResponse resp;
    resp.set_term(current_term_);
    resp.set_success(false);
    resp.set_match_index(0);

    // Step down if incoming term is higher.
    if (req.term() > current_term_) {
        become_follower(req.term());
        resp.set_term(current_term_);
    }

    // Reject if stale term.
    if (req.term() < current_term_) {
        logger_->debug("[term={}] Rejecting AppendEntries from node {} (stale term {})",
                       current_term_, req.leader_id(), req.term());
        co_return resp;
    }

    // Valid AppendEntries from current leader.
    // If we're a candidate, step down.
    if (state_ != NodeState::Follower) {
        become_follower(req.term());
        resp.set_term(current_term_);
    }

    leader_id_ = req.leader_id();
    reset_election_timer();

    // Consistency check + append.
    std::vector<LogEntry> entries(req.entries().begin(), req.entries().end());
    bool ok = log_.try_append(req.prev_log_index(), req.prev_log_term(), entries);

    if (!ok) {
        logger_->debug("[term={}] AppendEntries consistency check failed "
                       "(prevLogIndex={}, prevLogTerm={})",
                       current_term_, req.prev_log_index(), req.prev_log_term());
        co_return resp;
    }

    resp.set_success(true);
    resp.set_match_index(log_.last_index());

    // Update commitIndex.
    if (req.leader_commit() > commit_index_) {
        commit_index_ = std::min(req.leader_commit(), log_.last_index());
        apply_committed_entries();
    }

    logger_->debug("[term={}] AppendEntries OK from node {} (matchIndex={})",
                   current_term_, req.leader_id(), log_.last_index());

    co_return resp;
}

// ── Response handlers ────────────────────────────────────────────────────

void RaftNode::handle_vote_response(uint32_t from_peer,
                                     const RequestVoteResponse& resp) {
    // Ignore if we're no longer a candidate (e.g., already won or stepped down).
    if (state_ != NodeState::Candidate) return;

    // Step down if response has a higher term.
    if (resp.term() > current_term_) {
        logger_->info("[term={}] Vote response from node {} has higher term {}; stepping down",
                      current_term_, from_peer, resp.term());
        become_follower(resp.term());
        return;
    }

    // Ignore stale responses from a previous term.
    if (resp.term() != current_term_) return;

    if (resp.vote_granted()) {
        votes_received_++;
        int cluster_size = static_cast<int>(peer_ids_.size()) + 1;
        logger_->debug("[term={}] Received vote from node {} ({}/{})",
                       current_term_, from_peer, votes_received_, cluster_size);

        if (static_cast<int>(votes_received_) > cluster_size / 2) {
            become_leader();
        }
    } else {
        logger_->debug("[term={}] Vote denied by node {}", current_term_, from_peer);
    }
}

void RaftNode::handle_append_entries_response(uint32_t from_peer,
                                               const AppendEntriesResponse& resp) {
    // Ignore if we're no longer leader.
    if (state_ != NodeState::Leader) return;

    // Step down if response has a higher term.
    if (resp.term() > current_term_) {
        logger_->info("[term={}] AE response from node {} has higher term {}; stepping down",
                      current_term_, from_peer, resp.term());
        become_follower(resp.term());
        return;
    }

    // Ignore stale responses from a previous term.
    if (resp.term() != current_term_) return;

    if (resp.success()) {
        // Update matchIndex and nextIndex for this peer.
        match_index_[from_peer] = resp.match_index();
        next_index_[from_peer] = resp.match_index() + 1;

        logger_->debug("[term={}] AE success from node {} (matchIndex={})",
                       current_term_, from_peer, resp.match_index());

        // Try to advance the commit index based on new matchIndex.
        try_advance_commit();
    } else {
        // Decrement nextIndex and retry (Raft §5.3).
        if (next_index_[from_peer] > 1) {
            next_index_[from_peer]--;
        }
        logger_->debug("[term={}] AE rejected by node {} (nextIndex decremented to {})",
                       current_term_, from_peer, next_index_[from_peer]);

        // Immediately retry with lower nextIndex.
        asio::co_spawn(strand_, send_append_entries_to(from_peer), asio::detached);
    }
}

asio::awaitable<std::optional<RaftMessage>>
RaftNode::handle_message(uint32_t from_peer, const RaftMessage& msg) {
    if (msg.has_request_vote_req()) {
        auto resp = co_await handle_request_vote(msg.request_vote_req());
        RaftMessage reply;
        *reply.mutable_request_vote_resp() = std::move(resp);
        co_return reply;
    }

    if (msg.has_append_entries_req()) {
        auto resp = co_await handle_append_entries(msg.append_entries_req());
        RaftMessage reply;
        *reply.mutable_append_entries_resp() = std::move(resp);
        co_return reply;
    }

    if (msg.has_request_vote_resp()) {
        handle_vote_response(from_peer, msg.request_vote_resp());
        co_return std::nullopt;
    }

    if (msg.has_append_entries_resp()) {
        handle_append_entries_response(from_peer, msg.append_entries_resp());
        co_return std::nullopt;
    }

    logger_->warn("[term={}] Received unknown message type from node {}",
                  current_term_, from_peer);
    co_return std::nullopt;
}

// ── Client interaction ───────────────────────────────────────────────────────

bool RaftNode::submit(Command cmd) {
    if (state_ != NodeState::Leader) {
        return false;
    }

    LogEntry entry;
    entry.set_term(current_term_);
    entry.set_index(log_.last_index() + 1);
    *entry.mutable_command() = std::move(cmd);

    log_.append(std::move(entry));
    logger_->debug("[term={}] Appended entry at index {}", current_term_, log_.last_index());

    // Immediately trigger replication.
    asio::co_spawn(strand_, send_append_entries_to_all(), asio::detached);

    return true;
}

// ── State transitions ────────────────────────────────────────────────────────

void RaftNode::become_follower(uint64_t term) {
    NodeState prev = state_;
    state_ = NodeState::Follower;
    current_term_ = term;
    voted_for_ = std::nullopt;
    leader_id_ = std::nullopt;

    if (prev != NodeState::Follower) {
        logger_->info("[term={}] Stepped down to Follower (was {})",
                      current_term_,
                      prev == NodeState::Candidate ? "Candidate" : "Leader");

        // If we were leader, stop heartbeats.
        if (prev == NodeState::Leader) {
            heartbeat_timer_->cancel();
        }
    }
}

void RaftNode::become_candidate() {
    state_ = NodeState::Candidate;
    logger_->info("[term={}] Became Candidate", current_term_);
}

void RaftNode::become_leader() {
    state_ = NodeState::Leader;
    leader_id_ = node_id_;
    logger_->info("[term={}] Became Leader", current_term_);

    // Initialize nextIndex and matchIndex (Raft §5.3).
    next_index_.clear();
    match_index_.clear();
    for (uint32_t peer : peer_ids_) {
        next_index_[peer] = log_.last_index() + 1;
        match_index_[peer] = 0;
    }

    // Append a no-op entry for this term (Raft §8).
    Command noop;
    noop.set_type(CMD_NOOP);
    LogEntry entry;
    entry.set_term(current_term_);
    entry.set_index(log_.last_index() + 1);
    *entry.mutable_command() = std::move(noop);
    log_.append(std::move(entry));

    // Cancel election timer, start heartbeat timer.
    election_timer_->cancel();
    asio::co_spawn(strand_, heartbeat_timer_loop(), asio::detached);

    // Send initial heartbeat immediately.
    asio::co_spawn(strand_, send_append_entries_to_all(), asio::detached);
}

// ── Timer loops ──────────────────────────────────────────────────────────────

asio::awaitable<void> RaftNode::election_timer_loop() {
    while (!stopped_) {
        election_timer_->expires_after(random_election_timeout());
        bool fired = co_await election_timer_->async_wait();

        if (stopped_) co_return;
        if (!fired) continue;  // Timer was reset/cancelled — restart loop.

        // Timeout expired — start election (unless we're Leader).
        if (state_ != NodeState::Leader) {
            co_await start_election();
        }
    }
}

asio::awaitable<void> RaftNode::heartbeat_timer_loop() {
    while (!stopped_ && state_ == NodeState::Leader) {
        heartbeat_timer_->expires_after(kHeartbeatInterval);
        bool fired = co_await heartbeat_timer_->async_wait();

        if (stopped_ || state_ != NodeState::Leader) co_return;
        if (!fired) continue;

        co_await send_append_entries_to_all();
    }
}

// ── Election ─────────────────────────────────────────────────────────────────

asio::awaitable<void> RaftNode::start_election() {
    // Increment term, vote for self.
    current_term_++;
    voted_for_ = node_id_;
    votes_received_ = 1;  // Self-vote.
    leader_id_ = std::nullopt;
    become_candidate();

    logger_->info("[term={}] Starting election", current_term_);

    // Single-node cluster: immediately become leader.
    if (peer_ids_.empty()) {
        become_leader();
        co_return;
    }

    // Send RequestVote to all peers (concurrently, fire-and-forget on strand).
    for (uint32_t peer : peer_ids_) {
        asio::co_spawn(strand_, request_vote_from(peer), asio::detached);
    }
}

asio::awaitable<void> RaftNode::request_vote_from(uint32_t peer_id) {
    // Build request.
    RequestVoteRequest req;
    req.set_term(current_term_);
    req.set_candidate_id(node_id_);
    req.set_last_log_index(log_.last_index());
    req.set_last_log_term(log_.last_term());

    RaftMessage msg;
    *msg.mutable_request_vote_req() = req;

    uint64_t election_term = current_term_;

    logger_->debug("[term={}] Sending RequestVote to node {}", current_term_, peer_id);
    bool sent = co_await transport_.send(peer_id, std::move(msg));

    if (!sent) {
        logger_->debug("[term={}] Failed to send RequestVote to node {}",
                       election_term, peer_id);
        co_return;
    }

    // Note: In a real implementation, we'd await the response through the
    // transport. For now, vote responses are delivered via handle_vote_response().
}

// ── Log replication ──────────────────────────────────────────────────────────

asio::awaitable<void> RaftNode::send_append_entries_to_all() {
    if (state_ != NodeState::Leader) co_return;

    for (uint32_t peer : peer_ids_) {
        asio::co_spawn(strand_, send_append_entries_to(peer), asio::detached);
    }
}

asio::awaitable<void> RaftNode::send_append_entries_to(uint32_t peer_id) {
    if (state_ != NodeState::Leader) co_return;

    uint64_t next = next_index_[peer_id];
    uint64_t prev_log_index = next - 1;
    uint64_t prev_log_term = 0;
    if (prev_log_index > 0) {
        auto t = log_.term_at(prev_log_index);
        if (t) prev_log_term = *t;
    }

    AppendEntriesRequest req;
    req.set_term(current_term_);
    req.set_leader_id(node_id_);
    req.set_prev_log_index(prev_log_index);
    req.set_prev_log_term(prev_log_term);
    req.set_leader_commit(commit_index_);

    // Include log entries from next onward.
    auto entries = log_.entries_from(next);
    for (auto& e : entries) {
        *req.add_entries() = std::move(e);
    }

    RaftMessage msg;
    *msg.mutable_append_entries_req() = std::move(req);

    logger_->debug("[term={}] Sending AppendEntries to node {} (prevIdx={}, entries={})",
                   current_term_, peer_id, prev_log_index, entries.size());

    co_await transport_.send(peer_id, std::move(msg));
}

void RaftNode::try_advance_commit() {
    if (state_ != NodeState::Leader) return;

    // Find the highest N such that:
    //   - N > commitIndex
    //   - majority of matchIndex[i] >= N
    //   - log[N].term == currentTerm
    for (uint64_t n = log_.last_index(); n > commit_index_; --n) {
        auto term = log_.term_at(n);
        if (!term || *term != current_term_) continue;

        // Count replicas (including self).
        int count = 1;  // self
        for (uint32_t peer : peer_ids_) {
            if (match_index_[peer] >= n) count++;
        }

        // Majority: count > (cluster_size / 2)
        int cluster_size = static_cast<int>(peer_ids_.size()) + 1;
        if (count > cluster_size / 2) {
            commit_index_ = n;
            logger_->debug("[term={}] Commit index advanced to {}", current_term_, n);
            apply_committed_entries();
            break;
        }
    }
}

void RaftNode::apply_committed_entries() {
    while (last_applied_ < commit_index_) {
        last_applied_++;
        const auto* entry = log_.entry_at(last_applied_);
        if (entry && on_apply_) {
            on_apply_(*entry);
        }
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

bool RaftNode::maybe_step_down(uint64_t incoming_term) {
    if (incoming_term > current_term_) {
        become_follower(incoming_term);
        return true;
    }
    return false;
}

std::chrono::milliseconds RaftNode::random_election_timeout() {
    std::uniform_int_distribution<int> dist(
        static_cast<int>(kElectionTimeoutMin.count()),
        static_cast<int>(kElectionTimeoutMax.count()));
    return std::chrono::milliseconds{dist(rng_)};
}

void RaftNode::reset_election_timer() {
    if (election_timer_) {
        election_timer_->expires_after(random_election_timeout());
    }
}

} // namespace kv::raft
