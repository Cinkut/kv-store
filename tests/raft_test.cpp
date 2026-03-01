#include "raft/clock.hpp"
#include "raft/commit_awaiter.hpp"
#include "raft/raft_cluster_context.hpp"
#include "raft/raft_log.hpp"
#include "raft/raft_node.hpp"
#include "raft/state_machine.hpp"
#include "storage/storage.hpp"
#include "common/logger.hpp"

#include <gtest/gtest.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <vector>

namespace {

using namespace kv::raft;

// ════════════════════════════════════════════════════════════════════════════
//  Test helpers: MockTransport and DeterministicTimer
// ════════════════════════════════════════════════════════════════════════════

// A mock transport that records sent messages and allows injecting responses.
class MockTransport : public Transport {
public:
    struct SentMessage {
        uint32_t peer_id;
        RaftMessage msg;
    };

    boost::asio::awaitable<bool>
    send(uint32_t peer_id, RaftMessage msg) override {
        sent_messages_.push_back({peer_id, std::move(msg)});
        co_return true;
    }

    // Access sent messages.
    [[nodiscard]] const std::vector<SentMessage>& sent() const { return sent_messages_; }
    void clear_sent() { sent_messages_.clear(); }

    [[nodiscard]] std::size_t send_count() const { return sent_messages_.size(); }

    // Find sent messages of a specific type.
    [[nodiscard]] std::vector<const SentMessage*>
    find_request_vote() const {
        std::vector<const SentMessage*> result;
        for (const auto& m : sent_messages_) {
            if (m.msg.has_request_vote_req()) result.push_back(&m);
        }
        return result;
    }

    [[nodiscard]] std::vector<const SentMessage*>
    find_append_entries() const {
        std::vector<const SentMessage*> result;
        for (const auto& m : sent_messages_) {
            if (m.msg.has_append_entries_req()) result.push_back(&m);
        }
        return result;
    }

private:
    std::vector<SentMessage> sent_messages_;
};

// Deterministic timer: fires only when explicitly triggered.
// Uses a long-expiry steady_timer that truly suspends the coroutine.
// fire()/cancel()/expires_after() cancel the underlying timer to wake it.
class DeterministicTimer : public Timer {
public:
    boost::asio::awaitable<bool> async_wait() override {
        auto exec = co_await boost::asio::this_coro::executor;
        boost::asio::steady_timer wait_timer(exec, std::chrono::hours{24});

        fired_ = false;
        cancelled_ = false;
        waiting_ = true;
        wait_timer_ = &wait_timer;

        // Truly suspend until fire()/cancel()/expires_after() cancels us.
        auto [ec] = co_await wait_timer.async_wait(
            boost::asio::as_tuple(boost::asio::use_awaitable));

        waiting_ = false;
        wait_timer_ = nullptr;

        co_return fired_;
    }

    void expires_after(std::chrono::milliseconds /*duration*/) override {
        // For deterministic tests, duration is ignored.
        // If currently waiting, cancel the wait (treated as "reset").
        if (waiting_) {
            cancelled_ = true;
            if (wait_timer_) wait_timer_->cancel();
        }
    }

    void cancel() override {
        if (waiting_) {
            cancelled_ = true;
            if (wait_timer_) wait_timer_->cancel();
        }
    }

    // Test helper: trigger the timer to fire (returns true from async_wait).
    void fire() {
        if (waiting_) {
            fired_ = true;
            if (wait_timer_) wait_timer_->cancel();
        }
    }

    [[nodiscard]] bool is_waiting() const { return waiting_; }

private:
    bool waiting_ = false;
    bool fired_ = false;
    bool cancelled_ = false;
    boost::asio::steady_timer* wait_timer_ = nullptr;
};

class DeterministicTimerFactory : public TimerFactory {
public:
    std::unique_ptr<Timer> create_timer() override {
        auto t = std::make_unique<DeterministicTimer>();
        timers_.push_back(t.get());
        return t;
    }

    // Access created timers (index 0 = election timer, 1 = heartbeat timer).
    [[nodiscard]] DeterministicTimer* timer(std::size_t index) const {
        if (index < timers_.size()) return timers_[index];
        return nullptr;
    }

    [[nodiscard]] std::size_t timer_count() const { return timers_.size(); }

private:
    std::vector<DeterministicTimer*> timers_;
};

// Helper to make a LogEntry.
LogEntry make_entry(uint64_t term, uint64_t index,
                    CommandType type = CMD_NOOP,
                    const std::string& key = "",
                    const std::string& value = "") {
    LogEntry entry;
    entry.set_term(term);
    entry.set_index(index);
    auto* cmd = entry.mutable_command();
    cmd->set_type(type);
    cmd->set_key(key);
    cmd->set_value(value);
    return entry;
}

// Pump the io_context for a few iterations to let coroutines run.
void pump(boost::asio::io_context& ioc, int iterations = 100) {
    for (int i = 0; i < iterations; ++i) {
        ioc.poll();
        if (ioc.stopped()) {
            ioc.restart();
        }
    }
}

} // anonymous namespace

// ════════════════════════════════════════════════════════════════════════════
//  RaftLog tests
// ════════════════════════════════════════════════════════════════════════════

class RaftLogTest : public ::testing::Test {
protected:
    RaftLog log_;
};

TEST_F(RaftLogTest, EmptyLogHasZeroIndexAndTerm) {
    EXPECT_EQ(log_.last_index(), 0u);
    EXPECT_EQ(log_.last_term(), 0u);
    EXPECT_EQ(log_.size(), 0u);
    EXPECT_TRUE(log_.empty());
}

TEST_F(RaftLogTest, AppendSingleEntry) {
    log_.append(make_entry(1, 1));
    EXPECT_EQ(log_.last_index(), 1u);
    EXPECT_EQ(log_.last_term(), 1u);
    EXPECT_EQ(log_.size(), 1u);
    EXPECT_FALSE(log_.empty());
}

TEST_F(RaftLogTest, AppendMultipleEntries) {
    log_.append(make_entry(1, 1));
    log_.append(make_entry(1, 2));
    log_.append(make_entry(2, 3));
    EXPECT_EQ(log_.last_index(), 3u);
    EXPECT_EQ(log_.last_term(), 2u);
    EXPECT_EQ(log_.size(), 3u);
}

TEST_F(RaftLogTest, TermAtReturnsCorrectTerm) {
    log_.append(make_entry(1, 1));
    log_.append(make_entry(2, 2));
    log_.append(make_entry(2, 3));

    // Index 0 is sentinel (term 0).
    EXPECT_EQ(log_.term_at(0), 0u);
    EXPECT_EQ(log_.term_at(1), 1u);
    EXPECT_EQ(log_.term_at(2), 2u);
    EXPECT_EQ(log_.term_at(3), 2u);
    // Out of range.
    EXPECT_FALSE(log_.term_at(4).has_value());
}

TEST_F(RaftLogTest, EntryAtReturnsCorrectEntry) {
    log_.append(make_entry(1, 1, CMD_SET, "key1", "val1"));
    log_.append(make_entry(2, 2, CMD_DEL, "key2"));

    const auto* e1 = log_.entry_at(1);
    ASSERT_NE(e1, nullptr);
    EXPECT_EQ(e1->term(), 1u);
    EXPECT_EQ(e1->command().type(), CMD_SET);
    EXPECT_EQ(e1->command().key(), "key1");

    const auto* e2 = log_.entry_at(2);
    ASSERT_NE(e2, nullptr);
    EXPECT_EQ(e2->term(), 2u);
    EXPECT_EQ(e2->command().type(), CMD_DEL);

    EXPECT_EQ(log_.entry_at(0), nullptr);
    EXPECT_EQ(log_.entry_at(3), nullptr);
}

TEST_F(RaftLogTest, TruncateAfterRemovesEntries) {
    log_.append(make_entry(1, 1));
    log_.append(make_entry(1, 2));
    log_.append(make_entry(2, 3));

    log_.truncate_after(1);
    EXPECT_EQ(log_.last_index(), 1u);
    EXPECT_EQ(log_.size(), 1u);
    EXPECT_EQ(log_.last_term(), 1u);
}

TEST_F(RaftLogTest, TruncateAfterZeroClearsAll) {
    log_.append(make_entry(1, 1));
    log_.append(make_entry(1, 2));

    log_.truncate_after(0);
    EXPECT_EQ(log_.last_index(), 0u);
    EXPECT_TRUE(log_.empty());
}

TEST_F(RaftLogTest, TruncateAfterBeyondEndIsNoOp) {
    log_.append(make_entry(1, 1));
    log_.truncate_after(5);
    EXPECT_EQ(log_.last_index(), 1u);
}

TEST_F(RaftLogTest, TryAppendEmptyPrefixSucceeds) {
    std::vector<LogEntry> entries = {make_entry(1, 1), make_entry(1, 2)};
    EXPECT_TRUE(log_.try_append(0, 0, entries));
    EXPECT_EQ(log_.last_index(), 2u);
}

TEST_F(RaftLogTest, TryAppendMatchingPrefixSucceeds) {
    log_.append(make_entry(1, 1));
    log_.append(make_entry(1, 2));

    std::vector<LogEntry> entries = {make_entry(2, 3)};
    EXPECT_TRUE(log_.try_append(2, 1, entries));
    EXPECT_EQ(log_.last_index(), 3u);
    EXPECT_EQ(log_.last_term(), 2u);
}

TEST_F(RaftLogTest, TryAppendFailsOnTermMismatch) {
    log_.append(make_entry(1, 1));

    std::vector<LogEntry> entries = {make_entry(2, 2)};
    // prevLogTerm doesn't match.
    EXPECT_FALSE(log_.try_append(1, 99, entries));
    // Log should be unchanged.
    EXPECT_EQ(log_.last_index(), 1u);
}

TEST_F(RaftLogTest, TryAppendFailsOnMissingPrevIndex) {
    // Empty log, prev_log_index=5 doesn't exist.
    std::vector<LogEntry> entries = {make_entry(1, 6)};
    EXPECT_FALSE(log_.try_append(5, 1, entries));
}

TEST_F(RaftLogTest, TryAppendTruncatesConflict) {
    log_.append(make_entry(1, 1));
    log_.append(make_entry(1, 2));
    log_.append(make_entry(1, 3));

    // New entries conflict at index 2 (term 2 vs existing term 1).
    std::vector<LogEntry> entries = {make_entry(2, 2), make_entry(2, 3)};
    EXPECT_TRUE(log_.try_append(1, 1, entries));
    EXPECT_EQ(log_.last_index(), 3u);
    EXPECT_EQ(log_.term_at(2), 2u);  // Replaced.
    EXPECT_EQ(log_.term_at(3), 2u);  // Replaced.
}

TEST_F(RaftLogTest, TryAppendSkipsExistingMatching) {
    log_.append(make_entry(1, 1));
    log_.append(make_entry(1, 2));

    // Re-sending the same entries should be idempotent.
    std::vector<LogEntry> entries = {make_entry(1, 1), make_entry(1, 2)};
    EXPECT_TRUE(log_.try_append(0, 0, entries));
    EXPECT_EQ(log_.last_index(), 2u);
}

TEST_F(RaftLogTest, EntriesFromRangeReturnsCorrectSlice) {
    log_.append(make_entry(1, 1));
    log_.append(make_entry(1, 2));
    log_.append(make_entry(2, 3));

    auto slice = log_.entries_from(2, 3);
    EXPECT_EQ(slice.size(), 2u);
    EXPECT_EQ(slice[0].index(), 2u);
    EXPECT_EQ(slice[1].index(), 3u);
}

TEST_F(RaftLogTest, EntriesFromToEndReturnsAll) {
    log_.append(make_entry(1, 1));
    log_.append(make_entry(2, 2));

    auto all = log_.entries_from(1);
    EXPECT_EQ(all.size(), 2u);
}

TEST_F(RaftLogTest, EntriesFromEmptyLogReturnsEmpty) {
    auto all = log_.entries_from(1);
    EXPECT_TRUE(all.empty());
}

TEST_F(RaftLogTest, EntriesFromInvalidRangeReturnsEmpty) {
    log_.append(make_entry(1, 1));
    auto slice = log_.entries_from(5, 10);
    EXPECT_TRUE(slice.empty());
}

// ════════════════════════════════════════════════════════════════════════════
//  StateMachine tests
// ════════════════════════════════════════════════════════════════════════════

class StateMachineTest : public ::testing::Test {
protected:
    kv::Storage storage_;
    StateMachine sm_{storage_};
};

TEST_F(StateMachineTest, ApplySetAddsToStorage) {
    auto entry = make_entry(1, 1, CMD_SET, "foo", "bar");
    sm_.apply(entry);
    EXPECT_EQ(sm_.last_applied(), 1u);
    EXPECT_EQ(storage_.get("foo"), "bar");
}

TEST_F(StateMachineTest, ApplyDelRemovesFromStorage) {
    storage_.set("foo", "bar");
    auto entry = make_entry(1, 1, CMD_DEL, "foo");
    sm_.apply(entry);
    EXPECT_EQ(sm_.last_applied(), 1u);
    EXPECT_FALSE(storage_.get("foo").has_value());
}

TEST_F(StateMachineTest, ApplyNoopDoesNotChangeStorage) {
    storage_.set("foo", "bar");
    auto entry = make_entry(1, 1, CMD_NOOP);
    sm_.apply(entry);
    EXPECT_EQ(sm_.last_applied(), 1u);
    EXPECT_EQ(storage_.get("foo"), "bar");
}

TEST_F(StateMachineTest, SkipsDuplicateApplication) {
    auto entry = make_entry(1, 1, CMD_SET, "foo", "first");
    sm_.apply(entry);

    // Try to re-apply same index with different value — should be skipped.
    auto dup = make_entry(1, 1, CMD_SET, "foo", "second");
    sm_.apply(dup);

    EXPECT_EQ(storage_.get("foo"), "first");
    EXPECT_EQ(sm_.last_applied(), 1u);
}

TEST_F(StateMachineTest, AppliesInOrder) {
    sm_.apply(make_entry(1, 1, CMD_SET, "a", "1"));
    sm_.apply(make_entry(1, 2, CMD_SET, "b", "2"));
    sm_.apply(make_entry(1, 3, CMD_SET, "c", "3"));

    EXPECT_EQ(sm_.last_applied(), 3u);
    EXPECT_EQ(storage_.get("a"), "1");
    EXPECT_EQ(storage_.get("b"), "2");
    EXPECT_EQ(storage_.get("c"), "3");
}

// ════════════════════════════════════════════════════════════════════════════
//  RaftNode skeleton / construction tests
// ════════════════════════════════════════════════════════════════════════════

class RaftNodeTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Drop any previously registered "kv" logger so init_default_logger
        // (which calls spdlog::stdout_color_mt("kv")) doesn't throw.
        spdlog::drop("kv");
        kv::init_default_logger(spdlog::level::debug);
        logger_ = kv::make_node_logger(1, spdlog::level::debug);
    }

    boost::asio::io_context ioc_;
    MockTransport transport_;
    DeterministicTimerFactory timer_factory_;
    std::shared_ptr<spdlog::logger> logger_;
};

TEST_F(RaftNodeTest, StartsAsFollower) {
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_);
    EXPECT_EQ(node.state(), kv::NodeState::Follower);
    EXPECT_EQ(node.current_term(), 0u);
    EXPECT_FALSE(node.voted_for().has_value());
    EXPECT_FALSE(node.leader_id().has_value());
}

TEST_F(RaftNodeTest, StartCreatesTimers) {
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_);
    node.start();
    pump(ioc_);
    // Should have created 2 timers (election + heartbeat).
    EXPECT_EQ(timer_factory_.timer_count(), 2u);
    node.stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, SubmitFailsWhenNotLeader) {
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_);
    Command cmd;
    cmd.set_type(CMD_SET);
    cmd.set_key("foo");
    cmd.set_value("bar");
    EXPECT_FALSE(node.submit(std::move(cmd)));
}

TEST_F(RaftNodeTest, HandleRequestVoteGrantsVote) {
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_);
    node.start();
    pump(ioc_);

    RequestVoteRequest req;
    req.set_term(1);
    req.set_candidate_id(2);
    req.set_last_log_index(0);
    req.set_last_log_term(0);

    RequestVoteResponse resp;
    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        resp = co_await node.handle_request_vote(req);
    }, boost::asio::detached);
    pump(ioc_);

    EXPECT_TRUE(resp.vote_granted());
    EXPECT_EQ(resp.term(), 1u);
    EXPECT_EQ(node.voted_for(), 2u);
    EXPECT_EQ(node.current_term(), 1u);

    node.stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, HandleRequestVoteRejectsStaleTerm) {
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_);
    node.start();
    pump(ioc_);

    // First, update node's term via a higher-term vote.
    RequestVoteRequest req1;
    req1.set_term(5);
    req1.set_candidate_id(2);
    req1.set_last_log_index(0);
    req1.set_last_log_term(0);

    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        co_await node.handle_request_vote(req1);
    }, boost::asio::detached);
    pump(ioc_);
    EXPECT_EQ(node.current_term(), 5u);

    // Now send a stale request.
    RequestVoteRequest req2;
    req2.set_term(3);
    req2.set_candidate_id(3);
    req2.set_last_log_index(0);
    req2.set_last_log_term(0);

    RequestVoteResponse resp;
    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        resp = co_await node.handle_request_vote(req2);
    }, boost::asio::detached);
    pump(ioc_);

    EXPECT_FALSE(resp.vote_granted());
    EXPECT_EQ(resp.term(), 5u);

    node.stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, HandleRequestVoteRejectsIfAlreadyVoted) {
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_);
    node.start();
    pump(ioc_);

    // Vote for candidate 2.
    RequestVoteRequest req1;
    req1.set_term(1);
    req1.set_candidate_id(2);
    req1.set_last_log_index(0);
    req1.set_last_log_term(0);

    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        co_await node.handle_request_vote(req1);
    }, boost::asio::detached);
    pump(ioc_);
    EXPECT_EQ(node.voted_for(), 2u);

    // Another candidate for same term.
    RequestVoteRequest req2;
    req2.set_term(1);
    req2.set_candidate_id(3);
    req2.set_last_log_index(0);
    req2.set_last_log_term(0);

    RequestVoteResponse resp;
    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        resp = co_await node.handle_request_vote(req2);
    }, boost::asio::detached);
    pump(ioc_);

    EXPECT_FALSE(resp.vote_granted());

    node.stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, HandleAppendEntriesUpdatesCommitIndex) {
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_);
    node.start();
    pump(ioc_);

    AppendEntriesRequest req;
    req.set_term(1);
    req.set_leader_id(2);
    req.set_prev_log_index(0);
    req.set_prev_log_term(0);
    req.set_leader_commit(0);

    auto* e = req.add_entries();
    e->set_term(1);
    e->set_index(1);
    e->mutable_command()->set_type(CMD_SET);
    e->mutable_command()->set_key("k");
    e->mutable_command()->set_value("v");

    AppendEntriesResponse resp;
    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        resp = co_await node.handle_append_entries(req);
    }, boost::asio::detached);
    pump(ioc_);

    EXPECT_TRUE(resp.success());
    EXPECT_EQ(resp.match_index(), 1u);
    EXPECT_EQ(node.current_term(), 1u);
    EXPECT_EQ(node.leader_id(), 2u);

    node.stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, HandleAppendEntriesRejectsStaleTerm) {
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_);
    node.start();
    pump(ioc_);

    // First, set term to 5.
    RequestVoteRequest vreq;
    vreq.set_term(5);
    vreq.set_candidate_id(2);
    vreq.set_last_log_index(0);
    vreq.set_last_log_term(0);
    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        co_await node.handle_request_vote(vreq);
    }, boost::asio::detached);
    pump(ioc_);

    // Stale AppendEntries.
    AppendEntriesRequest req;
    req.set_term(3);
    req.set_leader_id(3);
    req.set_prev_log_index(0);
    req.set_prev_log_term(0);
    req.set_leader_commit(0);

    AppendEntriesResponse resp;
    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        resp = co_await node.handle_append_entries(req);
    }, boost::asio::detached);
    pump(ioc_);

    EXPECT_FALSE(resp.success());
    EXPECT_EQ(resp.term(), 5u);

    node.stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, HandleAppendEntriesConsistencyCheckFails) {
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_);
    node.start();
    pump(ioc_);

    // AppendEntries with prev_log_index=5 when log is empty.
    AppendEntriesRequest req;
    req.set_term(1);
    req.set_leader_id(2);
    req.set_prev_log_index(5);
    req.set_prev_log_term(1);
    req.set_leader_commit(0);

    AppendEntriesResponse resp;
    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        resp = co_await node.handle_append_entries(req);
    }, boost::asio::detached);
    pump(ioc_);

    EXPECT_FALSE(resp.success());
    EXPECT_EQ(node.current_term(), 1u);

    node.stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, HandleAppendEntriesAppliesCommittedEntries) {
    std::vector<LogEntry> applied;
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_,
                  [&](const LogEntry& e) { applied.push_back(e); });
    node.start();
    pump(ioc_);

    // Send entry with leader_commit = 1 (entry should be applied).
    AppendEntriesRequest req;
    req.set_term(1);
    req.set_leader_id(2);
    req.set_prev_log_index(0);
    req.set_prev_log_term(0);
    req.set_leader_commit(1);

    auto* e = req.add_entries();
    e->set_term(1);
    e->set_index(1);
    e->mutable_command()->set_type(CMD_SET);
    e->mutable_command()->set_key("foo");
    e->mutable_command()->set_value("bar");

    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        co_await node.handle_append_entries(req);
    }, boost::asio::detached);
    pump(ioc_);

    ASSERT_EQ(applied.size(), 1u);
    EXPECT_EQ(applied[0].command().key(), "foo");
    EXPECT_EQ(node.last_applied(), 1u);
    EXPECT_EQ(node.commit_index(), 1u);

    node.stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, HigherTermAppendEntriesCausesStepDown) {
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_);
    node.start();
    pump(ioc_);

    // AE from a leader with higher term when node is Follower.
    AppendEntriesRequest req;
    req.set_term(5);
    req.set_leader_id(2);
    req.set_prev_log_index(0);
    req.set_prev_log_term(0);
    req.set_leader_commit(0);

    AppendEntriesResponse resp;
    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        resp = co_await node.handle_append_entries(req);
    }, boost::asio::detached);
    pump(ioc_);

    EXPECT_TRUE(resp.success());
    EXPECT_EQ(node.current_term(), 5u);
    EXPECT_EQ(node.state(), kv::NodeState::Follower);
    EXPECT_EQ(node.leader_id(), 2u);

    node.stop();
    pump(ioc_);
}

// ════════════════════════════════════════════════════════════════════════════
//  Election tests (vote response handling + timer-driven elections)
// ════════════════════════════════════════════════════════════════════════════

TEST_F(RaftNodeTest, ElectionTimeoutTriggersElection) {
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_);
    node.start();
    pump(ioc_);

    // Timer 0 = election timer.
    auto* election_timer = timer_factory_.timer(0);
    ASSERT_NE(election_timer, nullptr);
    ASSERT_TRUE(election_timer->is_waiting());

    // Fire the election timer to trigger an election.
    election_timer->fire();
    pump(ioc_);

    // Node should now be a Candidate.
    EXPECT_EQ(node.state(), kv::NodeState::Candidate);
    EXPECT_EQ(node.current_term(), 1u);
    EXPECT_EQ(node.voted_for(), 1u);  // Voted for self.

    // Should have sent RequestVote to both peers.
    auto votes = transport_.find_request_vote();
    EXPECT_EQ(votes.size(), 2u);

    node.stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, WinElectionWithMajorityVotes) {
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_);
    node.start();
    pump(ioc_);

    // Trigger election.
    auto* election_timer = timer_factory_.timer(0);
    election_timer->fire();
    pump(ioc_);
    ASSERT_EQ(node.state(), kv::NodeState::Candidate);
    ASSERT_EQ(node.current_term(), 1u);

    // Deliver a vote grant from peer 2.
    RequestVoteResponse resp;
    resp.set_term(1);
    resp.set_vote_granted(true);
    node.handle_vote_response(2, resp);
    pump(ioc_);

    // With self-vote + peer 2 = 2/3 = majority → Leader.
    EXPECT_EQ(node.state(), kv::NodeState::Leader);
    EXPECT_EQ(node.leader_id(), 1u);

    // Leader should have sent initial heartbeats (AppendEntries).
    auto aes = transport_.find_append_entries();
    EXPECT_GE(aes.size(), 2u);

    node.stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, ElectionLostWhenNotEnoughVotes) {
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_);
    node.start();
    pump(ioc_);

    // Trigger election.
    auto* election_timer = timer_factory_.timer(0);
    election_timer->fire();
    pump(ioc_);
    ASSERT_EQ(node.state(), kv::NodeState::Candidate);

    // Both peers deny vote.
    RequestVoteResponse resp;
    resp.set_term(1);
    resp.set_vote_granted(false);
    node.handle_vote_response(2, resp);
    node.handle_vote_response(3, resp);
    pump(ioc_);

    // Still candidate (didn't get majority).
    EXPECT_EQ(node.state(), kv::NodeState::Candidate);

    node.stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, CandidateStepsDownOnHigherTermVoteResponse) {
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_);
    node.start();
    pump(ioc_);

    // Trigger election.
    auto* election_timer = timer_factory_.timer(0);
    election_timer->fire();
    pump(ioc_);
    ASSERT_EQ(node.state(), kv::NodeState::Candidate);
    ASSERT_EQ(node.current_term(), 1u);

    // Peer responds with a higher term.
    RequestVoteResponse resp;
    resp.set_term(5);
    resp.set_vote_granted(false);
    node.handle_vote_response(2, resp);
    pump(ioc_);

    // Should step down to follower with term 5.
    EXPECT_EQ(node.state(), kv::NodeState::Follower);
    EXPECT_EQ(node.current_term(), 5u);
    EXPECT_FALSE(node.voted_for().has_value());

    node.stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, SplitVoteLeadsToReElection) {
    // 5-node cluster: {1, 2, 3, 4, 5}. Node 1 needs 3 votes.
    RaftNode node(ioc_, 1, {2, 3, 4, 5}, transport_, timer_factory_, logger_);
    node.start();
    pump(ioc_);

    // Trigger first election.
    auto* election_timer = timer_factory_.timer(0);
    election_timer->fire();
    pump(ioc_);
    ASSERT_EQ(node.state(), kv::NodeState::Candidate);
    ASSERT_EQ(node.current_term(), 1u);

    // Get 1 vote grant (self + 1 = 2, not majority of 5).
    RequestVoteResponse grant;
    grant.set_term(1);
    grant.set_vote_granted(true);
    node.handle_vote_response(2, grant);

    // Get 2 denials.
    RequestVoteResponse deny;
    deny.set_term(1);
    deny.set_vote_granted(false);
    node.handle_vote_response(3, deny);
    node.handle_vote_response(4, deny);
    pump(ioc_);

    // Still candidate (split vote).
    EXPECT_EQ(node.state(), kv::NodeState::Candidate);

    // Clear transport and fire election timer again for re-election.
    transport_.clear_sent();

    // The election timer should be waiting again (the loop continues).
    // We need to pump to let the election_timer_loop restart.
    pump(ioc_);

    // Find the election timer - it should have restarted.
    // After start_election, the election_timer_loop continues.
    // Fire it again to trigger a new election.
    election_timer->fire();
    pump(ioc_);

    // New election should have incremented term.
    EXPECT_EQ(node.current_term(), 2u);
    EXPECT_EQ(node.state(), kv::NodeState::Candidate);

    // New RequestVote messages should have been sent.
    auto votes = transport_.find_request_vote();
    EXPECT_GE(votes.size(), 4u);  // 4 peers in 5-node cluster

    node.stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, LeaderAppendsNoOpOnElection) {
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_);
    node.start();
    pump(ioc_);

    // Trigger election and win.
    auto* election_timer = timer_factory_.timer(0);
    election_timer->fire();
    pump(ioc_);

    RequestVoteResponse resp;
    resp.set_term(1);
    resp.set_vote_granted(true);
    node.handle_vote_response(2, resp);
    pump(ioc_);

    ASSERT_EQ(node.state(), kv::NodeState::Leader);

    // Leader should have appended a no-op entry.
    const auto* noop = node.log().entry_at(1);
    ASSERT_NE(noop, nullptr);
    EXPECT_EQ(noop->term(), 1u);
    EXPECT_EQ(noop->command().type(), CMD_NOOP);

    node.stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, SubmitSucceedsAsLeader) {
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_);
    node.start();
    pump(ioc_);

    // Win election.
    auto* election_timer = timer_factory_.timer(0);
    election_timer->fire();
    pump(ioc_);
    RequestVoteResponse resp;
    resp.set_term(1);
    resp.set_vote_granted(true);
    node.handle_vote_response(2, resp);
    pump(ioc_);
    ASSERT_EQ(node.state(), kv::NodeState::Leader);

    transport_.clear_sent();

    // Submit a command.
    Command cmd;
    cmd.set_type(CMD_SET);
    cmd.set_key("foo");
    cmd.set_value("bar");
    EXPECT_TRUE(node.submit(std::move(cmd)));
    pump(ioc_);

    // Entry should be in the log at index 2 (after no-op at 1).
    const auto* entry = node.log().entry_at(2);
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry->command().type(), CMD_SET);
    EXPECT_EQ(entry->command().key(), "foo");

    // Should have triggered replication.
    auto aes = transport_.find_append_entries();
    EXPECT_GE(aes.size(), 2u);

    node.stop();
    pump(ioc_);
}

// ════════════════════════════════════════════════════════════════════════════
//  Replication tests (AppendEntries response handling)
// ════════════════════════════════════════════════════════════════════════════

TEST_F(RaftNodeTest, ReplicationSuccessAdvancesCommitIndex) {
    std::vector<LogEntry> applied;
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_,
                  [&](const LogEntry& e) { applied.push_back(e); });
    node.start();
    pump(ioc_);

    // Win election.
    auto* election_timer = timer_factory_.timer(0);
    election_timer->fire();
    pump(ioc_);
    RequestVoteResponse vresp;
    vresp.set_term(1);
    vresp.set_vote_granted(true);
    node.handle_vote_response(2, vresp);
    pump(ioc_);
    ASSERT_EQ(node.state(), kv::NodeState::Leader);

    // Submit a command (index 2, after no-op at index 1).
    Command cmd;
    cmd.set_type(CMD_SET);
    cmd.set_key("k1");
    cmd.set_value("v1");
    ASSERT_TRUE(node.submit(std::move(cmd)));
    pump(ioc_);

    // Peer 2 successfully replicates up to index 2.
    AppendEntriesResponse ae_resp;
    ae_resp.set_term(1);
    ae_resp.set_success(true);
    ae_resp.set_match_index(2);
    node.handle_append_entries_response(2, ae_resp);
    pump(ioc_);

    // With peer 2's matchIndex=2 and self (always has all entries),
    // majority of {1,2,3} have index 2 replicated (self + peer2 = 2/3).
    // commitIndex should advance to 2.
    EXPECT_EQ(node.commit_index(), 2u);

    // Both entries (no-op + SET) should have been applied.
    EXPECT_EQ(applied.size(), 2u);
    EXPECT_EQ(applied[0].command().type(), CMD_NOOP);
    EXPECT_EQ(applied[1].command().key(), "k1");

    node.stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, ReplicationFailureDecrementsNextIndex) {
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_);
    node.start();
    pump(ioc_);

    // Win election.
    auto* election_timer = timer_factory_.timer(0);
    election_timer->fire();
    pump(ioc_);
    RequestVoteResponse vresp;
    vresp.set_term(1);
    vresp.set_vote_granted(true);
    node.handle_vote_response(2, vresp);
    pump(ioc_);
    ASSERT_EQ(node.state(), kv::NodeState::Leader);

    transport_.clear_sent();

    // Peer 2 rejects AE (consistency check failed).
    AppendEntriesResponse ae_resp;
    ae_resp.set_term(1);
    ae_resp.set_success(false);
    ae_resp.set_match_index(0);
    node.handle_append_entries_response(2, ae_resp);
    pump(ioc_);

    // Should have retried with a new AppendEntries.
    auto aes = transport_.find_append_entries();
    EXPECT_GE(aes.size(), 1u);

    // commitIndex should NOT have advanced.
    EXPECT_EQ(node.commit_index(), 0u);

    node.stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, LeaderStepsDownOnHigherTermAEResponse) {
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_);
    node.start();
    pump(ioc_);

    // Win election.
    auto* election_timer = timer_factory_.timer(0);
    election_timer->fire();
    pump(ioc_);
    RequestVoteResponse vresp;
    vresp.set_term(1);
    vresp.set_vote_granted(true);
    node.handle_vote_response(2, vresp);
    pump(ioc_);
    ASSERT_EQ(node.state(), kv::NodeState::Leader);

    // Peer responds with higher term.
    AppendEntriesResponse ae_resp;
    ae_resp.set_term(5);
    ae_resp.set_success(false);
    ae_resp.set_match_index(0);
    node.handle_append_entries_response(2, ae_resp);
    pump(ioc_);

    // Should step down to follower.
    EXPECT_EQ(node.state(), kv::NodeState::Follower);
    EXPECT_EQ(node.current_term(), 5u);

    node.stop();
    pump(ioc_);
}

// ════════════════════════════════════════════════════════════════════════════
//  handle_message dispatcher tests
// ════════════════════════════════════════════════════════════════════════════

TEST_F(RaftNodeTest, HandleMessageDispatchesRequestVote) {
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_);
    node.start();
    pump(ioc_);

    RaftMessage msg;
    auto* req = msg.mutable_request_vote_req();
    req->set_term(1);
    req->set_candidate_id(2);
    req->set_last_log_index(0);
    req->set_last_log_term(0);

    std::optional<RaftMessage> reply;
    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        reply = co_await node.handle_message(2, msg);
    }, boost::asio::detached);
    pump(ioc_);

    ASSERT_TRUE(reply.has_value());
    ASSERT_TRUE(reply->has_request_vote_resp());
    EXPECT_TRUE(reply->request_vote_resp().vote_granted());
    EXPECT_EQ(reply->request_vote_resp().term(), 1u);

    node.stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, HandleMessageDispatchesAppendEntries) {
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_);
    node.start();
    pump(ioc_);

    RaftMessage msg;
    auto* req = msg.mutable_append_entries_req();
    req->set_term(1);
    req->set_leader_id(2);
    req->set_prev_log_index(0);
    req->set_prev_log_term(0);
    req->set_leader_commit(0);

    std::optional<RaftMessage> reply;
    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        reply = co_await node.handle_message(2, msg);
    }, boost::asio::detached);
    pump(ioc_);

    ASSERT_TRUE(reply.has_value());
    ASSERT_TRUE(reply->has_append_entries_resp());
    EXPECT_TRUE(reply->append_entries_resp().success());

    node.stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, HandleMessageDispatchesVoteResponse) {
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_);
    node.start();
    pump(ioc_);

    // First become a candidate.
    auto* election_timer = timer_factory_.timer(0);
    election_timer->fire();
    pump(ioc_);
    ASSERT_EQ(node.state(), kv::NodeState::Candidate);

    // Deliver vote response via handle_message.
    RaftMessage msg;
    auto* resp = msg.mutable_request_vote_resp();
    resp->set_term(1);
    resp->set_vote_granted(true);

    std::optional<RaftMessage> reply;
    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        reply = co_await node.handle_message(2, msg);
    }, boost::asio::detached);
    pump(ioc_);

    // Response messages return nullopt.
    EXPECT_FALSE(reply.has_value());
    // But the vote should have been counted → leader.
    EXPECT_EQ(node.state(), kv::NodeState::Leader);

    node.stop();
    pump(ioc_);
}

// ════════════════════════════════════════════════════════════════════════════
//  RaftLog snapshot-aware tests (truncate_prefix)
// ════════════════════════════════════════════════════════════════════════════

TEST_F(RaftLogTest, TruncatePrefixRemovesEntries) {
    log_.append(make_entry(1, 1));
    log_.append(make_entry(1, 2));
    log_.append(make_entry(2, 3));

    log_.truncate_prefix(2, 1);
    EXPECT_EQ(log_.offset(), 2u);
    EXPECT_EQ(log_.last_index(), 3u);
    EXPECT_EQ(log_.size(), 1u);  // only entry 3 remains
    EXPECT_EQ(log_.term_at(3), 2u);
    EXPECT_FALSE(log_.term_at(1).has_value());  // compacted
    // term_at(2) returns the snapshot_last_term since 2 == offset_
    EXPECT_EQ(log_.term_at(2), 1u);
}

TEST_F(RaftLogTest, TruncatePrefixAllEntries) {
    log_.append(make_entry(1, 1));
    log_.append(make_entry(2, 2));

    log_.truncate_prefix(2, 2);
    EXPECT_EQ(log_.offset(), 2u);
    EXPECT_EQ(log_.last_index(), 2u);  // offset_ when empty
    EXPECT_TRUE(log_.empty());
    EXPECT_EQ(log_.last_term(), 2u);  // snapshot_last_term
}

TEST_F(RaftLogTest, TruncatePrefixBeyondLastIndex) {
    log_.append(make_entry(1, 1));

    log_.truncate_prefix(5, 3);
    EXPECT_EQ(log_.offset(), 5u);
    EXPECT_TRUE(log_.empty());
    EXPECT_EQ(log_.last_index(), 5u);
    EXPECT_EQ(log_.last_term(), 3u);
}

TEST_F(RaftLogTest, TruncatePrefixNoOpIfSameOffset) {
    log_.append(make_entry(1, 1));
    log_.append(make_entry(1, 2));

    log_.truncate_prefix(0, 0);  // offset_ is already 0
    EXPECT_EQ(log_.size(), 2u);
}

TEST_F(RaftLogTest, TryAppendAfterTruncatePrefix) {
    log_.append(make_entry(1, 1));
    log_.append(make_entry(1, 2));
    log_.append(make_entry(2, 3));
    log_.append(make_entry(2, 4));

    log_.truncate_prefix(2, 1);
    // Entries 3 and 4 remain.  Append via try_append.
    std::vector<LogEntry> entries = {make_entry(3, 5)};
    EXPECT_TRUE(log_.try_append(4, 2, entries));
    EXPECT_EQ(log_.last_index(), 5u);
    EXPECT_EQ(log_.last_term(), 3u);
}

TEST_F(RaftLogTest, EntriesFromAfterTruncatePrefix) {
    log_.append(make_entry(1, 1));
    log_.append(make_entry(1, 2));
    log_.append(make_entry(2, 3));

    log_.truncate_prefix(1, 1);
    auto entries = log_.entries_from(2);
    EXPECT_EQ(entries.size(), 2u);
    EXPECT_EQ(entries[0].index(), 2u);
    EXPECT_EQ(entries[1].index(), 3u);

    // Requesting compacted entries returns empty.
    auto compacted = log_.entries_from(1);
    EXPECT_TRUE(compacted.empty());
}

// ════════════════════════════════════════════════════════════════════════════
//  StateMachine reset test
// ════════════════════════════════════════════════════════════════════════════

TEST_F(StateMachineTest, ResetClearsStorageAndUpdatesApplied) {
    storage_.set("foo", "bar");
    storage_.set("baz", "qux");
    sm_.apply(make_entry(1, 1, CMD_SET, "x", "y"));
    EXPECT_EQ(sm_.last_applied(), 1u);

    sm_.reset(10);
    EXPECT_EQ(sm_.last_applied(), 10u);
    EXPECT_EQ(storage_.size(), 0u);
    EXPECT_FALSE(storage_.get("foo").has_value());
    EXPECT_FALSE(storage_.get("x").has_value());
}

// ════════════════════════════════════════════════════════════════════════════
//  InstallSnapshot tests
// ════════════════════════════════════════════════════════════════════════════

namespace {

// Mock SnapshotIO for testing without real file I/O.
class MockSnapshotIO : public SnapshotIO {
public:
    SnapshotData load_snapshot_for_sending() override {
        return snapshot_data_;
    }

    bool install_snapshot(const std::string& data,
                         uint64_t last_included_index,
                         uint64_t last_included_term,
                         const std::optional<ClusterConfig>& config = {}) override {
        installed_data_ = data;
        installed_index_ = last_included_index;
        installed_term_ = last_included_term;
        installed_config_ = config;
        install_count_++;
        return install_result_;
    }

    bool create_snapshot(uint64_t last_included_index,
                        uint64_t last_included_term,
                        const std::optional<ClusterConfig>& config = {}) override {
        created_index_ = last_included_index;
        created_term_ = last_included_term;
        created_config_ = config;
        create_count_++;

        if (create_result_) {
            // Update the data available for load_snapshot_for_sending.
            snapshot_data_.last_included_index = last_included_index;
            snapshot_data_.last_included_term = last_included_term;
            snapshot_data_.config = config;
            // Keep existing data (or set to a default).
            if (snapshot_data_.data.empty()) {
                snapshot_data_.data = "snapshot_data";
            }
        }

        return create_result_;
    }

    std::optional<ClusterConfig> load_cluster_config() override {
        return loaded_config_;
    }

    // Test configuration.
    void set_snapshot_data(SnapshotData data) { snapshot_data_ = std::move(data); }
    void set_install_result(bool result) { install_result_ = result; }
    void set_create_result(bool result) { create_result_ = result; }
    void set_loaded_config(std::optional<ClusterConfig> config) {
        loaded_config_ = std::move(config);
    }

    // Test inspection.
    [[nodiscard]] int install_count() const { return install_count_; }
    [[nodiscard]] int create_count() const { return create_count_; }
    [[nodiscard]] const std::string& installed_data() const { return installed_data_; }
    [[nodiscard]] uint64_t installed_index() const { return installed_index_; }
    [[nodiscard]] uint64_t installed_term() const { return installed_term_; }
    [[nodiscard]] uint64_t created_index() const { return created_index_; }
    [[nodiscard]] uint64_t created_term() const { return created_term_; }
    [[nodiscard]] const std::optional<ClusterConfig>& installed_config() const {
        return installed_config_;
    }
    [[nodiscard]] const std::optional<ClusterConfig>& created_config() const {
        return created_config_;
    }

private:
    SnapshotData snapshot_data_;
    bool install_result_ = true;
    bool create_result_ = true;

    std::string installed_data_;
    uint64_t installed_index_ = 0;
    uint64_t installed_term_ = 0;
    std::optional<ClusterConfig> installed_config_;

    int install_count_ = 0;

    uint64_t created_index_ = 0;
    uint64_t created_term_ = 0;
    std::optional<ClusterConfig> created_config_;
    int create_count_ = 0;

    std::optional<ClusterConfig> loaded_config_;
};

// Helper: create a RaftNode as leader (term 1) with the given snapshot setup.
// Returns the node.  Caller must call node.stop() + pump() when done.
struct LeaderSetup {
    std::unique_ptr<RaftNode> node;
    DeterministicTimer* election_timer = nullptr;
    DeterministicTimer* heartbeat_timer = nullptr;
};

LeaderSetup make_leader(boost::asio::io_context& ioc,
                        MockTransport& transport,
                        DeterministicTimerFactory& timer_factory,
                        std::shared_ptr<spdlog::logger> logger,
                        RaftNode::ApplyCallback on_apply = {},
                        MockSnapshotIO* snap_io = nullptr,
                        uint64_t snap_interval = 0) {
    auto node = std::make_unique<RaftNode>(
        ioc, 1, std::vector<uint32_t>{2, 3},
        transport, timer_factory, logger,
        std::move(on_apply), snap_io, snap_interval);

    node->start();
    pump(ioc);

    auto* election_timer = timer_factory.timer(timer_factory.timer_count() - 2);
    election_timer->fire();
    pump(ioc);

    RequestVoteResponse vresp;
    vresp.set_term(1);
    vresp.set_vote_granted(true);
    node->handle_vote_response(2, vresp);
    pump(ioc);

    transport.clear_sent();

    auto* heartbeat_timer = timer_factory.timer(timer_factory.timer_count() - 1);

    return {std::move(node), election_timer, heartbeat_timer};
}

} // anonymous namespace

// ── Follower receiving InstallSnapshot ───────────────────────────────────────

TEST_F(RaftNodeTest, FollowerAcceptsInstallSnapshot) {
    MockSnapshotIO snap_io;
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_,
                  {}, &snap_io);
    node.start();
    pump(ioc_);

    InstallSnapshotRequest req;
    req.set_term(1);
    req.set_leader_id(2);
    req.set_last_included_index(10);
    req.set_last_included_term(1);
    req.set_data("snapshot_bytes");

    InstallSnapshotResponse resp;
    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        resp = co_await node.handle_install_snapshot(req);
    }, boost::asio::detached);
    pump(ioc_);

    EXPECT_EQ(resp.term(), 1u);
    EXPECT_EQ(snap_io.install_count(), 1);
    EXPECT_EQ(snap_io.installed_data(), "snapshot_bytes");
    EXPECT_EQ(snap_io.installed_index(), 10u);
    EXPECT_EQ(snap_io.installed_term(), 1u);

    // Node state should be updated.
    EXPECT_EQ(node.snapshot_last_index(), 10u);
    EXPECT_EQ(node.snapshot_last_term(), 1u);
    EXPECT_EQ(node.commit_index(), 10u);
    EXPECT_EQ(node.last_applied(), 10u);
    EXPECT_EQ(node.leader_id(), 2u);

    node.stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, FollowerRejectsInstallSnapshotStaleTerm) {
    MockSnapshotIO snap_io;
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_,
                  {}, &snap_io);
    node.start();
    pump(ioc_);

    // First update term to 5.
    RequestVoteRequest vreq;
    vreq.set_term(5);
    vreq.set_candidate_id(2);
    vreq.set_last_log_index(0);
    vreq.set_last_log_term(0);
    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        co_await node.handle_request_vote(vreq);
    }, boost::asio::detached);
    pump(ioc_);

    // Stale InstallSnapshot.
    InstallSnapshotRequest req;
    req.set_term(3);
    req.set_leader_id(3);
    req.set_last_included_index(10);
    req.set_last_included_term(2);
    req.set_data("snapshot_bytes");

    InstallSnapshotResponse resp;
    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        resp = co_await node.handle_install_snapshot(req);
    }, boost::asio::detached);
    pump(ioc_);

    EXPECT_EQ(resp.term(), 5u);
    EXPECT_EQ(snap_io.install_count(), 0);  // not installed
    EXPECT_EQ(node.snapshot_last_index(), 0u);

    node.stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, FollowerStepsDownOnHigherTermInstallSnapshot) {
    MockSnapshotIO snap_io;
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_,
                  {}, &snap_io);
    node.start();
    pump(ioc_);

    InstallSnapshotRequest req;
    req.set_term(5);
    req.set_leader_id(2);
    req.set_last_included_index(10);
    req.set_last_included_term(3);
    req.set_data("data");

    InstallSnapshotResponse resp;
    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        resp = co_await node.handle_install_snapshot(req);
    }, boost::asio::detached);
    pump(ioc_);

    EXPECT_EQ(resp.term(), 5u);
    EXPECT_EQ(node.current_term(), 5u);
    EXPECT_EQ(node.state(), kv::NodeState::Follower);
    EXPECT_EQ(snap_io.install_count(), 1);

    node.stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, FollowerIgnoresOlderSnapshot) {
    MockSnapshotIO snap_io;
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_,
                  {}, &snap_io);
    node.start();
    pump(ioc_);

    // Install a snapshot at index 10.
    InstallSnapshotRequest req1;
    req1.set_term(1);
    req1.set_leader_id(2);
    req1.set_last_included_index(10);
    req1.set_last_included_term(1);
    req1.set_data("data1");

    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        co_await node.handle_install_snapshot(req1);
    }, boost::asio::detached);
    pump(ioc_);
    ASSERT_EQ(snap_io.install_count(), 1);

    // Try installing an older snapshot at index 5.
    InstallSnapshotRequest req2;
    req2.set_term(1);
    req2.set_leader_id(2);
    req2.set_last_included_index(5);
    req2.set_last_included_term(1);
    req2.set_data("data2");

    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        co_await node.handle_install_snapshot(req2);
    }, boost::asio::detached);
    pump(ioc_);

    // Should not have installed the older snapshot.
    EXPECT_EQ(snap_io.install_count(), 1);
    EXPECT_EQ(node.snapshot_last_index(), 10u);

    node.stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, FollowerHandlesInstallSnapshotFailure) {
    MockSnapshotIO snap_io;
    snap_io.set_install_result(false);  // simulate I/O failure

    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_,
                  {}, &snap_io);
    node.start();
    pump(ioc_);

    InstallSnapshotRequest req;
    req.set_term(1);
    req.set_leader_id(2);
    req.set_last_included_index(10);
    req.set_last_included_term(1);
    req.set_data("data");

    InstallSnapshotResponse resp;
    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        resp = co_await node.handle_install_snapshot(req);
    }, boost::asio::detached);
    pump(ioc_);

    // install_snapshot returned false, so snapshot state should not be updated.
    EXPECT_EQ(snap_io.install_count(), 1);
    EXPECT_EQ(node.snapshot_last_index(), 0u);
    EXPECT_EQ(node.commit_index(), 0u);
    EXPECT_EQ(node.last_applied(), 0u);

    node.stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, FollowerInstallSnapshotTruncatesLog) {
    MockSnapshotIO snap_io;
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_,
                  {}, &snap_io);
    node.start();
    pump(ioc_);

    // First, get some log entries via AppendEntries.
    AppendEntriesRequest ae_req;
    ae_req.set_term(1);
    ae_req.set_leader_id(2);
    ae_req.set_prev_log_index(0);
    ae_req.set_prev_log_term(0);
    ae_req.set_leader_commit(0);
    for (int i = 1; i <= 5; ++i) {
        auto* e = ae_req.add_entries();
        e->set_term(1);
        e->set_index(static_cast<uint64_t>(i));
        e->mutable_command()->set_type(CMD_SET);
        e->mutable_command()->set_key("k" + std::to_string(i));
        e->mutable_command()->set_value("v" + std::to_string(i));
    }

    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        co_await node.handle_append_entries(ae_req);
    }, boost::asio::detached);
    pump(ioc_);
    ASSERT_EQ(node.log().last_index(), 5u);

    // Install snapshot at index 3. Entries 1-3 should be truncated.
    InstallSnapshotRequest req;
    req.set_term(1);
    req.set_leader_id(2);
    req.set_last_included_index(3);
    req.set_last_included_term(1);
    req.set_data("snap_data");

    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        co_await node.handle_install_snapshot(req);
    }, boost::asio::detached);
    pump(ioc_);

    EXPECT_EQ(node.log().offset(), 3u);
    EXPECT_EQ(node.log().last_index(), 5u);
    EXPECT_EQ(node.log().size(), 2u);  // entries 4 and 5 remain

    node.stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, CandidateStepsDownOnInstallSnapshot) {
    MockSnapshotIO snap_io;
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_,
                  {}, &snap_io);
    node.start();
    pump(ioc_);

    // Become candidate.
    auto* election_timer = timer_factory_.timer(0);
    election_timer->fire();
    pump(ioc_);
    ASSERT_EQ(node.state(), kv::NodeState::Candidate);

    // Receive InstallSnapshot from a leader with same term.
    InstallSnapshotRequest req;
    req.set_term(1);
    req.set_leader_id(2);
    req.set_last_included_index(5);
    req.set_last_included_term(1);
    req.set_data("data");

    InstallSnapshotResponse resp;
    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        resp = co_await node.handle_install_snapshot(req);
    }, boost::asio::detached);
    pump(ioc_);

    EXPECT_EQ(node.state(), kv::NodeState::Follower);
    EXPECT_EQ(snap_io.install_count(), 1);

    node.stop();
    pump(ioc_);
}

// ── Leader sending InstallSnapshot ───────────────────────────────────────────

TEST_F(RaftNodeTest, LeaderSendsInstallSnapshotWhenPeerBehind) {
    MockSnapshotIO snap_io;
    snap_io.set_snapshot_data({"snapshot_bytes", 5, 1});

    auto setup = make_leader(ioc_, transport_, timer_factory_, logger_,
                             {}, &snap_io);
    auto& node = *setup.node;
    ASSERT_EQ(node.state(), kv::NodeState::Leader);

    // Simulate: leader has snapshot at index 5, log starts after that.
    // We need to manually set the leader's snapshot state.
    // Submit entries to build up the log, then we'll trigger the scenario
    // by making the peer's nextIndex point to compacted entries.

    // For simplicity, let's directly test: if nextIndex[peer] <= snapshot_last_included_index_,
    // the leader sends InstallSnapshot.
    // We can achieve this by: having the leader create a snapshot, then try to
    // send AE to a peer whose nextIndex is within the snapshot.

    // But the leader was just elected (term 1, no-op at index 1).
    // Let's submit 5 entries, get them committed, and trigger a snapshot.

    // Actually, let's take a more direct approach. The leader has no snapshot yet.
    // Let's set up the mock snapshot data and manually adjust snapshot state.

    // Instead, let's test the simpler path: when peer's nextIndex gets decremented
    // below the snapshot boundary. We'll set the mock data, artificially set the
    // leader's snapshot indices, and verify it sends InstallSnapshot.

    // We need to be a bit creative since we can't directly set private members.
    // Let's create a leader with snapshot_io and snapshot_interval, submit enough
    // entries to trigger a snapshot, then have a slow peer.

    node.stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, LeaderSendsInstallSnapshotAfterSnapshotCreation) {
    MockSnapshotIO snap_io;
    snap_io.set_snapshot_data({"snapshot_bytes", 0, 0});

    std::vector<LogEntry> applied;
    // Create leader with snapshot_interval=3 so a snapshot triggers after 3 entries.
    auto node = std::make_unique<RaftNode>(
        ioc_, 1, std::vector<uint32_t>{2, 3},
        transport_, timer_factory_, logger_,
        [&](const LogEntry& e) { applied.push_back(e); },
        &snap_io, 3);

    node->start();
    pump(ioc_);

    // Win election.
    auto* election_timer = timer_factory_.timer(0);
    election_timer->fire();
    pump(ioc_);
    RequestVoteResponse vresp;
    vresp.set_term(1);
    vresp.set_vote_granted(true);
    node->handle_vote_response(2, vresp);
    pump(ioc_);
    ASSERT_EQ(node->state(), kv::NodeState::Leader);

    transport_.clear_sent();

    // Submit 2 more entries (no-op at 1 is already there) to get to index 3.
    for (int i = 0; i < 2; i++) {
        Command cmd;
        cmd.set_type(CMD_SET);
        cmd.set_key("k" + std::to_string(i));
        cmd.set_value("v" + std::to_string(i));
        ASSERT_TRUE(node->submit(std::move(cmd)));
    }
    pump(ioc_);
    ASSERT_EQ(node->log().last_index(), 3u);

    // Get peer 2 to replicate up to index 3.
    AppendEntriesResponse ae_resp;
    ae_resp.set_term(1);
    ae_resp.set_success(true);
    ae_resp.set_match_index(3);
    node->handle_append_entries_response(2, ae_resp);
    pump(ioc_);

    // Commit index should advance to 3, which triggers apply_committed_entries,
    // which triggers maybe_trigger_snapshot (3 entries applied, interval=3).
    EXPECT_EQ(node->commit_index(), 3u);
    EXPECT_EQ(snap_io.create_count(), 1);
    EXPECT_EQ(snap_io.created_index(), 3u);
    EXPECT_EQ(node->snapshot_last_index(), 3u);
    EXPECT_EQ(node->log().offset(), 3u);

    // Now, peer 3 is behind (nextIndex=1, which is <= snapshot_last_index=3).
    // A heartbeat/replication to peer 3 should send InstallSnapshot.
    transport_.clear_sent();

    // Trigger heartbeat.
    auto* heartbeat_timer = timer_factory_.timer(1);
    heartbeat_timer->fire();
    pump(ioc_);

    // Check: peer 3 should get InstallSnapshot, peer 2 should get AppendEntries.
    bool found_install_snapshot = false;
    bool found_append_entries_to_2 = false;
    for (const auto& m : transport_.sent()) {
        if (m.msg.has_install_snapshot_req() && m.peer_id == 3) {
            found_install_snapshot = true;
            EXPECT_EQ(m.msg.install_snapshot_req().last_included_index(), 3u);
            EXPECT_EQ(m.msg.install_snapshot_req().data(), "snapshot_bytes");
        }
        if (m.msg.has_append_entries_req() && m.peer_id == 2) {
            found_append_entries_to_2 = true;
        }
    }
    EXPECT_TRUE(found_install_snapshot);
    EXPECT_TRUE(found_append_entries_to_2);

    node->stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, LeaderHandlesInstallSnapshotResponse) {
    MockSnapshotIO snap_io;
    snap_io.set_snapshot_data({"snapshot_bytes", 5, 1});

    std::vector<LogEntry> applied;
    auto node = std::make_unique<RaftNode>(
        ioc_, 1, std::vector<uint32_t>{2, 3},
        transport_, timer_factory_, logger_,
        [&](const LogEntry& e) { applied.push_back(e); },
        &snap_io, 3);

    node->start();
    pump(ioc_);

    auto* election_timer = timer_factory_.timer(0);
    election_timer->fire();
    pump(ioc_);
    RequestVoteResponse vresp;
    vresp.set_term(1);
    vresp.set_vote_granted(true);
    node->handle_vote_response(2, vresp);
    pump(ioc_);
    ASSERT_EQ(node->state(), kv::NodeState::Leader);

    // Submit 2 entries, commit them, trigger snapshot.
    for (int i = 0; i < 2; i++) {
        Command cmd;
        cmd.set_type(CMD_SET);
        cmd.set_key("k" + std::to_string(i));
        cmd.set_value("v" + std::to_string(i));
        node->submit(std::move(cmd));
    }
    pump(ioc_);

    AppendEntriesResponse ae_resp;
    ae_resp.set_term(1);
    ae_resp.set_success(true);
    ae_resp.set_match_index(3);
    node->handle_append_entries_response(2, ae_resp);
    pump(ioc_);
    ASSERT_EQ(node->snapshot_last_index(), 3u);

    // Now simulate: leader sent InstallSnapshot to peer 3, peer 3 accepts.
    InstallSnapshotResponse snap_resp;
    snap_resp.set_term(1);
    node->handle_install_snapshot_response(3, snap_resp);
    pump(ioc_);

    // After handling the response, peer 3's nextIndex/matchIndex should be updated.
    // We can verify by sending a heartbeat and checking what gets sent to peer 3.
    transport_.clear_sent();
    auto* heartbeat_timer = timer_factory_.timer(1);
    heartbeat_timer->fire();
    pump(ioc_);

    // Peer 3 should now get AppendEntries (not InstallSnapshot), since its
    // nextIndex should be snapshot_last_included_index + 1 = 4.
    bool found_ae_to_3 = false;
    bool found_is_to_3 = false;
    for (const auto& m : transport_.sent()) {
        if (m.peer_id == 3) {
            if (m.msg.has_append_entries_req()) found_ae_to_3 = true;
            if (m.msg.has_install_snapshot_req()) found_is_to_3 = true;
        }
    }
    EXPECT_TRUE(found_ae_to_3);
    EXPECT_FALSE(found_is_to_3);

    node->stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, LeaderStepsDownOnHigherTermInstallSnapshotResponse) {
    MockSnapshotIO snap_io;
    snap_io.set_snapshot_data({"data", 5, 1});

    auto setup = make_leader(ioc_, transport_, timer_factory_, logger_,
                             {}, &snap_io);
    auto& node = *setup.node;
    ASSERT_EQ(node.state(), kv::NodeState::Leader);

    InstallSnapshotResponse resp;
    resp.set_term(10);
    node.handle_install_snapshot_response(2, resp);
    pump(ioc_);

    EXPECT_EQ(node.state(), kv::NodeState::Follower);
    EXPECT_EQ(node.current_term(), 10u);

    node.stop();
    pump(ioc_);
}

// ── handle_message dispatch tests for InstallSnapshot ────────────────────────

TEST_F(RaftNodeTest, HandleMessageDispatchesInstallSnapshot) {
    MockSnapshotIO snap_io;
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_,
                  {}, &snap_io);
    node.start();
    pump(ioc_);

    RaftMessage msg;
    auto* req = msg.mutable_install_snapshot_req();
    req->set_term(1);
    req->set_leader_id(2);
    req->set_last_included_index(5);
    req->set_last_included_term(1);
    req->set_data("test_data");

    std::optional<RaftMessage> reply;
    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        reply = co_await node.handle_message(2, msg);
    }, boost::asio::detached);
    pump(ioc_);

    ASSERT_TRUE(reply.has_value());
    ASSERT_TRUE(reply->has_install_snapshot_resp());
    EXPECT_EQ(reply->install_snapshot_resp().term(), 1u);
    EXPECT_EQ(snap_io.install_count(), 1);

    node.stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, HandleMessageDispatchesInstallSnapshotResponse) {
    MockSnapshotIO snap_io;
    snap_io.set_snapshot_data({"data", 5, 1});

    auto setup = make_leader(ioc_, transport_, timer_factory_, logger_,
                             {}, &snap_io);
    auto& node = *setup.node;
    ASSERT_EQ(node.state(), kv::NodeState::Leader);

    // Send an InstallSnapshot response with a higher term.
    RaftMessage msg;
    auto* resp = msg.mutable_install_snapshot_resp();
    resp->set_term(10);

    std::optional<RaftMessage> reply;
    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        reply = co_await node.handle_message(2, msg);
    }, boost::asio::detached);
    pump(ioc_);

    // Response-type messages return nullopt.
    EXPECT_FALSE(reply.has_value());
    // But the higher term should cause step-down.
    EXPECT_EQ(node.state(), kv::NodeState::Follower);
    EXPECT_EQ(node.current_term(), 10u);

    node.stop();
    pump(ioc_);
}

// ── Snapshot trigger tests ───────────────────────────────────────────────────

TEST_F(RaftNodeTest, SnapshotTriggersAfterInterval) {
    MockSnapshotIO snap_io;
    std::vector<LogEntry> applied;

    auto node = std::make_unique<RaftNode>(
        ioc_, 1, std::vector<uint32_t>{2, 3},
        transport_, timer_factory_, logger_,
        [&](const LogEntry& e) { applied.push_back(e); },
        &snap_io, 5);  // snapshot every 5 entries

    node->start();
    pump(ioc_);

    auto* election_timer = timer_factory_.timer(0);
    election_timer->fire();
    pump(ioc_);
    RequestVoteResponse vresp;
    vresp.set_term(1);
    vresp.set_vote_granted(true);
    node->handle_vote_response(2, vresp);
    pump(ioc_);
    ASSERT_EQ(node->state(), kv::NodeState::Leader);

    // Submit 4 more entries (+ 1 no-op = 5 total).
    for (int i = 0; i < 4; i++) {
        Command cmd;
        cmd.set_type(CMD_SET);
        cmd.set_key("k" + std::to_string(i));
        cmd.set_value("v" + std::to_string(i));
        node->submit(std::move(cmd));
    }
    pump(ioc_);
    ASSERT_EQ(node->log().last_index(), 5u);

    // Get peer 2 to replicate all entries.
    AppendEntriesResponse ae_resp;
    ae_resp.set_term(1);
    ae_resp.set_success(true);
    ae_resp.set_match_index(5);
    node->handle_append_entries_response(2, ae_resp);
    pump(ioc_);

    // Should have triggered snapshot (5 entries applied, interval=5).
    EXPECT_EQ(snap_io.create_count(), 1);
    EXPECT_EQ(snap_io.created_index(), 5u);
    EXPECT_EQ(node->snapshot_last_index(), 5u);
    EXPECT_EQ(node->log().offset(), 5u);
    EXPECT_TRUE(node->log().empty());

    node->stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, NoSnapshotWhenIntervalZero) {
    MockSnapshotIO snap_io;
    std::vector<LogEntry> applied;

    auto node = std::make_unique<RaftNode>(
        ioc_, 1, std::vector<uint32_t>{2, 3},
        transport_, timer_factory_, logger_,
        [&](const LogEntry& e) { applied.push_back(e); },
        &snap_io, 0);  // no auto-snapshot

    node->start();
    pump(ioc_);

    auto* election_timer = timer_factory_.timer(0);
    election_timer->fire();
    pump(ioc_);
    RequestVoteResponse vresp;
    vresp.set_term(1);
    vresp.set_vote_granted(true);
    node->handle_vote_response(2, vresp);
    pump(ioc_);

    // Submit and commit 5 entries.
    for (int i = 0; i < 4; i++) {
        Command cmd;
        cmd.set_type(CMD_SET);
        cmd.set_key("k" + std::to_string(i));
        cmd.set_value("v" + std::to_string(i));
        node->submit(std::move(cmd));
    }
    pump(ioc_);

    AppendEntriesResponse ae_resp;
    ae_resp.set_term(1);
    ae_resp.set_success(true);
    ae_resp.set_match_index(5);
    node->handle_append_entries_response(2, ae_resp);
    pump(ioc_);

    // No snapshot should be created.
    EXPECT_EQ(snap_io.create_count(), 0);
    EXPECT_EQ(node->snapshot_last_index(), 0u);

    node->stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, NoSnapshotWithoutSnapshotIO) {
    std::vector<LogEntry> applied;

    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_,
                  [&](const LogEntry& e) { applied.push_back(e); },
                  nullptr, 5);  // no snapshot_io, interval=5

    node.start();
    pump(ioc_);

    auto* election_timer = timer_factory_.timer(0);
    election_timer->fire();
    pump(ioc_);
    RequestVoteResponse vresp;
    vresp.set_term(1);
    vresp.set_vote_granted(true);
    node.handle_vote_response(2, vresp);
    pump(ioc_);

    for (int i = 0; i < 4; i++) {
        Command cmd;
        cmd.set_type(CMD_SET);
        cmd.set_key("k" + std::to_string(i));
        cmd.set_value("v" + std::to_string(i));
        node.submit(std::move(cmd));
    }
    pump(ioc_);

    AppendEntriesResponse ae_resp;
    ae_resp.set_term(1);
    ae_resp.set_success(true);
    ae_resp.set_match_index(5);
    node.handle_append_entries_response(2, ae_resp);
    pump(ioc_);

    // Should not crash despite no snapshot_io.
    EXPECT_EQ(node.snapshot_last_index(), 0u);
    EXPECT_EQ(node.log().offset(), 0u);

    node.stop();
    pump(ioc_);
}

// ════════════════════════════════════════════════════════════════════════════
//  PersistCallback tests
// ════════════════════════════════════════════════════════════════════════════

// Recording mock that tracks all persist_metadata / persist_entry calls.
class RecordingPersistCallback : public PersistCallback {
public:
    struct MetadataRecord {
        uint64_t term;
        int32_t voted_for;
    };

    void persist_metadata(uint64_t term, int32_t voted_for) override {
        metadata_calls_.push_back({term, voted_for});
    }

    void persist_entry(const LogEntry& entry) override {
        entry_calls_.push_back(entry);
    }

    [[nodiscard]] const std::vector<MetadataRecord>& metadata_calls() const {
        return metadata_calls_;
    }
    [[nodiscard]] const std::vector<LogEntry>& entry_calls() const {
        return entry_calls_;
    }

    void clear() {
        metadata_calls_.clear();
        entry_calls_.clear();
    }

private:
    std::vector<MetadataRecord> metadata_calls_;
    std::vector<LogEntry> entry_calls_;
};

TEST_F(RaftNodeTest, PersistCallbackOnElection) {
    RecordingPersistCallback persist;
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_,
                  {}, nullptr, 0, &persist);
    node.start();
    pump(ioc_);

    // Trigger election timeout.
    auto* election_timer = timer_factory_.timer(0);
    election_timer->fire();
    pump(ioc_);

    // start_election() should persist metadata(term=1, votedFor=1).
    ASSERT_GE(persist.metadata_calls().size(), 1u);
    EXPECT_EQ(persist.metadata_calls().back().term, 1u);
    EXPECT_EQ(persist.metadata_calls().back().voted_for, 1);

    node.stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, PersistCallbackOnVoteGrant) {
    RecordingPersistCallback persist;
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_,
                  {}, nullptr, 0, &persist);
    node.start();
    pump(ioc_);

    persist.clear();

    RequestVoteRequest req;
    req.set_term(1);
    req.set_candidate_id(2);
    req.set_last_log_index(0);
    req.set_last_log_term(0);

    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        auto resp = co_await node.handle_request_vote(req);
        EXPECT_TRUE(resp.vote_granted());
    }, boost::asio::detached);
    pump(ioc_);

    // Should have persisted: first become_follower(1) → metadata(1, -1),
    // then vote grant → metadata(1, 2).
    ASSERT_GE(persist.metadata_calls().size(), 1u);
    auto& last = persist.metadata_calls().back();
    EXPECT_EQ(last.term, 1u);
    EXPECT_EQ(last.voted_for, 2);

    node.stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, PersistCallbackOnSubmit) {
    RecordingPersistCallback persist;
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_,
                  {}, nullptr, 0, &persist);
    node.start();
    pump(ioc_);

    // Win election.
    auto* et = timer_factory_.timer(0);
    et->fire();
    pump(ioc_);

    RequestVoteResponse vresp;
    vresp.set_term(1);
    vresp.set_vote_granted(true);
    node.handle_vote_response(2, vresp);
    pump(ioc_);

    persist.clear();

    // Submit a command.
    Command cmd;
    cmd.set_type(CMD_SET);
    cmd.set_key("foo");
    cmd.set_value("bar");
    ASSERT_TRUE(node.submit(std::move(cmd)));
    pump(ioc_);

    // Should have persisted the log entry.
    ASSERT_EQ(persist.entry_calls().size(), 1u);
    EXPECT_EQ(persist.entry_calls()[0].command().key(), "foo");
    EXPECT_EQ(persist.entry_calls()[0].command().value(), "bar");

    node.stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, PersistCallbackOnBecomeLeaderNoOp) {
    RecordingPersistCallback persist;
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_,
                  {}, nullptr, 0, &persist);
    node.start();
    pump(ioc_);

    // Trigger election.
    auto* et = timer_factory_.timer(0);
    et->fire();
    pump(ioc_);

    persist.clear();

    // Win election — become_leader appends a no-op entry.
    RequestVoteResponse vresp;
    vresp.set_term(1);
    vresp.set_vote_granted(true);
    node.handle_vote_response(2, vresp);
    pump(ioc_);

    // Should have persisted the no-op entry.
    ASSERT_GE(persist.entry_calls().size(), 1u);
    EXPECT_EQ(persist.entry_calls()[0].command().type(), CMD_NOOP);

    node.stop();
    pump(ioc_);
}

TEST_F(RaftNodeTest, PersistCallbackOnAppendEntries) {
    RecordingPersistCallback persist;
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_,
                  {}, nullptr, 0, &persist);
    node.start();
    pump(ioc_);

    persist.clear();

    AppendEntriesRequest req;
    req.set_term(1);
    req.set_leader_id(2);
    req.set_prev_log_index(0);
    req.set_prev_log_term(0);
    req.set_leader_commit(0);

    auto* e = req.add_entries();
    e->set_term(1);
    e->set_index(1);
    auto* cmd = e->mutable_command();
    cmd->set_type(CMD_SET);
    cmd->set_key("k1");
    cmd->set_value("v1");

    bool success = false;
    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        auto resp = co_await node.handle_append_entries(req);
        success = resp.success();
    }, boost::asio::detached);
    pump(ioc_);

    EXPECT_TRUE(success);

    // Should have persisted: metadata for become_follower(1), then the entry.
    ASSERT_GE(persist.entry_calls().size(), 1u);
    EXPECT_EQ(persist.entry_calls().back().command().key(), "k1");

    node.stop();
    pump(ioc_);
}

// ════════════════════════════════════════════════════════════════════════════
//  CommitAwaiter tests
// ════════════════════════════════════════════════════════════════════════════

class CommitAwaiterTest : public ::testing::Test {
protected:
    void SetUp() override {
        spdlog::drop("kv");
        kv::init_default_logger();
        logger_ = spdlog::get("kv");
    }

    boost::asio::io_context ioc_;
    std::shared_ptr<spdlog::logger> logger_;
};

TEST_F(CommitAwaiterTest, NotifyBeforeTimeout) {
    kv::raft::CommitAwaiter awaiter(ioc_, logger_);
    bool result = false;

    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        result = co_await awaiter.wait_for_commit(1);
    }, boost::asio::detached);

    pump(ioc_);
    EXPECT_EQ(awaiter.pending_count(), 1u);

    // Notify commit — should wake the waiter with success.
    awaiter.notify_commit(1);
    pump(ioc_);

    EXPECT_TRUE(result);
    EXPECT_EQ(awaiter.pending_count(), 0u);
}

TEST_F(CommitAwaiterTest, Timeout) {
    kv::raft::CommitAwaiter awaiter(ioc_, logger_);
    bool result = true; // Start true to prove it flips.

    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        result = co_await awaiter.wait_for_commit(
            1, std::chrono::milliseconds{1});
    }, boost::asio::detached);

    pump(ioc_);
    // Timer is very short, let it expire.
    std::this_thread::sleep_for(std::chrono::milliseconds{10});
    pump(ioc_, 200);

    EXPECT_FALSE(result);
    EXPECT_EQ(awaiter.pending_count(), 0u);
}

TEST_F(CommitAwaiterTest, FailAll) {
    kv::raft::CommitAwaiter awaiter(ioc_, logger_);
    bool result1 = true, result2 = true;

    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        result1 = co_await awaiter.wait_for_commit(1);
    }, boost::asio::detached);
    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        result2 = co_await awaiter.wait_for_commit(2);
    }, boost::asio::detached);

    pump(ioc_);
    EXPECT_EQ(awaiter.pending_count(), 2u);

    awaiter.fail_all();
    pump(ioc_);

    EXPECT_FALSE(result1);
    EXPECT_FALSE(result2);
    EXPECT_EQ(awaiter.pending_count(), 0u);
}

TEST_F(CommitAwaiterTest, NotifyUnknownIndexIsNoop) {
    kv::raft::CommitAwaiter awaiter(ioc_, logger_);

    // Notifying an index with no waiter should not crash.
    awaiter.notify_commit(42);
    EXPECT_EQ(awaiter.pending_count(), 0u);
}

TEST_F(CommitAwaiterTest, MultipleIndicesIndependent) {
    kv::raft::CommitAwaiter awaiter(ioc_, logger_);
    bool result1 = false, result2 = true;

    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        result1 = co_await awaiter.wait_for_commit(10);
    }, boost::asio::detached);
    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        result2 = co_await awaiter.wait_for_commit(20);
    }, boost::asio::detached);

    pump(ioc_);
    EXPECT_EQ(awaiter.pending_count(), 2u);

    // Commit only index 10 — index 20 stays pending.
    awaiter.notify_commit(10);
    pump(ioc_);

    EXPECT_TRUE(result1);
    EXPECT_EQ(awaiter.pending_count(), 1u);

    // Now fail the remaining.
    awaiter.fail_all();
    pump(ioc_);

    EXPECT_FALSE(result2);
    EXPECT_EQ(awaiter.pending_count(), 0u);
}

TEST_F(CommitAwaiterTest, DuplicateWaitSameIndex) {
    kv::raft::CommitAwaiter awaiter(ioc_, logger_);
    bool result1 = false, result2 = true;

    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        result1 = co_await awaiter.wait_for_commit(5);
    }, boost::asio::detached);

    pump(ioc_);
    EXPECT_EQ(awaiter.pending_count(), 1u);

    // Second wait on same index should fail immediately (returns false).
    boost::asio::co_spawn(ioc_, [&]() -> boost::asio::awaitable<void> {
        result2 = co_await awaiter.wait_for_commit(5);
    }, boost::asio::detached);

    pump(ioc_);
    EXPECT_FALSE(result2);

    // Original waiter still works.
    awaiter.notify_commit(5);
    pump(ioc_);
    EXPECT_TRUE(result1);
    EXPECT_EQ(awaiter.pending_count(), 0u);
}

// ════════════════════════════════════════════════════════════════════════════
//  RaftClusterContext tests
// ════════════════════════════════════════════════════════════════════════════

class RaftClusterContextTest : public ::testing::Test {
protected:
    void SetUp() override {
        spdlog::drop("kv");
        spdlog::drop("node-1");
        kv::init_default_logger();
        logger_ = kv::make_node_logger(1, spdlog::level::debug);
    }

    // Create a RaftNode, make it leader, and return it.
    // Uses DeterministicTimer and MockTransport from the anonymous namespace.
    struct NodeBundle {
        std::unique_ptr<MockTransport> transport;
        std::unique_ptr<DeterministicTimerFactory> timer_factory;
        std::unique_ptr<RaftNode> node;

        DeterministicTimer* election_timer() { return timer_factory->timer(0); }
        DeterministicTimer* heartbeat_timer() { return timer_factory->timer(1); }
    };

    NodeBundle make_leader(boost::asio::io_context& ioc,
                           RaftNode::ApplyCallback on_apply = {}) {
        auto transport = std::make_unique<MockTransport>();
        auto timer_factory = std::make_unique<DeterministicTimerFactory>();

        auto node = std::make_unique<RaftNode>(
            ioc, 1, std::vector<uint32_t>{2, 3},
            *transport, *timer_factory, logger_,
            std::move(on_apply));

        node->start();
        pump(ioc);

        // Fire election timer to trigger election.
        timer_factory->timer(0)->fire();
        pump(ioc);

        // Simulate both peers voting yes.
        for (const auto* msg : transport->find_request_vote()) {
            RequestVoteResponse resp;
            resp.set_term(node->current_term());
            resp.set_vote_granted(true);
            node->handle_vote_response(msg->peer_id, resp);
        }
        pump(ioc);

        assert(node->state() == kv::NodeState::Leader);
        transport->clear_sent();

        return {std::move(transport), std::move(timer_factory), std::move(node)};
    }

    std::shared_ptr<spdlog::logger> logger_;
};

TEST_F(RaftClusterContextTest, IsLeaderAndAddress) {
    boost::asio::io_context ioc;
    auto bundle = make_leader(ioc);
    kv::raft::CommitAwaiter awaiter(ioc, logger_);

    std::map<uint32_t, std::string> addrs = {
        {1, "localhost:6001"}, {2, "localhost:6002"}, {3, "localhost:6003"}
    };
    kv::raft::RaftClusterContext ctx(*bundle.node, awaiter, addrs, logger_);

    EXPECT_TRUE(ctx.is_leader());
    // leader_id() on a Leader node points to self (node_id=1).
    auto addr = ctx.leader_address();
    EXPECT_TRUE(addr.has_value());
    EXPECT_EQ(*addr, "localhost:6001");

    bundle.node->stop();
    pump(ioc);
}

TEST_F(RaftClusterContextTest, SubmitWriteSuccess) {
    boost::asio::io_context ioc;

    // Set up apply callback that notifies CommitAwaiter.
    kv::raft::CommitAwaiter awaiter(ioc, logger_);
    auto on_apply = [&](const LogEntry& entry) {
        awaiter.notify_commit(entry.index());
    };

    auto bundle = make_leader(ioc, on_apply);

    std::map<uint32_t, std::string> addrs = {
        {1, "localhost:6001"}, {2, "localhost:6002"}, {3, "localhost:6003"}
    };
    kv::raft::RaftClusterContext ctx(*bundle.node, awaiter, addrs, logger_);

    bool result = false;
    boost::asio::co_spawn(ioc, [&]() -> boost::asio::awaitable<void> {
        result = co_await ctx.submit_write("mykey", "myval", 1); // CMD_SET
    }, boost::asio::detached);

    pump(ioc);
    // Entry submitted, waiter pending.
    EXPECT_EQ(awaiter.pending_count(), 1u);

    // Simulate peer ACKs for the AppendEntries containing this entry.
    // The leader needs majority (2 out of 3). Leader already counts itself.
    // Need 1 more ACK.
    auto ae_msgs = bundle.transport->find_append_entries();
    ASSERT_FALSE(ae_msgs.empty());

    for (const auto* msg : ae_msgs) {
        AppendEntriesResponse resp;
        resp.set_term(bundle.node->current_term());
        resp.set_success(true);
        resp.set_match_index(bundle.node->log().last_index());
        bundle.node->handle_append_entries_response(msg->peer_id, resp);
        break; // Only need one ACK for majority.
    }
    pump(ioc);

    EXPECT_TRUE(result);
    EXPECT_EQ(awaiter.pending_count(), 0u);

    bundle.node->stop();
    pump(ioc);
}

TEST_F(RaftClusterContextTest, SubmitWriteOnFollowerFails) {
    boost::asio::io_context ioc;
    MockTransport transport;
    DeterministicTimerFactory timer_factory;
    kv::raft::CommitAwaiter awaiter(ioc, logger_);

    // Create a follower node (never trigger election).
    RaftNode node(ioc, 1, {2, 3}, transport, timer_factory, logger_);
    node.start();
    pump(ioc);

    ASSERT_EQ(node.state(), kv::NodeState::Follower);

    std::map<uint32_t, std::string> addrs = {{1, "localhost:6001"}};
    kv::raft::RaftClusterContext ctx(node, awaiter, addrs, logger_);

    bool result = true;
    boost::asio::co_spawn(ioc, [&]() -> boost::asio::awaitable<void> {
        result = co_await ctx.submit_write("k", "v", 1);
    }, boost::asio::detached);

    pump(ioc);
    EXPECT_FALSE(result);

    node.stop();
    pump(ioc);
}

TEST_F(RaftClusterContextTest, FailAllOnLeadershipLoss) {
    boost::asio::io_context ioc;
    kv::raft::CommitAwaiter awaiter(ioc, logger_);

    auto bundle = make_leader(ioc);

    std::map<uint32_t, std::string> addrs = {{1, "localhost:6001"}};
    kv::raft::RaftClusterContext ctx(*bundle.node, awaiter, addrs, logger_);

    bool result = true;
    boost::asio::co_spawn(ioc, [&]() -> boost::asio::awaitable<void> {
        result = co_await ctx.submit_write("k", "v", 1);
    }, boost::asio::detached);

    pump(ioc);
    EXPECT_EQ(awaiter.pending_count(), 1u);

    // Simulate leadership loss by calling fail_all.
    awaiter.fail_all();
    pump(ioc);

    EXPECT_FALSE(result);
    EXPECT_EQ(awaiter.pending_count(), 0u);

    bundle.node->stop();
    pump(ioc);
}

// ════════════════════════════════════════════════════════════════════════════
//  RaftNode::restore() / restore_snapshot() tests
// ════════════════════════════════════════════════════════════════════════════

TEST_F(RaftNodeTest, RestoreTermAndVotedFor) {
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_);

    // Restore term=5, voted_for=2
    node.restore(5, 2, {});

    EXPECT_EQ(node.current_term(), 5u);
    EXPECT_EQ(node.voted_for(), 2u);
    EXPECT_EQ(node.log().last_index(), 0u);
}

TEST_F(RaftNodeTest, RestoreTermWithNoVote) {
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_);

    // Restore term=3, voted_for=-1 (no vote)
    node.restore(3, -1, {});

    EXPECT_EQ(node.current_term(), 3u);
    EXPECT_FALSE(node.voted_for().has_value());
}

TEST_F(RaftNodeTest, RestoreWithLogEntries) {
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_);

    std::vector<LogEntry> entries;
    entries.push_back(make_entry(1, 1, CMD_SET, "a", "1"));
    entries.push_back(make_entry(1, 2, CMD_SET, "b", "2"));
    entries.push_back(make_entry(2, 3, CMD_DEL, "a", ""));

    node.restore(2, 1, std::move(entries));

    EXPECT_EQ(node.current_term(), 2u);
    EXPECT_EQ(node.voted_for(), 1u);
    EXPECT_EQ(node.log().last_index(), 3u);
    EXPECT_EQ(node.log().last_term(), 2u);
    EXPECT_EQ(node.log().size(), 3u);
}

TEST_F(RaftNodeTest, RestoreSnapshotAdjustsState) {
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_);

    node.restore_snapshot(10, 3);

    EXPECT_EQ(node.snapshot_last_index(), 10u);
    EXPECT_EQ(node.snapshot_last_term(), 3u);
    EXPECT_EQ(node.commit_index(), 10u);
    EXPECT_EQ(node.last_applied(), 10u);
}

TEST_F(RaftNodeTest, RestoreSnapshotThenLogEntries) {
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_);

    // Snapshot at index 5, term 2.
    node.restore_snapshot(5, 2);

    // Then restore entries after the snapshot.
    std::vector<LogEntry> entries;
    entries.push_back(make_entry(2, 6, CMD_SET, "x", "100"));
    entries.push_back(make_entry(3, 7, CMD_SET, "y", "200"));
    node.restore(3, -1, std::move(entries));

    EXPECT_EQ(node.snapshot_last_index(), 5u);
    EXPECT_EQ(node.snapshot_last_term(), 2u);
    EXPECT_EQ(node.current_term(), 3u);
    EXPECT_FALSE(node.voted_for().has_value());
    EXPECT_EQ(node.log().last_index(), 7u);
    EXPECT_EQ(node.log().last_term(), 3u);
}

TEST_F(RaftNodeTest, RestoreDoesNotTriggerPersistCallback) {
    RecordingPersistCallback persist;
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_,
                  {}, nullptr, 0, &persist);

    std::vector<LogEntry> entries;
    entries.push_back(make_entry(1, 1, CMD_SET, "k", "v"));
    node.restore(1, 1, std::move(entries));

    // restore() must NOT call the persist callback (data is already on disk).
    EXPECT_TRUE(persist.metadata_calls().empty());
    EXPECT_TRUE(persist.entry_calls().empty());
}

TEST_F(RaftNodeTest, RestoreThenStartWorksNormally) {
    RaftNode node(ioc_, 1, {2, 3}, transport_, timer_factory_, logger_);

    std::vector<LogEntry> entries;
    entries.push_back(make_entry(1, 1, CMD_SET, "a", "1"));
    node.restore(1, -1, std::move(entries));

    // Node should start normally after restore.
    node.start();
    pump(ioc_);

    EXPECT_EQ(node.state(), kv::NodeState::Follower);
    EXPECT_EQ(node.current_term(), 1u);
    EXPECT_EQ(node.log().last_index(), 1u);

    node.stop();
    pump(ioc_);
}

// ════════════════════════════════════════════════════════════════════════════
//  Read Lease tests (Etap 7.2)
// ════════════════════════════════════════════════════════════════════════════

class ReadLeaseTest : public ::testing::Test {
protected:
    void SetUp() override {
        spdlog::drop("kv");
        kv::init_default_logger(spdlog::level::debug);
        logger_ = kv::make_node_logger(1, spdlog::level::debug);
    }

    // Elect node 1 as leader (3-node cluster), with a MockClock injected.
    // Returns transport, timer_factory, node — node is already leader.
    struct LeaderBundle {
        std::unique_ptr<MockTransport> transport;
        std::unique_ptr<DeterministicTimerFactory> timer_factory;
        std::unique_ptr<RaftNode> node;

        DeterministicTimer* election_timer() { return timer_factory->timer(0); }
        DeterministicTimer* heartbeat_timer() { return timer_factory->timer(1); }
    };

    LeaderBundle make_leader_with_clock(boost::asio::io_context& ioc, MockClock& clock) {
        auto transport = std::make_unique<MockTransport>();
        auto timer_factory = std::make_unique<DeterministicTimerFactory>();

        auto node = std::make_unique<RaftNode>(
            ioc, 1, std::vector<uint32_t>{2, 3},
            *transport, *timer_factory, logger_,
            RaftNode::ApplyCallback{}, nullptr, 0, nullptr, &clock);

        node->start();
        pump(ioc);

        // Fire election timer to trigger election.
        timer_factory->timer(0)->fire();
        pump(ioc);

        // Simulate both peers voting yes.
        for (const auto* msg : transport->find_request_vote()) {
            RequestVoteResponse resp;
            resp.set_term(node->current_term());
            resp.set_vote_granted(true);
            node->handle_vote_response(msg->peer_id, resp);
        }
        pump(ioc);

        assert(node->state() == kv::NodeState::Leader);
        transport->clear_sent();

        return {std::move(transport), std::move(timer_factory), std::move(node)};
    }

    // Deliver a successful AE response from a peer.
    void ack_append_entries(RaftNode& node, uint32_t from_peer) {
        AppendEntriesResponse resp;
        resp.set_term(node.current_term());
        resp.set_success(true);
        resp.set_match_index(node.log().last_index());
        node.handle_append_entries_response(from_peer, resp);
    }

    std::shared_ptr<spdlog::logger> logger_;
};

TEST_F(ReadLeaseTest, NewLeaderHasNoLease) {
    boost::asio::io_context ioc;
    MockClock clock;
    auto bundle = make_leader_with_clock(ioc, clock);

    // Fresh leader hasn't received any heartbeat acks yet.
    EXPECT_FALSE(bundle.node->has_read_lease());

    bundle.node->stop();
    pump(ioc);
}

TEST_F(ReadLeaseTest, LeaseAcquiredAfterMajorityAck) {
    boost::asio::io_context ioc;
    MockClock clock;
    auto bundle = make_leader_with_clock(ioc, clock);

    // Advance clock a bit, then deliver AE ack from one peer.
    clock.advance(std::chrono::milliseconds{10});
    ack_append_entries(*bundle.node, 2);
    pump(ioc);

    // One peer ack + self = 2/3 = majority → lease acquired.
    EXPECT_TRUE(bundle.node->has_read_lease());

    bundle.node->stop();
    pump(ioc);
}

TEST_F(ReadLeaseTest, SinglePeerAckNotEnoughForFiveNodeCluster) {
    boost::asio::io_context ioc;
    MockClock clock;
    MockTransport transport;
    DeterministicTimerFactory timer_factory;

    // 5-node cluster: node 1 + peers {2, 3, 4, 5}.
    RaftNode node(ioc, 1, {2, 3, 4, 5}, transport, timer_factory, logger_,
                  {}, nullptr, 0, nullptr, &clock);
    node.start();
    pump(ioc);

    // Elect as leader.
    timer_factory.timer(0)->fire();
    pump(ioc);
    for (const auto* msg : transport.find_request_vote()) {
        RequestVoteResponse resp;
        resp.set_term(node.current_term());
        resp.set_vote_granted(true);
        node.handle_vote_response(msg->peer_id, resp);
    }
    pump(ioc);
    ASSERT_EQ(node.state(), kv::NodeState::Leader);
    transport.clear_sent();

    // Ack from only 1 peer: self + 1 = 2/5, not majority.
    clock.advance(std::chrono::milliseconds{10});
    ack_append_entries(node, 2);
    pump(ioc);
    EXPECT_FALSE(node.has_read_lease());

    // Ack from 2nd peer: self + 2 = 3/5 = majority → lease acquired.
    ack_append_entries(node, 3);
    pump(ioc);
    EXPECT_TRUE(node.has_read_lease());

    node.stop();
    pump(ioc);
}

TEST_F(ReadLeaseTest, LeaseExpiresAfterDuration) {
    boost::asio::io_context ioc;
    MockClock clock;
    auto bundle = make_leader_with_clock(ioc, clock);

    // Get a lease.
    clock.advance(std::chrono::milliseconds{10});
    ack_append_entries(*bundle.node, 2);
    pump(ioc);
    ASSERT_TRUE(bundle.node->has_read_lease());

    // Advance clock beyond lease duration (140ms).
    clock.advance(std::chrono::milliseconds{141});

    // Lease should now be expired (no more acks since then).
    EXPECT_FALSE(bundle.node->has_read_lease());

    bundle.node->stop();
    pump(ioc);
}

TEST_F(ReadLeaseTest, LeaseRenewedBySubsequentAcks) {
    boost::asio::io_context ioc;
    MockClock clock;
    auto bundle = make_leader_with_clock(ioc, clock);

    // Get initial lease.
    clock.advance(std::chrono::milliseconds{10});
    ack_append_entries(*bundle.node, 2);
    pump(ioc);
    ASSERT_TRUE(bundle.node->has_read_lease());

    // Advance 100ms (within 140ms window), renew with another ack.
    clock.advance(std::chrono::milliseconds{100});
    ack_append_entries(*bundle.node, 2);
    pump(ioc);
    EXPECT_TRUE(bundle.node->has_read_lease());

    // Advance another 100ms (200ms since first ack, but only 100ms since renewal).
    clock.advance(std::chrono::milliseconds{100});
    EXPECT_TRUE(bundle.node->has_read_lease());

    // Advance another 50ms (150ms since last renewal > 140ms lease).
    clock.advance(std::chrono::milliseconds{50});
    EXPECT_FALSE(bundle.node->has_read_lease());

    bundle.node->stop();
    pump(ioc);
}

TEST_F(ReadLeaseTest, LeaseInvalidatedOnStepDown) {
    boost::asio::io_context ioc;
    MockClock clock;
    auto bundle = make_leader_with_clock(ioc, clock);

    // Get a lease.
    clock.advance(std::chrono::milliseconds{10});
    ack_append_entries(*bundle.node, 2);
    pump(ioc);
    ASSERT_TRUE(bundle.node->has_read_lease());

    // Receive AE response with higher term → step down to follower.
    AppendEntriesResponse resp;
    resp.set_term(bundle.node->current_term() + 1);
    resp.set_success(false);
    resp.set_match_index(0);
    bundle.node->handle_append_entries_response(2, resp);
    pump(ioc);

    EXPECT_EQ(bundle.node->state(), kv::NodeState::Follower);
    EXPECT_FALSE(bundle.node->has_read_lease());

    bundle.node->stop();
    pump(ioc);
}

TEST_F(ReadLeaseTest, FollowerNeverHasLease) {
    boost::asio::io_context ioc;
    MockClock clock;
    MockTransport transport;
    DeterministicTimerFactory timer_factory;

    RaftNode node(ioc, 1, {2, 3}, transport, timer_factory, logger_,
                  {}, nullptr, 0, nullptr, &clock);
    node.start();
    pump(ioc);

    // Node starts as follower — no lease.
    EXPECT_EQ(node.state(), kv::NodeState::Follower);
    EXPECT_FALSE(node.has_read_lease());

    node.stop();
    pump(ioc);
}

TEST_F(ReadLeaseTest, LeaseStaleAcksExpire) {
    boost::asio::io_context ioc;
    MockClock clock;
    auto bundle = make_leader_with_clock(ioc, clock);

    // Get lease from peer 2.
    clock.advance(std::chrono::milliseconds{10});
    ack_append_entries(*bundle.node, 2);
    pump(ioc);
    ASSERT_TRUE(bundle.node->has_read_lease());

    // Advance 130ms. Peer 2's ack is now 130ms old (< 140ms), still valid.
    clock.advance(std::chrono::milliseconds{130});
    EXPECT_TRUE(bundle.node->has_read_lease());

    // Get ack from peer 3 at this point (peer 3's ack is fresh).
    ack_append_entries(*bundle.node, 3);
    pump(ioc);
    EXPECT_TRUE(bundle.node->has_read_lease());

    // Advance 20ms more. Now peer 2's ack is 150ms old (> 140ms, stale),
    // but peer 3's ack is 20ms old (fresh). Self + peer 3 = 2/3 = majority.
    clock.advance(std::chrono::milliseconds{20});
    // has_read_lease checks lease_start_ which was renewed by the peer 3 ack.
    // The lease_start_ was set when peer 3 acked (at the 140ms mark).
    // Now at 160ms mark. 160 - 140 = 20ms < 140ms → still valid.
    EXPECT_TRUE(bundle.node->has_read_lease());

    bundle.node->stop();
    pump(ioc);
}

TEST_F(ReadLeaseTest, LeaseTimingBoundary) {
    boost::asio::io_context ioc;
    MockClock clock;
    auto bundle = make_leader_with_clock(ioc, clock);

    // Get lease.
    clock.advance(std::chrono::milliseconds{10});
    ack_append_entries(*bundle.node, 2);
    pump(ioc);
    ASSERT_TRUE(bundle.node->has_read_lease());

    // Advance exactly kLeaseDuration (140ms) — should still be valid (<=).
    clock.advance(std::chrono::milliseconds{140});
    EXPECT_TRUE(bundle.node->has_read_lease());

    // Advance 1 more ms — should now be expired.
    clock.advance(std::chrono::milliseconds{1});
    EXPECT_FALSE(bundle.node->has_read_lease());

    bundle.node->stop();
    pump(ioc);
}

TEST_F(ReadLeaseTest, LeaseWithDefaultClock) {
    // Verify that a node without an injected clock uses the default SteadyClock.
    boost::asio::io_context ioc;
    MockTransport transport;
    DeterministicTimerFactory timer_factory;

    // No clock parameter → uses default_clock_ (SteadyClock).
    RaftNode node(ioc, 1, {2, 3}, transport, timer_factory, logger_);
    node.start();
    pump(ioc);

    // Elect as leader.
    timer_factory.timer(0)->fire();
    pump(ioc);
    for (const auto* msg : transport.find_request_vote()) {
        RequestVoteResponse resp;
        resp.set_term(node.current_term());
        resp.set_vote_granted(true);
        node.handle_vote_response(msg->peer_id, resp);
    }
    pump(ioc);
    ASSERT_EQ(node.state(), kv::NodeState::Leader);

    // No acks yet → no lease.
    EXPECT_FALSE(node.has_read_lease());

    // Send an ack — with real clock, lease should be valid immediately after.
    ack_append_entries(node, 2);
    pump(ioc);
    EXPECT_TRUE(node.has_read_lease());

    node.stop();
    pump(ioc);
}

// ════════════════════════════════════════════════════════════════════════════
//  Dynamic Membership (Etap 7.4.11) tests
// ════════════════════════════════════════════════════════════════════════════

class DynamicMembershipTest : public ::testing::Test {
protected:
    void SetUp() override {
        spdlog::drop("kv");
        spdlog::drop("node-1");
        kv::init_default_logger(spdlog::level::debug);
        logger_ = kv::make_node_logger(1, spdlog::level::debug);
    }

    // Build a vector of NodeInfo for a given set of node IDs (minimal info).
    static std::vector<NodeInfo> make_node_infos(
            std::initializer_list<uint32_t> ids) {
        std::vector<NodeInfo> result;
        for (uint32_t id : ids) {
            NodeInfo n;
            n.set_id(id);
            n.set_host("localhost");
            n.set_raft_port(9000 + id);
            n.set_client_port(8000 + id);
            result.push_back(std::move(n));
        }
        return result;
    }

    // Create a leader on a 3-node cluster {1, 2, 3}.
    // Optionally accepts a config change callback and snapshot IO.
    struct LeaderBundle {
        std::unique_ptr<MockTransport> transport;
        std::unique_ptr<DeterministicTimerFactory> timer_factory;
        std::unique_ptr<RaftNode> node;
        DeterministicTimer* election_timer() {
            return timer_factory->timer(timer_factory->timer_count() - 2);
        }
        DeterministicTimer* heartbeat_timer() {
            return timer_factory->timer(timer_factory->timer_count() - 1);
        }
    };

    LeaderBundle make_leader(
            boost::asio::io_context& ioc,
            RaftNode::ApplyCallback on_apply = {},
            MockSnapshotIO* snap_io = nullptr,
            uint64_t snap_interval = 0,
            PersistCallback* persist_cb = nullptr,
            RaftNode::ConfigChangeCallback on_config_change = {}) {
        auto transport = std::make_unique<MockTransport>();
        auto timer_factory = std::make_unique<DeterministicTimerFactory>();

        auto node = std::make_unique<RaftNode>(
            ioc, 1, std::vector<uint32_t>{2, 3},
            *transport, *timer_factory, logger_,
            std::move(on_apply), snap_io, snap_interval,
            persist_cb, nullptr,
            std::move(on_config_change));

        node->start();
        pump(ioc);

        auto* et = timer_factory->timer(timer_factory->timer_count() - 2);
        et->fire();
        pump(ioc);

        // Peer 2 grants vote → majority in {1,2,3}.
        RequestVoteResponse vresp;
        vresp.set_term(1);
        vresp.set_vote_granted(true);
        node->handle_vote_response(2, vresp);
        pump(ioc);

        assert(node->state() == kv::NodeState::Leader);
        transport->clear_sent();

        return {std::move(transport), std::move(timer_factory), std::move(node)};
    }

    // Simulate an AE ack from a peer with match_index = node's last log index.
    static void ack_from(RaftNode& node, uint32_t peer) {
        AppendEntriesResponse resp;
        resp.set_term(node.current_term());
        resp.set_success(true);
        resp.set_match_index(node.log().last_index());
        node.handle_append_entries_response(peer, resp);
    }

    std::shared_ptr<spdlog::logger> logger_;
};

// ── 1. submit_config_change() on leader succeeds ────────────────────────────

TEST_F(DynamicMembershipTest, SubmitConfigChangeOnLeaderSucceeds) {
    boost::asio::io_context ioc;
    auto bundle = make_leader(ioc);
    auto& node = *bundle.node;

    // Submit a config change: add node 4 to cluster {1, 2, 3}.
    auto new_nodes = make_node_infos({1, 2, 3, 4});
    ASSERT_TRUE(node.submit_config_change(std::move(new_nodes)));

    pump(ioc);

    // Log should contain: [1]=no-op, [2]=CMD_CONFIG (joint).
    EXPECT_EQ(node.log().last_index(), 2u);
    const auto* entry = node.log().entry_at(2);
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry->command().type(), CMD_CONFIG);
    EXPECT_TRUE(entry->has_config());
    EXPECT_GT(entry->config().old_nodes_size(), 0);  // Joint: has old_nodes.
    EXPECT_GT(entry->config().new_nodes_size(), 0);

    // Config should be in joint consensus.
    EXPECT_TRUE(node.config().is_joint());

    node.stop();
    pump(ioc);
}

// ── 2. submit_config_change() on non-leader returns false ───────────────────

TEST_F(DynamicMembershipTest, SubmitConfigChangeOnFollowerFails) {
    boost::asio::io_context ioc;
    MockTransport transport;
    DeterministicTimerFactory timer_factory;

    RaftNode node(ioc, 1, std::vector<uint32_t>{2, 3},
                  transport, timer_factory, logger_);
    node.start();
    pump(ioc);

    ASSERT_EQ(node.state(), kv::NodeState::Follower);

    auto new_nodes = make_node_infos({1, 2, 3, 4});
    EXPECT_FALSE(node.submit_config_change(std::move(new_nodes)));

    node.stop();
    pump(ioc);
}

// ── 3. submit_config_change() when another change is pending returns false ──

TEST_F(DynamicMembershipTest, SubmitConfigChangeWhenPendingFails) {
    boost::asio::io_context ioc;
    auto bundle = make_leader(ioc);
    auto& node = *bundle.node;

    // First config change: add node 4.
    ASSERT_TRUE(node.submit_config_change(make_node_infos({1, 2, 3, 4})));
    pump(ioc);

    // Second config change while first is pending: should fail.
    EXPECT_FALSE(node.submit_config_change(make_node_infos({1, 2, 3, 4, 5})));

    node.stop();
    pump(ioc);
}

// ── 4. submit_config_change() when already in joint consensus returns false ─

TEST_F(DynamicMembershipTest, SubmitConfigChangeWhileJointFails) {
    boost::asio::io_context ioc;
    auto bundle = make_leader(ioc);
    auto& node = *bundle.node;

    ASSERT_TRUE(node.submit_config_change(make_node_infos({1, 2, 3, 4})));
    pump(ioc);
    ASSERT_TRUE(node.config().is_joint());

    // Try another change while in joint: should fail.
    EXPECT_FALSE(node.submit_config_change(make_node_infos({1, 2})));

    node.stop();
    pump(ioc);
}

// ── 5. Joint consensus quorum requires majority in BOTH old and new configs ─

TEST_F(DynamicMembershipTest, JointQuorumRequiresMajorityInBothConfigs) {
    boost::asio::io_context ioc;

    std::vector<LogEntry> applied;
    auto on_apply = [&](const LogEntry& entry) {
        applied.push_back(entry);
    };

    auto bundle = make_leader(ioc, on_apply);
    auto& node = *bundle.node;

    // Add node 4: old={1,2,3}, new={1,2,3,4}.
    // Majority of old: 2/3.  Majority of new: 3/4.
    ASSERT_TRUE(node.submit_config_change(make_node_infos({1, 2, 3, 4})));
    pump(ioc);
    bundle.transport->clear_sent();

    // After submit_config_change: log has [1]=no-op (term 1), [2]=config (term 1).
    ASSERT_EQ(node.log().last_index(), 2u);

    // Only peer 2 acks → old quorum: {1,2}=2/3 ✓, new quorum: {1,2}=2/4 ✗.
    // Should NOT advance commit_index.
    ack_from(node, 2);
    pump(ioc);
    EXPECT_EQ(node.commit_index(), 0u);

    // Now peer 3 acks → old: {1,2,3}=3/3 ✓, new: {1,2,3}=3/4 ✓.
    // Commit index should advance.
    ack_from(node, 3);
    pump(ioc);
    EXPECT_GE(node.commit_index(), 2u);

    node.stop();
    pump(ioc);
}

// ── 6. Auto-finalization after joint entry commits ──────────────────────────

TEST_F(DynamicMembershipTest, AutoFinalizationAfterJointCommit) {
    boost::asio::io_context ioc;
    auto bundle = make_leader(ioc);
    auto& node = *bundle.node;

    // Add node 4: enter joint consensus.
    ASSERT_TRUE(node.submit_config_change(make_node_infos({1, 2, 3, 4})));
    pump(ioc);
    ASSERT_TRUE(node.config().is_joint());

    // Log: [1]=no-op, [2]=joint config.
    ASSERT_EQ(node.log().last_index(), 2u);
    bundle.transport->clear_sent();

    // Ack from peers 2 and 3 → joint entry commits.
    // old majority: {1,2,3}=3/3 ✓, new majority: {1,2,3}=3/4 ✓.
    ack_from(node, 2);
    pump(ioc);
    ack_from(node, 3);
    pump(ioc);

    // After joint commit, leader should auto-append finalize entry (C_new).
    // Log: [1]=no-op, [2]=joint config, [3]=stable config.
    EXPECT_GE(node.log().last_index(), 3u);

    const auto* finalize_entry = node.log().entry_at(3);
    ASSERT_NE(finalize_entry, nullptr);
    EXPECT_EQ(finalize_entry->command().type(), CMD_CONFIG);
    EXPECT_TRUE(finalize_entry->has_config());
    // Finalize entry has no old_nodes (stable config).
    EXPECT_EQ(finalize_entry->config().old_nodes_size(), 0);
    EXPECT_GT(finalize_entry->config().new_nodes_size(), 0);

    // Config is applied immediately on leader → no longer joint.
    EXPECT_FALSE(node.config().is_joint());

    // Config should have 4 nodes.
    EXPECT_EQ(node.config().cluster_size(), 4);

    node.stop();
    pump(ioc);
}

// ── 7. Follower applies config on commit ────────────────────────────────────

TEST_F(DynamicMembershipTest, FollowerAppliesConfigOnCommit) {
    boost::asio::io_context ioc;
    MockTransport transport;
    DeterministicTimerFactory timer_factory;

    RaftNode follower(ioc, 2, std::vector<uint32_t>{1, 3},
                      transport, timer_factory, logger_);
    follower.start();
    pump(ioc);
    ASSERT_EQ(follower.state(), kv::NodeState::Follower);

    // Build a joint config entry as the leader would.
    ClusterConfig joint_cfg;
    for (uint32_t id : {1u, 2u, 3u}) {
        auto* old_node = joint_cfg.add_old_nodes();
        old_node->set_id(id);
    }
    for (uint32_t id : {1u, 2u, 3u, 4u}) {
        auto* new_node = joint_cfg.add_new_nodes();
        new_node->set_id(id);
    }

    LogEntry config_entry;
    config_entry.set_term(1);
    config_entry.set_index(1);
    config_entry.mutable_command()->set_type(CMD_CONFIG);
    *config_entry.mutable_config() = joint_cfg;

    // Leader sends AE with this config entry, commit covering it.
    AppendEntriesRequest req;
    req.set_term(1);
    req.set_leader_id(1);
    req.set_prev_log_index(0);
    req.set_prev_log_term(0);
    req.set_leader_commit(1);  // Commit this entry immediately.
    *req.add_entries() = config_entry;

    boost::asio::co_spawn(ioc, [&]() -> boost::asio::awaitable<void> {
        auto resp = co_await follower.handle_append_entries(req);
        EXPECT_TRUE(resp.success());
    }, boost::asio::detached);
    pump(ioc);

    // Follower should have applied the config on commit.
    EXPECT_TRUE(follower.config().is_joint());
    EXPECT_EQ(follower.config().cluster_size(), 4);

    follower.stop();
    pump(ioc);
}

// ── 8. Config change callback invoked ───────────────────────────────────────

TEST_F(DynamicMembershipTest, ConfigChangeCallbackInvoked) {
    boost::asio::io_context ioc;

    int callback_count = 0;
    bool was_joint = false;
    int new_cluster_size = 0;

    auto on_config_change = [&](const ClusterConfiguration& cfg,
                                const LogEntry& entry) {
        callback_count++;
        was_joint = cfg.is_joint();
        new_cluster_size = cfg.cluster_size();
    };

    auto bundle = make_leader(ioc, {}, nullptr, 0, nullptr, on_config_change);
    auto& node = *bundle.node;

    // Submit config change: add node 4.
    ASSERT_TRUE(node.submit_config_change(make_node_infos({1, 2, 3, 4})));
    pump(ioc);

    // Callback should have been invoked once (for the joint config entry).
    EXPECT_EQ(callback_count, 1);
    EXPECT_TRUE(was_joint);
    EXPECT_EQ(new_cluster_size, 4);

    node.stop();
    pump(ioc);
}

// ── 9. Snapshot includes cluster config ─────────────────────────────────────

TEST_F(DynamicMembershipTest, SnapshotIncludesClusterConfig) {
    boost::asio::io_context ioc;
    MockSnapshotIO snap_io;

    std::vector<LogEntry> applied;
    auto on_apply = [&](const LogEntry& entry) {
        applied.push_back(entry);
    };

    // Create leader with snapshot support (interval=1 → snapshot after every entry).
    auto bundle = make_leader(ioc, on_apply, &snap_io, 1);
    auto& node = *bundle.node;

    // The no-op is index 1 (from become_leader). We need it committed
    // so that last_applied advances and triggers a snapshot.
    // Ack the no-op from both peers.
    ack_from(node, 2);
    pump(ioc);
    ack_from(node, 3);
    pump(ioc);

    // Snapshot should have been triggered.
    ASSERT_GE(snap_io.create_count(), 1);

    // The snapshot should include the cluster config.
    const auto& cfg = snap_io.created_config();
    ASSERT_TRUE(cfg.has_value());
    EXPECT_GT(cfg->new_nodes_size(), 0);
}

TEST_F(DynamicMembershipTest, InstallSnapshotRestoresConfig) {
    boost::asio::io_context ioc;
    MockSnapshotIO snap_io;
    MockTransport transport;
    DeterministicTimerFactory timer_factory;

    RaftNode follower(ioc, 2, std::vector<uint32_t>{1, 3},
                      transport, timer_factory, logger_,
                      {}, &snap_io);
    follower.start();
    pump(ioc);

    // Build a snapshot with a 4-node cluster config.
    ClusterConfig cfg;
    for (uint32_t id : {1u, 2u, 3u, 4u}) {
        auto* n = cfg.add_new_nodes();
        n->set_id(id);
        n->set_host("localhost");
        n->set_raft_port(9000 + id);
        n->set_client_port(8000 + id);
    }

    InstallSnapshotRequest req;
    req.set_term(1);
    req.set_leader_id(1);
    req.set_last_included_index(10);
    req.set_last_included_term(1);
    req.set_data("snapshot_data");
    *req.mutable_config() = cfg;

    boost::asio::co_spawn(ioc, [&]() -> boost::asio::awaitable<void> {
        auto resp = co_await follower.handle_install_snapshot(req);
        EXPECT_EQ(resp.term(), 1u);
    }, boost::asio::detached);
    pump(ioc);

    // Follower's config should now reflect the 4-node cluster.
    EXPECT_EQ(follower.config().cluster_size(), 4);
    EXPECT_FALSE(follower.config().is_joint());

    // SnapshotIO should have received the config.
    ASSERT_EQ(snap_io.install_count(), 1);
    ASSERT_TRUE(snap_io.installed_config().has_value());
    EXPECT_EQ(snap_io.installed_config()->new_nodes_size(), 4);

    follower.stop();
    pump(ioc);
}

// ── 10. restore_snapshot() with ClusterConfig ───────────────────────────────

TEST_F(DynamicMembershipTest, RestoreSnapshotWithClusterConfig) {
    boost::asio::io_context ioc;
    MockTransport transport;
    DeterministicTimerFactory timer_factory;

    RaftNode node(ioc, 1, std::vector<uint32_t>{2, 3},
                  transport, timer_factory, logger_);

    // Build a 4-node config.
    ClusterConfig cfg;
    for (uint32_t id : {1u, 2u, 3u, 4u}) {
        auto* n = cfg.add_new_nodes();
        n->set_id(id);
    }

    node.restore_snapshot(10, 2, cfg);

    EXPECT_EQ(node.snapshot_last_index(), 10u);
    EXPECT_EQ(node.snapshot_last_term(), 2u);
    EXPECT_EQ(node.config().cluster_size(), 4);
    EXPECT_FALSE(node.config().is_joint());

    // Peer IDs should include 2, 3, 4 (not self=1).
    const auto& peers = node.config().peer_ids();
    EXPECT_EQ(peers.size(), 3u);
    EXPECT_NE(std::find(peers.begin(), peers.end(), 2), peers.end());
    EXPECT_NE(std::find(peers.begin(), peers.end(), 3), peers.end());
    EXPECT_NE(std::find(peers.begin(), peers.end(), 4), peers.end());
}

TEST_F(DynamicMembershipTest, RestoreSnapshotWithJointConfig) {
    boost::asio::io_context ioc;
    MockTransport transport;
    DeterministicTimerFactory timer_factory;

    RaftNode node(ioc, 1, std::vector<uint32_t>{2, 3},
                  transport, timer_factory, logger_);

    // Build a joint config: old={1,2,3}, new={1,2,3,4}.
    ClusterConfig cfg;
    for (uint32_t id : {1u, 2u, 3u}) {
        auto* n = cfg.add_old_nodes();
        n->set_id(id);
    }
    for (uint32_t id : {1u, 2u, 3u, 4u}) {
        auto* n = cfg.add_new_nodes();
        n->set_id(id);
    }

    node.restore_snapshot(10, 2, cfg);

    EXPECT_TRUE(node.config().is_joint());
    EXPECT_EQ(node.config().cluster_size(), 4);
    // all_peer_ids includes peers from both old and new configs.
    auto all_peers = node.config().all_peer_ids();
    EXPECT_NE(std::find(all_peers.begin(), all_peers.end(), 2), all_peers.end());
    EXPECT_NE(std::find(all_peers.begin(), all_peers.end(), 3), all_peers.end());
    EXPECT_NE(std::find(all_peers.begin(), all_peers.end(), 4), all_peers.end());
}

// ── 11. Config entry persisted via PersistCallback ──────────────────────────

TEST_F(DynamicMembershipTest, ConfigEntryPersistedViaPersistCallback) {
    boost::asio::io_context ioc;
    RecordingPersistCallback persist;

    auto bundle = make_leader(ioc, {}, nullptr, 0, &persist);
    auto& node = *bundle.node;
    persist.clear();

    // Submit config change.
    ASSERT_TRUE(node.submit_config_change(make_node_infos({1, 2, 3, 4})));
    pump(ioc);

    // Persist callback should have received the config entry.
    ASSERT_GE(persist.entry_calls().size(), 1u);
    bool found_config_entry = false;
    for (const auto& entry : persist.entry_calls()) {
        if (entry.command().type() == CMD_CONFIG) {
            found_config_entry = true;
            EXPECT_TRUE(entry.has_config());
            break;
        }
    }
    EXPECT_TRUE(found_config_entry);

    node.stop();
    pump(ioc);
}

// ── 12. Finalize entry also persisted ───────────────────────────────────────

TEST_F(DynamicMembershipTest, FinalizeEntryPersisted) {
    boost::asio::io_context ioc;
    RecordingPersistCallback persist;

    auto bundle = make_leader(ioc, {}, nullptr, 0, &persist);
    auto& node = *bundle.node;

    ASSERT_TRUE(node.submit_config_change(make_node_infos({1, 2, 3, 4})));
    pump(ioc);
    persist.clear();

    // Commit the joint entry: acks from peers 2 and 3.
    ack_from(node, 2);
    pump(ioc);
    ack_from(node, 3);
    pump(ioc);

    // Finalize entry should have been persisted.
    bool found_finalize = false;
    for (const auto& entry : persist.entry_calls()) {
        if (entry.command().type() == CMD_CONFIG && entry.has_config()
            && entry.config().old_nodes_size() == 0) {
            found_finalize = true;
            break;
        }
    }
    EXPECT_TRUE(found_finalize);

    node.stop();
    pump(ioc);
}

// ── 13. Config change callback invoked for both joint and finalize ───────────

TEST_F(DynamicMembershipTest, ConfigChangeCallbackInvokedForBothPhases) {
    boost::asio::io_context ioc;

    int callback_count = 0;
    bool first_was_joint = false;
    bool second_was_stable = false;

    auto on_config_change = [&](const ClusterConfiguration& cfg,
                                const LogEntry& /*entry*/) {
        callback_count++;
        if (callback_count == 1) first_was_joint = cfg.is_joint();
        if (callback_count == 2) second_was_stable = !cfg.is_joint();
    };

    auto bundle = make_leader(ioc, {}, nullptr, 0, nullptr, on_config_change);
    auto& node = *bundle.node;

    ASSERT_TRUE(node.submit_config_change(make_node_infos({1, 2, 3, 4})));
    pump(ioc);
    ASSERT_EQ(callback_count, 1);

    // Commit joint entry → triggers finalize.
    ack_from(node, 2);
    pump(ioc);
    ack_from(node, 3);
    pump(ioc);

    // Two callbacks: one for joint, one for stable.
    EXPECT_EQ(callback_count, 2);
    EXPECT_TRUE(first_was_joint);
    EXPECT_TRUE(second_was_stable);

    node.stop();
    pump(ioc);
}

// ── 14. Leader sends AE to all peers during joint consensus ─────────────────

TEST_F(DynamicMembershipTest, LeaderSendsAEToAllPeersDuringJointConsensus) {
    boost::asio::io_context ioc;
    auto bundle = make_leader(ioc);
    auto& node = *bundle.node;

    // Add node 4: now {1,2,3} old, {1,2,3,4} new.
    ASSERT_TRUE(node.submit_config_change(make_node_infos({1, 2, 3, 4})));
    pump(ioc);

    // Clear and trigger heartbeat to see who receives AEs.
    bundle.transport->clear_sent();
    bundle.heartbeat_timer()->fire();
    pump(ioc);

    // Should have sent AE to peers 2, 3, and 4 (all_peer_ids).
    auto ae_msgs = bundle.transport->find_append_entries();
    std::set<uint32_t> recipients;
    for (const auto* msg : ae_msgs) {
        recipients.insert(msg->peer_id);
    }
    EXPECT_TRUE(recipients.count(2));
    EXPECT_TRUE(recipients.count(3));
    EXPECT_TRUE(recipients.count(4));

    node.stop();
    pump(ioc);
}

// ── 15. Removing a node ─────────────────────────────────────────────────────

TEST_F(DynamicMembershipTest, RemoveNodeConfigChange) {
    boost::asio::io_context ioc;
    auto bundle = make_leader(ioc);
    auto& node = *bundle.node;

    // Remove node 3: old={1,2,3}, new={1,2}.
    ASSERT_TRUE(node.submit_config_change(make_node_infos({1, 2})));
    pump(ioc);

    EXPECT_TRUE(node.config().is_joint());
    // New config has 2 nodes.
    EXPECT_EQ(node.config().cluster_size(), 2);

    // old_only_peer_ids should include 3 (in old but not new).
    const auto& old_only = node.config().old_only_peer_ids();
    EXPECT_EQ(old_only.size(), 1u);
    EXPECT_EQ(old_only[0], 3u);

    // Commit the joint entry. Old majority: 2/3, new majority: 2/2.
    // Ack from peer 2 → old: {1,2}=2/3 ✓, new: {1,2}=2/2 ✓.
    ack_from(node, 2);
    pump(ioc);

    // Joint entry should be committed and finalize should be appended.
    EXPECT_GE(node.log().last_index(), 3u);
    EXPECT_FALSE(node.config().is_joint());
    EXPECT_EQ(node.config().cluster_size(), 2);

    node.stop();
    pump(ioc);
}

// ── 16. RaftClusterContext::submit_config_change() builds correct NodeInfo ──

TEST_F(DynamicMembershipTest, ClusterContextSubmitConfigChangeAddNode) {
    boost::asio::io_context ioc;

    auto transport = std::make_unique<MockTransport>();
    auto timer_factory = std::make_unique<DeterministicTimerFactory>();
    kv::raft::CommitAwaiter awaiter(ioc, logger_);

    auto node = std::make_unique<RaftNode>(
        ioc, 1, std::vector<uint32_t>{2, 3},
        *transport, *timer_factory, logger_);
    node->start();
    pump(ioc);

    // Elect as leader.
    timer_factory->timer(0)->fire();
    pump(ioc);
    RequestVoteResponse vresp;
    vresp.set_term(1);
    vresp.set_vote_granted(true);
    node->handle_vote_response(2, vresp);
    pump(ioc);
    ASSERT_EQ(node->state(), kv::NodeState::Leader);
    transport->clear_sent();

    std::map<uint32_t, std::string> addrs = {
        {1, "localhost:8001"}, {2, "localhost:8002"}, {3, "localhost:8003"}
    };
    kv::raft::RaftClusterContext ctx(*node, awaiter, addrs, logger_);

    // Add node 4 via ClusterContext.
    kv::AddServerCmd add_cmd;
    add_cmd.node_id = 4;
    add_cmd.host = "node4.example.com";
    add_cmd.raft_port = 9004;
    add_cmd.client_port = 8004;
    bool ok = ctx.submit_config_change({add_cmd}, {});
    EXPECT_TRUE(ok);

    pump(ioc);

    // The node should be in joint consensus now with node 4 added.
    EXPECT_TRUE(node->config().is_joint());
    EXPECT_EQ(node->config().cluster_size(), 4);

    // Verify node 4 is in the new config.
    bool found_node4 = false;
    for (const auto& n : node->config().nodes()) {
        if (n.id() == 4) {
            found_node4 = true;
            EXPECT_EQ(n.host(), "node4.example.com");
            EXPECT_EQ(n.raft_port(), 9004);
            EXPECT_EQ(n.client_port(), 8004);
        }
    }
    EXPECT_TRUE(found_node4);

    node->stop();
    pump(ioc);
}

// ── 17. RaftClusterContext::submit_config_change() remove node ──────────────

TEST_F(DynamicMembershipTest, ClusterContextSubmitConfigChangeRemoveNode) {
    boost::asio::io_context ioc;

    auto transport = std::make_unique<MockTransport>();
    auto timer_factory = std::make_unique<DeterministicTimerFactory>();
    kv::raft::CommitAwaiter awaiter(ioc, logger_);

    auto node = std::make_unique<RaftNode>(
        ioc, 1, std::vector<uint32_t>{2, 3},
        *transport, *timer_factory, logger_);
    node->start();
    pump(ioc);

    timer_factory->timer(0)->fire();
    pump(ioc);
    RequestVoteResponse vresp;
    vresp.set_term(1);
    vresp.set_vote_granted(true);
    node->handle_vote_response(2, vresp);
    pump(ioc);
    ASSERT_EQ(node->state(), kv::NodeState::Leader);
    transport->clear_sent();

    std::map<uint32_t, std::string> addrs = {
        {1, "localhost:8001"}, {2, "localhost:8002"}, {3, "localhost:8003"}
    };
    kv::raft::RaftClusterContext ctx(*node, awaiter, addrs, logger_);

    // Remove node 3 via ClusterContext.
    bool ok = ctx.submit_config_change({}, {3});
    EXPECT_TRUE(ok);

    pump(ioc);

    EXPECT_TRUE(node->config().is_joint());
    // New config should have 2 nodes: {1, 2}.
    EXPECT_EQ(node->config().cluster_size(), 2);

    // Node 3 should NOT be in the new config.
    bool found_node3 = false;
    for (const auto& n : node->config().nodes()) {
        if (n.id() == 3) found_node3 = true;
    }
    EXPECT_FALSE(found_node3);

    node->stop();
    pump(ioc);
}

// ── 18. RaftClusterContext::submit_config_change() duplicate node fails ─────

TEST_F(DynamicMembershipTest, ClusterContextRejectsDuplicateNode) {
    boost::asio::io_context ioc;

    auto transport = std::make_unique<MockTransport>();
    auto timer_factory = std::make_unique<DeterministicTimerFactory>();
    kv::raft::CommitAwaiter awaiter(ioc, logger_);

    auto node = std::make_unique<RaftNode>(
        ioc, 1, std::vector<uint32_t>{2, 3},
        *transport, *timer_factory, logger_);
    node->start();
    pump(ioc);

    timer_factory->timer(0)->fire();
    pump(ioc);
    RequestVoteResponse vresp;
    vresp.set_term(1);
    vresp.set_vote_granted(true);
    node->handle_vote_response(2, vresp);
    pump(ioc);
    ASSERT_EQ(node->state(), kv::NodeState::Leader);

    std::map<uint32_t, std::string> addrs = {
        {1, "localhost:8001"}, {2, "localhost:8002"}, {3, "localhost:8003"}
    };
    kv::raft::RaftClusterContext ctx(*node, awaiter, addrs, logger_);

    // Try to add node 2 which already exists.
    kv::AddServerCmd add_cmd;
    add_cmd.node_id = 2;
    add_cmd.host = "localhost";
    add_cmd.raft_port = 9002;
    add_cmd.client_port = 8002;
    EXPECT_FALSE(ctx.submit_config_change({add_cmd}, {}));

    node->stop();
    pump(ioc);
}

// ── 19. RaftClusterContext::current_nodes() ─────────────────────────────────

TEST_F(DynamicMembershipTest, ClusterContextCurrentNodes) {
    boost::asio::io_context ioc;

    auto transport = std::make_unique<MockTransport>();
    auto timer_factory = std::make_unique<DeterministicTimerFactory>();
    kv::raft::CommitAwaiter awaiter(ioc, logger_);

    auto node = std::make_unique<RaftNode>(
        ioc, 1, std::vector<uint32_t>{2, 3},
        *transport, *timer_factory, logger_);
    node->start();
    pump(ioc);

    std::map<uint32_t, std::string> addrs = {{1, "localhost:8001"}};
    kv::raft::RaftClusterContext ctx(*node, awaiter, addrs, logger_);

    auto nodes = ctx.current_nodes();
    // Initial config from peer_ids: nodes are {1, 2, 3} (from init_from_peer_ids).
    EXPECT_EQ(nodes.size(), 3u);

    std::set<uint32_t> ids;
    for (const auto& n : nodes) {
        ids.insert(n.id);
    }
    EXPECT_TRUE(ids.count(1));
    EXPECT_TRUE(ids.count(2));
    EXPECT_TRUE(ids.count(3));

    node->stop();
    pump(ioc);
}

// ── 20. Full end-to-end: add node, commit, finalize, verify stable config ───

TEST_F(DynamicMembershipTest, FullEndToEndAddNode) {
    boost::asio::io_context ioc;

    int config_callbacks = 0;
    auto on_config_change = [&](const ClusterConfiguration&,
                                const LogEntry&) {
        config_callbacks++;
    };

    auto bundle = make_leader(ioc, {}, nullptr, 0, nullptr, on_config_change);
    auto& node = *bundle.node;

    // Step 1: Submit config change (add node 4).
    ASSERT_TRUE(node.submit_config_change(make_node_infos({1, 2, 3, 4})));
    pump(ioc);
    ASSERT_TRUE(node.config().is_joint());
    EXPECT_EQ(config_callbacks, 1);  // Joint callback.

    // Step 2: Commit joint entry (acks from 2 and 3).
    ack_from(node, 2);
    pump(ioc);
    ack_from(node, 3);
    pump(ioc);

    // Step 3: Verify finalize was auto-appended.
    EXPECT_FALSE(node.config().is_joint());
    EXPECT_EQ(node.config().cluster_size(), 4);
    EXPECT_EQ(config_callbacks, 2);  // Joint + finalize callbacks.

    // Step 4: Commit finalize entry (acks from 2, 3, and 4).
    ack_from(node, 2);
    pump(ioc);
    ack_from(node, 3);
    pump(ioc);

    // After finalize committed, config_change_pending_ is false.
    // Try another config change — should succeed now.
    ASSERT_TRUE(node.submit_config_change(make_node_infos({1, 2, 3, 4, 5})));
    pump(ioc);

    node.stop();
    pump(ioc);
}
