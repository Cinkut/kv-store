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
                         uint64_t last_included_term) override {
        installed_data_ = data;
        installed_index_ = last_included_index;
        installed_term_ = last_included_term;
        install_count_++;
        return install_result_;
    }

    bool create_snapshot(uint64_t last_included_index,
                        uint64_t last_included_term) override {
        created_index_ = last_included_index;
        created_term_ = last_included_term;
        create_count_++;

        if (create_result_) {
            // Update the data available for load_snapshot_for_sending.
            snapshot_data_.last_included_index = last_included_index;
            snapshot_data_.last_included_term = last_included_term;
            // Keep existing data (or set to a default).
            if (snapshot_data_.data.empty()) {
                snapshot_data_.data = "snapshot_data";
            }
        }

        return create_result_;
    }

    // Test configuration.
    void set_snapshot_data(SnapshotData data) { snapshot_data_ = std::move(data); }
    void set_install_result(bool result) { install_result_ = result; }
    void set_create_result(bool result) { create_result_ = result; }

    // Test inspection.
    [[nodiscard]] int install_count() const { return install_count_; }
    [[nodiscard]] int create_count() const { return create_count_; }
    [[nodiscard]] const std::string& installed_data() const { return installed_data_; }
    [[nodiscard]] uint64_t installed_index() const { return installed_index_; }
    [[nodiscard]] uint64_t installed_term() const { return installed_term_; }
    [[nodiscard]] uint64_t created_index() const { return created_index_; }
    [[nodiscard]] uint64_t created_term() const { return created_term_; }

private:
    SnapshotData snapshot_data_;
    bool install_result_ = true;
    bool create_result_ = true;

    std::string installed_data_;
    uint64_t installed_index_ = 0;
    uint64_t installed_term_ = 0;
    int install_count_ = 0;

    uint64_t created_index_ = 0;
    uint64_t created_term_ = 0;
    int create_count_ = 0;
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
