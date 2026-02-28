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
