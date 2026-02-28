#include "raft/cluster_config.hpp"
#include "common/logger.hpp"

#include <gtest/gtest.h>

#include <set>
#include <vector>

namespace {

using namespace kv::raft;

// ════════════════════════════════════════════════════════════════════════════
//  Helper: build a NodeInfo with just an ID
// ════════════════════════════════════════════════════════════════════════════

NodeInfo make_node(uint32_t id) {
    NodeInfo n;
    n.set_id(id);
    return n;
}

// ════════════════════════════════════════════════════════════════════════════
//  ClusterConfigurationTest fixture
// ════════════════════════════════════════════════════════════════════════════

class ClusterConfigurationTest : public ::testing::Test {
protected:
    void SetUp() override {
        spdlog::drop("kv");
        kv::init_default_logger();
    }
};

// ────────────────────────────────────────────────────────────────────────────
//  Construction from peer IDs
// ────────────────────────────────────────────────────────────────────────────

TEST_F(ClusterConfigurationTest, ConstructFromPeerIds) {
    ClusterConfiguration cfg(1, {2, 3});

    EXPECT_EQ(cfg.peer_ids().size(), 2u);
    EXPECT_EQ(cfg.peer_ids()[0], 2u);
    EXPECT_EQ(cfg.peer_ids()[1], 3u);
    EXPECT_FALSE(cfg.is_joint());
    EXPECT_TRUE(cfg.old_only_peer_ids().empty());
}

TEST_F(ClusterConfigurationTest, ConstructFromPeerIdsEmpty) {
    ClusterConfiguration cfg(1, std::vector<uint32_t>{});

    EXPECT_TRUE(cfg.peer_ids().empty());
    EXPECT_FALSE(cfg.is_joint());
}

// ────────────────────────────────────────────────────────────────────────────
//  Construction from protobuf (stable config)
// ────────────────────────────────────────────────────────────────────────────

TEST_F(ClusterConfigurationTest, ConstructFromProtoStable) {
    ClusterConfig proto;
    *proto.add_new_nodes() = make_node(1);
    *proto.add_new_nodes() = make_node(2);
    *proto.add_new_nodes() = make_node(3);
    // No old_nodes => stable.

    ClusterConfiguration cfg(1, proto);

    EXPECT_FALSE(cfg.is_joint());
    EXPECT_EQ(cfg.peer_ids().size(), 2u);  // 2 and 3, not self (1)
    EXPECT_TRUE(cfg.old_only_peer_ids().empty());
    EXPECT_EQ(cfg.cluster_size(), 3);

    // nodes() should have all 3
    EXPECT_EQ(cfg.nodes().size(), 3u);
}

// ────────────────────────────────────────────────────────────────────────────
//  Construction from protobuf (joint config)
// ────────────────────────────────────────────────────────────────────────────

TEST_F(ClusterConfigurationTest, ConstructFromProtoJoint) {
    ClusterConfig proto;
    // C_new: {1, 2, 3, 4}
    *proto.add_new_nodes() = make_node(1);
    *proto.add_new_nodes() = make_node(2);
    *proto.add_new_nodes() = make_node(3);
    *proto.add_new_nodes() = make_node(4);
    // C_old: {1, 2, 3}
    *proto.add_old_nodes() = make_node(1);
    *proto.add_old_nodes() = make_node(2);
    *proto.add_old_nodes() = make_node(3);

    ClusterConfiguration cfg(1, proto);

    EXPECT_TRUE(cfg.is_joint());
    // peer_ids = {2, 3, 4} (new config minus self)
    EXPECT_EQ(cfg.peer_ids().size(), 3u);
    // old_only_peer_ids should be empty since {2,3} are in new config too
    EXPECT_TRUE(cfg.old_only_peer_ids().empty());
}

TEST_F(ClusterConfigurationTest, ConstructFromProtoJointWithOldOnlyPeers) {
    ClusterConfig proto;
    // C_new: {1, 2, 4}  (node 3 removed)
    *proto.add_new_nodes() = make_node(1);
    *proto.add_new_nodes() = make_node(2);
    *proto.add_new_nodes() = make_node(4);
    // C_old: {1, 2, 3}
    *proto.add_old_nodes() = make_node(1);
    *proto.add_old_nodes() = make_node(2);
    *proto.add_old_nodes() = make_node(3);

    ClusterConfiguration cfg(1, proto);

    EXPECT_TRUE(cfg.is_joint());
    // peer_ids = {2, 4} (new config minus self)
    EXPECT_EQ(cfg.peer_ids().size(), 2u);
    // old_only_peer_ids = {3} (in old but not new)
    EXPECT_EQ(cfg.old_only_peer_ids().size(), 1u);
    EXPECT_EQ(cfg.old_only_peer_ids()[0], 3u);
}

// ────────────────────────────────────────────────────────────────────────────
//  Quorum checking — stable config
// ────────────────────────────────────────────────────────────────────────────

TEST_F(ClusterConfigurationTest, QuorumStableThreeNodes) {
    // Cluster {1, 2, 3}, self=1.  Need 2/3 majority.
    ClusterConfiguration cfg(1, {2, 3});
    cfg.init_from_peer_ids(1, {2, 3});

    // Only self voted — not enough.
    EXPECT_FALSE(cfg.has_quorum({1}));

    // Self + one peer — majority (2 of 3).
    EXPECT_TRUE(cfg.has_quorum({1, 2}));
    EXPECT_TRUE(cfg.has_quorum({1, 3}));

    // All three — majority.
    EXPECT_TRUE(cfg.has_quorum({1, 2, 3}));

    // Two peers without self — still majority (2 of 3).
    EXPECT_TRUE(cfg.has_quorum({2, 3}));

    // Single non-self peer — not enough.
    EXPECT_FALSE(cfg.has_quorum({2}));
}

TEST_F(ClusterConfigurationTest, QuorumStableFiveNodes) {
    // Cluster {1, 2, 3, 4, 5}, self=1.  Need 3/5 majority.
    ClusterConfiguration cfg(1, {2, 3, 4, 5});
    cfg.init_from_peer_ids(1, {2, 3, 4, 5});

    EXPECT_FALSE(cfg.has_quorum({1}));
    EXPECT_FALSE(cfg.has_quorum({1, 2}));
    EXPECT_TRUE(cfg.has_quorum({1, 2, 3}));
    EXPECT_TRUE(cfg.has_quorum({1, 2, 3, 4}));
    EXPECT_TRUE(cfg.has_quorum({1, 2, 3, 4, 5}));
}

TEST_F(ClusterConfigurationTest, QuorumStableSingleNode) {
    // Cluster {1}, self=1.  Need 1/1 majority.
    ClusterConfiguration cfg(1, std::vector<uint32_t>{});
    cfg.init_from_peer_ids(1, {});

    EXPECT_TRUE(cfg.has_quorum({1}));
    EXPECT_FALSE(cfg.has_quorum({}));
}

// ────────────────────────────────────────────────────────────────────────────
//  Quorum checking — joint config
// ────────────────────────────────────────────────────────────────────────────

TEST_F(ClusterConfigurationTest, QuorumJointNeedsBothMajorities) {
    // C_old: {1, 2, 3}   C_new: {1, 2, 3, 4}
    ClusterConfig proto;
    *proto.add_new_nodes() = make_node(1);
    *proto.add_new_nodes() = make_node(2);
    *proto.add_new_nodes() = make_node(3);
    *proto.add_new_nodes() = make_node(4);
    *proto.add_old_nodes() = make_node(1);
    *proto.add_old_nodes() = make_node(2);
    *proto.add_old_nodes() = make_node(3);

    ClusterConfiguration cfg(1, proto);

    // C_old needs 2/3, C_new needs 3/4.
    // {1, 4} — has 1/3 in C_old (only 1), fails.
    EXPECT_FALSE(cfg.has_quorum({1, 4}));

    // {1, 2} — C_old: 2/3 OK, C_new: 2/4 not enough.
    EXPECT_FALSE(cfg.has_quorum({1, 2}));

    // {1, 2, 4} — C_old: 2/3 OK, C_new: 3/4 OK.
    EXPECT_TRUE(cfg.has_quorum({1, 2, 4}));

    // {1, 2, 3} — C_old: 3/3 OK, C_new: 3/4 OK.
    EXPECT_TRUE(cfg.has_quorum({1, 2, 3}));

    // {1, 2, 3, 4} — both OK.
    EXPECT_TRUE(cfg.has_quorum({1, 2, 3, 4}));
}

TEST_F(ClusterConfigurationTest, QuorumJointNodeRemoval) {
    // C_old: {1, 2, 3}   C_new: {1, 2}  (node 3 being removed)
    ClusterConfig proto;
    *proto.add_new_nodes() = make_node(1);
    *proto.add_new_nodes() = make_node(2);
    *proto.add_old_nodes() = make_node(1);
    *proto.add_old_nodes() = make_node(2);
    *proto.add_old_nodes() = make_node(3);

    ClusterConfiguration cfg(1, proto);

    // C_old needs 2/3, C_new needs 2/2.
    // {1} alone — C_new: 1/2 fail.
    EXPECT_FALSE(cfg.has_quorum({1}));

    // {1, 2} — C_old: 2/3 OK, C_new: 2/2 OK.
    EXPECT_TRUE(cfg.has_quorum({1, 2}));

    // {1, 3} — C_old: 2/3 OK, C_new: 1/2 fail.
    EXPECT_FALSE(cfg.has_quorum({1, 3}));

    // {2, 3} — C_old: 2/3 OK, C_new: 1/2 fail.
    EXPECT_FALSE(cfg.has_quorum({2, 3}));

    // {1, 2, 3} — C_old: 3/3 OK, C_new: 2/2 OK.
    EXPECT_TRUE(cfg.has_quorum({1, 2, 3}));
}

// ────────────────────────────────────────────────────────────────────────────
//  all_peer_ids()
// ────────────────────────────────────────────────────────────────────────────

TEST_F(ClusterConfigurationTest, AllPeerIdsStable) {
    ClusterConfiguration cfg(1, {2, 3});

    auto all = cfg.all_peer_ids();
    EXPECT_EQ(all.size(), 2u);
    EXPECT_EQ(all[0], 2u);
    EXPECT_EQ(all[1], 3u);
}

TEST_F(ClusterConfigurationTest, AllPeerIdsJointIncludesOldOnly) {
    ClusterConfig proto;
    // C_new: {1, 2, 4}, C_old: {1, 2, 3}
    *proto.add_new_nodes() = make_node(1);
    *proto.add_new_nodes() = make_node(2);
    *proto.add_new_nodes() = make_node(4);
    *proto.add_old_nodes() = make_node(1);
    *proto.add_old_nodes() = make_node(2);
    *proto.add_old_nodes() = make_node(3);

    ClusterConfiguration cfg(1, proto);

    auto all = cfg.all_peer_ids();
    std::set<uint32_t> all_set(all.begin(), all.end());

    // Should include 2, 3, 4 (not self=1).
    EXPECT_EQ(all_set.size(), 3u);
    EXPECT_TRUE(all_set.contains(2));
    EXPECT_TRUE(all_set.contains(3));
    EXPECT_TRUE(all_set.contains(4));
}

// ────────────────────────────────────────────────────────────────────────────
//  self_in_new_config() / self_in_old_config()
// ────────────────────────────────────────────────────────────────────────────

TEST_F(ClusterConfigurationTest, SelfInConfigStable) {
    ClusterConfig proto;
    *proto.add_new_nodes() = make_node(1);
    *proto.add_new_nodes() = make_node(2);
    *proto.add_new_nodes() = make_node(3);

    ClusterConfiguration cfg(1, proto);

    EXPECT_TRUE(cfg.self_in_new_config());
    EXPECT_TRUE(cfg.self_in_old_config());  // stable => delegates to new
}

TEST_F(ClusterConfigurationTest, SelfBeingRemoved) {
    // C_old: {1, 2, 3}, C_new: {2, 3}  (self=1 being removed)
    ClusterConfig proto;
    *proto.add_new_nodes() = make_node(2);
    *proto.add_new_nodes() = make_node(3);
    *proto.add_old_nodes() = make_node(1);
    *proto.add_old_nodes() = make_node(2);
    *proto.add_old_nodes() = make_node(3);

    ClusterConfiguration cfg(1, proto);

    EXPECT_FALSE(cfg.self_in_new_config());
    EXPECT_TRUE(cfg.self_in_old_config());
}

TEST_F(ClusterConfigurationTest, SelfBeingAdded) {
    // C_old: {2, 3}, C_new: {1, 2, 3}  (self=1 being added)
    ClusterConfig proto;
    *proto.add_new_nodes() = make_node(1);
    *proto.add_new_nodes() = make_node(2);
    *proto.add_new_nodes() = make_node(3);
    *proto.add_old_nodes() = make_node(2);
    *proto.add_old_nodes() = make_node(3);

    ClusterConfiguration cfg(1, proto);

    EXPECT_TRUE(cfg.self_in_new_config());
    EXPECT_FALSE(cfg.self_in_old_config());
}

// ────────────────────────────────────────────────────────────────────────────
//  begin_joint() / finalize()
// ────────────────────────────────────────────────────────────────────────────

TEST_F(ClusterConfigurationTest, BeginJointTransition) {
    // Start with stable {1, 2, 3}.
    ClusterConfig proto;
    *proto.add_new_nodes() = make_node(1);
    *proto.add_new_nodes() = make_node(2);
    *proto.add_new_nodes() = make_node(3);

    ClusterConfiguration cfg(1, proto);
    ASSERT_FALSE(cfg.is_joint());

    // Add node 4: new config becomes {1, 2, 3, 4}.
    std::vector<NodeInfo> new_nodes = {make_node(1), make_node(2),
                                        make_node(3), make_node(4)};
    cfg.begin_joint(new_nodes);

    EXPECT_TRUE(cfg.is_joint());
    // peer_ids (new config, minus self) = {2, 3, 4}
    EXPECT_EQ(cfg.peer_ids().size(), 3u);
    // old_only_peer_ids should be empty (all old peers are in new too)
    EXPECT_TRUE(cfg.old_only_peer_ids().empty());

    // old_nodes should be the previous stable config
    EXPECT_EQ(cfg.old_nodes().size(), 3u);
    // nodes should be the new config
    EXPECT_EQ(cfg.nodes().size(), 4u);
}

TEST_F(ClusterConfigurationTest, BeginJointWithRemoval) {
    // Start with stable {1, 2, 3}.
    ClusterConfig proto;
    *proto.add_new_nodes() = make_node(1);
    *proto.add_new_nodes() = make_node(2);
    *proto.add_new_nodes() = make_node(3);

    ClusterConfiguration cfg(1, proto);

    // Remove node 3: new config = {1, 2}.
    std::vector<NodeInfo> new_nodes = {make_node(1), make_node(2)};
    cfg.begin_joint(new_nodes);

    EXPECT_TRUE(cfg.is_joint());
    // peer_ids (new, minus self) = {2}
    EXPECT_EQ(cfg.peer_ids().size(), 1u);
    EXPECT_EQ(cfg.peer_ids()[0], 2u);
    // old_only_peer_ids = {3} (in old but not new)
    EXPECT_EQ(cfg.old_only_peer_ids().size(), 1u);
    EXPECT_EQ(cfg.old_only_peer_ids()[0], 3u);
}

TEST_F(ClusterConfigurationTest, FinalizeTransition) {
    ClusterConfig proto;
    *proto.add_new_nodes() = make_node(1);
    *proto.add_new_nodes() = make_node(2);
    *proto.add_new_nodes() = make_node(3);
    *proto.add_new_nodes() = make_node(4);
    *proto.add_old_nodes() = make_node(1);
    *proto.add_old_nodes() = make_node(2);
    *proto.add_old_nodes() = make_node(3);

    ClusterConfiguration cfg(1, proto);
    ASSERT_TRUE(cfg.is_joint());

    cfg.finalize();

    EXPECT_FALSE(cfg.is_joint());
    EXPECT_TRUE(cfg.old_only_peer_ids().empty());
    EXPECT_TRUE(cfg.old_nodes().empty());
    // peer_ids should remain from new config
    EXPECT_EQ(cfg.peer_ids().size(), 3u);
}

// ────────────────────────────────────────────────────────────────────────────
//  Protobuf serialization round-trip
// ────────────────────────────────────────────────────────────────────────────

TEST_F(ClusterConfigurationTest, ToProtoStable) {
    ClusterConfig proto;
    *proto.add_new_nodes() = make_node(1);
    *proto.add_new_nodes() = make_node(2);
    *proto.add_new_nodes() = make_node(3);

    ClusterConfiguration cfg(1, proto);
    auto out = cfg.to_proto();

    EXPECT_EQ(out.new_nodes_size(), 3);
    EXPECT_EQ(out.old_nodes_size(), 0);
}

TEST_F(ClusterConfigurationTest, ToProtoJoint) {
    ClusterConfig proto;
    *proto.add_new_nodes() = make_node(1);
    *proto.add_new_nodes() = make_node(2);
    *proto.add_new_nodes() = make_node(3);
    *proto.add_new_nodes() = make_node(4);
    *proto.add_old_nodes() = make_node(1);
    *proto.add_old_nodes() = make_node(2);
    *proto.add_old_nodes() = make_node(3);

    ClusterConfiguration cfg(1, proto);
    auto out = cfg.to_proto();

    EXPECT_EQ(out.new_nodes_size(), 4);
    EXPECT_EQ(out.old_nodes_size(), 3);
}

TEST_F(ClusterConfigurationTest, ToStableProto) {
    ClusterConfig proto;
    *proto.add_new_nodes() = make_node(1);
    *proto.add_new_nodes() = make_node(2);
    *proto.add_new_nodes() = make_node(3);
    *proto.add_new_nodes() = make_node(4);
    *proto.add_old_nodes() = make_node(1);
    *proto.add_old_nodes() = make_node(2);
    *proto.add_old_nodes() = make_node(3);

    ClusterConfiguration cfg(1, proto);
    auto out = cfg.to_stable_proto();

    EXPECT_EQ(out.new_nodes_size(), 4);
    EXPECT_EQ(out.old_nodes_size(), 0);  // No old nodes in stable proto.
}

TEST_F(ClusterConfigurationTest, ProtoRoundTrip) {
    // Build a config, serialize, reconstruct, verify equality.
    ClusterConfig proto;
    *proto.add_new_nodes() = make_node(1);
    *proto.add_new_nodes() = make_node(2);
    *proto.add_new_nodes() = make_node(3);
    *proto.add_new_nodes() = make_node(4);
    *proto.add_old_nodes() = make_node(1);
    *proto.add_old_nodes() = make_node(2);
    *proto.add_old_nodes() = make_node(3);

    ClusterConfiguration cfg1(1, proto);
    auto serialized = cfg1.to_proto();
    ClusterConfiguration cfg2(1, serialized);

    EXPECT_EQ(cfg2.is_joint(), cfg1.is_joint());
    EXPECT_EQ(cfg2.peer_ids().size(), cfg1.peer_ids().size());
    EXPECT_EQ(cfg2.old_only_peer_ids().size(), cfg1.old_only_peer_ids().size());
    EXPECT_EQ(cfg2.nodes().size(), cfg1.nodes().size());
    EXPECT_EQ(cfg2.old_nodes().size(), cfg1.old_nodes().size());
}

// ────────────────────────────────────────────────────────────────────────────
//  init_from_peer_ids()
// ────────────────────────────────────────────────────────────────────────────

TEST_F(ClusterConfigurationTest, InitFromPeerIds) {
    ClusterConfiguration cfg(1, {2, 3});
    cfg.init_from_peer_ids(1, {2, 3});

    // nodes_ should now contain {1, 2, 3}
    EXPECT_EQ(cfg.nodes().size(), 3u);
    EXPECT_EQ(cfg.nodes()[0].id(), 1u);  // self first
    EXPECT_EQ(cfg.nodes()[1].id(), 2u);
    EXPECT_EQ(cfg.nodes()[2].id(), 3u);
    EXPECT_EQ(cfg.cluster_size(), 3);
}

// ────────────────────────────────────────────────────────────────────────────
//  Full transition workflow
// ────────────────────────────────────────────────────────────────────────────

TEST_F(ClusterConfigurationTest, FullAddNodeWorkflow) {
    // Step 1: Start with stable {1, 2, 3}.
    ClusterConfiguration cfg(1, {2, 3});
    cfg.init_from_peer_ids(1, {2, 3});
    ASSERT_FALSE(cfg.is_joint());

    // Verify stable quorum works.
    EXPECT_TRUE(cfg.has_quorum({1, 2}));
    EXPECT_FALSE(cfg.has_quorum({1}));

    // Step 2: Begin joint consensus to add node 4.
    std::vector<NodeInfo> new_nodes = {make_node(1), make_node(2),
                                        make_node(3), make_node(4)};
    cfg.begin_joint(new_nodes);
    ASSERT_TRUE(cfg.is_joint());

    // C_old: {1,2,3} needs 2/3.  C_new: {1,2,3,4} needs 3/4.
    EXPECT_FALSE(cfg.has_quorum({1, 2}));      // C_new: only 2/4
    EXPECT_TRUE(cfg.has_quorum({1, 2, 3}));    // C_old: 3/3, C_new: 3/4 — OK
    EXPECT_TRUE(cfg.has_quorum({1, 2, 4}));    // C_old: 2/3, C_new: 3/4 — OK
    EXPECT_FALSE(cfg.has_quorum({1, 4}));       // C_old: 1/3 — fail

    // Step 3: Finalize — C_new becomes the stable config.
    cfg.finalize();
    ASSERT_FALSE(cfg.is_joint());
    EXPECT_EQ(cfg.cluster_size(), 4);

    // Now quorum is 3/4.
    EXPECT_FALSE(cfg.has_quorum({1, 2}));
    EXPECT_TRUE(cfg.has_quorum({1, 2, 3}));
    EXPECT_TRUE(cfg.has_quorum({1, 2, 4}));
    EXPECT_TRUE(cfg.has_quorum({1, 2, 3, 4}));
}

TEST_F(ClusterConfigurationTest, FullRemoveNodeWorkflow) {
    // Step 1: Start with stable {1, 2, 3, 4, 5}.
    ClusterConfiguration cfg(1, {2, 3, 4, 5});
    cfg.init_from_peer_ids(1, {2, 3, 4, 5});
    ASSERT_FALSE(cfg.is_joint());
    // Majority = 3/5.
    EXPECT_TRUE(cfg.has_quorum({1, 2, 3}));

    // Step 2: Remove node 5.
    std::vector<NodeInfo> new_nodes = {make_node(1), make_node(2),
                                        make_node(3), make_node(4)};
    cfg.begin_joint(new_nodes);
    ASSERT_TRUE(cfg.is_joint());

    // C_old: {1,2,3,4,5} needs 3/5.  C_new: {1,2,3,4} needs 3/4.
    // {1,2} — C_old: 2/5 fail.
    EXPECT_FALSE(cfg.has_quorum({1, 2}));
    // {1,2,3} — C_old: 3/5 OK, C_new: 3/4 OK.
    EXPECT_TRUE(cfg.has_quorum({1, 2, 3}));
    // {1,2,5} — C_old: 3/5 OK, C_new: 2/4 fail.
    EXPECT_FALSE(cfg.has_quorum({1, 2, 5}));
    // {1,2,3,5} — C_old: 4/5 OK, C_new: 3/4 OK.
    EXPECT_TRUE(cfg.has_quorum({1, 2, 3, 5}));

    // old_only_peer_ids should contain 5.
    EXPECT_EQ(cfg.old_only_peer_ids().size(), 1u);
    EXPECT_EQ(cfg.old_only_peer_ids()[0], 5u);

    // Step 3: Finalize.
    cfg.finalize();
    ASSERT_FALSE(cfg.is_joint());
    EXPECT_EQ(cfg.cluster_size(), 4);
    // Majority = 3/4.
    EXPECT_TRUE(cfg.has_quorum({1, 2, 3}));
    EXPECT_FALSE(cfg.has_quorum({1, 2}));
}

} // namespace
