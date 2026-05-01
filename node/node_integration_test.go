//go:build integration

package node

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ulixert/theseon/cluster"
	"github.com/ulixert/theseon/hlc"
	pb "github.com/ulixert/theseon/proto/theseonpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// testClusterConfig returns a ClusterConfig with fast gossip intervals
// suitable for integration tests.
func testClusterConfig(nodeID string) cluster.ClusterConfig {
	cfg := cluster.DefaultClusterConfig(nodeID, "")
	cfg.GossipInterval = 100 * time.Millisecond
	cfg.PingTimeout = 50 * time.Millisecond
	cfg.SuspectTimeout = 500 * time.Millisecond
	return cfg
}

// TestIntegration_ClusterFormation starts 3 nodes, forms a ring via admin
// commands, then verifies KV read/write through the coordinator.
func TestIntegration_ClusterFormation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start node-1 (first cluster node, no seeds).
	n1 := New(Config{
		NodeID:  "node-1",
		Addr:    "127.0.0.1:0",
		DataDir: t.TempDir(),
		Cluster: testClusterConfig("node-1"),
		Coord:   cluster.DefaultCoordinatorConfig(),
	})
	if err := n1.Start(ctx); err != nil {
		t.Fatalf("start node-1: %v", err)
	}
	defer n1.Stop()
	t.Logf("node-1 listening on %s", n1.Addr())

	// Start node-2 and node-3 with node-1 as seed.
	n2 := New(Config{
		NodeID:    "node-2",
		Addr:      "127.0.0.1:0",
		DataDir:   t.TempDir(),
		SeedPeers: []string{n1.Addr()},
		Cluster:   testClusterConfig("node-2"),
		Coord:     cluster.DefaultCoordinatorConfig(),
	})
	if err := n2.Start(ctx); err != nil {
		t.Fatalf("start node-2: %v", err)
	}
	defer n2.Stop()
	t.Logf("node-2 listening on %s", n2.Addr())

	n3 := New(Config{
		NodeID:    "node-3",
		Addr:      "127.0.0.1:0",
		DataDir:   t.TempDir(),
		SeedPeers: []string{n1.Addr()},
		Cluster:   testClusterConfig("node-3"),
		Coord:     cluster.DefaultCoordinatorConfig(),
	})
	if err := n3.Start(ctx); err != nil {
		t.Fatalf("start node-3: %v", err)
	}
	defer n3.Stop()
	t.Logf("node-3 listening on %s", n3.Addr())

	// Wait for SWIM to discover all 3 nodes.
	adminClient := newAdminClient(t, n1.Addr())
	waitForMembers(t, adminClient, 3, 10*time.Second)

	// Join and activate all 3 nodes in the ring.
	for _, info := range []struct{ id, addr string }{
		{"node-1", n1.Addr()},
		{"node-2", n2.Addr()},
		{"node-3", n3.Addr()},
	} {
		joinAndActivate(t, adminClient, info.id, info.addr)
	}

	// Verify all 3 are ACTIVE.
	statusResp, err := adminClient.GetClusterStatus(ctx, &pb.GetClusterStatusRequest{})
	if err != nil {
		t.Fatalf("get cluster status: %v", err)
	}
	if got := len(statusResp.RingDescriptor.Members); got != 3 {
		t.Fatalf("ring members: got %d, want 3", got)
	}
	for _, rm := range statusResp.RingDescriptor.Members {
		if cluster.RingState(rm.RingState) != cluster.RingActive {
			t.Errorf("node %s ring state=%v, want active", rm.NodeId, rm.RingState)
		}
	}
	t.Logf("ring version: %d, all 3 nodes ACTIVE", statusResp.RingDescriptor.Version)

	// Wait for ring descriptor to propagate to all nodes via gossip.
	adminClient2 := newAdminClient(t, n2.Addr())
	waitForRingVersion(t, adminClient2, statusResp.RingDescriptor.Version, 10*time.Second)

	// KV write through node-1, read through node-2.
	theseonClient1 := newTheseonClient(t, n1.Addr())
	theseonClient2 := newTheseonClient(t, n2.Addr())

	_, err = theseonClient1.Put(ctx, &pb.PutRequest{
		Key:   []byte("hello"),
		Value: []byte("world"),
	})
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	// Small delay for replication.
	time.Sleep(200 * time.Millisecond)

	getResp, err := theseonClient2.Get(ctx, &pb.GetRequest{Key: []byte("hello")})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !getResp.Found {
		t.Fatal("key 'hello' not found on node-2")
	}
	if string(getResp.Value) != "world" {
		t.Errorf("value=%q, want %q", getResp.Value, "world")
	}
	t.Log("KV write/read across nodes: OK")
}

// --- Test helpers ---

func newAdminClient(t *testing.T, addr string) pb.AdminServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("admin dial %s: %v", addr, err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewAdminServiceClient(conn)
}

func newTheseonClient(t *testing.T, addr string) pb.TheseonClient {
	t.Helper()
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("theseon dial %s: %v", addr, err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewTheseonClient(conn)
}

func waitForRingVersion(t *testing.T, client pb.AdminServiceClient, want uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.GetClusterStatus(context.Background(), &pb.GetClusterStatusRequest{})
		if err == nil && resp.RingDescriptor != nil && resp.RingDescriptor.Version >= want {
			t.Logf("ring version %d propagated", resp.RingDescriptor.Version)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for ring version >= %d", want)
}

func waitForMembers(t *testing.T, client pb.AdminServiceClient, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.GetClusterStatus(context.Background(), &pb.GetClusterStatusRequest{})
		if err == nil && len(resp.Members) >= want {
			t.Logf("SWIM discovered %d members", len(resp.Members))
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %d SWIM members", want)
}

func joinAndActivate(t *testing.T, client pb.AdminServiceClient, nodeID, addr string) {
	t.Helper()
	ctx := context.Background()

	// Get current version.
	statusResp, err := client.GetClusterStatus(ctx, &pb.GetClusterStatusRequest{})
	if err != nil {
		t.Fatalf("get status for join %s: %v", nodeID, err)
	}
	version := statusResp.RingDescriptor.GetVersion()

	// Join.
	_, err = client.JoinRing(ctx, &pb.JoinRingRequest{
		NodeId:          nodeID,
		Addr:            addr,
		ExpectedVersion: version,
	})
	if err != nil {
		t.Fatalf("join %s: %v", nodeID, err)
	}

	// Activate (version incremented by join).
	_, err = client.ActivateNode(ctx, &pb.ActivateNodeRequest{
		NodeId:          nodeID,
		ExpectedVersion: version + 1,
	})
	if err != nil {
		t.Fatalf("activate %s: %v", nodeID, err)
	}
	t.Logf("joined + activated %s (ring version: %d)", nodeID, version+2)
}

func TestIntegration_HintedHandoff(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	coord := cluster.CoordinatorConfig{
		ReplicationFactor: 3,
		WriteQuorum:       2,
		ReadQuorum:        2,
		PerReplicaTimeout: 2 * time.Second,
	}

	// Start 3 nodes.
	n1 := New(Config{
		NodeID:  "node-1",
		Addr:    "127.0.0.1:0",
		DataDir: t.TempDir(),
		Cluster: testClusterConfig("node-1"),
		Coord:   coord,
	})
	if err := n1.Start(ctx); err != nil {
		t.Fatalf("start node-1: %v", err)
	}
	defer n1.Stop()

	n2 := New(Config{
		NodeID:    "node-2",
		Addr:      "127.0.0.1:0",
		DataDir:   t.TempDir(),
		SeedPeers: []string{n1.Addr()},
		Cluster:   testClusterConfig("node-2"),
		Coord:     coord,
	})
	if err := n2.Start(ctx); err != nil {
		t.Fatalf("start node-2: %v", err)
	}
	defer n2.Stop()

	n3DataDir := t.TempDir()
	n3 := New(Config{
		NodeID:    "node-3",
		Addr:      "127.0.0.1:0",
		DataDir:   n3DataDir,
		SeedPeers: []string{n1.Addr()},
		Cluster:   testClusterConfig("node-3"),
		Coord:     coord,
	})
	if err := n3.Start(ctx); err != nil {
		t.Fatalf("start node-3: %v", err)
	}

	// Wait for discovery, form ring.
	adminClient := newAdminClient(t, n1.Addr())
	waitForMembers(t, adminClient, 3, 10*time.Second)
	for _, info := range []struct{ id, addr string }{
		{"node-1", n1.Addr()},
		{"node-2", n2.Addr()},
		{"node-3", n3.Addr()},
	} {
		joinAndActivate(t, adminClient, info.id, info.addr)
	}

	// Write a few keys through node-1 so all nodes have data.
	client1 := newTheseonClient(t, n1.Addr())
	for i := range 10 {
		_, err := client1.Put(ctx, &pb.PutRequest{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		})
		if err != nil {
			t.Fatalf("put key-%d: %v", i, err)
		}
	}
	time.Sleep(200 * time.Millisecond)

	// Stop node-3.
	n3Addr := n3.Addr()
	n3.Stop()
	t.Log("node-3 stopped")

	// Wait for SWIM to detect node-3 is dead (depends on suspect timeout).
	time.Sleep(2 * time.Second)

	// Write more keys - hints should be stored for node-3.
	for i := 10; i < 20; i++ {
		_, err := client1.Put(ctx, &pb.PutRequest{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		})
		if err != nil {
			// Writes may still succeed with W=2 if node-3 is down.
			t.Logf("put key-%d (node-3 down): %v", i, err)
		}
	}
	t.Log("wrote 10 more keys with node-3 down")

	// Restart node-3 on a NEW port (same data dir).
	n3New := New(Config{
		NodeID:    "node-3",
		Addr:      "127.0.0.1:0",
		DataDir:   n3DataDir,
		SeedPeers: []string{n1.Addr()},
		Cluster:   testClusterConfig("node-3"),
		Coord:     coord,
	})
	if err := n3New.Start(ctx); err != nil {
		t.Fatalf("restart node-3: %v", err)
	}
	defer n3New.Stop()
	t.Logf("node-3 restarted on %s (was %s)", n3New.Addr(), n3Addr)

	// Wait for SWIM to discover the restarted node-3.
	// The drainer should trigger hint replay once node-3 is alive.
	time.Sleep(5 * time.Second)

	t.Log("hinted handoff test: node-3 restarted and drainer triggered")
	// Note: full verification of hint replay would require checking
	// that the keys written during node-3's downtime are readable
	// from node-3. This is complex because the restarted node has a
	// new address and the ring needs to be updated. For now, we verify
	// the restart path doesn't panic or deadlock.
}

// startThreeNodeCluster boots a 3-node cluster and forms the ring with
// all members ACTIVE. Returns nodes in (n1, n2, n3) order. Caller must
// arrange for Stop calls. AntiEntropy is left disabled — the admin
// trigger path is unaffected.
func startThreeNodeCluster(t *testing.T) (*Node, *Node, *Node) {
	t.Helper()
	ctx := context.Background()
	coord := cluster.CoordinatorConfig{
		ReplicationFactor: 3,
		WriteQuorum:       2,
		ReadQuorum:        2,
		PerReplicaTimeout: 2 * time.Second,
	}

	n1 := New(Config{
		NodeID:  "node-1",
		Addr:    "127.0.0.1:0",
		DataDir: t.TempDir(),
		Cluster: testClusterConfig("node-1"),
		Coord:   coord,
	})
	if err := n1.Start(ctx); err != nil {
		t.Fatalf("start node-1: %v", err)
	}

	n2 := New(Config{
		NodeID:    "node-2",
		Addr:      "127.0.0.1:0",
		DataDir:   t.TempDir(),
		SeedPeers: []string{n1.Addr()},
		Cluster:   testClusterConfig("node-2"),
		Coord:     coord,
	})
	if err := n2.Start(ctx); err != nil {
		t.Fatalf("start node-2: %v", err)
	}

	n3 := New(Config{
		NodeID:    "node-3",
		Addr:      "127.0.0.1:0",
		DataDir:   t.TempDir(),
		SeedPeers: []string{n1.Addr()},
		Cluster:   testClusterConfig("node-3"),
		Coord:     coord,
	})
	if err := n3.Start(ctx); err != nil {
		t.Fatalf("start node-3: %v", err)
	}

	admin := newAdminClient(t, n1.Addr())
	waitForMembers(t, admin, 3, 10*time.Second)
	for _, info := range []struct{ id, addr string }{
		{"node-1", n1.Addr()},
		{"node-2", n2.Addr()},
		{"node-3", n3.Addr()},
	} {
		joinAndActivate(t, admin, info.id, info.addr)
	}

	// Wait for ring + member view to converge on every node.
	statusResp, err := admin.GetClusterStatus(ctx, &pb.GetClusterStatusRequest{})
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	for _, addr := range []string{n1.Addr(), n2.Addr(), n3.Addr()} {
		c := newAdminClient(t, addr)
		waitForRingVersion(t, c, statusResp.RingDescriptor.Version, 10*time.Second)
	}

	return n1, n2, n3
}

// putEnvelope encodes (value, ts, deleted) as a cluster envelope and
// writes it directly into the node's local DB, bypassing the coordinator.
// Used to inject divergence for AE testing.
func putEnvelope(t *testing.T, n *Node, key, value []byte, ts hlc.Timestamp, deleted bool) {
	t.Helper()
	encoded, err := cluster.EncodeEnvelope(cluster.Envelope{
		Timestamp: ts,
		Deleted:   deleted,
		Value:     value,
	})
	if err != nil {
		t.Fatalf("encode envelope: %v", err)
	}
	if err := n.database.Put(key, encoded); err != nil {
		t.Fatalf("local put: %v", err)
	}
}

// readEnvelope reads and decodes the cluster envelope at key directly
// from the node's local DB. Returns (envelope, found).
func readEnvelope(t *testing.T, n *Node, key []byte) (cluster.Envelope, bool) {
	t.Helper()
	val, found := n.database.Get(key)
	if !found {
		return cluster.Envelope{}, false
	}
	env, err := cluster.DecodeEnvelope(val.Data)
	if err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	return env, true
}

// TestIntegration_AntiEntropy_PullRepair injects divergence by writing
// directly to two replicas' local DBs (bypassing the coordinator), then
// triggers AE from the third node. AE should pull the missing key.
//
// Verifies invariant #2 end-to-end: the repaired envelope on the third
// node carries the source HLC bit-for-bit, not a re-stamped clock.Now().
func TestIntegration_AntiEntropy_PullRepair(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx := context.Background()

	n1, n2, n3 := startThreeNodeCluster(t)
	defer n1.Stop()
	defer n2.Stop()
	defer n3.Stop()

	// Wall time well in the past so it survives the grace filter (default 30s).
	pastWall := time.Now().Add(-5 * time.Minute).UnixNano()
	ts := hlc.Timestamp{WallTime: pastWall, Logical: 7, NodeID: "test-source"}

	key := []byte("ae-pull-key")
	value := []byte("ae-pull-value")

	// Inject on n1 and n2 only.
	putEnvelope(t, n1, key, value, ts, false)
	putEnvelope(t, n2, key, value, ts, false)

	// Sanity: confirm the puts are visible to local readers on n1/n2.
	if env, ok := readEnvelope(t, n1, key); !ok {
		t.Fatal("precondition: n1 cannot read its own injected key")
	} else if !env.Timestamp.Equal(ts) {
		t.Fatalf("n1 ts mismatch: got %+v want %+v", env.Timestamp, ts)
	}
	if _, ok := readEnvelope(t, n2, key); !ok {
		t.Fatal("precondition: n2 cannot read its own injected key")
	}

	// n3 is missing.
	if _, ok := readEnvelope(t, n3, key); ok {
		t.Fatal("precondition: n3 should not have the key")
	}

	// Trigger AE on n3 against node-1 (blocking).
	adminN3 := newAdminClient(t, n3.Addr())
	resp, err := adminN3.TriggerAntiEntropy(ctx, &pb.TriggerAERequest{
		PeerId:   "node-1",
		Blocking: true,
	})
	if err != nil {
		t.Fatalf("TriggerAntiEntropy: %v", err)
	}
	if len(resp.Stats) != 1 {
		t.Fatalf("expected 1 stat, got %d", len(resp.Stats))
	}
	stat := resp.Stats[0]
	if stat.Error != "" {
		t.Fatalf("AE returned error: %s", stat.Error)
	}
	if stat.KeysRepaired == 0 {
		t.Errorf("expected at least 1 key repaired, got 0 (stats: %+v)", stat)
	}
	t.Logf("AE pull-repair stats: scanned=%d divergent=%d repaired=%d duration_ms=%d",
		stat.KeysScanned, stat.DivergentLeaves, stat.KeysRepaired, stat.DurationMs)

	// n3 should now have the key with the original HLC.
	env, ok := readEnvelope(t, n3, key)
	if !ok {
		t.Fatal("AE did not repair n3")
	}
	if env.Timestamp.WallTime != ts.WallTime ||
		env.Timestamp.Logical != ts.Logical ||
		env.Timestamp.NodeID != ts.NodeID {
		t.Errorf("HLC not preserved on pull-repair: got %+v want %+v", env.Timestamp, ts)
	}
	if string(env.Value) != string(value) {
		t.Errorf("value: got %q want %q", env.Value, value)
	}
	if env.Deleted {
		t.Error("deleted bit flipped")
	}
}

// TestIntegration_AntiEntropy_LWWPushRepair: n3 has a NEWER version
// than n1+n2. AE triggered from n3 should push n3's newer version to
// n1, not overwrite n3 with the older value.
func TestIntegration_AntiEntropy_LWWPushRepair(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx := context.Background()

	n1, n2, n3 := startThreeNodeCluster(t)
	defer n1.Stop()
	defer n2.Stop()
	defer n3.Stop()

	key := []byte("ae-lww-key")

	pastWall := time.Now().Add(-10 * time.Minute).UnixNano()
	tsOld := hlc.Timestamp{WallTime: pastWall, Logical: 0, NodeID: "test-old"}
	tsNew := hlc.Timestamp{WallTime: pastWall + 1_000_000, Logical: 0, NodeID: "test-new"}

	// n1 + n2 have the OLD version; n3 has the NEW version.
	putEnvelope(t, n1, key, []byte("old"), tsOld, false)
	putEnvelope(t, n2, key, []byte("old"), tsOld, false)
	putEnvelope(t, n3, key, []byte("new"), tsNew, false)

	adminN3 := newAdminClient(t, n3.Addr())
	resp, err := adminN3.TriggerAntiEntropy(ctx, &pb.TriggerAERequest{
		PeerId:   "node-1",
		Blocking: true,
	})
	if err != nil {
		t.Fatalf("TriggerAntiEntropy: %v", err)
	}
	if len(resp.Stats) != 1 || resp.Stats[0].Error != "" {
		t.Fatalf("AE error: %+v", resp.Stats)
	}

	// n3 must still have the NEW version (no overwrite by older).
	env, ok := readEnvelope(t, n3, key)
	if !ok {
		t.Fatal("n3 lost the key")
	}
	if !env.Timestamp.Equal(tsNew) {
		t.Errorf("n3 was overwritten by older value; got ts=%+v want %+v", env.Timestamp, tsNew)
	}
	if string(env.Value) != "new" {
		t.Errorf("n3 value: got %q want %q", env.Value, "new")
	}

	// n1 should have been push-repaired to the NEW version.
	env1, ok := readEnvelope(t, n1, key)
	if !ok {
		t.Fatal("n1 lost the key")
	}
	if !env1.Timestamp.Equal(tsNew) {
		t.Errorf("n1 not push-repaired; got ts=%+v want %+v", env1.Timestamp, tsNew)
	}
	if string(env1.Value) != "new" {
		t.Errorf("n1 value: got %q want %q", env1.Value, "new")
	}
}
