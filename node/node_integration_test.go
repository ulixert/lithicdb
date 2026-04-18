//go:build integration

package node

import (
	"context"
	"testing"
	"time"

	"github.com/ulixert/theseon/cluster"
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
