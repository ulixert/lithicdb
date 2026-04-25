package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ulixert/theseon/db"
	"github.com/ulixert/theseon/hashring"
	"github.com/ulixert/theseon/hlc"
	pb "github.com/ulixert/theseon/proto/theseonpb"
	"google.golang.org/grpc"
)

// --- Mock infrastructure ---

// mockClient implements pb.InternalServiceClient for testing.
type mockClient struct {
	writeFn func(ctx context.Context, req *pb.ReplicateWriteRequest) (*pb.ReplicateWriteResponse, error)
	readFn  func(ctx context.Context, req *pb.ReplicateReadRequest) (*pb.ReplicateReadResponse, error)
}

func (m *mockClient) Ping(context.Context, *pb.PingRequest, ...grpc.CallOption) (*pb.PingResponse, error) {
	return nil, nil
}
func (m *mockClient) PingReq(context.Context, *pb.PingReqRequest, ...grpc.CallOption) (*pb.PingReqResponse, error) {
	return nil, nil
}
func (m *mockClient) GossipSync(context.Context, *pb.GossipSyncRequest, ...grpc.CallOption) (*pb.GossipSyncResponse, error) {
	return nil, nil
}
func (m *mockClient) ReplicateWriteBatch(context.Context, *pb.ReplicateWriteBatchRequest, ...grpc.CallOption) (*pb.ReplicateWriteBatchResponse, error) {
	return nil, nil
}

func (m *mockClient) ReplicateVectorWrite(_ context.Context, _ *pb.ReplicateVectorWriteRequest, _ ...grpc.CallOption) (*pb.ReplicateVectorWriteResponse, error) {
	return nil, nil
}
func (m *mockClient) ReplicateVectorDelete(_ context.Context, _ *pb.ReplicateVectorDeleteRequest, _ ...grpc.CallOption) (*pb.ReplicateVectorDeleteResponse, error) {
	return nil, nil
}
func (m *mockClient) ReplicateVectorSearch(_ context.Context, _ *pb.ReplicateVectorSearchRequest, _ ...grpc.CallOption) (*pb.ReplicateVectorSearchResponse, error) {
	return nil, nil
}

// Anti-entropy stubs — coordinator tests don't exercise AE.
func (m *mockClient) ComputeAERoot(context.Context, *pb.AERootRequest, ...grpc.CallOption) (*pb.AERootResponse, error) {
	return nil, nil
}
func (m *mockClient) GetAESubtree(context.Context, *pb.AESubtreeRequest, ...grpc.CallOption) (*pb.AESubtreeResponse, error) {
	return nil, nil
}
func (m *mockClient) GetAELeafKeys(context.Context, *pb.AELeafRequest, ...grpc.CallOption) (pb.InternalService_GetAELeafKeysClient, error) {
	return nil, nil
}

func (m *mockClient) ReplicateWrite(ctx context.Context, req *pb.ReplicateWriteRequest, _ ...grpc.CallOption) (*pb.ReplicateWriteResponse, error) {
	if m.writeFn != nil {
		return m.writeFn(ctx, req)
	}
	return &pb.ReplicateWriteResponse{}, nil
}

func (m *mockClient) ReplicateRead(ctx context.Context, req *pb.ReplicateReadRequest, _ ...grpc.CallOption) (*pb.ReplicateReadResponse, error) {
	if m.readFn != nil {
		return m.readFn(ctx, req)
	}
	return &pb.ReplicateReadResponse{Found: false}, nil
}

// mockDialer implements ReplicaDialer for testing.
type mockDialer struct {
	mu      sync.Mutex
	clients map[string]*mockClient
}

func newMockDialer() *mockDialer {
	return &mockDialer{clients: make(map[string]*mockClient)}
}

func (d *mockDialer) setClient(addr string, c *mockClient) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.clients[addr] = c
}

func (d *mockDialer) GetClient(addr string) (pb.InternalServiceClient, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	c, ok := d.clients[addr]
	if !ok {
		return nil, fmt.Errorf("mock: no client for %s", addr)
	}
	return c, nil
}

func (d *mockDialer) Close() {}

// --- Test helpers ---

// setupCoordinator creates a Coordinator with a 3-node ring where
// "self" is the local node (node-1). Returns the coordinator, the mock
// dialer (for wiring remote behavior), and a cleanup function.
func setupCoordinator(t *testing.T, cfg CoordinatorConfig) (*Coordinator, *mockDialer, func()) {
	t.Helper()

	dir := t.TempDir()
	database, err := db.Open(db.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	var phys atomic.Int64
	phys.Store(time.Now().UnixNano())
	clock := hlc.NewClock("node-1", func() int64 { return phys.Load() })

	ring := hashring.New(150)
	ring.AddNode(hashring.Node{ID: "node-1", Addr: "127.0.0.1:9001"})
	ring.AddNode(hashring.Node{ID: "node-2", Addr: "127.0.0.1:9002"})
	ring.AddNode(hashring.Node{ID: "node-3", Addr: "127.0.0.1:9003"})

	memberCfg := DefaultClusterConfig("node-1", "127.0.0.1:9001")
	membership := NewMembership(memberCfg, nil)
	// Mark all nodes as alive + active in the ring.
	membership.mu.Lock()
	for _, id := range []string{"node-1", "node-2", "node-3"} {
		membership.members[id] = &MemberState{
			NodeID:   id,
			Addr:     "127.0.0.1:900" + id[len(id)-1:],
			Liveness: Alive,
			Ring:     RingActive,
		}
	}
	membership.mu.Unlock()

	dialer := newMockDialer()
	// Default: remote writes/reads succeed.
	dialer.setClient("127.0.0.1:9002", &mockClient{})
	dialer.setClient("127.0.0.1:9003", &mockClient{})

	coord := NewCoordinator(cfg, "node-1", ring, membership, clock, database, dialer, nil)

	cleanup := func() {
		// Give background goroutines (collectAndRepair, readRepair) time
		// to finish before closing the database. Without this, background
		// repairs race against db.Close and hit "WAL file already closed".
		time.Sleep(50 * time.Millisecond)
		database.Close()
	}
	return coord, dialer, cleanup
}

// localKey returns a key whose hash ring placement includes node-1 (the
// coordinator's selfID). ring.GetNodes is deterministic for a given key,
// but varies across keys. Tests that check coord.localDB after a write
// need a key that hashes to include the local node.
func localKey(ring *hashring.Ring) []byte {
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		nodes := ring.GetNodes(key, 3)
		for _, n := range nodes {
			if n.ID == "node-1" {
				return key
			}
		}
	}
	panic("no key hashes to node-1 in a 3-node ring — check ring setup")
}

// --- Tests ---

func TestCoordinator_Write_QuorumMet(t *testing.T) {
	cfg := DefaultCoordinatorConfig() // N=3, W=2
	coord, dialer, cleanup := setupCoordinator(t, cfg)
	defer cleanup()

	// Track which remote replicas received the write.
	var node2Written, node3Written atomic.Bool
	dialer.setClient("127.0.0.1:9002", &mockClient{
		writeFn: func(_ context.Context, req *pb.ReplicateWriteRequest) (*pb.ReplicateWriteResponse, error) {
			node2Written.Store(true)
			return &pb.ReplicateWriteResponse{}, nil
		},
	})
	dialer.setClient("127.0.0.1:9003", &mockClient{
		writeFn: func(_ context.Context, req *pb.ReplicateWriteRequest) (*pb.ReplicateWriteResponse, error) {
			node3Written.Store(true)
			return &pb.ReplicateWriteResponse{}, nil
		},
	})

	key := localKey(coord.ring)
	err := coord.Write(context.Background(), key, []byte("value1"))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Write returned with quorum (W=2). With early exit, remaining
	// goroutines may still be in-flight. Give them time to complete.
	time.Sleep(50 * time.Millisecond)

	// All three replicas should have received the write: local + 2 remotes.
	val, found := coord.localDB.Get(key)
	if !found {
		t.Fatal("key not found locally after write")
	}
	env, err := DecodeEnvelope(val.Data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(env.Value) != "value1" {
		t.Fatalf("got value %q, want %q", env.Value, "value1")
	}
	if !node2Written.Load() || !node3Written.Load() {
		t.Fatal("expected all remotes to receive the write")
	}
}

func TestCoordinator_Write_QuorumNotMet(t *testing.T) {
	cfg := DefaultCoordinatorConfig()
	coord, dialer, cleanup := setupCoordinator(t, cfg)
	defer cleanup()

	// Make both remote replicas fail.
	failClient := &mockClient{
		writeFn: func(context.Context, *pb.ReplicateWriteRequest) (*pb.ReplicateWriteResponse, error) {
			return nil, errors.New("connection refused")
		},
	}
	dialer.setClient("127.0.0.1:9002", failClient)
	dialer.setClient("127.0.0.1:9003", failClient)

	err := coord.Write(context.Background(), []byte("key1"), []byte("value1"))
	if err == nil {
		t.Fatal("expected write quorum error, got nil")
	}
	if !errors.Is(err, ErrWriteQuorumNotMet) {
		t.Fatalf("expected ErrWriteQuorumNotMet, got: %v", err)
	}
}

func TestCoordinator_Write_EmptyKey(t *testing.T) {
	cfg := DefaultCoordinatorConfig()
	coord, _, cleanup := setupCoordinator(t, cfg)
	defer cleanup()

	err := coord.Write(context.Background(), nil, []byte("value"))
	if !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("expected ErrEmptyKey, got: %v", err)
	}
}

func TestCoordinator_Write_DeadNodeSkipped(t *testing.T) {
	cfg := DefaultCoordinatorConfig()
	coord, _, cleanup := setupCoordinator(t, cfg)
	defer cleanup()

	// Mark node-2 as dead.
	coord.membership.mu.Lock()
	coord.membership.members["node-2"].Liveness = Dead
	coord.membership.mu.Unlock()

	// Write should still succeed: self (node-1) + node-3 = 2 acks ≥ W=2.
	err := coord.Write(context.Background(), []byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Write with one dead node: %v", err)
	}
}

func TestCoordinator_Delete(t *testing.T) {
	cfg := DefaultCoordinatorConfig()
	coord, _, cleanup := setupCoordinator(t, cfg)
	defer cleanup()

	key := localKey(coord.ring)
	err := coord.Delete(context.Background(), key)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Early exit may return before the local goroutine completes.
	time.Sleep(50 * time.Millisecond)

	// Verify local tombstone (key is guaranteed to hash to node-1).
	val, found := coord.localDB.Get(key)
	if !found {
		t.Fatal("key not found locally after delete")
	}
	env, err := DecodeEnvelope(val.Data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !env.Deleted {
		t.Fatal("expected tombstone")
	}
}

func TestCoordinator_Read_QuorumMet(t *testing.T) {
	cfg := DefaultCoordinatorConfig() // N=3, R=2
	coord, dialer, cleanup := setupCoordinator(t, cfg)
	defer cleanup()

	// Write locally first so the local read finds it.
	ts := coord.clock.Now()
	encoded, _ := EncodeEnvelope(Envelope{
		Timestamp: ts, Value: []byte("value1"),
	})
	coord.localDB.Put([]byte("key1"), encoded)

	// Remote replicas also have the same value.
	dialer.setClient("127.0.0.1:9002", &mockClient{
		readFn: func(_ context.Context, req *pb.ReplicateReadRequest) (*pb.ReplicateReadResponse, error) {
			return &pb.ReplicateReadResponse{
				Found: true,
				Value: []byte("value1"),
				Timestamp: &pb.HLCTimestamp{
					WallTime: ts.WallTime, Logical: ts.Logical, NodeId: ts.NodeID,
				},
			}, nil
		},
	})
	dialer.setClient("127.0.0.1:9003", &mockClient{
		readFn: func(_ context.Context, req *pb.ReplicateReadRequest) (*pb.ReplicateReadResponse, error) {
			return &pb.ReplicateReadResponse{
				Found: true,
				Value: []byte("value1"),
				Timestamp: &pb.HLCTimestamp{
					WallTime: ts.WallTime, Logical: ts.Logical, NodeId: ts.NodeID,
				},
			}, nil
		},
	})

	res, err := coord.Read(context.Background(), []byte("key1"))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !res.Found {
		t.Fatal("expected found=true")
	}
	if string(res.Value) != "value1" {
		t.Fatalf("got %q, want %q", res.Value, "value1")
	}
}

// TestCoordinator_Read_NewestWins verifies LWW: the coordinator returns
// the newest envelope across all quorum responses.
//
// Scenario (valid W=2, N=3): write at newTS went to node-2 and node-3.
// node-1 (local) has an older value. Any R=2 quorum includes at least
// one of {node-2, node-3}, so the returned value is always "new".
func TestCoordinator_Read_NewestWins(t *testing.T) {
	cfg := DefaultCoordinatorConfig()
	coord, dialer, cleanup := setupCoordinator(t, cfg)
	defer cleanup()

	// node-1 (local): old value (missed the newer write).
	oldTS := hlc.Timestamp{WallTime: 1000, Logical: 1, NodeID: "node-1"}
	oldEnc, _ := EncodeEnvelope(Envelope{Timestamp: oldTS, Value: []byte("old")})
	coord.localDB.Put([]byte("key1"), oldEnc)

	// node-2 and node-3: both have the newer value (received the W=2 write).
	newTS := hlc.Timestamp{WallTime: 2000, Logical: 1, NodeID: "node-2"}
	newResp := func(_ context.Context, _ *pb.ReplicateReadRequest) (*pb.ReplicateReadResponse, error) {
		return &pb.ReplicateReadResponse{
			Found: true, Value: []byte("new"),
			Timestamp: &pb.HLCTimestamp{
				WallTime: newTS.WallTime, Logical: newTS.Logical, NodeId: newTS.NodeID,
			},
		}, nil
	}
	dialer.setClient("127.0.0.1:9002", &mockClient{readFn: newResp})
	dialer.setClient("127.0.0.1:9003", &mockClient{readFn: newResp})

	res, err := coord.Read(context.Background(), []byte("key1"))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(res.Value) != "new" {
		t.Fatalf("got %q, want %q", res.Value, "new")
	}
}

// TestCoordinator_Read_RemoteRepairTriggered tests that a stale replica
// receives the newest value via read repair.
//
// Scenario (valid W=2, N=3): write went to node-1 and node-2.
// node-3 missed the write and has old data.
// Any R=2 quorum includes at least one "new" node (node-1 or node-2).
func TestCoordinator_Read_RemoteRepairTriggered(t *testing.T) {
	cfg := DefaultCoordinatorConfig()
	coord, dialer, cleanup := setupCoordinator(t, cfg)
	defer cleanup()

	newTS := hlc.Timestamp{WallTime: 2000, Logical: 1, NodeID: "node-1"}
	oldTS := hlc.Timestamp{WallTime: 1000, Logical: 1, NodeID: "node-3"}

	// node-1 (local): has the new value.
	newEnc, _ := EncodeEnvelope(Envelope{Timestamp: newTS, Value: []byte("new")})
	coord.localDB.Put([]byte("key1"), newEnc)

	// node-2: also has the new value (both received the W=2 write).
	dialer.setClient("127.0.0.1:9002", &mockClient{
		readFn: func(_ context.Context, _ *pb.ReplicateReadRequest) (*pb.ReplicateReadResponse, error) {
			return &pb.ReplicateReadResponse{
				Found: true, Value: []byte("new"),
				Timestamp: &pb.HLCTimestamp{
					WallTime: newTS.WallTime, Logical: newTS.Logical, NodeId: newTS.NodeID,
				},
			}, nil
		},
	})

	// node-3: has old value, tracks whether it receives a repair write.
	var repairReceived atomic.Bool
	dialer.setClient("127.0.0.1:9003", &mockClient{
		readFn: func(_ context.Context, _ *pb.ReplicateReadRequest) (*pb.ReplicateReadResponse, error) {
			return &pb.ReplicateReadResponse{
				Found: true, Value: []byte("old"),
				Timestamp: &pb.HLCTimestamp{
					WallTime: oldTS.WallTime, Logical: oldTS.Logical, NodeId: oldTS.NodeID,
				},
			}, nil
		},
		writeFn: func(_ context.Context, req *pb.ReplicateWriteRequest) (*pb.ReplicateWriteResponse, error) {
			if string(req.Value) == "new" {
				repairReceived.Store(true)
			}
			return &pb.ReplicateWriteResponse{}, nil
		},
	})

	res, err := coord.Read(context.Background(), []byte("key1"))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(res.Value) != "new" {
		t.Fatalf("got %q, want %q", res.Value, "new")
	}

	// Give async read repair (phase 1 + phase 2) time to complete.
	// phase 2 (collectAndRepair) runs as background goroutine.
	time.Sleep(200 * time.Millisecond)

	if !repairReceived.Load() {
		t.Fatal("expected read repair to node-3, but it was not triggered")
	}
}

// TestCoordinator_Read_LocalRepairTriggered tests that the local node is
// repaired when it holds stale data.
//
// Scenario (valid W=2, N=3): write went to node-2 and node-3.
// Local node (node-1) missed the write.
// Any R=2 quorum includes at least one of {node-2, node-3}.
func TestCoordinator_Read_LocalRepairTriggered(t *testing.T) {
	cfg := DefaultCoordinatorConfig()
	coord, dialer, cleanup := setupCoordinator(t, cfg)
	defer cleanup()

	newTS := hlc.Timestamp{WallTime: 2000, Logical: 1, NodeID: "node-2"}
	oldTS := hlc.Timestamp{WallTime: 1000, Logical: 1, NodeID: "node-1"}

	// node-1 (local): has old value (missed the write).
	oldEnc, _ := EncodeEnvelope(Envelope{Timestamp: oldTS, Value: []byte("old")})
	coord.localDB.Put([]byte("key1"), oldEnc)

	// node-2: has new value.
	dialer.setClient("127.0.0.1:9002", &mockClient{
		readFn: func(_ context.Context, _ *pb.ReplicateReadRequest) (*pb.ReplicateReadResponse, error) {
			return &pb.ReplicateReadResponse{
				Found: true, Value: []byte("new"),
				Timestamp: &pb.HLCTimestamp{
					WallTime: newTS.WallTime, Logical: newTS.Logical, NodeId: newTS.NodeID,
				},
			}, nil
		},
	})

	// node-3: also has new value.
	dialer.setClient("127.0.0.1:9003", &mockClient{
		readFn: func(_ context.Context, _ *pb.ReplicateReadRequest) (*pb.ReplicateReadResponse, error) {
			return &pb.ReplicateReadResponse{
				Found: true, Value: []byte("new"),
				Timestamp: &pb.HLCTimestamp{
					WallTime: newTS.WallTime, Logical: newTS.Logical, NodeId: newTS.NodeID,
				},
			}, nil
		},
	})

	res, err := coord.Read(context.Background(), []byte("key1"))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(res.Value) != "new" {
		t.Fatalf("got %q, want %q", res.Value, "new")
	}

	// Phase 2 (collectAndRepair) runs in background and repairs local stale data.
	time.Sleep(200 * time.Millisecond)

	val, found := coord.localDB.Get([]byte("key1"))
	if !found {
		t.Fatal("local key missing after repair")
	}
	env, _ := DecodeEnvelope(val.Data)
	if string(env.Value) != "new" {
		t.Fatalf("local repair: got %q, want %q", env.Value, "new")
	}
}

func TestCoordinator_Read_NotFound(t *testing.T) {
	cfg := DefaultCoordinatorConfig()
	coord, _, cleanup := setupCoordinator(t, cfg)
	defer cleanup()

	// All replicas return found=false (default mock behavior).
	res, err := coord.Read(context.Background(), []byte("missing"))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if res.Found {
		t.Fatal("expected found=false for missing key")
	}
}

func TestCoordinator_Read_JoiningExcluded(t *testing.T) {
	cfg := CoordinatorConfig{
		ReplicationFactor: 3,
		WriteQuorum:       2,
		ReadQuorum:        2,
		PerReplicaTimeout: 5 * time.Second,
	}
	coord, dialer, cleanup := setupCoordinator(t, cfg)
	defer cleanup()

	// Mark node-3 as JOINING.
	coord.membership.mu.Lock()
	coord.membership.members["node-3"].Ring = RingJoining
	coord.membership.mu.Unlock()

	// Write should still include JOINING node (write-only).
	key := localKey(coord.ring)
	err := coord.Write(context.Background(), key, []byte("value1"))
	if err != nil {
		t.Fatalf("Write with JOINING node: %v", err)
	}

	// For reads: only node-1 and node-2 are readable (node-3 is JOINING).
	// Both readable nodes have the same value — no repair needed.
	ts := coord.clock.Now()
	sameResp := func(_ context.Context, _ *pb.ReplicateReadRequest) (*pb.ReplicateReadResponse, error) {
		return &pb.ReplicateReadResponse{
			Found: true, Value: []byte("value1"),
			Timestamp: &pb.HLCTimestamp{
				WallTime: ts.WallTime, Logical: ts.Logical, NodeId: ts.NodeID,
			},
		}, nil
	}
	dialer.setClient("127.0.0.1:9002", &mockClient{readFn: sameResp})

	res, err := coord.Read(context.Background(), key)
	if err != nil {
		t.Fatalf("Read with JOINING excluded: %v", err)
	}
	if !res.Found {
		t.Fatal("expected found=true")
	}
}

func TestCoordinator_Read_QuorumNotMet(t *testing.T) {
	cfg := DefaultCoordinatorConfig()
	coord, dialer, cleanup := setupCoordinator(t, cfg)
	defer cleanup()

	// Make both remote replicas fail reads.
	failClient := &mockClient{
		readFn: func(context.Context, *pb.ReplicateReadRequest) (*pb.ReplicateReadResponse, error) {
			return nil, errors.New("connection refused")
		},
	}
	dialer.setClient("127.0.0.1:9002", failClient)
	dialer.setClient("127.0.0.1:9003", failClient)

	// Only local node responds → 1 response < R=2.
	_, err := coord.Read(context.Background(), []byte("key1"))
	if err == nil {
		t.Fatal("expected read quorum error")
	}
	if !errors.Is(err, ErrReadQuorumNotMet) {
		t.Fatalf("expected ErrReadQuorumNotMet, got: %v", err)
	}
}

func TestCoordinator_Read_EmptyKey(t *testing.T) {
	cfg := DefaultCoordinatorConfig()
	coord, _, cleanup := setupCoordinator(t, cfg)
	defer cleanup()

	_, err := coord.Read(context.Background(), nil)
	if !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("expected ErrEmptyKey, got: %v", err)
	}
}

func TestCoordinator_Write_NotEnoughReplicas(t *testing.T) {
	dir := t.TempDir()
	database, err := db.Open(db.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer database.Close()

	var phys atomic.Int64
	phys.Store(time.Now().UnixNano())
	clock := hlc.NewClock("node-1", func() int64 { return phys.Load() })

	// Ring with only 1 node, but W=2.
	ring := hashring.New(150)
	ring.AddNode(hashring.Node{ID: "node-1", Addr: "127.0.0.1:9001"})

	memberCfg := DefaultClusterConfig("node-1", "127.0.0.1:9001")
	membership := NewMembership(memberCfg, nil)
	membership.mu.Lock()
	membership.members["node-1"] = &MemberState{
		NodeID: "node-1", Addr: "127.0.0.1:9001",
		Liveness: Alive, Ring: RingActive,
	}
	membership.mu.Unlock()

	cfg := CoordinatorConfig{
		ReplicationFactor: 3,
		WriteQuorum:       2,
		ReadQuorum:        2,
		PerReplicaTimeout: 5 * time.Second,
	}
	coord := NewCoordinator(cfg, "node-1", ring, membership, clock, database, newMockDialer(), nil)

	err = coord.Write(context.Background(), []byte("key1"), []byte("value1"))
	if !errors.Is(err, ErrNotEnoughReplicas) {
		t.Fatalf("expected ErrNotEnoughReplicas, got: %v", err)
	}
}

func TestCoordinator_Read_DeletedKeyReturnsFound(t *testing.T) {
	cfg := DefaultCoordinatorConfig()
	coord, dialer, cleanup := setupCoordinator(t, cfg)
	defer cleanup()

	// Write a tombstone locally.
	ts := coord.clock.Now()
	encoded, _ := EncodeEnvelope(Envelope{
		Timestamp: ts, Deleted: true,
	})
	coord.localDB.Put([]byte("key1"), encoded)

	// Remote also returns tombstone.
	dialer.setClient("127.0.0.1:9002", &mockClient{
		readFn: func(_ context.Context, _ *pb.ReplicateReadRequest) (*pb.ReplicateReadResponse, error) {
			return &pb.ReplicateReadResponse{
				Found:   true,
				Deleted: true,
				Timestamp: &pb.HLCTimestamp{
					WallTime: ts.WallTime, Logical: ts.Logical, NodeId: ts.NodeID,
				},
			}, nil
		},
	})

	res, err := coord.Read(context.Background(), []byte("key1"))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	// Coordinator returns Found=true, Deleted=true. The server handler
	// decides whether to expose tombstones to clients.
	if !res.Found {
		t.Fatal("expected found=true for tombstone")
	}
	if !res.Deleted {
		t.Fatal("expected deleted=true for tombstone")
	}
}
