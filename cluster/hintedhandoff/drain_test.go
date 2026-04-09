package hintedhandoff_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ulixert/theseon/cluster"
	"github.com/ulixert/theseon/cluster/hintedhandoff"
	"github.com/ulixert/theseon/hlc"
	pb "github.com/ulixert/theseon/proto/theseonpb"
	"google.golang.org/grpc"
)

// --- mock membership ---

type mockMember struct {
	info     hintedhandoff.MemberInfo
	liveness cluster.LivenessState
}

type mockMembership struct {
	mu      sync.RWMutex
	members map[string]mockMember
}

func newMockMembership() *mockMembership {
	return &mockMembership{members: make(map[string]mockMember)}
}

func (m *mockMembership) addMember(nodeID, addr string, liveness cluster.LivenessState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.members[nodeID] = mockMember{
		info:     hintedhandoff.MemberInfo{NodeID: nodeID, Addr: addr},
		liveness: liveness,
	}
}

func (m *mockMembership) IsAlive(nodeID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ms, ok := m.members[nodeID]
	return ok && ms.liveness == cluster.Alive
}

func (m *mockMembership) GetMemberInfos() []hintedhandoff.MemberInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]hintedhandoff.MemberInfo, 0, len(m.members))
	for _, ms := range m.members {
		result = append(result, ms.info)
	}
	return result
}

// --- mock dialer ---

type mockDialer struct {
	client *mockInternalClient
}

func newMockDialer() *mockDialer {
	return &mockDialer{client: &mockInternalClient{}}
}

func (d *mockDialer) GetClient(_ string) (pb.InternalServiceClient, error) {
	return d.client, nil
}

func (d *mockDialer) Close() {}

// --- mock gRPC client ---

type mockInternalClient struct {
	mu        sync.Mutex
	batches   []*pb.ReplicateWriteBatchRequest
	batchErr  error
	callCount atomic.Int64
	singleErr error
}

func (c *mockInternalClient) ReplicateWriteBatch(
	_ context.Context,
	req *pb.ReplicateWriteBatchRequest,
	_ ...grpc.CallOption,
) (*pb.ReplicateWriteBatchResponse, error) {
	c.callCount.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.batchErr != nil {
		return nil, c.batchErr
	}
	c.batches = append(c.batches, req)
	return &pb.ReplicateWriteBatchResponse{}, nil
}

func (c *mockInternalClient) ReplicateWrite(
	_ context.Context,
	_ *pb.ReplicateWriteRequest,
	_ ...grpc.CallOption,
) (*pb.ReplicateWriteResponse, error) {
	return nil, c.singleErr
}

func (c *mockInternalClient) ReplicateRead(
	_ context.Context,
	_ *pb.ReplicateReadRequest,
	_ ...grpc.CallOption,
) (*pb.ReplicateReadResponse, error) {
	return nil, nil
}

func (c *mockInternalClient) Ping(
	_ context.Context, _ *pb.PingRequest, _ ...grpc.CallOption,
) (*pb.PingResponse, error) {
	return nil, nil
}

func (c *mockInternalClient) PingReq(
	_ context.Context, _ *pb.PingReqRequest, _ ...grpc.CallOption,
) (*pb.PingReqResponse, error) {
	return nil, nil
}

func (c *mockInternalClient) GossipSync(
	_ context.Context, _ *pb.GossipSyncRequest, _ ...grpc.CallOption,
) (*pb.GossipSyncResponse, error) {
	return nil, nil
}

func (c *mockInternalClient) getBatches() []*pb.ReplicateWriteBatchRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.batches
}

// --- helpers ---

func testStoreConfig(dir string) hintedhandoff.StoreConfig {
	return hintedhandoff.StoreConfig{
		Dir:      dir,
		MaxBytes: 1024 * 1024,
		HintTTL:  time.Hour,
	}
}

func iterCount(t *testing.T, iter interface {
	IsValid() bool
	Next()
	Close() error
}) int {
	t.Helper()
	defer iter.Close()
	n := 0
	for iter.IsValid() {
		n++
		iter.Next()
	}
	return n
}

func makeEnvelope(t *testing.T, ts hlc.Timestamp, value []byte, deleted bool) []byte {
	t.Helper()
	env := cluster.Envelope{
		Timestamp: ts,
		Value:     value,
		Deleted:   deleted,
	}
	encoded, err := cluster.EncodeEnvelope(env)
	if err != nil {
		t.Fatalf("EncodeEnvelope: %v", err)
	}
	return encoded
}

func testDecodeEnvelope(b []byte) (hintedhandoff.DecodedEnvelope, error) {
	env, err := cluster.DecodeEnvelope(b)
	if err != nil {
		return hintedhandoff.DecodedEnvelope{}, err
	}
	return hintedhandoff.DecodedEnvelope{
		Timestamp: env.Timestamp,
		Deleted:   env.Deleted,
		Value:     env.Value,
	}, nil
}

// --- tests ---

func TestDrainer_ReplayOnTrigger(t *testing.T) {
	dir := t.TempDir()
	store, err := hintedhandoff.NewStore(testStoreConfig(dir))
	if err != nil {
		t.Fatalf("hintedhandoff.NewStore: %v", err)
	}
	defer store.Close()

	membership := newMockMembership()
	membership.addMember("node-1", "127.0.0.1:9001", cluster.Alive)

	dialer := newMockDialer()

	ts := hlc.Timestamp{WallTime: time.Now().UnixNano(), Logical: 1, NodeID: "coord"}
	env := makeEnvelope(t, ts, []byte("hello"), false)
	store.Add("node-1", []byte("key-a"), env, ts)

	drainer := hintedhandoff.NewDrainer(hintedhandoff.DrainerConfig{
		Store:          store,
		Dialer:         dialer,
		Membership:     membership,
		DecodeEnvelope: testDecodeEnvelope,
	})

	drainer.TriggerDrain("node-1")
	// Wait for drain to complete
	time.Sleep(200 * time.Millisecond)
	drainer.Stop()

	batches := dialer.client.getBatches()
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
	if len(batches[0].Entries) != 1 {
		t.Fatalf("expected 1 entry in batch, got %d", len(batches[0].Entries))
	}

	entry := batches[0].Entries[0]
	if string(entry.Key) != "key-a" {
		t.Errorf("key = %q, want %q", entry.Key, "key-a")
	}
	if string(entry.Value) != "hello" {
		t.Errorf("value = %q, want %q", entry.Value, "hello")
	}
	if entry.Timestamp.WallTime != ts.WallTime {
		t.Errorf("timestamp = %d, want %d", entry.Timestamp.WallTime, ts.WallTime)
	}

	// Hint should be removed from store
	count := iterCount(t, store.Iterate("node-1"))
	if count != 0 {
		t.Errorf("expected 0 remaining hints, got %d", count)
	}
}

func TestDrainer_StopOnConsecutiveFailures(t *testing.T) {
	dir := t.TempDir()
	store, err := hintedhandoff.NewStore(testStoreConfig(dir))
	if err != nil {
		t.Fatalf("hintedhandoff.NewStore: %v", err)
	}
	defer store.Close()

	membership := newMockMembership()
	membership.addMember("node-1", "127.0.0.1:9001", cluster.Alive)

	dialer := newMockDialer()
	dialer.client.batchErr = errors.New("connection refused")

	ts := hlc.Timestamp{WallTime: time.Now().UnixNano(), Logical: 1, NodeID: "coord"}
	env := makeEnvelope(t, ts, []byte("data"), false)
	store.Add("node-1", []byte("key-a"), env, ts)

	drainer := hintedhandoff.NewDrainer(hintedhandoff.DrainerConfig{
		Store:          store,
		Dialer:         dialer,
		Membership:     membership,
		DecodeEnvelope: testDecodeEnvelope,
		MaxRetries:     2,
		RetryDelay:     10 * time.Millisecond,
	})

	drainer.TriggerDrain("node-1")
	time.Sleep(200 * time.Millisecond)
	drainer.Stop()

	// 1 initial + 2 retries = 3 total calls
	calls := dialer.client.callCount.Load()
	if calls != 3 {
		t.Errorf("expected 3 RPC calls (1 + 2 retries), got %d", calls)
	}

	// Hint should still exist (not deleted on failure)
	count := iterCount(t, store.Iterate("node-1"))
	if count != 1 {
		t.Errorf("expected 1 remaining hint, got %d", count)
	}
}

func TestDrainer_TTLSkip(t *testing.T) {
	dir := t.TempDir()
	cfg := testStoreConfig(dir)
	cfg.HintTTL = time.Millisecond // very short TTL

	store, err := hintedhandoff.NewStore(cfg)
	if err != nil {
		t.Fatalf("hintedhandoff.NewStore: %v", err)
	}
	defer store.Close()

	membership := newMockMembership()
	membership.addMember("node-1", "127.0.0.1:9001", cluster.Alive)

	dialer := newMockDialer()

	// Use old timestamp so hint is already expired
	ts := hlc.Timestamp{WallTime: time.Now().Add(-time.Hour).UnixNano(), Logical: 1, NodeID: "coord"}
	env := makeEnvelope(t, ts, []byte("old-data"), false)
	store.Add("node-1", []byte("key-a"), env, ts)

	time.Sleep(5 * time.Millisecond) // ensure TTL expired

	drainer := hintedhandoff.NewDrainer(hintedhandoff.DrainerConfig{
		Store:          store,
		Dialer:         dialer,
		Membership:     membership,
		DecodeEnvelope: testDecodeEnvelope,
	})

	drainer.TriggerDrain("node-1")
	time.Sleep(200 * time.Millisecond)
	drainer.Stop()

	// Expired hint should be deleted, NOT replayed
	batches := dialer.client.getBatches()
	if len(batches) != 0 {
		t.Errorf("expected 0 batches (hint was expired), got %d", len(batches))
	}

	count := iterCount(t, store.Iterate("node-1"))
	if count != 0 {
		t.Errorf("expected 0 remaining hints (expired should be purged), got %d", count)
	}
}

func TestDrainer_ConcurrentDrainDedup(t *testing.T) {
	dir := t.TempDir()
	store, err := hintedhandoff.NewStore(testStoreConfig(dir))
	if err != nil {
		t.Fatalf("hintedhandoff.NewStore: %v", err)
	}
	defer store.Close()

	membership := newMockMembership()
	membership.addMember("node-1", "127.0.0.1:9001", cluster.Alive)

	dialer := newMockDialer()

	ts := hlc.Timestamp{WallTime: time.Now().UnixNano(), Logical: 1, NodeID: "coord"}
	env := makeEnvelope(t, ts, []byte("data"), false)
	store.Add("node-1", []byte("key-a"), env, ts)

	drainer := hintedhandoff.NewDrainer(hintedhandoff.DrainerConfig{
		Store:          store,
		Dialer:         dialer,
		Membership:     membership,
		DecodeEnvelope: testDecodeEnvelope,
	})

	// Trigger twice rapidly — second should be skipped
	drainer.TriggerDrain("node-1")
	drainer.TriggerDrain("node-1")
	time.Sleep(200 * time.Millisecond)
	drainer.Stop()

	// Should only have 1 batch total (not duplicated)
	batches := dialer.client.getBatches()
	if len(batches) != 1 {
		t.Errorf("expected 1 batch (dedup), got %d", len(batches))
	}
}

func TestDrainer_SweepSkipsDeadTargets(t *testing.T) {
	dir := t.TempDir()
	store, err := hintedhandoff.NewStore(testStoreConfig(dir))
	if err != nil {
		t.Fatalf("hintedhandoff.NewStore: %v", err)
	}
	defer store.Close()

	membership := newMockMembership()
	membership.addMember("node-1", "127.0.0.1:9001", cluster.Dead)

	dialer := newMockDialer()

	ts := hlc.Timestamp{WallTime: time.Now().UnixNano(), Logical: 1, NodeID: "coord"}
	env := makeEnvelope(t, ts, []byte("data"), false)
	store.Add("node-1", []byte("key-a"), env, ts)

	drainer := hintedhandoff.NewDrainer(hintedhandoff.DrainerConfig{
		Store:          store,
		Dialer:         dialer,
		Membership:     membership,
		DecodeEnvelope: testDecodeEnvelope,
		SweepInterval:  50 * time.Millisecond,
	})

	drainer.Start()
	time.Sleep(200 * time.Millisecond)
	drainer.Stop()

	// Dead target should NOT have been drained
	batches := dialer.client.getBatches()
	if len(batches) != 0 {
		t.Errorf("expected 0 batches (target is Dead), got %d", len(batches))
	}
}

func TestDrainer_ByteBasedBatching(t *testing.T) {
	dir := t.TempDir()
	store, err := hintedhandoff.NewStore(testStoreConfig(dir))
	if err != nil {
		t.Fatalf("hintedhandoff.NewStore: %v", err)
	}
	defer store.Close()

	membership := newMockMembership()
	membership.addMember("node-1", "127.0.0.1:9001", cluster.Alive)

	dialer := newMockDialer()

	// Add 5 hints, each ~200 bytes of envelope data
	for i := 0; i < 5; i++ {
		ts := hlc.Timestamp{WallTime: time.Now().UnixNano() + int64(i), Logical: uint32(i), NodeID: "coord"}
		value := make([]byte, 200)
		env := makeEnvelope(t, ts, value, false)
		store.Add("node-1", []byte{byte(i)}, env, ts)
	}

	drainer := hintedhandoff.NewDrainer(hintedhandoff.DrainerConfig{
		Store:          store,
		Dialer:         dialer,
		Membership:     membership,
		DecodeEnvelope: testDecodeEnvelope,
		MaxBatchBytes:  500, // Should produce multiple batches (~200 bytes per hint)
		MaxBatchItems:  1000,
	})

	drainer.TriggerDrain("node-1")
	time.Sleep(300 * time.Millisecond)
	drainer.Stop()

	batches := dialer.client.getBatches()
	if len(batches) < 2 {
		t.Errorf("expected multiple batches with 500-byte limit, got %d", len(batches))
	}

	// All hints should be drained
	totalEntries := 0
	for _, b := range batches {
		totalEntries += len(b.Entries)
	}
	if totalEntries != 5 {
		t.Errorf("total entries = %d, want 5", totalEntries)
	}
}
