package server_test

import (
	"bytes"
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ulixert/theseon/cluster"
	"github.com/ulixert/theseon/db"
	"github.com/ulixert/theseon/hlc"
	pb "github.com/ulixert/theseon/proto/theseonpb"
	"github.com/ulixert/theseon/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// setupInternal creates an InternalService gRPC client backed by a real
// db.DB and HLC clock. The clock uses a controllable physical time source.
func setupInternal(t *testing.T, physicalNanos *atomic.Int64) (pb.InternalServiceClient, func()) {
	t.Helper()

	dir := t.TempDir()
	database, err := db.Open(db.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	clock := hlc.NewClock("test-node", func() int64 {
		return physicalNanos.Load()
	})

	cfg := cluster.DefaultClusterConfig("test-node", "127.0.0.1:0")
	membership := cluster.NewMembership(cfg, nil)

	lis := bufconn.Listen(bufSize)
	srv := server.New(database, nil,
		server.WithMembership(membership),
		server.WithReplication(clock, database),
	)
	go srv.Serve(lis)

	conn, err := grpc.NewClient(
		"passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	client := pb.NewInternalServiceClient(conn)
	cleanup := func() {
		conn.Close()
		srv.GracefulStop()
		database.Close()
	}
	return client, cleanup
}

func TestReplicateWriteAndRead(t *testing.T) {
	var phys atomic.Int64
	phys.Store(time.Now().UnixNano())
	client, cleanup := setupInternal(t, &phys)
	defer cleanup()
	ctx := context.Background()

	ts := &pb.HLCTimestamp{WallTime: phys.Load(), Logical: 1, NodeId: "coordinator"}

	_, err := client.ReplicateWrite(ctx, &pb.ReplicateWriteRequest{
		Key:       []byte("key1"),
		Value:     []byte("value1"),
		Timestamp: ts,
	})
	if err != nil {
		t.Fatalf("ReplicateWrite: %v", err)
	}

	resp, err := client.ReplicateRead(ctx, &pb.ReplicateReadRequest{
		Key: []byte("key1"),
	})
	if err != nil {
		t.Fatalf("ReplicateRead: %v", err)
	}
	if !resp.Found {
		t.Fatal("expected found=true")
	}
	if resp.Deleted {
		t.Fatal("expected deleted=false")
	}
	if !bytes.Equal(resp.Value, []byte("value1")) {
		t.Fatalf("value mismatch: got %q, want %q", resp.Value, "value1")
	}
	if resp.Timestamp.WallTime != ts.WallTime || resp.Timestamp.Logical != ts.Logical {
		t.Fatalf("timestamp mismatch: got %+v, want wall=%d logical=%d",
			resp.Timestamp, ts.WallTime, ts.Logical)
	}
}

func TestReplicateWriteTombstone(t *testing.T) {
	var phys atomic.Int64
	phys.Store(time.Now().UnixNano())
	client, cleanup := setupInternal(t, &phys)
	defer cleanup()
	ctx := context.Background()

	ts := &pb.HLCTimestamp{WallTime: phys.Load(), Logical: 0, NodeId: "coord"}

	_, err := client.ReplicateWrite(ctx, &pb.ReplicateWriteRequest{
		Key:       []byte("del-key"),
		Timestamp: ts,
		Deleted:   true,
	})
	if err != nil {
		t.Fatalf("ReplicateWrite tombstone: %v", err)
	}

	resp, err := client.ReplicateRead(ctx, &pb.ReplicateReadRequest{
		Key: []byte("del-key"),
	})
	if err != nil {
		t.Fatalf("ReplicateRead: %v", err)
	}
	if !resp.Found {
		t.Fatal("expected found=true for tombstone envelope")
	}
	if !resp.Deleted {
		t.Fatal("expected deleted=true")
	}
}

func TestReplicateWriteBatch(t *testing.T) {
	var phys atomic.Int64
	phys.Store(time.Now().UnixNano())
	client, cleanup := setupInternal(t, &phys)
	defer cleanup()
	ctx := context.Background()

	wall := phys.Load()
	_, err := client.ReplicateWriteBatch(ctx, &pb.ReplicateWriteBatchRequest{
		Entries: []*pb.ReplicateWriteRequest{
			{Key: []byte("a"), Value: []byte("1"), Timestamp: &pb.HLCTimestamp{WallTime: wall, Logical: 0, NodeId: "c"}},
			{Key: []byte("b"), Value: []byte("2"), Timestamp: &pb.HLCTimestamp{WallTime: wall, Logical: 1, NodeId: "c"}},
			{Key: []byte("c"), Deleted: true, Timestamp: &pb.HLCTimestamp{WallTime: wall, Logical: 2, NodeId: "c"}},
		},
	})
	if err != nil {
		t.Fatalf("ReplicateWriteBatch: %v", err)
	}

	for _, tc := range []struct {
		key     string
		wantVal string
		wantDel bool
	}{
		{"a", "1", false},
		{"b", "2", false},
		{"c", "", true},
	} {
		resp, err := client.ReplicateRead(ctx, &pb.ReplicateReadRequest{Key: []byte(tc.key)})
		if err != nil {
			t.Fatalf("ReplicateRead %s: %v", tc.key, err)
		}
		if !resp.Found {
			t.Fatalf("ReplicateRead %s: expected found", tc.key)
		}
		if resp.Deleted != tc.wantDel {
			t.Fatalf("ReplicateRead %s: deleted=%v, want %v", tc.key, resp.Deleted, tc.wantDel)
		}
		if !tc.wantDel && string(resp.Value) != tc.wantVal {
			t.Fatalf("ReplicateRead %s: got %q, want %q", tc.key, resp.Value, tc.wantVal)
		}
	}
}

func TestReplicateReadNotFound(t *testing.T) {
	var phys atomic.Int64
	phys.Store(time.Now().UnixNano())
	client, cleanup := setupInternal(t, &phys)
	defer cleanup()
	ctx := context.Background()

	resp, err := client.ReplicateRead(ctx, &pb.ReplicateReadRequest{Key: []byte("nope")})
	if err != nil {
		t.Fatalf("ReplicateRead: %v", err)
	}
	if resp.Found {
		t.Fatal("expected found=false for missing key")
	}
}

func TestReplicateWriteEmptyKeyRejected(t *testing.T) {
	var phys atomic.Int64
	phys.Store(time.Now().UnixNano())
	client, cleanup := setupInternal(t, &phys)
	defer cleanup()
	ctx := context.Background()

	_, err := client.ReplicateWrite(ctx, &pb.ReplicateWriteRequest{
		Key:       nil,
		Value:     []byte("v"),
		Timestamp: &pb.HLCTimestamp{WallTime: phys.Load(), NodeId: "c"},
	})
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Fatalf("ReplicateWrite empty key: got %v, want InvalidArgument", err)
	}
}

func TestReplicateReadEmptyKeyRejected(t *testing.T) {
	var phys atomic.Int64
	phys.Store(time.Now().UnixNano())
	client, cleanup := setupInternal(t, &phys)
	defer cleanup()
	ctx := context.Background()

	_, err := client.ReplicateRead(ctx, &pb.ReplicateReadRequest{Key: nil})
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Fatalf("ReplicateRead empty key: got %v, want InvalidArgument", err)
	}
}

func TestReplicateWriteNilTimestampRejected(t *testing.T) {
	var phys atomic.Int64
	phys.Store(time.Now().UnixNano())
	client, cleanup := setupInternal(t, &phys)
	defer cleanup()
	ctx := context.Background()

	_, err := client.ReplicateWrite(ctx, &pb.ReplicateWriteRequest{
		Key:   []byte("k"),
		Value: []byte("v"),
	})
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Fatalf("ReplicateWrite nil timestamp: got %v, want InvalidArgument", err)
	}
}

func TestReplicateWriteClockDrift(t *testing.T) {
	var phys atomic.Int64
	phys.Store(time.Now().UnixNano())
	client, cleanup := setupInternal(t, &phys)
	defer cleanup()
	ctx := context.Background()

	// Send a timestamp 2 minutes in the future — exceeds DefaultMaxDrift (1 minute).
	futureWall := phys.Load() + int64(2*time.Minute)

	_, err := client.ReplicateWrite(ctx, &pb.ReplicateWriteRequest{
		Key:       []byte("k"),
		Value:     []byte("v"),
		Timestamp: &pb.HLCTimestamp{WallTime: futureWall, Logical: 0, NodeId: "drifted"},
	})
	if err == nil {
		t.Fatal("expected error for clock drift")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.Internal {
		t.Fatalf("expected Internal error for clock drift, got %v", err)
	}
}

func TestReplicateWriteHLCSynchronization(t *testing.T) {
	var phys atomic.Int64
	now := time.Now().UnixNano()
	phys.Store(now)
	client, cleanup := setupInternal(t, &phys)
	defer cleanup()
	ctx := context.Background()

	// Send a write with a timestamp slightly ahead of local physical time.
	// The local clock should advance to at least this timestamp after Update().
	aheadWall := now + int64(10*time.Second)

	_, err := client.ReplicateWrite(ctx, &pb.ReplicateWriteRequest{
		Key:       []byte("sync-key"),
		Value:     []byte("sync-val"),
		Timestamp: &pb.HLCTimestamp{WallTime: aheadWall, Logical: 5, NodeId: "remote"},
	})
	if err != nil {
		t.Fatalf("ReplicateWrite: %v", err)
	}

	// Read the stored envelope back and verify the timestamp was preserved.
	resp, err := client.ReplicateRead(ctx, &pb.ReplicateReadRequest{Key: []byte("sync-key")})
	if err != nil {
		t.Fatalf("ReplicateRead: %v", err)
	}
	if resp.Timestamp.WallTime != aheadWall || resp.Timestamp.Logical != 5 {
		t.Fatalf("envelope timestamp not preserved: got wall=%d logical=%d, want wall=%d logical=5",
			resp.Timestamp.WallTime, resp.Timestamp.Logical, aheadWall)
	}
}
