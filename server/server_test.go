package server_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"sort"
	"testing"

	"github.com/ulixert/lithicdb/db"
	pb "github.com/ulixert/lithicdb/proto/lithicpb"
	"github.com/ulixert/lithicdb/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

func setup(t *testing.T) (pb.LithicDBClient, func()) {
	t.Helper()

	dir := t.TempDir()
	database, err := db.Open(db.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	lis := bufconn.Listen(bufSize)
	srv := server.New(database, nil)
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

	client := pb.NewLithicDBClient(conn)
	cleanup := func() {
		conn.Close()
		srv.GracefulStop()
		database.Close()
	}
	return client, cleanup
}

func TestPutGet(t *testing.T) {
	client, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()

	_, err := client.Put(ctx, &pb.PutRequest{Key: []byte("hello"), Value: []byte("world")})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	resp, err := client.Get(ctx, &pb.GetRequest{Key: []byte("hello")})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !resp.Found {
		t.Fatal("Get: expected found=true")
	}
	if !bytes.Equal(resp.Value, []byte("world")) {
		t.Fatalf("Get: got %q, want %q", resp.Value, "world")
	}
}

func TestGetNotFound(t *testing.T) {
	client, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()

	resp, err := client.Get(ctx, &pb.GetRequest{Key: []byte("missing")})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if resp.Found {
		t.Fatal("Get: expected found=false for missing key")
	}
}

func TestDelete(t *testing.T) {
	client, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()

	_, err := client.Put(ctx, &pb.PutRequest{Key: []byte("key"), Value: []byte("val")})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	_, err = client.Delete(ctx, &pb.DeleteRequest{Key: []byte("key")})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}

	resp, err := client.Get(ctx, &pb.GetRequest{Key: []byte("key")})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if resp.Found {
		t.Fatal("Get after Delete: expected found=false")
	}
}

func TestDeleteNonExistent(t *testing.T) {
	client, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()

	_, err := client.Delete(ctx, &pb.DeleteRequest{Key: []byte("never-existed")})
	if err != nil {
		t.Fatalf("Delete nonexistent key: %v", err)
	}
}

func TestScan(t *testing.T) {
	client, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()

	keys := []string{"banana", "apple", "cherry", "date"}
	for _, k := range keys {
		_, err := client.Put(ctx, &pb.PutRequest{Key: []byte(k), Value: []byte("v-" + k)})
		if err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}

	stream, err := client.Scan(ctx, &pb.ScanRequest{})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	var gotKeys []string
	var gotValues []string
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Scan Recv: %v", err)
		}
		gotKeys = append(gotKeys, string(resp.Key))
		gotValues = append(gotValues, string(resp.Value))
	}

	// Keys should be in sorted order (LSM tree sorted iteration).
	sort.Strings(keys)
	if len(gotKeys) != len(keys) {
		t.Fatalf("Scan: got %d keys, want %d", len(gotKeys), len(keys))
	}
	for i := range keys {
		if gotKeys[i] != keys[i] {
			t.Errorf("Scan key[%d]: got %q, want %q", i, gotKeys[i], keys[i])
		}
		want := "v-" + keys[i]
		if gotValues[i] != want {
			t.Errorf("Scan value[%d]: got %q, want %q", i, gotValues[i], want)
		}
	}
}

func TestScanEmpty(t *testing.T) {
	client, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()

	stream, err := client.Scan(ctx, &pb.ScanRequest{})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	_, err = stream.Recv()
	if err != io.EOF {
		t.Fatalf("Scan on empty DB: expected EOF, got %v", err)
	}
}

func TestScanSkipsTombstones(t *testing.T) {
	client, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()

	_, err := client.Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("1")})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	_, err = client.Put(ctx, &pb.PutRequest{Key: []byte("b"), Value: []byte("2")})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	_, err = client.Delete(ctx, &pb.DeleteRequest{Key: []byte("a")})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}

	stream, err := client.Scan(ctx, &pb.ScanRequest{})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	var gotKeys []string
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Scan Recv: %v", err)
		}
		gotKeys = append(gotKeys, string(resp.Key))
	}

	if len(gotKeys) != 1 || gotKeys[0] != "b" {
		t.Fatalf("Scan after delete: got keys %v, want [b]", gotKeys)
	}
}

func TestBatchWrite(t *testing.T) {
	client, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate a key that will be deleted by the batch.
	_, err := client.Put(ctx, &pb.PutRequest{Key: []byte("del-me"), Value: []byte("old")})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	_, err = client.BatchWrite(ctx, &pb.BatchWriteRequest{
		Entries: []*pb.BatchEntry{
			{Key: []byte("key1"), Value: []byte("val1")},
			{Key: []byte("key2"), Value: []byte("val2")},
			{Key: []byte("del-me"), IsDelete: true},
		},
	})
	if err != nil {
		t.Fatalf("BatchWrite: %v", err)
	}

	// Verify puts.
	for _, tc := range []struct {
		key, want string
	}{
		{"key1", "val1"},
		{"key2", "val2"},
	} {
		resp, err := client.Get(ctx, &pb.GetRequest{Key: []byte(tc.key)})
		if err != nil {
			t.Fatalf("Get %s: %v", tc.key, err)
		}
		if !resp.Found {
			t.Fatalf("Get %s: expected found", tc.key)
		}
		if !bytes.Equal(resp.Value, []byte(tc.want)) {
			t.Fatalf("Get %s: got %q, want %q", tc.key, resp.Value, tc.want)
		}
	}

	// Verify delete.
	resp, err := client.Get(ctx, &pb.GetRequest{Key: []byte("del-me")})
	if err != nil {
		t.Fatalf("Get del-me: %v", err)
	}
	if resp.Found {
		t.Fatal("Get del-me: expected found=false after batch delete")
	}
}

func TestBatchWriteEmpty(t *testing.T) {
	client, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()

	_, err := client.BatchWrite(ctx, &pb.BatchWriteRequest{})
	if err != nil {
		t.Fatalf("BatchWrite empty: %v", err)
	}
}

func TestPutGetLargeValue(t *testing.T) {
	client, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()

	largeVal := make([]byte, 64*1024) // 64KB
	for i := range largeVal {
		largeVal[i] = byte(i % 251) // non-trivial pattern
	}

	_, err := client.Put(ctx, &pb.PutRequest{Key: []byte("big"), Value: largeVal})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	resp, err := client.Get(ctx, &pb.GetRequest{Key: []byte("big")})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !resp.Found {
		t.Fatal("Get: expected found=true")
	}
	if !bytes.Equal(resp.Value, largeVal) {
		t.Fatal("Get: large value mismatch")
	}
}

func TestEmptyKeyRejected(t *testing.T) {
	client, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()

	// Put with empty key.
	_, err := client.Put(ctx, &pb.PutRequest{Key: nil, Value: []byte("val")})
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Fatalf("Put empty key: got %v, want InvalidArgument", err)
	}

	// Get with empty key.
	_, err = client.Get(ctx, &pb.GetRequest{Key: nil})
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Fatalf("Get empty key: got %v, want InvalidArgument", err)
	}

	// Delete with empty key.
	_, err = client.Delete(ctx, &pb.DeleteRequest{Key: nil})
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Fatalf("Delete empty key: got %v, want InvalidArgument", err)
	}

	// BatchWrite with empty key in one entry.
	_, err = client.BatchWrite(ctx, &pb.BatchWriteRequest{
		Entries: []*pb.BatchEntry{
			{Key: []byte("good"), Value: []byte("ok")},
			{Key: nil, IsDelete: true},
		},
	})
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Fatalf("BatchWrite empty key: got %v, want InvalidArgument", err)
	}
}
