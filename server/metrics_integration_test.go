package server_test

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/ulixert/theseon/metrics"
	pb "github.com/ulixert/theseon/proto/theseonpb"
)

// TestMetricsSmokeSignals verifies that the Prometheus counters and histograms
// declared in the metrics package actually advance when KV operations go
// through the gRPC server. Serves as the regression guard for the /metrics
// smoke test documented in benchmarks/README.md.
//
// Assertions are deltas — not absolute values — because the default
// Prometheus registry is process-global and shared across all tests in the
// package.
func TestMetricsSmokeSignals(t *testing.T) {
	client, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()

	putCounter := metrics.KVWrites.WithLabelValues("put", "standalone")
	delCounter := metrics.KVWrites.WithLabelValues("delete", "standalone")
	hitCounter := metrics.KVReads.WithLabelValues("hit", "standalone")
	missCounter := metrics.KVReads.WithLabelValues("miss", "standalone")

	before := struct {
		puts, deletes, hits, misses float64
	}{
		puts:    testutil.ToFloat64(putCounter),
		deletes: testutil.ToFloat64(delCounter),
		hits:    testutil.ToFloat64(hitCounter),
		misses:  testutil.ToFloat64(missCounter),
	}

	const (
		putCount    = 5
		hitCount    = 2
		missCount   = 1
		deleteCount = 1
	)

	for i := 0; i < putCount; i++ {
		key := []byte{'k', byte('0' + i)}
		if _, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: []byte("v")}); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	hitKeys := [][]byte{{'k', '0'}, {'k', '1'}}
	for _, k := range hitKeys {
		if _, err := client.Get(ctx, &pb.GetRequest{Key: k}); err != nil {
			t.Fatalf("Get hit: %v", err)
		}
	}

	missKey := []byte("does-not-exist")
	if _, err := client.Get(ctx, &pb.GetRequest{Key: missKey}); err != nil {
		t.Fatalf("Get miss: %v", err)
	}

	if _, err := client.Delete(ctx, &pb.DeleteRequest{Key: []byte{'k', '0'}}); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	got := struct {
		puts, deletes, hits, misses float64
	}{
		puts:    testutil.ToFloat64(putCounter) - before.puts,
		deletes: testutil.ToFloat64(delCounter) - before.deletes,
		hits:    testutil.ToFloat64(hitCounter) - before.hits,
		misses:  testutil.ToFloat64(missCounter) - before.misses,
	}

	if got.puts != putCount {
		t.Errorf("put counter delta = %v, want %d", got.puts, putCount)
	}
	if got.deletes != deleteCount {
		t.Errorf("delete counter delta = %v, want %d", got.deletes, deleteCount)
	}
	if got.hits != hitCount {
		t.Errorf("hit counter delta = %v, want %d", got.hits, hitCount)
	}
	if got.misses != missCount {
		t.Errorf("miss counter delta = %v, want %d", got.misses, missCount)
	}
}
