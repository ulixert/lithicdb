// Package common defines shared types for the Theseon KV benchmark harnesses:
// a KVClient interface that both the single-node and cluster harnesses satisfy,
// a Workload description, and a worker loop that produces latency/throughput
// Results.
package common

import (
	"time"
)

// KVClient is the minimum surface the workload generator needs. Engine-
// specific harnesses (Theseon, Pebble, cluster) wrap their native APIs to
// satisfy this interface.
type KVClient interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, bool, error)
	Delete(key []byte) error

	// AwaitReady waits for any background work (flushes, compactions) to
	// settle so that the measured window isn't dominated by post-prefill
	// LSM stabilization. Called after PreFill, before Run.
	AwaitReady() error

	// Close releases the underlying resources (DB handles, gRPC conns).
	Close() error
}

// BatchedKVClient is an optional extension: engines that support atomic
// multi-key writes (one fsync per batch) satisfy it. PreFill uses this path
// when available to keep setup time reasonable at 500K+ keyspace sizes,
// where per-key fsync would otherwise dominate the whole run.
type BatchedKVClient interface {
	KVClient
	PutBatch(keys [][]byte, values [][]byte) error
}

// Workload describes the mix and shape of operations to drive.
type Workload struct {
	Name         string
	ReadFrac     float64 // [0,1]; ReadFrac + WriteFrac + DeleteFrac must equal 1
	WriteFrac    float64
	DeleteFrac   float64
	KeyspaceSize int           // number of distinct keys (keys are indexed 0..KeyspaceSize-1)
	ValueSize    int           // bytes per value
	Duration     time.Duration // measured run length
	Concurrency  int           // number of concurrent worker goroutines
	ZipfS        float64       // >1.0 → zipfian with this `s` parameter; <=1.0 → uniform
	Seed         int64         // RNG seed (0 → time-based)
}

// YCSB-lite predefined mixes. Callers override KeyspaceSize / ValueSize /
// Duration / Concurrency as needed for the specific run.
func YCSBA() Workload {
	return Workload{Name: "YCSB-A", ReadFrac: 0.5, WriteFrac: 0.5, Concurrency: 1}
}
func YCSBB() Workload {
	return Workload{Name: "YCSB-B", ReadFrac: 0.95, WriteFrac: 0.05, Concurrency: 1}
}
func YCSBC() Workload {
	return Workload{Name: "YCSB-C", ReadFrac: 1.0, Concurrency: 1}
}
