// Package common defines shared types for the Theseon KV benchmark harnesses:
// a KVClient interface that both the single-node and cluster harnesses satisfy,
// a Workload description, and a worker loop that produces latency/throughput
// Results.
package common

import (
	"cmp"
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
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

// Results summarize one measured run.
type Results struct {
	Workload  string
	OpsPerSec float64
	P50       time.Duration
	P95       time.Duration
	P99       time.Duration
	Errors    int64
	TotalOps  int64
	Duration  time.Duration
}

// MakeKey returns a stable fixed-width key for index i. The 20-byte form is
// long enough to avoid collisions across KeyspaceSize values yet short enough
// to stay cache-friendly.
func MakeKey(i int) []byte {
	return []byte(fmt.Sprintf("key-%016d", i))
}

// MakeValue returns a fresh zero-filled value buffer of the requested size.
// Zeros are fine: compression ratio is not a variable the benchmarks study,
// and the engines handle the value as opaque bytes regardless of content.
func MakeValue(size int) []byte {
	return make([]byte, size)
}

// PreFill sequentially writes all keys in [0, keyspaceSize) with a fresh
// value of valueSize bytes, then calls kv.AwaitReady(). Pre-fill time is not
// reported - the caller's measured Run() comes after this returns.
//
// If kv implements BatchedKVClient, PreFill chunks writes so each chunk
// incurs a single fsync. This is the difference between a PreFill that
// finishes in seconds and one that dominates the benchmark runtime.
func PreFill(kv KVClient, keyspaceSize, valueSize int) error {
	val := MakeValue(valueSize)

	if bkv, ok := kv.(BatchedKVClient); ok {
		const chunkSize = 1024
		keys := make([][]byte, 0, chunkSize)
		vals := make([][]byte, 0, chunkSize)
		for start := 0; start < keyspaceSize; start += chunkSize {
			end := start + chunkSize
			if end > keyspaceSize {
				end = keyspaceSize
			}
			keys = keys[:0]
			vals = vals[:0]
			for j := start; j < end; j++ {
				keys = append(keys, MakeKey(j))
				vals = append(vals, val)
			}
			if err := bkv.PutBatch(keys, vals); err != nil {
				return fmt.Errorf("prefill batch @%d: %w", start, err)
			}
		}
	} else {
		for i := 0; i < keyspaceSize; i++ {
			if err := kv.Put(MakeKey(i), val); err != nil {
				return fmt.Errorf("prefill key %d: %w", i, err)
			}
		}
	}

	if err := kv.AwaitReady(); err != nil {
		return fmt.Errorf("prefill AwaitReady: %w", err)
	}
	return nil
}

// Run drives the configured workload against kv for w.Duration using
// w.Concurrency workers. Each worker has its own RNG and latency buffer;
// results are merged at return.
func (w Workload) Run(kv KVClient) (Results, error) {
	if err := validate(w); err != nil {
		return Results{}, err
	}

	workers := w.Concurrency
	if workers < 1 {
		workers = 1
	}

	seed := w.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}

	deadline := time.Now().Add(w.Duration)
	var (
		wg       sync.WaitGroup
		errCount int64
		mu       sync.Mutex
		lats     []time.Duration
	)

	val := MakeValue(w.ValueSize)

	for w_ := 0; w_ < workers; w_++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			rng := rand.New(rand.NewSource(seed + int64(workerID)))
			var zipf *rand.Zipf
			if w.ZipfS > 1.0 {
				zipf = rand.NewZipf(rng, w.ZipfS, 1.0, uint64(w.KeyspaceSize-1))
			}

			// Per-worker latency buffer to avoid contention; merged at end.
			localLats := make([]time.Duration, 0, 1<<16)

			readThresh := w.ReadFrac
			writeThresh := w.ReadFrac + w.WriteFrac

			for time.Now().Before(deadline) {
				keyIdx := pickKey(rng, zipf, w.KeyspaceSize)
				key := MakeKey(keyIdx)

				opRoll := rng.Float64()
				start := time.Now()
				var err error
				switch {
				case opRoll < readThresh:
					_, _, err = kv.Get(key)
				case opRoll < writeThresh:
					err = kv.Put(key, val)
				default:
					err = kv.Delete(key)
				}
				elapsed := time.Since(start)

				if err != nil {
					atomic.AddInt64(&errCount, 1)
				} else {
					localLats = append(localLats, elapsed)
				}
			}

			mu.Lock()
			lats = append(lats, localLats...)
			mu.Unlock()
		}(w_)
	}

	wg.Wait()
	actualDuration := w.Duration

	return summarize(w.Name, lats, errCount, actualDuration), nil
}

func validate(w Workload) error {
	sum := w.ReadFrac + w.WriteFrac + w.DeleteFrac
	if sum < 0.999 || sum > 1.001 {
		return fmt.Errorf("workload %q: read+write+delete fractions = %v, want ~1.0", w.Name, sum)
	}
	if w.KeyspaceSize <= 0 {
		return fmt.Errorf("workload %q: keyspace size must be > 0", w.Name)
	}
	if w.ValueSize < 0 {
		return fmt.Errorf("workload %q: value size must be >= 0", w.Name)
	}
	if w.Duration <= 0 {
		return fmt.Errorf("workload %q: duration must be > 0", w.Name)
	}
	return nil
}

func pickKey(rng *rand.Rand, zipf *rand.Zipf, keyspaceSize int) int {
	if zipf != nil {
		return int(zipf.Uint64())
	}
	return rng.Intn(keyspaceSize)
}

func summarize(name string, lats []time.Duration, errors int64, duration time.Duration) Results {
	total := int64(len(lats)) + errors
	r := Results{
		Workload: name,
		Errors:   errors,
		TotalOps: total,
		Duration: duration,
	}
	if total > 0 {
		r.OpsPerSec = float64(total) / duration.Seconds()
	}
	if len(lats) > 0 {
		slices.SortFunc(lats, func(a, b time.Duration) int { return cmp.Compare(a, b) })
		r.P50 = lats[pIdx(len(lats), 0.50)]
		r.P95 = lats[pIdx(len(lats), 0.95)]
		r.P99 = lats[pIdx(len(lats), 0.99)]
	}
	return r
}

func pIdx(n int, q float64) int {
	i := int(float64(n-1) * q)
	if i < 0 {
		return 0
	}
	if i >= n {
		return n - 1
	}
	return i
}
