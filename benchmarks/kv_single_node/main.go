// Command kv_single_node benchmarks single-node KV throughput and latency,
// comparing Theseon against Pebble under three YCSB-lite workloads.
//
// Usage:
//
//	go run ./benchmarks/kv_single_node [flags]
//
// Output: benchmarks/out/kv_single_node.csv with columns
//
//	engine,workload,rep,ops_per_sec,p50_ms,p95_ms,p99_ms,errors
package main

import (
	"bytes"
	"cmp"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/ulixert/theseon/benchmarks/common"
	"github.com/ulixert/theseon/db"
	"github.com/ulixert/theseon/metrics"
)

func main() {
	duration := flag.Duration("duration", 60*time.Second, "measured-run duration per (engine,workload,rep)")
	warmup := flag.Duration("warmup", 15*time.Second, "pre-measurement warmup (same mix, discarded); evens out cold-cache artifacts after Pebble's forced compact")
	keyspaceSize := flag.Int("keyspace-size", 1_000_000, "number of distinct keys to pre-fill")
	valueSize := flag.Int("value-size", 256, "value size in bytes")
	reps := flag.Int("reps", 3, "repetitions per (engine,workload); medians reported")
	concurrency := flag.Int("concurrency", 1, "concurrent worker goroutines")
	outPath := flag.String("out", "benchmarks/out/kv_single_node.csv", "output CSV path")
	flag.Parse()

	if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil {
		log.Fatalf("mkdir out: %v", err)
	}
	f, err := os.Create(*outPath)
	if err != nil {
		log.Fatalf("create csv: %v", err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()
	_ = w.Write([]string{"engine", "workload", "rep", "ops_per_sec", "p50_ms", "p95_ms", "p99_ms", "errors"})

	engines := []string{"theseon", "pebble"}
	workloads := []common.Workload{common.YCSBA(), common.YCSBB(), common.YCSBC()}

	for _, engine := range engines {
		for _, wl := range workloads {
			wl.KeyspaceSize = *keyspaceSize
			wl.ValueSize = *valueSize
			wl.Duration = *duration
			wl.Warmup = *warmup
			wl.Concurrency = *concurrency

			var perRep []common.Results
			for rep := 0; rep < *reps; rep++ {
				log.Printf("%s / %s / rep %d …", engine, wl.Name, rep)
				r, err := runOne(engine, wl)
				if err != nil {
					log.Fatalf("%s/%s/rep%d: %v", engine, wl.Name, rep, err)
				}
				perRep = append(perRep, r)
				_ = w.Write([]string{
					engine, wl.Name, strconv.Itoa(rep),
					fmt.Sprintf("%.2f", r.OpsPerSec),
					fmt.Sprintf("%.3f", float64(r.P50)/float64(time.Millisecond)),
					fmt.Sprintf("%.3f", float64(r.P95)/float64(time.Millisecond)),
					fmt.Sprintf("%.3f", float64(r.P99)/float64(time.Millisecond)),
					strconv.FormatInt(r.Errors, 10),
				})
				w.Flush()
			}
			med := medianResults(perRep)
			log.Printf("%s / %s MEDIAN: %.0f ops/sec, p50=%.2fms p95=%.2fms p99=%.2fms",
				engine, wl.Name, med.OpsPerSec,
				ms(med.P50), ms(med.P95), ms(med.P99))
		}
	}
}

func runOne(engine string, wl common.Workload) (common.Results, error) {
	dir, err := os.MkdirTemp("", fmt.Sprintf("bench-%s-", engine))
	if err != nil {
		return common.Results{}, err
	}
	defer os.RemoveAll(dir)

	kv, err := openEngine(engine, dir)
	if err != nil {
		return common.Results{}, fmt.Errorf("open %s: %w", engine, err)
	}
	defer kv.Close()

	if err := common.PreFill(kv, wl.KeyspaceSize, wl.ValueSize); err != nil {
		return common.Results{}, fmt.Errorf("prefill: %w", err)
	}
	return wl.Run(kv)
}

func openEngine(name, dir string) (common.KVClient, error) {
	switch name {
	case "theseon":
		opts := db.DefaultOptions(dir)
		d, err := db.Open(opts)
		if err != nil {
			return nil, err
		}
		return &theseonClient{db: d}, nil
	case "pebble":
		// Block cache sized to hold the full 2M × 256B working set plus
		// headroom. Without this, Pebble's default (~8MB) bottlenecks reads
		// on syscall-per-block-miss while Theseon's mmap'd SSTables enjoy
		// free OS page cache. Matching caches lets the comparison measure
		// engine code paths, not cache policy.
		p, err := pebble.Open(dir, &pebble.Options{
			Cache:                     pebble.NewCache(1 << 30),
			MemTableSize:              64 << 20,
			L0CompactionFileThreshold: 4,
			LBaseMaxBytes:             256 << 20,
			MaxConcurrentCompactions:  func() int { return 1 },
		})
		if err != nil {
			return nil, err
		}
		return &pebbleClient{db: p}, nil
	default:
		return nil, fmt.Errorf("unknown engine %q", name)
	}
}

// theseonClient adapts *db.DB to common.KVClient.
type theseonClient struct{ db *db.DB }

func (c *theseonClient) Put(k, v []byte) error { return c.db.Put(k, v) }

func (c *theseonClient) Get(k []byte) ([]byte, bool, error) {
	val, found := c.db.Get(k)
	if !found || val.Tombstone {
		return nil, false, nil
	}
	return val.Data, true, nil
}

func (c *theseonClient) Delete(k []byte) error { return c.db.Delete(k) }

func (c *theseonClient) PutBatch(keys, values [][]byte) error {
	batch := c.db.NewWriteBatch()
	for i := range keys {
		batch.Put(keys[i], values[i])
	}
	return batch.Commit()
}

// AwaitReady polls the theseon_compactions_total counter and returns once
// it hasn't advanced for three consecutive 1-second samples, i.e. background
// compactions have quiesced. Bounded by a 30s ceiling so a badly-behaved
// engine doesn't hang the benchmark.
func (c *theseonClient) AwaitReady() error {
	const maxWait = 30 * time.Second
	const stableFor = 3

	deadline := time.Now().Add(maxWait)
	prev := testutil.ToFloat64(metrics.Compactions)
	stable := 0
	for time.Now().Before(deadline) {
		time.Sleep(time.Second)
		cur := testutil.ToFloat64(metrics.Compactions)
		if cur == prev {
			stable++
			if stable >= stableFor {
				return nil
			}
		} else {
			stable = 0
			prev = cur
		}
	}
	return nil // timed out; proceed anyway
}

func (c *theseonClient) Close() error { return c.db.Close() }

// pebbleClient adapts *pebble.DB to common.KVClient.
type pebbleClient struct{ db *pebble.DB }

func (c *pebbleClient) Put(k, v []byte) error {
	return c.db.Set(k, v, pebble.Sync)
}

func (c *pebbleClient) Get(k []byte) ([]byte, bool, error) {
	v, closer, err := c.db.Get(k)
	if err == pebble.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	out := make([]byte, len(v))
	copy(out, v)
	_ = closer.Close()
	return out, true, nil
}

func (c *pebbleClient) Delete(k []byte) error {
	return c.db.Delete(k, pebble.Sync)
}

func (c *pebbleClient) PutBatch(keys, values [][]byte) error {
	batch := c.db.NewBatch()
	for i := range keys {
		if err := batch.Set(keys[i], values[i], nil); err != nil {
			return err
		}
	}
	return c.db.Apply(batch, pebble.Sync)
}

// AwaitReady forces Pebble to flush the memtable and fully compact so the
// measured window sees the same kind of LSM shape Theseon settles into.
func (c *pebbleClient) AwaitReady() error {
	if err := c.db.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	end := bytes.Repeat([]byte{0xff}, 32)
	if err := c.db.Compact([]byte{0x00}, end, false); err != nil {
		return fmt.Errorf("compact: %w", err)
	}
	return nil
}

func (c *pebbleClient) Close() error { return c.db.Close() }

// medianResults picks the rep with the median OpsPerSec and returns it,
// so p-percentile latency numbers come from a single coherent run rather
// than a percentile-of-percentiles.
func medianResults(rs []common.Results) common.Results {
	cp := append([]common.Results(nil), rs...)
	slices.SortFunc(cp, func(a, b common.Results) int { return cmp.Compare(a.OpsPerSec, b.OpsPerSec) })
	return cp[len(cp)/2]
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
