// Command kv_cluster benchmarks a 3-node in-process Theseon cluster under
// three quorum configurations: (N=3, W=2, R=2), (3,3,1), (3,1,3). Each run
// exercises the coordinator fan-out, gRPC, HLC timestamping, and the peer
// pool; the nodes live in the same process but communicate over real TCP.
//
// Usage:
//
//	go run ./benchmarks/kv_cluster [flags]
//
// Output: benchmarks/out/kv_cluster.csv with columns
//
//	N,W,R,workload,rep,ops_per_sec,p50_ms,p95_ms,p99_ms,errors
package main

import (
	"cmp"
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/ulixert/theseon/benchmarks/common"
	"github.com/ulixert/theseon/cluster"
)

type quorum struct{ N, W, R int }

func main() {
	duration := flag.Duration("duration", 60*time.Second, "measured-run duration per (quorum,workload,rep)")
	keyspaceSize := flag.Int("keyspace-size", 100_000, "number of distinct keys to pre-fill (cluster prefill is slower than single-node)")
	valueSize := flag.Int("value-size", 256, "value size in bytes")
	reps := flag.Int("reps", 3, "repetitions per (quorum,workload); medians reported")
	concurrency := flag.Int("concurrency", 8, "concurrent worker goroutines; higher hides per-op RPC latency")
	outPath := flag.String("out", "benchmarks/out/kv_cluster.csv", "output CSV path")
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
	_ = w.Write([]string{"N", "W", "R", "workload", "rep", "ops_per_sec", "p50_ms", "p95_ms", "p99_ms", "errors"})

	quorums := []quorum{{3, 2, 2}, {3, 3, 1}, {3, 1, 3}}
	workloads := []common.Workload{common.YCSBA(), common.YCSBB(), common.YCSBC()}

	for _, q := range quorums {
		for _, wl := range workloads {
			wl.KeyspaceSize = *keyspaceSize
			wl.ValueSize = *valueSize
			wl.Duration = *duration
			wl.Concurrency = *concurrency

			var perRep []common.Results
			for rep := 0; rep < *reps; rep++ {
				log.Printf("(%d,%d,%d) / %s / rep %d …", q.N, q.W, q.R, wl.Name, rep)
				r, err := runOne(q, wl)
				if err != nil {
					log.Fatalf("(%d,%d,%d)/%s/rep%d: %v", q.N, q.W, q.R, wl.Name, rep, err)
				}
				perRep = append(perRep, r)
				_ = w.Write([]string{
					strconv.Itoa(q.N), strconv.Itoa(q.W), strconv.Itoa(q.R),
					wl.Name, strconv.Itoa(rep),
					fmt.Sprintf("%.2f", r.OpsPerSec),
					fmt.Sprintf("%.3f", ms(r.P50)),
					fmt.Sprintf("%.3f", ms(r.P95)),
					fmt.Sprintf("%.3f", ms(r.P99)),
					strconv.FormatInt(r.Errors, 10),
				})
				w.Flush()
			}
			med := medianResults(perRep)
			log.Printf("(%d,%d,%d) / %s MEDIAN: %.0f ops/sec, p50=%.2fms p95=%.2fms p99=%.2fms",
				q.N, q.W, q.R, wl.Name, med.OpsPerSec,
				ms(med.P50), ms(med.P95), ms(med.P99))
		}
	}
}

// runOne spins up a 3-node cluster, pre-fills, runs the workload through
// the coordinator on node-1, then tears the cluster down.
func runOne(q quorum, wl common.Workload) (common.Results, error) {
	ctx := context.Background()
	coordCfg := cluster.DefaultCoordinatorConfig()
	coordCfg.ReplicationFactor = q.N
	coordCfg.WriteQuorum = q.W
	coordCfg.ReadQuorum = q.R

	cl, err := startCluster(ctx, coordCfg)
	if err != nil {
		return common.Results{}, fmt.Errorf("start cluster: %w", err)
	}
	defer cl.stop()

	kv, err := newClusterClient(cl.nodes[0].Addr())
	if err != nil {
		return common.Results{}, fmt.Errorf("dial coordinator: %w", err)
	}
	defer kv.Close()

	if err := common.PreFill(kv, wl.KeyspaceSize, wl.ValueSize); err != nil {
		return common.Results{}, fmt.Errorf("prefill: %w", err)
	}
	return wl.Run(kv)
}

// --- small utilities ---

func medianResults(rs []common.Results) common.Results {
	cp := append([]common.Results(nil), rs...)
	slices.SortFunc(cp, func(a, b common.Results) int { return cmp.Compare(a.OpsPerSec, b.OpsPerSec) })
	return cp[len(cp)/2]
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
