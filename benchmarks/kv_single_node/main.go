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

	"github.com/ulixert/theseon/benchmarks/common"
)

func main() {
	duration := flag.Duration("duration", 60*time.Second, "measured-run duration per (engine,workload,rep)")
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

// medianResults picks the rep with the median OpsPerSec and returns it,
// so p-percentile latency numbers come from a single coherent run rather
// than a percentile-of-percentiles.
func medianResults(rs []common.Results) common.Results {
	cp := append([]common.Results(nil), rs...)
	slices.SortFunc(cp, func(a, b common.Results) int { return cmp.Compare(a.OpsPerSec, b.OpsPerSec) })
	return cp[len(cp)/2]
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
