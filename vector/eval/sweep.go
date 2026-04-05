package eval

import (
	"encoding/csv"
	"fmt"
	"strings"
	"time"

	"github.com/ulixert/theseon/vector/hnsw"
)

// SweepConfig defines the parameter grid for a sweep.
type SweepConfig struct {
	Ms           []int // M values to test
	EfConstructs []int // efConstruction values to test
	EfSearchs    []int // efSearch values to test
	NumVectors   int
	Dim          int
	K            int
	Dist         hnsw.DistanceFunc
}

// SweepResult wraps a BenchmarkResult for use in sweep output.
type SweepResult struct {
	BenchmarkResult
}

// RunSweep runs benchmarks for every combination in the parameter grid.
// For each (M, efConstruct) pair, the index is built once and then queried
// with each efSearch value. The callback is invoked after each benchmark
// completes (useful for progress reporting).
func RunSweep(cfg SweepConfig, baseVecs, queryVecs *Vectors, callback func(SweepResult)) []SweepResult {
	if cfg.Dist == nil {
		cfg.Dist = hnsw.DistanceL2Squared
	}

	// Generate data once.
	if baseVecs == nil {
		baseVecs = randomVectors(cfg.NumVectors, cfg.Dim)
	}
	numQueries := 100
	if queryVecs == nil {
		queryVecs = randomVectors(numQueries, cfg.Dim)
	}

	var results []SweepResult

	for _, m := range cfg.Ms {
		for _, efC := range cfg.EfConstructs {
			// Build the index once per (M, efC) pair.
			// We run the first efSearch benchmark which also builds the graph,
			// then reuse the graph for subsequent efSearch values by running
			// full benchmarks. For simplicity in Phase 1, we rebuild per
			// (M, efC, efS) triple since benchmark.go encapsulates the build.
			// The extra cost is acceptable for eval workloads.
			for _, efS := range cfg.EfSearchs {
				params := BenchmarkParams{
					M:           m,
					EfConstruct: efC,
					EfSearch:    efS,
					NumVectors:  cfg.NumVectors,
					Dim:         cfg.Dim,
					K:           cfg.K,
				}
				result, err := RunBenchmark(params, baseVecs, queryVecs, cfg.Dist)
				if err != nil {
					continue // skip invalid param combos
				}
				sr := SweepResult{*result}
				results = append(results, sr)
				if callback != nil {
					callback(sr)
				}
			}
		}
	}

	return results
}

// FormatTable returns a human-readable table of sweep results.
func FormatTable(results []SweepResult) string {
	if len(results) == 0 {
		return ""
	}
	var sb strings.Builder
	k := results[0].Params.K
	header := fmt.Sprintf("%-4s  %-5s  %-5s  %-12s  %-10s  %-10s  %-10s  %-10s  %-10s",
		"M", "efC", "efS", fmt.Sprintf("Recall@%d", k), "P50", "P95", "P99", "QPS", "Memory")
	sb.WriteString(header)
	sb.WriteString("\n")
	sb.WriteString(strings.Repeat("-", len(header)))
	sb.WriteString("\n")
	for _, r := range results {
		sb.WriteString(fmt.Sprintf("%-4d  %-5d  %-5d  %-12.4f  %-10s  %-10s  %-10s  %-10.0f  %-10s\n",
			r.Params.M, r.Params.EfConstruct, r.Params.EfSearch,
			r.Recall,
			r.P50.Round(time.Microsecond),
			r.P95.Round(time.Microsecond),
			r.P99.Round(time.Microsecond),
			r.QPS,
			formatBytes(r.MemoryBytes)))
	}
	return sb.String()
}

// FormatCSV returns sweep results as CSV suitable for machine parsing.
func FormatCSV(results []SweepResult) string {
	var sb strings.Builder
	w := csv.NewWriter(&sb)
	w.Write([]string{"M", "efConstruct", "efSearch", "recall", "p50_us", "p95_us", "p99_us", "qps", "memory_bytes"})
	for _, r := range results {
		w.Write([]string{
			fmt.Sprintf("%d", r.Params.M),
			fmt.Sprintf("%d", r.Params.EfConstruct),
			fmt.Sprintf("%d", r.Params.EfSearch),
			fmt.Sprintf("%.4f", r.Recall),
			fmt.Sprintf("%d", r.P50.Microseconds()),
			fmt.Sprintf("%d", r.P95.Microseconds()),
			fmt.Sprintf("%d", r.P99.Microseconds()),
			fmt.Sprintf("%.0f", r.QPS),
			fmt.Sprintf("%d", r.MemoryBytes),
		})
	}
	w.Flush()
	return sb.String()
}

func formatBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1fGB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1fMB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1fKB", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%dB", b)
	}
}
