package eval

import (
	"fmt"
	"math/rand"
	"runtime"
	"slices"
	"time"

	"github.com/ulixert/theseon/vector/hnsw"
)

// BenchmarkParams defines the parameters for a single benchmark run.
type BenchmarkParams struct {
	M, EfConstruct, EfSearch int
	NumVectors, Dim, K       int
}

// BenchmarkResult holds the output of a single benchmark run.
type BenchmarkResult struct {
	Recall             float64
	P50, P95, P99      time.Duration
	QPS                float64
	IndexingThroughput float64 // vectors/sec
	IndexingDuration   time.Duration
	MemoryBytes        int64
	HeapAllocBytes     uint64
	Params             BenchmarkParams
}

// String returns a human-readable summary line.
func (r *BenchmarkResult) String() string {
	return fmt.Sprintf("M=%-3d efC=%-4d efS=%-4d  recall@%d=%.4f  p50=%v  p95=%v  p99=%v  QPS=%.0f  idx=%.0f vec/s  mem=%dMB",
		r.Params.M, r.Params.EfConstruct, r.Params.EfSearch, r.Params.K,
		r.Recall, r.P50.Round(time.Microsecond), r.P95.Round(time.Microsecond), r.P99.Round(time.Microsecond),
		r.QPS, r.IndexingThroughput, r.MemoryBytes/(1024*1024))
}

// RunBenchmark builds an HNSW index, computes ground truth, and measures
// search performance. Uses random vectors if baseVecs/queryVecs are nil.
func RunBenchmark(params BenchmarkParams, baseVecs, queryVecs *Vectors, dist hnsw.DistanceFunc) (*BenchmarkResult, error) {
	if dist == nil {
		dist = hnsw.DistanceL2Squared
	}

	// Generate random data if not provided.
	if baseVecs == nil {
		baseVecs = randomVectors(params.NumVectors, params.Dim)
	}
	numQueries := 100
	if queryVecs == nil {
		queryVecs = randomVectors(numQueries, params.Dim)
	} else {
		numQueries = queryVecs.N
	}

	// Build index.
	g, err := hnsw.New(hnsw.Options{
		M:           params.M,
		EfConstruct: params.EfConstruct,
		EfSearch:    params.EfSearch,
		Dim:         params.Dim,
		Dist:        dist,
	})
	if err != nil {
		return nil, err
	}

	start := time.Now()
	for i := 0; i < baseVecs.N; i++ {
		if err := g.Insert(uint64(i), baseVecs.Vec(i)); err != nil {
			return nil, fmt.Errorf("insert %d: %w", i, err)
		}
	}
	indexDuration := time.Since(start)

	// GC before measuring query performance for stable latency numbers.
	runtime.GC()

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	// Compute ground truth.
	trueNNs := make([][]uint64, numQueries)
	for q := 0; q < numQueries; q++ {
		trueNNs[q] = BruteForceKNN(baseVecs, queryVecs.Vec(q), params.K, dist)
	}

	// Time each search individually.
	latencies := make([]time.Duration, numQueries)
	approxNNs := make([][]uint64, numQueries)
	for q := 0; q < numQueries; q++ {
		t0 := time.Now()
		results, err := g.Search(queryVecs.Vec(q), params.K, nil)
		latencies[q] = time.Since(t0)
		if err != nil {
			return nil, fmt.Errorf("search %d: %w", q, err)
		}
		ids := make([]uint64, len(results))
		for i, r := range results {
			ids[i] = r.ID
		}
		approxNNs[q] = ids
	}

	// Compute metrics.
	slices.Sort(latencies)
	totalLatency := time.Duration(0)
	for _, l := range latencies {
		totalLatency += l
	}

	stats := g.Stats()

	return &BenchmarkResult{
		Recall:             MeanRecallAtK(trueNNs, approxNNs, params.K),
		P50:                percentile(latencies, 50),
		P95:                percentile(latencies, 95),
		P99:                percentile(latencies, 99),
		QPS:                float64(numQueries) / totalLatency.Seconds(),
		IndexingThroughput: float64(baseVecs.N) / indexDuration.Seconds(),
		IndexingDuration:   indexDuration,
		MemoryBytes:        stats.MemoryBytes,
		HeapAllocBytes:     ms.HeapAlloc,
		Params:             params,
	}, nil
}

func percentile(sorted []time.Duration, pct int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := pct * len(sorted) / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func randomVectors(n, dim int) *Vectors {
	data := make([]float32, n*dim)
	for i := range data {
		data[i] = rand.Float32()*2 - 1
	}
	return &Vectors{Data: data, N: n, Dim: dim}
}
