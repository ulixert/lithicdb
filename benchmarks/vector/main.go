// Command vector benchmarks single-node HNSW search on SIFT-1M, sweeping
// ef_search and reporting recall@10 + QPS + latency percentiles at each
// point. Builds the index once and reuses it across sweep points.
//
// Usage:
//
//	go run ./benchmarks/vector [flags]
//
// The SIFT-1M dataset is expected at benchmarks/data/sift/sift/. See the
// benchmarks README for download instructions.
//
// Output: benchmarks/out/vector.csv with columns
//
//	ef_search,recall_at_10,qps,p50_ms,p95_ms,p99_ms,num_queries
package main

import (
	"cmp"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime/pprof"
	"slices"
	"strconv"
	"time"

	"github.com/ulixert/theseon/vector/eval"
	"github.com/ulixert/theseon/vector/hnsw"
)

func main() {
	baseFile := flag.String("base", "benchmarks/data/sift/sift/sift_base.fvecs", "SIFT base vectors (fvecs)")
	queryFile := flag.String("query", "benchmarks/data/sift/sift/sift_query.fvecs", "SIFT query vectors (fvecs)")
	gtFile := flag.String("gt", "benchmarks/data/sift/sift/sift_groundtruth.ivecs", "ground-truth neighbors (ivecs)")
	numQueries := flag.Int("num-queries", 1000, "number of queries to run per sweep point (full is 10000)")
	maxBase := flag.Int("max-base", 0, "cap base-vector count (0 = use all 1M); useful for quick sanity runs")
	m := flag.Int("M", 16, "HNSW M (max connections per node)")
	efConstruct := flag.Int("ef-construct", 200, "HNSW EfConstruct")
	k := flag.Int("k", 10, "k for recall@k")
	outPath := flag.String("out", "benchmarks/out/vector.csv", "output CSV path")
	cpuProfile := flag.String("cpuprofile", "", "write CPU profile of the search sweep to this path (build phase is excluded)")
	flag.Parse()

	if *cpuProfile != "" {
		if err := os.MkdirAll(filepath.Dir(*cpuProfile), 0o755); err != nil {
			log.Fatalf("cpuprofile dir: %v", err)
		}
		// Touch-test the file so it will fail before a long build.
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatalf("cpuprofile create: %v", err)
		}
		_ = f.Close()
		_ = os.Remove(*cpuProfile)
	}

	efSweep := []int{20, 50, 100, 200, 500, 1000}

	log.Printf("loading SIFT-1M …")
	base, err := eval.LoadFvecs(*baseFile)
	if err != nil {
		log.Fatalf("load base %q: %v", *baseFile, err)
	}
	queries, err := eval.LoadFvecs(*queryFile)
	if err != nil {
		log.Fatalf("load queries %q: %v", *queryFile, err)
	}
	gt, err := eval.LoadIvecs(*gtFile)
	if err != nil {
		log.Fatalf("load ground-truth %q: %v", *gtFile, err)
	}
	if *maxBase > 0 && *maxBase < base.N {
		base = &eval.Vectors{
			Data: base.Data[:*maxBase*base.Dim],
			N:    *maxBase,
			Dim:  base.Dim,
		}
	}
	log.Printf("loaded: base N=%d dim=%d, queries N=%d, gt K=%d",
		base.N, base.Dim, queries.N, gt.K)
	if gt.K < *k {
		log.Fatalf("ground-truth K=%d < requested k=%d", gt.K, *k)
	}
	if *numQueries > queries.N {
		*numQueries = queries.N
	}

	if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil {
		log.Fatalf("mkdir out: %v", err)
	}

	log.Printf("building HNSW: M=%d EfConstruct=%d N=%d dim=%d …", *m, *efConstruct, base.N, base.Dim)
	graph, err := hnsw.New(hnsw.Options{
		M:           *m,
		EfConstruct: *efConstruct,
		EfSearch:    efSweep[0], // will be overridden per-search anyway
		Dim:         base.Dim,
		Dist:        hnsw.DistanceL2Squared,
	})
	if err != nil {
		log.Fatalf("hnsw.New: %v", err)
	}

	buildStart := time.Now()
	reportEvery := base.N / 20
	if reportEvery < 1 {
		reportEvery = 1
	}
	for i := 0; i < base.N; i++ {
		if err := graph.Insert(uint64(i), [16]byte{}, base.Vec(i)); err != nil {
			log.Fatalf("insert %d: %v", i, err)
		}
		if (i+1)%reportEvery == 0 {
			rate := float64(i+1) / time.Since(buildStart).Seconds()
			log.Printf("  inserted %d / %d (%.0f vec/s)", i+1, base.N, rate)
		}
	}
	buildDuration := time.Since(buildStart)
	log.Printf("build done in %v (%.0f vec/s)",
		buildDuration.Round(time.Second),
		float64(base.N)/buildDuration.Seconds())

	// Prepare ground truth in uint64 for recall helpers.
	trueNN := make([][]uint64, *numQueries)
	for q := 0; q < *numQueries; q++ {
		raw := gt.Neighbors(q)
		ids := make([]uint64, *k)
		for i := 0; i < *k; i++ {
			ids[i] = uint64(raw[i])
		}
		trueNN[q] = ids
	}

	f, err := os.Create(*outPath)
	if err != nil {
		log.Fatalf("create csv: %v", err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	_ = w.Write([]string{"ef_search", "recall_at_10", "qps", "p50_ms", "p95_ms", "p99_ms", "num_queries"})

	if *cpuProfile != "" {
		pf, err := os.Create(*cpuProfile)
		if err != nil {
			log.Printf("cpuprofile create failed (continuing without profile): %v", err)
		} else {
			defer pf.Close()
			if err := pprof.StartCPUProfile(pf); err != nil {
				log.Printf("start cpuprofile failed: %v", err)
			} else {
				defer pprof.StopCPUProfile()
				log.Printf("cpu profile: %s", *cpuProfile)
			}
		}
	}

	for _, ef := range efSweep {
		log.Printf("sweep ef_search=%d …", ef)
		approx := make([][]uint64, *numQueries)
		lats := make([]time.Duration, *numQueries)
		sopts := &hnsw.SearchOptions{EfSearch: ef}

		for q := 0; q < *numQueries; q++ {
			t0 := time.Now()
			results, err := graph.Search(queries.Vec(q), *k, sopts)
			lats[q] = time.Since(t0)
			if err != nil {
				log.Fatalf("search %d (ef=%d): %v", q, ef, err)
			}
			ids := make([]uint64, len(results))
			for i, r := range results {
				ids[i] = r.ID
			}
			approx[q] = ids
		}

		recall := eval.MeanRecallAtK(trueNN, approx, *k)

		slices.SortFunc(lats, func(a, b time.Duration) int { return cmp.Compare(a, b) })
		totalLat := time.Duration(0)
		for _, l := range lats {
			totalLat += l
		}
		qps := float64(*numQueries) / totalLat.Seconds()

		row := []string{
			strconv.Itoa(ef),
			fmt.Sprintf("%.4f", recall),
			fmt.Sprintf("%.0f", qps),
			fmt.Sprintf("%.3f", ms(lats[pIdx(len(lats), 0.50)])),
			fmt.Sprintf("%.3f", ms(lats[pIdx(len(lats), 0.95)])),
			fmt.Sprintf("%.3f", ms(lats[pIdx(len(lats), 0.99)])),
			strconv.Itoa(*numQueries),
		}
		_ = w.Write(row)
		w.Flush()

		log.Printf("  ef=%d  recall@%d=%.4f  QPS=%.0f  p50=%.2fms p95=%.2fms p99=%.2fms",
			ef, *k, recall, qps,
			ms(lats[pIdx(len(lats), 0.50)]),
			ms(lats[pIdx(len(lats), 0.95)]),
			ms(lats[pIdx(len(lats), 0.99)]))
	}

	log.Printf("vector benchmark done; CSV at %s", *outPath)
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

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
