package eval

import "testing"

func TestRunBenchmark_Smoke(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping benchmark smoke test in short mode")
	}

	result, err := RunBenchmark(BenchmarkParams{
		M: 8, EfConstruct: 50, EfSearch: 50,
		NumVectors: 500, Dim: 32, K: 10,
	}, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	if result.Recall <= 0 {
		t.Error("Recall should be > 0")
	}
	if result.P50 <= 0 {
		t.Error("P50 should be > 0")
	}
	if result.P95 <= 0 {
		t.Error("P95 should be > 0")
	}
	if result.P99 <= 0 {
		t.Error("P99 should be > 0")
	}
	if result.QPS <= 0 {
		t.Error("QPS should be > 0")
	}
	if result.IndexingThroughput <= 0 {
		t.Error("IndexingThroughput should be > 0")
	}
	if result.IndexingDuration <= 0 {
		t.Error("IndexingDuration should be > 0")
	}
	if result.MemoryBytes <= 0 {
		t.Error("MemoryBytes should be > 0")
	}
	t.Log(result.String())
}
