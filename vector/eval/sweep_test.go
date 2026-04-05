package eval

import (
	"strings"
	"testing"
)

func TestRunSweep_SinglePoint(t *testing.T) {
	results := RunSweep(SweepConfig{
		Ms:           []int{8},
		EfConstructs: []int{50},
		EfSearchs:    []int{50},
		NumVectors:   200,
		Dim:          16,
		K:            5,
	}, nil, nil, nil)

	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}
	if results[0].Recall <= 0 {
		t.Error("Recall should be > 0")
	}
}

func TestRunSweep_CallbackCount(t *testing.T) {
	count := 0
	RunSweep(SweepConfig{
		Ms:           []int{4, 8},
		EfConstructs: []int{50},
		EfSearchs:    []int{25, 50},
		NumVectors:   100,
		Dim:          8,
		K:            5,
	}, nil, nil, func(SweepResult) { count++ })

	// 2 M values * 1 efC * 2 efS = 4
	if count != 4 {
		t.Errorf("callback fired %d times, want 4", count)
	}
}

func TestFormatTable_ContainsHeaders(t *testing.T) {
	results := RunSweep(SweepConfig{
		Ms:           []int{4},
		EfConstructs: []int{50},
		EfSearchs:    []int{50},
		NumVectors:   100,
		Dim:          8,
		K:            5,
	}, nil, nil, nil)

	table := FormatTable(results)
	if !strings.Contains(table, "M") || !strings.Contains(table, "Recall@5") {
		t.Errorf("table missing headers:\n%s", table)
	}
}

func TestFormatCSV_Parseable(t *testing.T) {
	results := RunSweep(SweepConfig{
		Ms:           []int{4},
		EfConstructs: []int{50},
		EfSearchs:    []int{50},
		NumVectors:   100,
		Dim:          8,
		K:            5,
	}, nil, nil, nil)

	csvOut := FormatCSV(results)
	lines := strings.Split(strings.TrimSpace(csvOut), "\n")
	if len(lines) != 2 { // header + 1 result
		t.Errorf("got %d CSV lines, want 2", len(lines))
	}
	if !strings.HasPrefix(lines[0], "M,") {
		t.Errorf("CSV header: %s", lines[0])
	}
}

func TestFormatTable_Empty(t *testing.T) {
	table := FormatTable(nil)
	if table != "" {
		t.Errorf("expected empty string, got %q", table)
	}
}
