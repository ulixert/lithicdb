package hnsw

import (
	"math"
	"testing"
)

func TestDistanceL2Squared(t *testing.T) {
	tests := []struct {
		name string
		a, b []float32
		want float32
	}{
		{"identical", []float32{1, 2, 3}, []float32{1, 2, 3}, 0},
		{"unit diff", []float32{0, 0, 0}, []float32{1, 0, 0}, 1},
		{"known", []float32{1, 2, 3}, []float32{4, 5, 6}, 27},
		{"single dim", []float32{3}, []float32{7}, 16},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DistanceL2Squared(tt.a, tt.b)
			if !approxEqual(got, tt.want, 1e-6) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDistanceL2Squared_Symmetry(t *testing.T) {
	a := []float32{1.5, -2.3, 0.7, 4.1}
	b := []float32{-0.5, 3.2, 1.1, -0.9}
	if DistanceL2Squared(a, b) != DistanceL2Squared(b, a) {
		t.Error("L2 squared distance is not symmetric")
	}
}

func TestDistanceCosine(t *testing.T) {
	tests := []struct {
		name string
		a, b []float32
		want float32
	}{
		{"identical", []float32{1, 0, 0}, []float32{1, 0, 0}, 0},
		{"orthogonal", []float32{1, 0}, []float32{0, 1}, 1},
		{"opposite", []float32{1, 0}, []float32{-1, 0}, 2},
		{"zero vector", []float32{0, 0}, []float32{1, 1}, 0},
		{"parallel scaled", []float32{1, 2, 3}, []float32{2, 4, 6}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DistanceCosine(tt.a, tt.b)
			if !approxEqual(got, tt.want, 1e-6) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDistanceCosine_Symmetry(t *testing.T) {
	a := []float32{1.5, -2.3, 0.7}
	b := []float32{-0.5, 3.2, 1.1}
	if DistanceCosine(a, b) != DistanceCosine(b, a) {
		t.Error("cosine distance is not symmetric")
	}
}

func TestDistanceInnerProduct(t *testing.T) {
	tests := []struct {
		name string
		a, b []float32
		want float32
	}{
		{"orthogonal", []float32{1, 0}, []float32{0, 1}, 0},
		{"parallel", []float32{1, 2, 3}, []float32{1, 2, 3}, -14},
		{"opposite", []float32{1, 0}, []float32{-1, 0}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DistanceInnerProduct(tt.a, tt.b)
			if !approxEqual(got, tt.want, 1e-6) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDistanceL2Squared_NonNegative(t *testing.T) {
	a := []float32{-3.1, 2.7, 0, -1.5}
	b := []float32{4.2, -0.3, 1.8, 2.9}
	if d := DistanceL2Squared(a, b); d < 0 {
		t.Errorf("L2 squared returned negative: %v", d)
	}
}

func approxEqual(a, b, eps float32) bool {
	return float32(math.Abs(float64(a-b))) <= eps
}
