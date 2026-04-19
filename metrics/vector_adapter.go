package metrics

import (
	"slices"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// VectorAdapter implements vector.Metrics (vector/metrics.go) using lazily
// registered Prometheus CounterVec / HistogramVec / GaugeVec collectors. Each
// distinct metric name seen at runtime creates one collector; the label set is
// derived from the tag keys (stable ordering).
//
// Collectors are registered against the default Prometheus registry so they
// appear alongside the package globals above at /metrics.
type VectorAdapter struct {
	mu         sync.Mutex
	counters   map[string]*prometheus.CounterVec
	gauges     map[string]*prometheus.GaugeVec
	histograms map[string]*prometheus.HistogramVec
}

func NewVectorAdapter() *VectorAdapter {
	return &VectorAdapter{
		counters:   make(map[string]*prometheus.CounterVec),
		gauges:     make(map[string]*prometheus.GaugeVec),
		histograms: make(map[string]*prometheus.HistogramVec),
	}
}

// promName maps "vector.foo.bar" → "vector_foo_bar" to match Prometheus naming.
func promName(name string) string {
	return "theseon_" + strings.ReplaceAll(name, ".", "_")
}

func sortedKeys(tags map[string]string) []string {
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func values(tags map[string]string, keys []string) []string {
	out := make([]string, len(keys))
	for i, k := range keys {
		out[i] = tags[k]
	}
	return out
}

func (a *VectorAdapter) Counter(name string, delta int64, tags map[string]string) {
	keys := sortedKeys(tags)
	a.mu.Lock()
	cv, ok := a.counters[name]
	if !ok {
		cv = prometheus.NewCounterVec(
			prometheus.CounterOpts{Name: promName(name), Help: "vector-store counter"},
			keys,
		)
		if err := prometheus.Register(cv); err != nil {
			// Registration races with a duplicate registration are benign;
			// fall back to the already-registered collector.
			if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
				cv = are.ExistingCollector.(*prometheus.CounterVec)
			} else {
				a.mu.Unlock()
				return
			}
		}
		a.counters[name] = cv
	}
	a.mu.Unlock()
	cv.WithLabelValues(values(tags, keys)...).Add(float64(delta))
}

func (a *VectorAdapter) Gauge(name string, value float64, tags map[string]string) {
	keys := sortedKeys(tags)
	a.mu.Lock()
	gv, ok := a.gauges[name]
	if !ok {
		gv = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{Name: promName(name), Help: "vector-store gauge"},
			keys,
		)
		if err := prometheus.Register(gv); err != nil {
			if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
				gv = are.ExistingCollector.(*prometheus.GaugeVec)
			} else {
				a.mu.Unlock()
				return
			}
		}
		a.gauges[name] = gv
	}
	a.mu.Unlock()
	gv.WithLabelValues(values(tags, keys)...).Set(value)
}
