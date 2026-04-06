package vector

// Metrics provides an interface for recording operational metrics
// from the vector store. All methods must be safe for concurrent use.
type Metrics interface {
	Histogram(name string, value float64, tags map[string]string)
	Gauge(name string, value float64, tags map[string]string)
	Counter(name string, delta int64, tags map[string]string)
}

// noopMetrics is the default Metrics implementation that discards all data.
type noopMetrics struct{}

func (noopMetrics) Histogram(string, float64, map[string]string) {}
func (noopMetrics) Gauge(string, float64, map[string]string)     {}
func (noopMetrics) Counter(string, int64, map[string]string)     {}
