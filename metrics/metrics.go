// Package metrics exposes Prometheus metrics for the Theseon storage engine,
// cluster fabric, and vector store. Metric variables are package globals; call
// sites `.Inc()` / `.Observe()` them directly. The `/metrics` HTTP endpoint is
// served by Handler().
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	KVWrites = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "theseon_kv_writes_total",
			Help: "Total KV writes, labeled by operation and engine mode.",
		},
		[]string{"op", "mode"},
	)

	KVReads = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "theseon_kv_reads_total",
			Help: "Total KV reads, labeled by result (hit|miss|tombstone) and engine mode.",
		},
		[]string{"result", "mode"},
	)

	KVWriteLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "theseon_kv_write_latency_seconds",
			Help:    "End-to-end latency of KV writes.",
			Buckets: prometheus.ExponentialBuckets(50e-6, 2, 16), // 50us ... ~1.6s
		},
		[]string{"op"},
	)

	KVReadLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "theseon_kv_read_latency_seconds",
			Help:    "End-to-end latency of KV reads.",
			Buckets: prometheus.ExponentialBuckets(50e-6, 2, 16),
		},
	)

	Compactions = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "theseon_compactions_total",
			Help: "Total successfully-completed compactions.",
		},
	)

	SSTableCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "theseon_sstable_count",
			Help: "Number of live SSTables per LSM level.",
		},
		[]string{"level"},
	)

	HintDrainBatches = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "theseon_hint_drain_batches_total",
			Help: "Hinted-handoff drain batches replayed, labeled by result.",
		},
		[]string{"result"},
	)

	ClusterRPCDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "theseon_cluster_rpc_duration_seconds",
			Help:    "Duration of internal replication RPC handlers.",
			Buckets: prometheus.ExponentialBuckets(100e-6, 2, 16), // 100us ... ~3.2s
		},
		[]string{"rpc"},
	)

	// --- Anti-entropy ---

	AEDivergentLeaves = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "theseon_ae_divergent_leaves_total",
			Help: "Total Merkle-tree leaves found divergent across all reconciles.",
		},
	)

	AEKeysRepaired = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "theseon_ae_keys_repaired_total",
			Help: "Keys repaired by anti-entropy, labeled by direction.",
		},
		[]string{"direction"}, // push | pull
	)

	AEKeysScanned = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "theseon_ae_keys_scanned_total",
			Help: "Keys scanned during anti-entropy tree builds.",
		},
	)

	AEReconcileDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "theseon_ae_reconcile_duration_seconds",
			Help:    "End-to-end duration of a single anti-entropy reconcile round.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 16), // 10ms ... ~5min
		},
	)
)

func init() {
	prometheus.MustRegister(
		KVWrites,
		KVReads,
		KVWriteLatency,
		KVReadLatency,
		Compactions,
		SSTableCount,
		HintDrainBatches,
		ClusterRPCDuration,
		AEDivergentLeaves,
		AEKeysRepaired,
		AEKeysScanned,
		AEReconcileDuration,
	)
}

// Handler returns the HTTP handler that serves Prometheus exposition format.
func Handler() http.Handler {
	return promhttp.Handler()
}
