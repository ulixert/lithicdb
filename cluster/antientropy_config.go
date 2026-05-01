package cluster

import "time"

// AntiEntropyConfig configures the Merkle-tree anti-entropy background
// reconciler. Anti-entropy catches silent divergence that read repair
// and hinted handoff miss: cold keys, expired hints, and transient
// failures where quorum succeeded but a replica silently missed the
// write.
//
// Disabled by default. Manual reconciles via the admin RPC still work
// when Enabled is false.
type AntiEntropyConfig struct {
	// Enabled turns on the periodic background reconciler and the
	// on-recovery trigger. The admin-triggered path is always available.
	Enabled bool

	// Interval is the time between periodic reconcile ticks. Each tick
	// reconciles one peer. Default: 10m.
	Interval time.Duration

	// Depth is the Merkle tree depth. Total leaf buckets = Fanout^Depth.
	// Default: 4 (65,536 buckets with fanout 16).
	Depth int

	// Fanout is the number of children per internal node. Default: 16.
	Fanout int

	// GracePeriod excludes keys whose HLC wall time is within this
	// duration of now. Prevents in-flight writes from appearing as
	// divergence. Default: 30s.
	GracePeriod time.Duration

	// MaxConcurrent caps simultaneous reconciles across all peers.
	// Default: 2.
	MaxConcurrent int

	// MaxRepairPerRound caps the number of keys repaired in a single
	// reconcile. Overflow resumes next tick. Default: 10,000.
	MaxRepairPerRound int

	// ScanKeysPerTick controls scan pacing: after this many keys during
	// local tree build, the goroutine yields to avoid starving foreground
	// traffic. Default: 1,000.
	ScanKeysPerTick int
}

// DefaultAntiEntropyConfig returns a disabled configuration with
// production-reasonable defaults applied to the other fields.
func DefaultAntiEntropyConfig() AntiEntropyConfig {
	return AntiEntropyConfig{
		Enabled:           false,
		Interval:          10 * time.Minute,
		Depth:             4,
		Fanout:            16,
		GracePeriod:       30 * time.Second,
		MaxConcurrent:     2,
		MaxRepairPerRound: 10000,
		ScanKeysPerTick:   1000,
	}
}
