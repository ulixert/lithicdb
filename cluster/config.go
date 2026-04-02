// Package cluster implements the distributed layer for Theseon using
// the SWIM protocol for decentralized failure detection and gossip-based
// membership dissemination. Ring ownership is tracked separately and
// only changes via explicit admin commands.
package cluster

import "time"

// ClusterConfig holds the configuration for a cluster node.
type ClusterConfig struct {
	// NodeID is the unique identifier for this node.
	NodeID string

	// Addr is this node's advertised gRPC address (e.g. "10.0.0.1:9090").
	Addr string

	// SeedPeers are addresses of initial peers to contact for discovery.
	// The node will GossipSync with each seed on the startup to learn
	// about the rest of the cluster. Only 1-2 seeds are needed - gossip
	// discovers the rest.
	SeedPeers []string

	// GossipInterval is the time between SWIM probe rounds. Each round
	// picks one random member to probe. Default: 1s.
	GossipInterval time.Duration

	// PingTimeout is the deadline for a direct Ping RPC. If the target
	// doesn't respond within this window, the node falls back to
	// indirect pinging. Default: 500ms.
	PingTimeout time.Duration

	// SuspectTimeout is how long a node stays in the Suspect state before
	// being declared Dead. During this window, the suspected node can be
	// refuted by incrementing its incarnation number. Default: 5s.
	SuspectTimeout time.Duration

	// IndirectPeers is the number of random peers (K) asked to perform
	// indirect pings when a direct ping times out. Default: 3.
	IndirectPeers int

	// RetransmitMult controls how many times a gossip update is
	// piggybacked on outgoing messages. Each update is retransmitted
	// RetransmitMult * ceil(log2(N)) times, where N is the cluster
	// size. Higher values improve convergence speed at the cost of
	// bandwidth. Default: 4.
	RetransmitMult int

	// MaxPiggyback is the max gossip updates piggybacked on each
	// outgoing ping/ack message. Higher values speed up convergence
	// but increase per-message size. Default: 10.
	MaxPiggyback int

	// MaxBroadcasts caps the gossip retransmit buffer size. When the
	// buffer exceeds this limit, oldest entries are dropped. Default: 128.
	MaxBroadcasts int

	// DeadNodeReapTimeout is how long a Dead+RingNone node stays in the
	// members map before being reaped. A Dead node that is still in the
	// ring (RingJoining/RingActive) is never reaped — it stays for
	// hinted handoff and admin remove tracking. Default: 24h.
	DeadNodeReapTimeout time.Duration
}

// DefaultClusterConfig returns a ClusterConfig with sane defaults.
func DefaultClusterConfig(nodeID, addr string) ClusterConfig {
	return ClusterConfig{
		NodeID:              nodeID,
		Addr:                addr,
		GossipInterval:      1 * time.Second,
		PingTimeout:         500 * time.Millisecond,
		SuspectTimeout:      5 * time.Second,
		IndirectPeers:       3,
		RetransmitMult:      4,
		MaxPiggyback:        10,
		MaxBroadcasts:       128,
		DeadNodeReapTimeout: 24 * time.Hour,
	}
}
