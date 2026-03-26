package cluster

import (
	"context"
	"sync"
	"time"
)

// broadcast is an entry in the gossip dissemination queue.
// Each state change is piggybacked on outgoing messages until
// the retransmit counter reaches zero.
type broadcast struct {
	state       MemberState
	retransmits int
}

// Membership implements the SWIM protocol for decentralized failure
// detection and gossip-based membership dissemination.
//
// Two independent axes are tracked per node:
//   - Liveness (SWIM, automatic): Alive → Suspect → Dead
//   - Ring ownership (admin, explicit): RingNone → RingJoining → RingActive
//
// SWIM state transitions never modify ring ownership.
type Membership struct {
	mu      sync.RWMutex
	cfg     ClusterConfig
	tp      Transport
	members map[string]*MemberState // nodeID → state
	selfID  string

	// Gossip broadcast queue - recent state changes to piggyback
	// on outgoing messages.
	broadcasts []broadcast

	// Suspect timers - one per suspected node. Fires SuspectTimeout
	// after a node enters the Suspect state. Canceled on refutation.
	suspectTimers map[string]*time.Timer

	// Probe target ordering - round-robin over a shuffled list.
	// Re-shuffled when a full round completes or membership changes.
	probeOrder []string // nodeIDs
	probeIdx   int

	// Ring descriptor - versioned, only mutated by admin commands.
	ringDesc RingDescriptor

	// Callbacks
	onLivenessChange func(nodeID string, from, to LivenessState)
	onRingChange     func(RingDescriptor)

	// Lifecycle
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewMembership creates a new SWIM membership tracker. The node
// adds itself as Alive. Call Start to begin the probe loop and
// discover seed peers.
func NewMembership(cfg ClusterConfig, transport Transport) *Membership {
	m := &Membership{
		cfg:           cfg,
		tp:            transport,
		members:       make(map[string]*MemberState),
		selfID:        cfg.NodeID,
		suspectTimers: make(map[string]*time.Timer),
		stopCh:        make(chan struct{}),
	}

	// Add self as the first alive member.
	self := &MemberState{
		NodeID:      cfg.NodeID,
		Addr:        cfg.Addr,
		Liveness:    Alive,
		Ring:        RingNone,
		Incarnation: 1,
	}
	m.members[cfg.NodeID] = self

	return m
}

// Start begins the SWIM probe loop and discovers seed peers.
// Call Stop to shut down.
func (m *Membership) Start(ctx context.Context) error {
	// Discover cluster via seed peers.
	for _, addr := range m.cfg.SeedPeers {
		m.syncWithPeer(ctx, addr)
	}

	m.wg.Add(1)
	go m.probeLoop()

	return nil
}

// Stop shuts down the probe loop and cancels all suspect timers.
func (m *Membership) Stop() {
	close(m.stopCh)
	m.wg.Wait()

	m.mu.Lock()
	defer m.mu.Unlock()
	for id, timer := range m.suspectTimers {
		timer.Stop()
		delete(m.suspectTimers, id)
	}
}
