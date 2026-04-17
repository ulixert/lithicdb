package cluster

import (
	"context"
	"sync"
	"time"
)

// broadcast is an entry in the gossip retransmit buffer.
// Each state change is piggybacked on outgoing messages until
// the retransmit counter reaches zero.
type broadcast struct {
	state       MemberState
	retransmits int
}

// livenessEvent captures a liveness transition for deferred callback
// invocation outside the membership lock.
type livenessEvent struct {
	nodeID string
	from   LivenessState
	to     LivenessState
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

	// Gossip retransmit buffer - recent state changes to piggyback
	// on outgoing messages. Items stay until their retransmit counter
	// reaches zero (infection-style dissemination).
	broadcasts []broadcast

	// Suspect timers - one per suspected node. Fires SuspectTimeout
	// after a node enters the Suspect state. Canceled on refutation.
	suspectTimers map[string]*time.Timer

	// Probe target ordering - round-robin over a shuffled list.
	// Re-shuffled when a full round completes or membership changes.
	probeOrder    []string // nodeIDs
	probeIdx      int
	probeOrderGen uint64 // generation when probe order was last built

	// memberGen increments on every membership change (add, liveness
	// transition). Used to detect when the probe order is stale
	// without the ABA problem of counting members.
	memberGen uint64

	// Dead node reaping - tracks when nodes entered Dead state.
	deadSince map[string]time.Time

	// Ring descriptor - versioned, only mutated by admin commands.
	// Disseminated via gossip piggybacking on ping/ack and GossipSync.
	ringDesc RingDescriptor

	// Callbacks - fired outside the lock to prevent deadlocks.
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
		deadSince:     make(map[string]time.Time),
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

// OnLivenessChange registers a callback invoked when a node's liveness
// state changes. The callback is fired outside the membership lock.
func (m *Membership) OnLivenessChange(fn func(nodeID string, from, to LivenessState)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onLivenessChange = fn
}

// OnRingChange registers a callback invoked when the ring descriptor
// changes. The callback is fired outside the membership lock.
func (m *Membership) OnRingChange(fn func(RingDescriptor)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onRingChange = fn
}

// --- Query methods ---

// IsAlive returns true if the given node is in the Alive state.
// For routing decisions, use IsRoutable instead - it treats Suspect
// nodes as reachable.
func (m *Membership) IsAlive(nodeID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ms, ok := m.members[nodeID]
	return ok && ms.Liveness == Alive
}

// IsRoutable returns true if the node is reachable enough for the
// coordinator to attempt RPCs. Suspect nodes are treated as routable
// because a single dropped ping doesn't mean the node is unreachable
// for data-plane traffic. Only Dead nodes are unroutable.
func (m *Membership) IsRoutable(nodeID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ms, ok := m.members[nodeID]
	return ok && ms.Liveness != Dead
}

// AliveMembers returns all members in the Alive state.
func (m *Membership) AliveMembers() []MemberState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []MemberState
	for _, ms := range m.members {
		if ms.Liveness == Alive {
			result = append(result, *ms)
		}
	}
	return result
}

// ActiveRingMembers returns members that are both Alive and RingActive.
func (m *Membership) ActiveRingMembers() []MemberState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []MemberState
	for _, ms := range m.members {
		if ms.Liveness == Alive && ms.Ring == RingActive {
			result = append(result, *ms)
		}
	}
	return result
}

// RingStateOf returns the ring state of the given node.
// Returns RingNone if the node is unknown.
func (m *Membership) RingStateOf(nodeID string) RingState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if ms, ok := m.members[nodeID]; ok {
		return ms.Ring
	}
	return RingNone
}

// AddrOf returns the network address of the given node, or an empty
// string if the node is unknown.
func (m *Membership) AddrOf(nodeID string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if ms, ok := m.members[nodeID]; ok {
		return ms.Addr
	}
	return ""
}

// GetMembers returns a snapshot of all known members.
func (m *Membership) GetMembers() []MemberState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]MemberState, 0, len(m.members))
	for _, ms := range m.members {
		result = append(result, *ms)
	}
	return result
}

// GetRingDescriptor returns the current versioned ring descriptor.
func (m *Membership) GetRingDescriptor() RingDescriptor {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ringDesc
}

// ApplyRingDescriptor applies a new ring descriptor if its version
// is higher than the current one. Updates member ring states and
// queues gossip broadcasts for each changed member.
// The onRingChange callback is fired outside the lock.
func (m *Membership) ApplyRingDescriptor(rd RingDescriptor) error {
	var ringCb func(RingDescriptor)

	m.mu.Lock()

	if rd.Version <= m.ringDesc.Version {
		m.mu.Unlock()
		return nil // stale, ignore
	}

	m.applyRingDescLocked(rd)
	ringCb = m.onRingChange

	m.mu.Unlock()

	// Fire callback outside the lock to prevent deadlocks.
	if ringCb != nil {
		ringCb(rd)
	}

	return nil
}

// applyRingDescLocked applies a ring descriptor. Caller must hold m.mu
// and must have verified rd.Version > m.ringDesc.Version.
// Does NOT fire onRingChange — caller does that outside the lock.
func (m *Membership) applyRingDescLocked(rd RingDescriptor) {
	ringMembers := make(map[string]RingState, len(rd.Members))
	for _, rm := range rd.Members {
		ringMembers[rm.NodeID] = rm.State
	}

	for nodeID, ms := range m.members {
		newState, inRing := ringMembers[nodeID]
		if !inRing {
			newState = RingNone
		}
		if ms.Ring != newState {
			ms.Ring = newState
			m.queueBroadcastLocked(*ms)
		}
	}

	m.ringDesc = rd
}

// ringStateFromDescriptor looks up the ring state for a node from the
// current ring descriptor. Returns RingNone if the node is not in the ring.
func (m *Membership) ringStateFromDescriptor(nodeID string) RingState {
	for _, rm := range m.ringDesc.Members {
		if rm.NodeID == nodeID {
			return rm.State
		}
	}
	return RingNone
}
