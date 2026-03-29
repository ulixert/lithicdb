package cluster

import (
	"context"
	"math"
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

// OnLivenessChange registers a callback invoked when a node's liveness
// state changes. The callback is called under the membership lock -
// it must not block.
func (m *Membership) OnLivenessChange(fn func(nodeID string, from, to LivenessState)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onLivenessChange = fn
}

// OnRingChange registers a callback invoked when the ring descriptor
// changes. The callback is called under the membership lock.
func (m *Membership) OnRingChange(fn func(RingDescriptor)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onRingChange = fn
}

// --- Query methods ---

// IsAlive returns true if the given node is in the Alive state.
func (m *Membership) IsAlive(nodeID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ms, ok := m.members[nodeID]
	return ok && ms.Liveness == Alive
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
func (m *Membership) ApplyRingDescriptor(rd RingDescriptor) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if rd.Version <= m.ringDesc.Version {
		return nil // stale, ignore
	}

	// Build a set of ring members from the new descriptor.
	ringMembers := make(map[string]RingState, len(rd.Members))
	for _, rm := range rd.Members {
		ringMembers[rm.NodeID] = rm.State
	}

	// Update member ring states.
	// Ring state is NOT propagated via incarnation - incarnation is purely
	// a SWIM liveness concept (only a node increments its own incarnation
	// to refute suspicion). Ring state is conveyed via the RingDescriptor's
	// Version field. We still broadcast the updated member so peers learn
	// the ring state change, but without bumping incarnation.
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

	if m.onRingChange != nil {
		m.onRingChange(rd)
	}

	return nil
}

// --- SWIM probe loop ---

const maxPiggyback = 10 // max gossip updates per message

func (m *Membership) probeLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.cfg.GossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.probe()
		}
	}
}

// probe runs a single SWIM probe cycle: pick a target, direct ping,
// indirect ping on failure, suspect/dead transitions.
func (m *Membership) probe() {
	target := m.nextProbeTarget()
	if target == nil {
		return
	}

	// Direct ping.
	ctx, cancel := context.WithTimeout(context.Background(), m.cfg.PingTimeout)
	defer cancel()

	m.mu.RLock()
	updates := m.getBroadcastsLocked(maxPiggyback)
	m.mu.RUnlock()

	resp, err := m.tp.Ping(ctx, target.Addr, &PingMessage{
		SenderID:   m.selfID,
		SenderAddr: m.cfg.Addr,
		Updates:    updates,
	})

	if err == nil {
		// Direct ping succeeded - merge response and ensure alive.
		m.mu.Lock()
		for _, u := range resp.Updates {
			m.mergeLocked(u)
		}
		// If target was Suspect, refute it back to Alive.
		if ms, ok := m.members[target.NodeID]; ok && ms.Liveness == Suspect {
			m.setLivenessLocked(target.NodeID, Alive)
		}
		m.mu.Unlock()
		return
	}

	// Direct ping failed - try indirect pings.
	if m.indirectPing(target) {
		// Indirect ping succeeded.
		m.mu.Lock()
		if ms, ok := m.members[target.NodeID]; ok && ms.Liveness == Suspect {
			m.setLivenessLocked(target.NodeID, Alive)
		}
		m.mu.Unlock()
		return
	}

	// All pings failed - suspect the target.
	m.mu.Lock()
	if ms, ok := m.members[target.NodeID]; ok && ms.Liveness == Alive {
		m.setLivenessLocked(target.NodeID, Suspect)
		m.startSuspectTimerLocked(target.NodeID)
	}
	m.mu.Unlock()
}

// --- State transitions ---

func (m *Membership) setLivenessLocked(nodeID string, to LivenessState) {
	ms, ok := m.members[nodeID]
	if !ok {
		return
	}

	from := ms.Liveness
	if from == to {
		return
	}

	ms.Liveness = to

	// Cancel suspect timer if transitioning away from Suspect.
	if from == Suspect {
		if timer, ok := m.suspectTimers[nodeID]; ok {
			timer.Stop()
			delete(m.suspectTimers, nodeID)
		}
	}

	m.queueBroadcastLocked(*ms)

	if m.onLivenessChange != nil {
		m.onLivenessChange(nodeID, from, to)
	}
}

func (m *Membership) startSuspectTimerLocked(nodeID string) {
	// Don't create duplicate timers.
	if _, exists := m.suspectTimers[nodeID]; exists {
		return
	}

	m.suspectTimers[nodeID] = time.AfterFunc(m.cfg.SuspectTimeout, func() {
		m.mu.Lock()
		defer m.mu.Unlock()

		ms, ok := m.members[nodeID]
		if !ok || ms.Liveness != Suspect {
			return // already refuted or removed
		}

		m.setLivenessLocked(nodeID, Dead)
	})
}

// --- Gossip merge ---

// mergeLocked merges a remote member state update into the local table.
// Returns true if the update was applied (newer than local state).
// Caller must hold m.mu.
func (m *Membership) mergeLocked(remote MemberState) bool {
	local, exists := m.members[remote.NodeID]

	if !exists {
		// New node - accept unconditionally.
		m.members[remote.NodeID] = new(remote)
		return true
	}

	// Self-refutation: if someone thinks we're Suspect or Dead,
	// increment our incarnation and broadcast that we're Alive.
	if remote.NodeID == m.selfID && remote.Liveness > Alive {
		if remote.Incarnation >= local.Incarnation {
			local.Incarnation = remote.Incarnation + 1
			local.Liveness = Alive
			m.queueBroadcastLocked(*local)
		}
		return false // we refuted - the remote state was not applied
	}

	// Higher incarnation always wins.
	if remote.Incarnation > local.Incarnation {
		*local = remote
		return true
	}

	// Same incarnation: higher liveness state wins (Dead > Suspect > Alive).
	// This ensures convergence toward detecting failures.
	if remote.Incarnation == local.Incarnation && remote.Liveness > local.Liveness {
		local.Liveness = remote.Liveness
		return true
	}

	return false // stale
}

// --- Gossip broadcast queue ---

// retransmitLimit returns how many times a broadcast should be
// piggybacked: RetransmitMult * ceil(log2(N)).
func (m *Membership) retransmitLimit() int {
	n := len(m.members)
	if n < 2 {
		return m.cfg.RetransmitMult
	}
	return m.cfg.RetransmitMult * int(math.Ceil(math.Log2(float64(n))))
}

func (m *Membership) queueBroadcastLocked(state MemberState) {
	m.broadcasts = append(m.broadcasts, broadcast{
		state:       state,
		retransmits: m.retransmitLimit(),
	})

	// Cap queue size to prevent unbounded growth. Keep the most recent
	// entries (highest retransmit counts).
	const maxBroadcasts = 128
	if len(m.broadcasts) > maxBroadcasts {
		m.broadcasts = m.broadcasts[len(m.broadcasts)-maxBroadcasts:]
	}
}

// getBroadcastsLocked returns up to limit updates for piggybacking.
// Decrements retransmit counters and remove exhausted entries.
// Caller must hold at least m.mu.RLock (but we mutate broadcasts,
// so this should be called under write lock in practice).
func (m *Membership) getBroadcastsLocked(limit int) []MemberState {
	if len(m.broadcasts) == 0 {
		return nil
	}

	n := limit
	if n > len(m.broadcasts) {
		n = len(m.broadcasts)
	}

	result := make([]MemberState, 0, n)
	alive := m.broadcasts[:0]

	for i := range m.broadcasts {
		if len(result) < limit {
			result = append(result, m.broadcasts[i].state)
		}
		m.broadcasts[i].retransmits--
		if m.broadcasts[i].retransmits > 0 {
			alive = append(alive, m.broadcasts[i])
		}
	}

	m.broadcasts = alive
	return result
}

// --- Discovery ---

func (m *Membership) syncWithPeer(ctx context.Context, addr string) {
	m.mu.RLock()
	local := make([]MemberState, 0, len(m.members))
	for _, ms := range m.members {
		local = append(local, *ms)
	}
	m.mu.RUnlock()

	remote, err := m.tp.GossipSync(ctx, addr, local)
	if err != nil {
		return // seed unreachable - not fatal, try others
	}

	m.mu.Lock()
	for _, u := range remote {
		m.mergeLocked(u)
	}
	m.mu.Unlock()
}
