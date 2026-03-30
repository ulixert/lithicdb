package cluster

import (
	"context"
	"math"
	"sort"
	"time"
)

// --- Incoming RPC handlers ---

// HandlePing processes an incoming SWIM Ping, merges piggybacked
// gossip updates and ring descriptor, and returns an ack with our own.
func (m *Membership) HandlePing(msg *PingMessage) *PingMessage {
	var events []livenessEvent
	var ringCb func(RingDescriptor)
	var ringDesc RingDescriptor

	m.mu.Lock()
	for _, u := range msg.Updates {
		if ev := m.mergeLocked(u); ev != nil {
			events = append(events, *ev)
		}
	}

	// Apply piggybacked ring descriptor if newer.
	if msg.RingDesc != nil && msg.RingDesc.Version > m.ringDesc.Version {
		m.applyRingDescLocked(*msg.RingDesc)
		ringCb = m.onRingChange
		ringDesc = m.ringDesc
	}

	updates := m.getBroadcastsLocked(m.maxPiggyback())
	localRingDesc := m.ringDesc
	cb := m.onLivenessChange
	m.mu.Unlock()

	if ringCb != nil {
		ringCb(ringDesc)
	}
	m.fireEvents(events, cb)

	return &PingMessage{
		SenderID:   m.selfID,
		SenderAddr: m.cfg.Addr,
		Updates:    updates,
		RingDesc:   &localRingDesc,
	}
}

// HandlePingReq processes an indirect ping request: ping the target
// on behalf of the requester and report whether it acked.
func (m *Membership) HandlePingReq(targetID, targetAddr string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), m.cfg.PingTimeout)
	defer cancel()

	resp, err := m.tp.Ping(ctx, targetAddr, &PingMessage{
		SenderID:   m.selfID,
		SenderAddr: m.cfg.Addr,
	})
	if err != nil {
		return false
	}

	// Merge any updates from the target's response.
	var events []livenessEvent
	m.mu.Lock()
	for _, u := range resp.Updates {
		if ev := m.mergeLocked(u); ev != nil {
			events = append(events, *ev)
		}
	}
	cb := m.onLivenessChange
	m.mu.Unlock()

	m.fireEvents(events, cb)
	return true
}

// HandleGossipSync processes a full state exchange. Merges the
// remote's membership table and ring descriptor, returns our full table
// and ring descriptor.
func (m *Membership) HandleGossipSync(remote []MemberState, remoteRingDesc *RingDescriptor) ([]MemberState, *RingDescriptor) {
	var events []livenessEvent
	var ringCb func(RingDescriptor)
	var ringDesc RingDescriptor

	m.mu.Lock()

	for _, u := range remote {
		if ev := m.mergeLocked(u); ev != nil {
			events = append(events, *ev)
		}
	}

	// Apply remote ring descriptor if newer.
	if remoteRingDesc != nil && remoteRingDesc.Version > m.ringDesc.Version {
		m.applyRingDescLocked(*remoteRingDesc)
		ringCb = m.onRingChange
		ringDesc = m.ringDesc
	}

	result := make([]MemberState, 0, len(m.members))
	for _, ms := range m.members {
		result = append(result, *ms)
	}
	localRingDesc := m.ringDesc
	cb := m.onLivenessChange

	m.mu.Unlock()

	if ringCb != nil {
		ringCb(ringDesc)
	}
	m.fireEvents(events, cb)

	return result, &localRingDesc
}

// fireEvents invokes the liveness callback for each event.
func (m *Membership) fireEvents(events []livenessEvent, cb func(string, LivenessState, LivenessState)) {
	if cb == nil {
		return
	}
	for _, ev := range events {
		cb(ev.nodeID, ev.from, ev.to)
	}
}

// --- Gossip merge ---

// mergeLocked merges a remote member state update into the local table.
// Returns a *livenessEvent if the merge caused a liveness transition
// (so the caller can fire the callback outside the lock), or nil.
//
// Ring state is NOT merged from gossip. It is authoritative only from
// the RingDescriptor. This prevents a stale gossip update from
// overwriting the correct ring state.
//
// Liveness changes are routed through setLivenessLocked, so all side
// effects (deadSince tracking, suspect timer cancelatioon, memberGen
// increment, broadcast queueing) happen in one place - whether the
// change was detected locally or learned via gossip.
//
// Caller must hold m.mu.
func (m *Membership) mergeLocked(remote MemberState) *livenessEvent {
	local, exists := m.members[remote.NodeID]

	if !exists {
		// New node - accept unconditionally, but set ring state
		// from our ring descriptor (not from the gossip update).
		state := remote
		state.Ring = m.ringStateFromDescriptor(remote.NodeID)
		m.members[remote.NodeID] = &state
		m.memberGen++
		// New node with non-Alive state → record via setLivenessLocked
		// for proper deadSince tracking. But the node was just added,
		// so there's no "from" state to transition from - only track
		// Dead for reaping.
		if state.Liveness == Dead {
			m.deadSince[remote.NodeID] = time.Now()
		}
		return nil
	}

	// Self-refutation: if someone thinks we're Suspect or Dead,
	// increment our incarnation and broadcast that we're Alive.
	if remote.NodeID == m.selfID && remote.Liveness > Alive {
		if remote.Incarnation >= local.Incarnation {
			local.Incarnation = remote.Incarnation + 1
			local.Liveness = Alive
			m.queueBroadcastLocked(*local)
		}
		return nil
	}

	// Higher incarnation always wins for addr + incarnation.
	// Ring state is preserved - only the RingDescriptor can change it.
	// Liveness changes are routed through setLivenessLocked for proper
	// side effects (deadSince, suspect timer, callback).
	if remote.Incarnation > local.Incarnation {
		localRing := local.Ring
		newLiveness := remote.Liveness
		oldLiveness := local.Liveness

		// Apply non-liveness fields (addr, incarnation).
		*local = remote
		local.Ring = localRing
		local.Liveness = oldLiveness
		m.memberGen++

		if newLiveness != oldLiveness {
			// setLivenessLocked handles: liveness mutation, deadSince,
			// suspect timer, memberGen, queueBroadcast.
			return m.setLivenessLocked(remote.NodeID, newLiveness)
		}
		// No liveness change - still broadcast the updated incarnation/addr.
		m.queueBroadcastLocked(*local)
		return nil
	}

	// Same incarnation: higher liveness state wins (Dead > Suspect > Alive).
	if remote.Incarnation == local.Incarnation && remote.Liveness > local.Liveness {
		return m.setLivenessLocked(remote.NodeID, remote.Liveness)
	}

	return nil
}

// --- Gossip retransmit buffer ---

func (m *Membership) maxPiggyback() int {
	if m.cfg.MaxPiggyback > 0 {
		return m.cfg.MaxPiggyback
	}
	return defaultMaxPiggyback
}

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

	// Cap buffer size to prevent unbounded growth. Keep the items
	// with the highest retransmit counts (freshest, least disseminated).
	maxBroadcasts := m.cfg.MaxBroadcasts
	if maxBroadcasts <= 0 {
		maxBroadcasts = 128
	}
	if len(m.broadcasts) > maxBroadcasts {
		sort.Slice(m.broadcasts, func(i, j int) bool {
			return m.broadcasts[i].retransmits > m.broadcasts[j].retransmits
		})
		m.broadcasts = m.broadcasts[:maxBroadcasts]
	}
}

// getBroadcastsLocked returns up to limit updates for piggybacking,
// prioritizing the freshest gossip (highest remaining retransmit count).
// Only piggybacked items have their retransmit counters decremented;
// items that didn't fit keep their full count for the next round.
// Exhausted entries (retransmits <= 0) are removed.
// Caller must hold m.mu (write lock — this mutates broadcasts).
func (m *Membership) getBroadcastsLocked(limit int) []MemberState {
	if len(m.broadcasts) == 0 {
		return nil
	}

	// Sort by retransmit count descending - freshest gossip first.
	sort.Slice(m.broadcasts, func(i, j int) bool {
		return m.broadcasts[i].retransmits > m.broadcasts[j].retransmits
	})

	n := limit
	if n > len(m.broadcasts) {
		n = len(m.broadcasts)
	}

	result := make([]MemberState, 0, n)
	alive := m.broadcasts[:0]

	for i := range m.broadcasts {
		if len(result) < limit {
			result = append(result, m.broadcasts[i].state)
			m.broadcasts[i].retransmits--
		}
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
	localRingDesc := m.ringDesc
	m.mu.RUnlock()

	remote, remoteRingDesc, err := m.tp.GossipSync(ctx, addr, local, &localRingDesc)
	if err != nil {
		return // seed unreachable - not fatal, try others
	}

	var events []livenessEvent
	var ringCb func(RingDescriptor)
	var ringDesc RingDescriptor

	m.mu.Lock()
	for _, u := range remote {
		if ev := m.mergeLocked(u); ev != nil {
			events = append(events, *ev)
		}
	}
	if remoteRingDesc != nil && remoteRingDesc.Version > m.ringDesc.Version {
		m.applyRingDescLocked(*remoteRingDesc)
		ringCb = m.onRingChange
		ringDesc = m.ringDesc
	}
	cb := m.onLivenessChange
	m.mu.Unlock()

	if ringCb != nil {
		ringCb(ringDesc)
	}
	m.fireEvents(events, cb)
}
