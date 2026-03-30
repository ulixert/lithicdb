package cluster

import (
	"context"
	"math/rand/v2"
	"time"
)

// --- SWIM probe loop ---

// defaultMaxPiggyback is the default max gossip updates per message.
const defaultMaxPiggyback = 10

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
	// Reap dead+RingNone nodes that have exceeded the reap timeout.
	m.reapDeadNodes()

	target := m.nextProbeTarget()
	if target == nil {
		return
	}

	// Direct ping.
	ctx, cancel := context.WithTimeout(context.Background(), m.cfg.PingTimeout)
	defer cancel()

	// Collect broadcasts under write lock (getBroadcastsLocked mutates).
	m.mu.Lock()
	updates := m.getBroadcastsLocked(m.maxPiggyback())
	localRingDesc := m.ringDesc
	m.mu.Unlock()

	resp, err := m.tp.Ping(ctx, target.Addr, &PingMessage{
		SenderID:   m.selfID,
		SenderAddr: m.cfg.Addr,
		Updates:    updates,
		RingDesc:   &localRingDesc,
	})

	if err == nil {
		// Direct ping succeeded - merge response and ensure alive.
		var events []livenessEvent

		m.mu.Lock()
		for _, u := range resp.Updates {
			if ev := m.mergeLocked(u); ev != nil {
				events = append(events, *ev)
			}
		}
		// Apply piggybacked ring descriptor if newer.
		var ringCb func(RingDescriptor)
		var ringDesc RingDescriptor
		if resp.RingDesc != nil && resp.RingDesc.Version > m.ringDesc.Version {
			m.applyRingDescLocked(*resp.RingDesc)
			ringCb = m.onRingChange
			ringDesc = m.ringDesc
		}
		// If target was Suspect, transition back to Alive.
		if ms, ok := m.members[target.NodeID]; ok && ms.Liveness == Suspect {
			if ev := m.setLivenessLocked(target.NodeID, Alive); ev != nil {
				events = append(events, *ev)
			}
		}
		cb := m.onLivenessChange
		m.mu.Unlock()

		if ringCb != nil {
			ringCb(ringDesc)
		}
		m.fireEvents(events, cb)
		return
	}

	// Direct ping failed - try indirect pings.
	if m.indirectPing(target) {
		m.mu.Lock()
		var event *livenessEvent
		if ms, ok := m.members[target.NodeID]; ok && ms.Liveness == Suspect {
			event = m.setLivenessLocked(target.NodeID, Alive)
		}
		cb := m.onLivenessChange
		m.mu.Unlock()

		if event != nil && cb != nil {
			cb(event.nodeID, event.from, event.to)
		}
		return
	}

	// All pings failed - suspect the target.
	m.mu.Lock()
	var event *livenessEvent
	if ms, ok := m.members[target.NodeID]; ok && ms.Liveness == Alive {
		event = m.setLivenessLocked(target.NodeID, Suspect)
		m.startSuspectTimerLocked(target.NodeID)
	}
	cb := m.onLivenessChange
	m.mu.Unlock()

	if event != nil && cb != nil {
		cb(event.nodeID, event.from, event.to)
	}
}

// indirectPing asks up to K random alive peers to ping the target
// on our behalf. Returns true if any peer reports success.
func (m *Membership) indirectPing(target *MemberState) bool {
	m.mu.RLock()
	peers := m.randomAlivePeersLocked(m.cfg.IndirectPeers, target.NodeID)
	m.mu.RUnlock()

	if len(peers) == 0 {
		return false
	}

	type result struct{ ack bool }
	results := make(chan result, len(peers))

	for _, peer := range peers {
		go func(addr string) {
			ctx, cancel := context.WithTimeout(context.Background(), m.cfg.PingTimeout)
			defer cancel()
			ack, err := m.tp.PingReq(ctx, addr, target.NodeID, target.Addr)
			results <- result{ack: err == nil && ack}
		}(peer.Addr)
	}

	for range peers {
		r := <-results
		if r.ack {
			return true
		}
	}

	return false
}

// --- Probe target selection ---

// nextProbeTarget returns the next member to probe using round-robin
// over a shuffled list. Returns nil if there are no probe targets.
func (m *Membership) nextProbeTarget() *MemberState {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Rebuild probe order if exhausted or membership changed.
	if m.probeIdx >= len(m.probeOrder) || m.probeOrderGen != m.memberGen {
		m.rebuildProbeOrderLocked()
	}

	if len(m.probeOrder) == 0 {
		return nil
	}

	nodeID := m.probeOrder[m.probeIdx]
	m.probeIdx++

	ms, ok := m.members[nodeID]
	if !ok || ms.Liveness == Dead {
		// Skip dead or removed nodes - try the next one.
		return m.nextProbeTargetLocked()
	}

	return new(*ms)
}

func (m *Membership) nextProbeTargetLocked() *MemberState {
	for m.probeIdx < len(m.probeOrder) {
		nodeID := m.probeOrder[m.probeIdx]
		m.probeIdx++
		if ms, ok := m.members[nodeID]; ok && ms.Liveness != Dead {
			return new(*ms)
		}
	}
	return nil
}

func (m *Membership) rebuildProbeOrderLocked() {
	m.probeOrder = m.probeOrder[:0]
	for id, ms := range m.members {
		if id != m.selfID && ms.Liveness != Dead {
			m.probeOrder = append(m.probeOrder, id)
		}
	}
	rand.Shuffle(len(m.probeOrder), func(i, j int) {
		m.probeOrder[i], m.probeOrder[j] = m.probeOrder[j], m.probeOrder[i]
	})
	m.probeIdx = 0
	m.probeOrderGen = m.memberGen
}

// randomAlivePeersLocked returns up to k random alive peers, excluding
// self and excludeID. Uses partial Fisher-Yates: only shuffles the
// first k positions, O(N) to build candidates + O(k) to shuffle.
func (m *Membership) randomAlivePeersLocked(k int, excludeID string) []MemberState {
	var candidates []MemberState
	for id, ms := range m.members {
		if id != m.selfID && id != excludeID && ms.Liveness == Alive {
			candidates = append(candidates, *ms)
		}
	}
	if len(candidates) <= k {
		return candidates
	}
	// Partial Fisher-Yates: swap first k positions only.
	for i := 0; i < k; i++ {
		j := i + rand.IntN(len(candidates)-i)
		candidates[i], candidates[j] = candidates[j], candidates[i]
	}
	return candidates[:k]
}

// --- State transitions ---

// setLivenessLocked transitions a node's liveness state and returns
// a livenessEvent for deferred callback invocation. Returns nil if
// no transition occurred.
//
// Incarnation is NOT bumped - in SWIM, only a node itself increments
// its own incarnation (to refute suspicion). Liveness changes by
// other nodes are resolved via the merge rule: at the same incarnation,
// higher liveness state wins (Dead > Suspect > Alive).
func (m *Membership) setLivenessLocked(nodeID string, to LivenessState) *livenessEvent {
	ms, ok := m.members[nodeID]
	if !ok {
		return nil
	}

	from := ms.Liveness
	if from == to {
		return nil
	}

	ms.Liveness = to
	m.memberGen++

	// Track when nodes enter Dead state for reaping.
	if to == Dead {
		m.deadSince[nodeID] = time.Now()
	} else {
		delete(m.deadSince, nodeID)
	}

	// Cancel suspect timer if transitioning away from Suspect.
	if from == Suspect {
		if timer, ok := m.suspectTimers[nodeID]; ok {
			timer.Stop()
			delete(m.suspectTimers, nodeID)
		}
	}

	m.queueBroadcastLocked(*ms)

	return &livenessEvent{nodeID: nodeID, from: from, to: to}
}

func (m *Membership) startSuspectTimerLocked(nodeID string) {
	// Don't create duplicate timers.
	if _, exists := m.suspectTimers[nodeID]; exists {
		return
	}

	m.suspectTimers[nodeID] = time.AfterFunc(m.cfg.SuspectTimeout, func() {
		m.mu.Lock()
		ms, ok := m.members[nodeID]
		if !ok || ms.Liveness != Suspect {
			m.mu.Unlock()
			return // already refuted or removed
		}
		event := m.setLivenessLocked(nodeID, Dead)
		cb := m.onLivenessChange
		m.mu.Unlock()

		if event != nil && cb != nil {
			cb(event.nodeID, event.from, event.to)
		}
	})
}

// reapDeadNodes removes Dead+RingNone nodes that have exceeded the
// reap timeout. Nodes that are Dead but still in the ring (RingJoining
// or RingActive) are kept - they're needed for hinted handoff and
// admin remove tracking.
func (m *Membership) reapDeadNodes() {
	reapTimeout := m.cfg.DeadNodeReapTimeout
	if reapTimeout <= 0 {
		return // reaping disabled
	}

	now := time.Now()

	m.mu.Lock()
	defer m.mu.Unlock()

	for nodeID, deadAt := range m.deadSince {
		if now.Sub(deadAt) < reapTimeout {
			continue
		}
		ms, ok := m.members[nodeID]
		if !ok {
			delete(m.deadSince, nodeID)
			continue
		}
		// Only reap if still Dead AND not in the ring.
		if ms.Liveness == Dead && ms.Ring == RingNone {
			delete(m.members, nodeID)
			delete(m.deadSince, nodeID)
			m.memberGen++
		}
	}
}
