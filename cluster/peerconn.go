package cluster

import (
	"errors"
	"fmt"
	"sync"
	"time"

	pb "github.com/ulixert/lithicdb/proto/lithicpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var ErrPoolClosed = errors.New("peer pool is closed")

// ReplicaDialer abstracts connection management for data-plane RPCs.
// PeerPool is the production implementation; tests can supply a mock.
type ReplicaDialer interface {
	GetClient(addr string) (pb.InternalServiceClient, error)
	Close()
}

// PeerPoolConfig tunes the data-plane connection pool.
type PeerPoolConfig struct {
	// IdleTimeout closes connections that have not been used for this
	// duration. Zero disables idle eviction.
	IdleTimeout time.Duration
}

// DefaultPeerPoolConfig returns sensible defaults.
func DefaultPeerPoolConfig() PeerPoolConfig {
	return PeerPoolConfig{
		IdleTimeout: 5 * time.Minute,
	}
}

// peerEntry holds a single gRPC connection and its derived client.
type peerEntry struct {
	conn     *grpc.ClientConn
	client   pb.InternalServiceClient
	lastUsed time.Time
}

// PeerPool manages gRPC connections to peer nodes for data-plane RPCs
// (ReplicateWrite, ReplicateRead). It is separate from the SWIM
// GRPCTransport to avoid idle-timeout conflicts with probe traffic.
//
// gRPC connections are multiplexed, so one connection per peer is
// sufficient for concurrent RPCs.
//
// When IdleTimeout > 0, a background goroutine evicts connections that
// have not been used for that duration, preventing unbounded growth as
// nodes join, leave, or change addresses.
type PeerPool struct {
	mu       sync.Mutex
	peers    map[string]*peerEntry
	closed   bool
	cfg      PeerPoolConfig
	dialOpts []grpc.DialOption

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewPeerPool creates a pool with the given config. If IdleTimeout > 0,
// a background goroutine starts immediately to evict idle connections.
func NewPeerPool(cfg PeerPoolConfig, dialOpts ...grpc.DialOption) *PeerPool {
	if len(dialOpts) == 0 {
		dialOpts = []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}
	}
	p := &PeerPool{
		peers:    make(map[string]*peerEntry),
		cfg:      cfg,
		dialOpts: dialOpts,
		stopCh:   make(chan struct{}),
	}
	if cfg.IdleTimeout > 0 {
		p.wg.Add(1)
		go p.cleanupLoop()
	}
	return p
}

// GetClient returns an InternalServiceClient for the given address,
// reusing an existing connection if available. Returns ErrPoolClosed
// after Close has been called.
func (p *PeerPool) GetClient(addr string) (pb.InternalServiceClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, ErrPoolClosed
	}

	if entry, ok := p.peers[addr]; ok {
		entry.lastUsed = time.Now()
		return entry.client, nil
	}

	conn, err := grpc.NewClient(addr, p.dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("peer pool: dial %s: %w", addr, err)
	}

	client := pb.NewInternalServiceClient(conn)
	p.peers[addr] = &peerEntry{
		conn:     conn,
		client:   client,
		lastUsed: time.Now(),
	}
	return client, nil
}

// Close stops the background cleanup goroutine, closes all pooled
// connections, and rejects future GetClient calls with ErrPoolClosed.
func (p *PeerPool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.stopCh)
	p.mu.Unlock()

	p.wg.Wait()

	p.mu.Lock()
	defer p.mu.Unlock()
	for addr, entry := range p.peers {
		_ = entry.conn.Close()
		delete(p.peers, addr)
	}
}

// cleanupLoop periodically evicts connections that have been idle
// longer than cfg.IdleTimeout. Runs at IdleTimeout/2 intervals so
// entries are evicted within at most 1.5 * IdleTimeout.
func (p *PeerPool) cleanupLoop() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.cfg.IdleTimeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.evictIdle()
		case <-p.stopCh:
			return
		}
	}
}

// evictIdle closes and removes connections that have been idle longer
// than cfg.IdleTimeout.
func (p *PeerPool) evictIdle() {
	cutoff := time.Now().Add(-p.cfg.IdleTimeout)
	p.mu.Lock()
	defer p.mu.Unlock()

	for addr, entry := range p.peers {
		if entry.lastUsed.Before(cutoff) {
			_ = entry.conn.Close()
			delete(p.peers, addr)
		}
	}
}
