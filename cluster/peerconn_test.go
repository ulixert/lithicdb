package cluster

import (
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func testDialOpts() []grpc.DialOption {
	return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
}

func TestPeerPool_GetClient_Reuse(t *testing.T) {
	pool := NewPeerPool(DefaultPeerPoolConfig(), testDialOpts()...)
	defer pool.Close()

	c1, err := pool.GetClient("localhost:9999")
	if err != nil {
		t.Fatalf("GetClient: %v", err)
	}
	c2, err := pool.GetClient("localhost:9999")
	if err != nil {
		t.Fatalf("GetClient: %v", err)
	}

	pool.mu.Lock()
	n := len(pool.peers)
	pool.mu.Unlock()
	if n != 1 {
		t.Fatalf("expected 1 pooled connection, got %d", n)
	}
	_ = c1
	_ = c2
}

func TestPeerPool_GetClient_DifferentAddrs(t *testing.T) {
	pool := NewPeerPool(DefaultPeerPoolConfig(), testDialOpts()...)
	defer pool.Close()

	_, err := pool.GetClient("localhost:9001")
	if err != nil {
		t.Fatalf("GetClient 9001: %v", err)
	}
	_, err = pool.GetClient("localhost:9002")
	if err != nil {
		t.Fatalf("GetClient 9002: %v", err)
	}

	pool.mu.Lock()
	n := len(pool.peers)
	pool.mu.Unlock()
	if n != 2 {
		t.Fatalf("expected 2 pooled connections, got %d", n)
	}
}

func TestPeerPool_Close_RejectsNewClients(t *testing.T) {
	pool := NewPeerPool(DefaultPeerPoolConfig(), testDialOpts()...)

	_, _ = pool.GetClient("localhost:9001")
	pool.Close()

	// Connections cleaned up.
	pool.mu.Lock()
	n := len(pool.peers)
	pool.mu.Unlock()
	if n != 0 {
		t.Fatalf("expected 0 pooled connections after Close, got %d", n)
	}

	// New calls rejected.
	_, err := pool.GetClient("localhost:9002")
	if !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("expected ErrPoolClosed after Close, got: %v", err)
	}
}

func TestPeerPool_IdleEviction(t *testing.T) {
	cfg := PeerPoolConfig{
		IdleTimeout: 50 * time.Millisecond,
	}
	pool := NewPeerPool(cfg, testDialOpts()...)
	defer pool.Close()

	_, err := pool.GetClient("localhost:9001")
	if err != nil {
		t.Fatalf("GetClient: %v", err)
	}

	pool.mu.Lock()
	n := len(pool.peers)
	pool.mu.Unlock()
	if n != 1 {
		t.Fatalf("expected 1 connection before idle, got %d", n)
	}

	// Wait for IdleTimeout + one cleanup tick (IdleTimeout/2 interval).
	// Max wait = 1.5 * IdleTimeout = 75ms; use 200ms for safety.
	time.Sleep(200 * time.Millisecond)

	pool.mu.Lock()
	n = len(pool.peers)
	pool.mu.Unlock()
	if n != 0 {
		t.Fatalf("expected 0 connections after idle eviction, got %d", n)
	}
}

func TestPeerPool_IdleEviction_RefreshedConnectionNotEvicted(t *testing.T) {
	cfg := PeerPoolConfig{
		IdleTimeout: 100 * time.Millisecond,
	}
	pool := NewPeerPool(cfg, testDialOpts()...)
	defer pool.Close()

	_, _ = pool.GetClient("localhost:9001")

	// Keep refreshing lastUsed every 30ms — connection should not be evicted.
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(30 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_, _ = pool.GetClient("localhost:9001")
			case <-done:
				return
			}
		}
	}()

	time.Sleep(250 * time.Millisecond)
	close(done)

	pool.mu.Lock()
	n := len(pool.peers)
	pool.mu.Unlock()
	if n != 1 {
		t.Fatalf("active connection should not be evicted, got %d connections", n)
	}
}

func TestPeerPool_NoIdleEviction_WhenDisabled(t *testing.T) {
	cfg := PeerPoolConfig{
		IdleTimeout: 0, // disabled
	}
	pool := NewPeerPool(cfg, testDialOpts()...)
	defer pool.Close()

	_, _ = pool.GetClient("localhost:9001")
	_, _ = pool.GetClient("localhost:9002")

	pool.mu.Lock()
	n := len(pool.peers)
	pool.mu.Unlock()
	if n != 2 {
		t.Fatalf("expected 2 connections with idle eviction disabled, got %d", n)
	}
}
