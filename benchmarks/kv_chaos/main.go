// Command kv_chaos drives a steady YCSB-A load against a 3-node in-process
// cluster while killing and restarting node-2 mid-run. The output is a
// per-second throughput timeline annotated with kill/restart events.
//
// Usage:
//
//	go run ./benchmarks/kv_chaos [flags]
//
// Output: benchmarks/out/kv_chaos.csv with columns
//
//	t_seconds,ops_per_sec,error_rate,event
//
// event is one of: "", "kill", "restart". Throughput & error-rate are
// computed over 1s windows.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/ulixert/theseon/benchmarks/common"
	"github.com/ulixert/theseon/cluster"
	"github.com/ulixert/theseon/node"
	pb "github.com/ulixert/theseon/proto/theseonpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	duration := flag.Duration("duration", 120*time.Second, "total chaos window")
	killAt := flag.Duration("kill-at", 30*time.Second, "wall-clock offset at which node-2 is stopped")
	restartAt := flag.Duration("restart-at", 60*time.Second, "wall-clock offset at which node-2 is restarted")
	keyspaceSize := flag.Int("keyspace-size", 100_000, "pre-fill keyspace size")
	valueSize := flag.Int("value-size", 256, "value size in bytes")
	concurrency := flag.Int("concurrency", 8, "concurrent worker goroutines")
	outPath := flag.String("out", "benchmarks/out/kv_chaos.csv", "output CSV path")
	flag.Parse()

	if *killAt <= 0 || *restartAt <= *killAt || *restartAt >= *duration {
		log.Fatalf("require 0 < kill-at < restart-at < duration; got %v / %v / %v", *killAt, *restartAt, *duration)
	}
	if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil {
		log.Fatalf("mkdir out: %v", err)
	}

	ctx := context.Background()
	cl, err := startCluster(ctx, cluster.DefaultCoordinatorConfig())
	if err != nil {
		log.Fatalf("start cluster: %v", err)
	}
	defer cl.stop()
	log.Printf("cluster up: %s %s %s", cl.nodes[0].Addr(), cl.nodes[1].Addr(), cl.nodes[2].Addr())

	coord, err := newClusterClient(cl.nodes[0].Addr())
	if err != nil {
		log.Fatalf("dial coordinator: %v", err)
	}
	defer coord.Close()

	log.Printf("pre-filling %d keys …", *keyspaceSize)
	if err := common.PreFill(coord, *keyspaceSize, *valueSize); err != nil {
		log.Fatalf("prefill: %v", err)
	}
	log.Printf("pre-fill done; starting chaos run for %v", *duration)
}

// --- cluster bring-up ---

type testCluster struct {
	nodes []*node.Node
	dirs  []string
}

func (c *testCluster) stop() {
	for _, n := range c.nodes {
		if n != nil {
			n.Stop()
		}
	}
}

func makeClusterCfg(id string) cluster.ClusterConfig {
	cfg := cluster.DefaultClusterConfig(id, "")
	cfg.GossipInterval = 100 * time.Millisecond
	cfg.PingTimeout = 50 * time.Millisecond
	cfg.SuspectTimeout = 500 * time.Millisecond
	return cfg
}

func startCluster(ctx context.Context, coordCfg cluster.CoordinatorConfig) (*testCluster, error) {
	dirs := make([]string, 3)
	for i := range dirs {
		d, err := os.MkdirTemp("", fmt.Sprintf("bench-chaos-n%d-", i+1))
		if err != nil {
			return nil, err
		}
		dirs[i] = d
	}

	n1 := node.New(node.Config{
		NodeID: "node-1", Addr: "127.0.0.1:0", DataDir: dirs[0],
		Cluster: makeClusterCfg("node-1"), Coord: coordCfg,
	})
	if err := n1.Start(ctx); err != nil {
		return nil, fmt.Errorf("start node-1: %w", err)
	}

	n2 := node.New(node.Config{
		NodeID: "node-2", Addr: "127.0.0.1:0", DataDir: dirs[1],
		SeedPeers: []string{n1.Addr()}, Cluster: makeClusterCfg("node-2"), Coord: coordCfg,
	})
	if err := n2.Start(ctx); err != nil {
		n1.Stop()
		return nil, fmt.Errorf("start node-2: %w", err)
	}

	n3 := node.New(node.Config{
		NodeID: "node-3", Addr: "127.0.0.1:0", DataDir: dirs[2],
		SeedPeers: []string{n1.Addr()}, Cluster: makeClusterCfg("node-3"), Coord: coordCfg,
	})
	if err := n3.Start(ctx); err != nil {
		n1.Stop()
		n2.Stop()
		return nil, fmt.Errorf("start node-3: %w", err)
	}

	cl := &testCluster{nodes: []*node.Node{n1, n2, n3}, dirs: dirs}

	admin, err := dialAdmin(n1.Addr())
	if err != nil {
		cl.stop()
		return nil, err
	}
	defer admin.close()

	if err := waitForMembers(admin.c, 3, 10*time.Second); err != nil {
		cl.stop()
		return nil, err
	}
	for _, info := range []struct{ id, addr string }{
		{"node-1", n1.Addr()}, {"node-2", n2.Addr()}, {"node-3", n3.Addr()},
	} {
		if err := joinAndActivate(admin.c, info.id, info.addr); err != nil {
			cl.stop()
			return nil, err
		}
	}

	statusResp, err := admin.c.GetClusterStatus(context.Background(), &pb.GetClusterStatusRequest{})
	if err != nil {
		cl.stop()
		return nil, err
	}
	wantVer := statusResp.RingDescriptor.Version
	for _, n := range cl.nodes[1:] {
		ah, err := dialAdmin(n.Addr())
		if err != nil {
			cl.stop()
			return nil, err
		}
		err = waitForRingVersion(ah.c, wantVer, 10*time.Second)
		ah.close()
		if err != nil {
			cl.stop()
			return nil, err
		}
	}

	return cl, nil
}

// --- admin helpers ---

type adminHandle struct {
	c    pb.AdminServiceClient
	conn *grpc.ClientConn
}

func (a *adminHandle) close() { _ = a.conn.Close() }

func dialAdmin(addr string) (*adminHandle, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &adminHandle{c: pb.NewAdminServiceClient(conn), conn: conn}, nil
}

func waitForMembers(c pb.AdminServiceClient, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := c.GetClusterStatus(context.Background(), &pb.GetClusterStatusRequest{})
		if err == nil && len(resp.Members) >= want {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timeout: want %d SWIM members", want)
}

func waitForRingVersion(c pb.AdminServiceClient, want uint64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := c.GetClusterStatus(context.Background(), &pb.GetClusterStatusRequest{})
		if err == nil && resp.RingDescriptor != nil && resp.RingDescriptor.Version >= want {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timeout: want ring version >= %d", want)
}

func joinAndActivate(c pb.AdminServiceClient, nodeID, addr string) error {
	ctx := context.Background()
	statusResp, err := c.GetClusterStatus(ctx, &pb.GetClusterStatusRequest{})
	if err != nil {
		return err
	}
	version := statusResp.RingDescriptor.GetVersion()
	if _, err = c.JoinRing(ctx, &pb.JoinRingRequest{NodeId: nodeID, Addr: addr, ExpectedVersion: version}); err != nil {
		return fmt.Errorf("join %s: %w", nodeID, err)
	}
	if _, err = c.ActivateNode(ctx, &pb.ActivateNodeRequest{NodeId: nodeID, ExpectedVersion: version + 1}); err != nil {
		return fmt.Errorf("activate %s: %w", nodeID, err)
	}
	return nil
}

// --- KVClient against the Theseon gRPC service ---

type clusterClient struct {
	c    pb.TheseonClient
	conn *grpc.ClientConn
}

func newClusterClient(addr string) (*clusterClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &clusterClient{c: pb.NewTheseonClient(conn), conn: conn}, nil
}

func (c *clusterClient) Put(k, v []byte) error {
	_, err := c.c.Put(context.Background(), &pb.PutRequest{Key: k, Value: v})
	return err
}

func (c *clusterClient) Get(k []byte) ([]byte, bool, error) {
	resp, err := c.c.Get(context.Background(), &pb.GetRequest{Key: k})
	if err != nil {
		return nil, false, err
	}
	return resp.Value, resp.Found, nil
}

func (c *clusterClient) Delete(k []byte) error {
	_, err := c.c.Delete(context.Background(), &pb.DeleteRequest{Key: k})
	return err
}

func (c *clusterClient) PutBatch(keys, values [][]byte) error {
	req := &pb.BatchWriteRequest{Entries: make([]*pb.BatchEntry, len(keys))}
	for i := range keys {
		req.Entries[i] = &pb.BatchEntry{Key: keys[i], Value: values[i]}
	}
	_, err := c.c.BatchWrite(context.Background(), req)
	return err
}

func (c *clusterClient) AwaitReady() error {
	time.Sleep(2 * time.Second)
	return nil
}

func (c *clusterClient) Close() error { return c.conn.Close() }
