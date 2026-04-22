// Command kv_cluster benchmarks a 3-node in-process Theseon cluster under
// three quorum configurations: (N=3, W=2, R=2), (3,3,1), (3,1,3). Each run
// exercises the coordinator fan-out, gRPC, HLC timestamping, and the peer
// pool; the nodes live in the same process but communicate over real TCP.
//
// Usage:
//
//	go run ./benchmarks/kv_cluster [flags]
//
// Output: benchmarks/out/kv_cluster.csv with columns
//
//	N,W,R,workload,rep,ops_per_sec,p50_ms,p95_ms,p99_ms,errors
package main

import (
	"cmp"
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/ulixert/theseon/benchmarks/common"
	"github.com/ulixert/theseon/cluster"
	"github.com/ulixert/theseon/node"
	pb "github.com/ulixert/theseon/proto/theseonpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type quorum struct{ N, W, R int }

func main() {
	duration := flag.Duration("duration", 60*time.Second, "measured-run duration per (quorum,workload,rep)")
	keyspaceSize := flag.Int("keyspace-size", 100_000, "number of distinct keys to pre-fill (cluster prefill is slower than single-node)")
	valueSize := flag.Int("value-size", 256, "value size in bytes")
	reps := flag.Int("reps", 3, "repetitions per (quorum,workload); medians reported")
	concurrency := flag.Int("concurrency", 8, "concurrent worker goroutines; higher hides per-op RPC latency")
	outPath := flag.String("out", "benchmarks/out/kv_cluster.csv", "output CSV path")
	quorumFilter := flag.String("quorum", "", "optional filter: \"N,W,R\" (e.g. \"3,1,3\") to run only one quorum config")
	workloadFilter := flag.String("workload", "", "optional filter: YCSB-A | YCSB-B | YCSB-C")
	flag.Parse()

	if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil {
		log.Fatalf("mkdir out: %v", err)
	}
	f, err := os.Create(*outPath)
	if err != nil {
		log.Fatalf("create csv: %v", err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()
	_ = w.Write([]string{"N", "W", "R", "workload", "rep", "ops_per_sec", "p50_ms", "p95_ms", "p99_ms", "errors"})

	quorums := []quorum{{3, 2, 2}, {3, 3, 1}, {3, 1, 3}}
	workloads := []common.Workload{common.YCSBA(), common.YCSBB(), common.YCSBC()}

	if *quorumFilter != "" {
		var parsed quorum
		if _, err := fmt.Sscanf(*quorumFilter, "%d,%d,%d", &parsed.N, &parsed.W, &parsed.R); err != nil {
			log.Fatalf("parse --quorum=%q: %v", *quorumFilter, err)
		}
		quorums = []quorum{parsed}
	}
	if *workloadFilter != "" {
		var filtered []common.Workload
		for _, w := range workloads {
			if w.Name == *workloadFilter {
				filtered = append(filtered, w)
			}
		}
		if len(filtered) == 0 {
			log.Fatalf("--workload=%q matched none of YCSB-A/B/C", *workloadFilter)
		}
		workloads = filtered
	}

	for _, q := range quorums {
		for _, wl := range workloads {
			wl.KeyspaceSize = *keyspaceSize
			wl.ValueSize = *valueSize
			wl.Duration = *duration
			wl.Concurrency = *concurrency

			var perRep []common.Results
			for rep := 0; rep < *reps; rep++ {
				log.Printf("(%d,%d,%d) / %s / rep %d …", q.N, q.W, q.R, wl.Name, rep)
				r, err := runOne(q, wl)
				if err != nil {
					log.Fatalf("(%d,%d,%d)/%s/rep%d: %v", q.N, q.W, q.R, wl.Name, rep, err)
				}
				perRep = append(perRep, r)
				_ = w.Write([]string{
					strconv.Itoa(q.N), strconv.Itoa(q.W), strconv.Itoa(q.R),
					wl.Name, strconv.Itoa(rep),
					fmt.Sprintf("%.2f", r.OpsPerSec),
					fmt.Sprintf("%.3f", ms(r.P50)),
					fmt.Sprintf("%.3f", ms(r.P95)),
					fmt.Sprintf("%.3f", ms(r.P99)),
					strconv.FormatInt(r.Errors, 10),
				})
				w.Flush()
			}
			med := medianResults(perRep)
			log.Printf("(%d,%d,%d) / %s MEDIAN: %.0f ops/sec, p50=%.2fms p95=%.2fms p99=%.2fms",
				q.N, q.W, q.R, wl.Name, med.OpsPerSec,
				ms(med.P50), ms(med.P95), ms(med.P99))
		}
	}
}

// runOne spins up a 3-node cluster, pre-fills, runs the workload through
// the coordinator on node-1, then tears the cluster down.
func runOne(q quorum, wl common.Workload) (common.Results, error) {
	ctx := context.Background()
	coordCfg := cluster.DefaultCoordinatorConfig()
	coordCfg.ReplicationFactor = q.N
	coordCfg.WriteQuorum = q.W
	coordCfg.ReadQuorum = q.R

	cl, err := startCluster(ctx, coordCfg)
	if err != nil {
		return common.Results{}, fmt.Errorf("start cluster: %w", err)
	}
	defer cl.stop()

	kv, err := newClusterClient(cl.nodes[0].Addr())
	if err != nil {
		return common.Results{}, fmt.Errorf("dial coordinator: %w", err)
	}
	defer kv.Close()

	if err := common.PreFill(kv, wl.KeyspaceSize, wl.ValueSize); err != nil {
		return common.Results{}, fmt.Errorf("prefill: %w", err)
	}
	return wl.Run(kv)
}

// --- Cluster bring-up ---

type testCluster struct {
	nodes []*node.Node
}

func (c *testCluster) stop() {
	for _, n := range c.nodes {
		n.Stop()
	}
}

func startCluster(ctx context.Context, coordCfg cluster.CoordinatorConfig) (*testCluster, error) {
	// Fast gossip so ring formation completes quickly.
	makeClusterCfg := func(id string) cluster.ClusterConfig {
		cfg := cluster.DefaultClusterConfig(id, "")
		cfg.GossipInterval = 100 * time.Millisecond
		cfg.PingTimeout = 50 * time.Millisecond
		cfg.SuspectTimeout = 500 * time.Millisecond
		return cfg
	}

	dirs := make([]string, 3)
	for i := range dirs {
		d, err := os.MkdirTemp("", fmt.Sprintf("bench-cluster-n%d-", i+1))
		if err != nil {
			return nil, err
		}
		dirs[i] = d
	}

	n1 := node.New(node.Config{
		NodeID:  "node-1",
		Addr:    "127.0.0.1:0",
		DataDir: dirs[0],
		Cluster: makeClusterCfg("node-1"),
		Coord:   coordCfg,
	})
	if err := n1.Start(ctx); err != nil {
		return nil, fmt.Errorf("start node-1: %w", err)
	}

	n2 := node.New(node.Config{
		NodeID:    "node-2",
		Addr:      "127.0.0.1:0",
		DataDir:   dirs[1],
		SeedPeers: []string{n1.Addr()},
		Cluster:   makeClusterCfg("node-2"),
		Coord:     coordCfg,
	})
	if err := n2.Start(ctx); err != nil {
		n1.Stop()
		return nil, fmt.Errorf("start node-2: %w", err)
	}

	n3 := node.New(node.Config{
		NodeID:    "node-3",
		Addr:      "127.0.0.1:0",
		DataDir:   dirs[2],
		SeedPeers: []string{n1.Addr()},
		Cluster:   makeClusterCfg("node-3"),
		Coord:     coordCfg,
	})
	if err := n3.Start(ctx); err != nil {
		n1.Stop()
		n2.Stop()
		return nil, fmt.Errorf("start node-3: %w", err)
	}

	cl := &testCluster{nodes: []*node.Node{n1, n2, n3}}

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
		return nil, fmt.Errorf("final status: %w", err)
	}
	wantVer := statusResp.RingDescriptor.Version
	admin2, err := dialAdmin(n2.Addr())
	if err != nil {
		cl.stop()
		return nil, err
	}
	defer admin2.close()
	if err := waitForRingVersion(admin2.c, wantVer, 10*time.Second); err != nil {
		cl.stop()
		return nil, err
	}
	admin3, err := dialAdmin(n3.Addr())
	if err != nil {
		cl.stop()
		return nil, err
	}
	defer admin3.close()
	if err := waitForRingVersion(admin3.c, wantVer, 10*time.Second); err != nil {
		cl.stop()
		return nil, err
	}

	return cl, nil
}

type adminHandle struct {
	c    pb.AdminServiceClient
	conn *grpc.ClientConn
}

func (a *adminHandle) close() { _ = a.conn.Close() }

func dialAdmin(addr string) (*adminHandle, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("admin dial %s: %w", addr, err)
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
		return fmt.Errorf("status before join %s: %w", nodeID, err)
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

// --- KVClient over the Theseon gRPC service ---

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

func (c *clusterClient) Put(key, value []byte) error {
	_, err := c.c.Put(context.Background(), &pb.PutRequest{Key: key, Value: value})
	return err
}

func (c *clusterClient) Get(key []byte) ([]byte, bool, error) {
	resp, err := c.c.Get(context.Background(), &pb.GetRequest{Key: key})
	if err != nil {
		return nil, false, err
	}
	return resp.Value, resp.Found, nil
}

func (c *clusterClient) Delete(key []byte) error {
	_, err := c.c.Delete(context.Background(), &pb.DeleteRequest{Key: key})
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

// AwaitReady is a no-op at the cluster level: per-node compaction state is
// best-effort, and the cluster settles quickly after prefill finishes because
// replicated writes also go through local memtable + WAL on each replica.
// A short sleep absorbs any in-flight replication tail.
func (c *clusterClient) AwaitReady() error {
	time.Sleep(2 * time.Second)
	return nil
}

func (c *clusterClient) Close() error { return c.conn.Close() }

// --- small utilities ---

func medianResults(rs []common.Results) common.Results {
	cp := append([]common.Results(nil), rs...)
	slices.SortFunc(cp, func(a, b common.Results) int { return cmp.Compare(a.OpsPerSec, b.OpsPerSec) })
	return cp[len(cp)/2]
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
