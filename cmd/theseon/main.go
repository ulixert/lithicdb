// Command theseon starts a theseon node in standalone or cluster mode
// and provides admin subcommands for cluster management.
//
// Usage:
//
//	theseon serve [flags]          - start a node
//	theseon admin status [flags]   - print cluster status
//	theseon admin join [flags]     - add node to ring as JOINING
//	theseon admin activate [flags] - transition JOINING → ACTIVE
//	theseon admin remove [flags]   - remove node from ring
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"

	"github.com/ulixert/theseon/cluster"
	"github.com/ulixert/theseon/db"
	"github.com/ulixert/theseon/node"
	pb "github.com/ulixert/theseon/proto/theseonpb"
	"github.com/ulixert/theseon/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}

	switch os.Args[1] {
	case "serve":
		cmdServe(os.Args[2:])
	case "admin":
		if len(os.Args) < 3 {
			adminUsage()
		}
		switch os.Args[2] {
		case "status":
			cmdAdminStatus(os.Args[3:])
		case "join":
			cmdAdminJoin(os.Args[3:])
		case "activate":
			cmdAdminActivate(os.Args[3:])
		case "remove":
			cmdAdminRemove(os.Args[3:])
		default:
			adminUsage()
		}
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: theseon <command> [flags]")
	fmt.Fprintln(os.Stderr, "commands: serve, admin")
	os.Exit(1)
}

func adminUsage() {
	fmt.Fprintln(os.Stderr, "usage: theseon admin <command> [flags]")
	fmt.Fprintln(os.Stderr, "commands: status, join, activate, remove")
	os.Exit(1)
}

// cmdServe starts a theseon node. When --node-id is set, the node runs
// in cluster mode with full distributed stack. Otherwise, standalone.
func cmdServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", ":50051", "gRPC listen address")
	dataDir := fs.String("data-dir", "./data", "database directory")
	nodeID := fs.String("node-id", "", "unique node identifier (enables cluster mode)")
	seeds := fs.String("seeds", "", "comma-separated seed peer addresses")
	replFactor := fs.Int("replication-factor", 3, "replication factor (N)")
	writeQuorum := fs.Int("write-quorum", 2, "write quorum (W)")
	readQuorum := fs.Int("read-quorum", 2, "read quorum (R)")
	fs.Parse(args)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if *nodeID == "" {
		// Standalone mode - no cluster.
		runStandalone(ctx, *addr, *dataDir)
		return
	}

	// Cluster mode.
	var seedPeers []string
	if *seeds != "" {
		seedPeers = strings.Split(*seeds, ",")
	}

	n := node.New(node.Config{
		NodeID:    *nodeID,
		Addr:      *addr,
		DataDir:   *dataDir,
		SeedPeers: seedPeers,
		Coord: cluster.CoordinatorConfig{
			ReplicationFactor: *replFactor,
			WriteQuorum:       *writeQuorum,
			ReadQuorum:        *readQuorum,
		},
	})

	if err := n.Start(ctx); err != nil {
		log.Fatalf("start node: %v", err)
	}

	log.Printf("theseon cluster node %q listening on %s", *nodeID, n.Addr())

	<-ctx.Done()
	log.Println("shutting down...")
	n.Stop()
}

// runStandalone starts a standalone (non-clustered) theseon server.
func runStandalone(ctx context.Context, addr, dataDir string) {
	opts := db.DefaultOptions(dataDir)
	database, err := db.Open(opts)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		_ = database.Close()
		log.Fatalf("listen %s: %v", addr, err)
	}

	srv := server.New(database, nil)

	go func() {
		<-ctx.Done()
		log.Println("shutting down...")
		srv.GracefulStop()
	}()

	log.Printf("theseon standalone listening on %s (data: %s)", addr, dataDir)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}

	// Serve returns after GracefulStop completes - safe to close DB.
	if err := database.Close(); err != nil {
		log.Printf("close database: %v", err)
	}
}

// --- Admin commands ---

func adminClient(target string) (pb.AdminServiceClient, func()) {
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("connect to %s: %v", target, err)
	}
	return pb.NewAdminServiceClient(conn), func() { conn.Close() }
}

func cmdAdminStatus(args []string) {
	fs := flag.NewFlagSet("admin status", flag.ExitOnError)
	target := fs.String("target", "", "address of cluster node (required)")
	fs.Parse(args)

	if *target == "" {
		fmt.Fprintln(os.Stderr, "error: --target is required")
		os.Exit(1)
	}

	client, cleanup := adminClient(*target)
	defer cleanup()

	resp, err := client.GetClusterStatus(context.Background(), &pb.GetClusterStatusRequest{})
	if err != nil {
		log.Fatalf("get cluster status: %v", err)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "NODE ID\tADDRESS\tLIVENESS\tRING STATE")
	for _, m := range resp.Members {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n",
			m.NodeId, m.Addr,
			livenessStr(m.Liveness), ringStateStr(m.RingState))
	}
	w.Flush()

	if resp.RingDescriptor != nil {
		fmt.Printf("\nRing version: %d\n", resp.RingDescriptor.Version)
		if len(resp.RingDescriptor.Members) > 0 {
			fmt.Println("Ring members:")
			for _, rm := range resp.RingDescriptor.Members {
				fmt.Printf("  %s (%s) - %s\n", rm.NodeId, rm.Addr, ringStateStr(rm.RingState))
			}
		}
	}
}

func cmdAdminJoin(args []string) {
	fs := flag.NewFlagSet("admin join", flag.ExitOnError)
	target := fs.String("target", "", "address of cluster node (required)")
	nodeID := fs.String("node-id", "", "ID of node to join (required)")
	nodeAddr := fs.String("addr", "", "address of node to join (required)")
	fs.Parse(args)

	if *target == "" || *nodeID == "" || *nodeAddr == "" {
		fmt.Fprintln(os.Stderr, "error: --target, --node-id, and --addr are all required")
		os.Exit(1)
	}

	client, cleanup := adminClient(*target)
	defer cleanup()

	// Get current ring version for CAS.
	statusResp, err := client.GetClusterStatus(context.Background(), &pb.GetClusterStatusRequest{})
	if err != nil {
		log.Fatalf("get cluster status: %v", err)
	}
	version := statusResp.RingDescriptor.GetVersion()

	resp, err := client.JoinRing(context.Background(), &pb.JoinRingRequest{
		NodeId:          *nodeID,
		Addr:            *nodeAddr,
		ExpectedVersion: version,
	})
	if err != nil {
		log.Fatalf("join ring: %v", err)
	}
	fmt.Printf("node %q joined ring as JOINING (ring version: %d)\n", resp.NodeId, version+1)
}

func cmdAdminActivate(args []string) {
	fs := flag.NewFlagSet("admin activate", flag.ExitOnError)
	target := fs.String("target", "", "address of cluster node (required)")
	nodeID := fs.String("node-id", "", "ID of node to activate (required)")
	fs.Parse(args)

	if *target == "" || *nodeID == "" {
		fmt.Fprintln(os.Stderr, "error: --target and --node-id are required")
		os.Exit(1)
	}

	client, cleanup := adminClient(*target)
	defer cleanup()

	statusResp, err := client.GetClusterStatus(context.Background(), &pb.GetClusterStatusRequest{})
	if err != nil {
		log.Fatalf("get cluster status: %v", err)
	}
	version := statusResp.RingDescriptor.GetVersion()

	_, err = client.ActivateNode(context.Background(), &pb.ActivateNodeRequest{
		NodeId:          *nodeID,
		ExpectedVersion: version,
	})
	if err != nil {
		log.Fatalf("activate node: %v", err)
	}
	fmt.Printf("node %q activated (ring version: %d)\n", *nodeID, version+1)
}

func cmdAdminRemove(args []string) {
	fs := flag.NewFlagSet("admin remove", flag.ExitOnError)
	target := fs.String("target", "", "address of cluster node (required)")
	nodeID := fs.String("node-id", "", "ID of node to remove (required)")
	fs.Parse(args)

	if *target == "" || *nodeID == "" {
		fmt.Fprintln(os.Stderr, "error: --target and --node-id are required")
		os.Exit(1)
	}

	client, cleanup := adminClient(*target)
	defer cleanup()

	statusResp, err := client.GetClusterStatus(context.Background(), &pb.GetClusterStatusRequest{})
	if err != nil {
		log.Fatalf("get cluster status: %v", err)
	}
	version := statusResp.RingDescriptor.GetVersion()

	_, err = client.RemoveNode(context.Background(), &pb.RemoveNodeRequest{
		NodeId:          *nodeID,
		ExpectedVersion: version,
	})
	if err != nil {
		log.Fatalf("remove node: %v", err)
	}
	fmt.Printf("node %q removed (ring version: %d)\n", *nodeID, version+1)
}

// --- Display helpers ---

func livenessStr(l int32) string {
	switch cluster.LivenessState(l) {
	case cluster.Alive:
		return "alive"
	case cluster.Suspect:
		return "suspect"
	case cluster.Dead:
		return "dead"
	default:
		return fmt.Sprintf("unknown(%d)", l)
	}
}

func ringStateStr(s int32) string {
	switch cluster.RingState(s) {
	case cluster.RingNone:
		return "none"
	case cluster.RingJoining:
		return "joining"
	case cluster.RingActive:
		return "active"
	default:
		return fmt.Sprintf("unknown(%d)", s)
	}
}
