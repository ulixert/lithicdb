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

	"github.com/ulixert/theseon/cluster"
	"github.com/ulixert/theseon/db"
	"github.com/ulixert/theseon/node"
	"github.com/ulixert/theseon/server"
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
