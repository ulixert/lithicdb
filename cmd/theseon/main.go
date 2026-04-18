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
	"fmt"
	"log"
	"net"
	"os"

	"github.com/ulixert/theseon/db"
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
