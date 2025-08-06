// Program dfs starts a single node of the distributed key/value store.
package main

import (
	"context"
	"log"
	"net"
	"strconv"
	"strings"

	"google.golang.org/grpc"

	"dfs"
	"dfs/internal/config"
	dfsfs "dfs/internal/fusefs"
	"dfs/internal/node"
	"dfs/internal/server"
	pb "dfs/proto"
)

// withDefaultPort ensures the address has a port. If missing, defaultPort
// is appended. An empty host is allowed and results in ":port".
func withDefaultPort(addr string, defaultPort int) string {
	if _, _, err := net.SplitHostPort(addr); err == nil {
		return addr
	}
	return net.JoinHostPort(addr, strconv.Itoa(defaultPort))
}

func main() {
	const (
		defaultRaftPort = 12000
		defaultGRPCPort = 13000
	)

	cfg, err := config.Load("")
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	rAddr := withDefaultPort(cfg.Raft, defaultRaftPort)
	gAddr := withDefaultPort(cfg.GRPC, defaultGRPCPort)
	peerStr := ""
	if len(cfg.Peers) > 0 {
		var ps []string
		for _, p := range cfg.Peers {
			ps = append(ps, withDefaultPort(p, defaultRaftPort))
		}
		peerStr = strings.Join(ps, ",")
	}

	n, err := node.New(cfg.ID, rAddr, cfg.Data, peerStr)
	if err != nil {
		log.Fatalf("node: %v", err)
	}
	dfs.SetNode(n)

	// Start FUSE filesystem and cache watcher.
	go func() {
		if err := dfsfs.Mount("/mnt/dfs", "/mnt/hostfs"); err != nil {
			log.Fatalf("mount: %v", err)
		}
	}()
	go func() {
		if err := dfsfs.Watch(context.Background(), "/mnt/hostfs"); err != nil {
			log.Fatalf("watch: %v", err)
		}
	}()

	lis, err := net.Listen("tcp", gAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterFileServiceServer(s, server.New(n))
	log.Printf("gRPC listening on %s", gAddr)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
