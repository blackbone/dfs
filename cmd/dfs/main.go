// Program dfs starts a single node of the distributed key/value store.
package main

import (
	"context"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/soheilhy/cmux"
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
		defaultPort   = 13000
		listenNet     = "tcp"
		mountPoint    = "/mnt/dfs"
		cacheDir      = "/mnt/hostfs"
		checkInterval = time.Minute
	)

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	addr := withDefaultPort(cfg.GRPC, defaultPort)
	if cfg.Raft != "" {
		addr = withDefaultPort(cfg.Raft, defaultPort)
	}
	peerStr := ""
	if len(cfg.Peers) > 0 {
		var ps []string
		for _, p := range cfg.Peers {
			ps = append(ps, withDefaultPort(p, defaultPort))
		}
		peerStr = strings.Join(ps, ",")
	}

	lis, err := net.Listen(listenNet, addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	mux := cmux.New(lis)
	grpcL := mux.Match(cmux.HTTP2())
	raftL := mux.Match(cmux.Any())

	n, err := node.NewWithListener(cfg.ID, raftL, cfg.Data, peerStr, !cfg.Join)
	if err != nil {
		log.Fatalf("node: %v", err)
	}
	dfs.SetNode(n)

	// Start FUSE filesystem, cache watcher and consistency checker.
	go func() {
		if err := dfsfs.Mount(mountPoint, cacheDir); err != nil {
			log.Fatalf("mount: %v", err)
		}
	}()
	go func() {
		if err := dfsfs.Watch(context.Background(), cacheDir); err != nil {
			log.Fatalf("watch: %v", err)
		}
	}()
	go dfsfs.Check(context.Background(), cacheDir, checkInterval)

	s := grpc.NewServer()
	pb.RegisterFileServiceServer(s, server.New(n))
	go func() {
		log.Printf("gRPC listening on %s", addr)
		if err := s.Serve(grpcL); err != nil {
			log.Fatalf("grpc: %v", err)
		}
	}()
	if err := mux.Serve(); err != nil {
		log.Fatalf("mux: %v", err)
	}
}
