// Program dfs starts a single node of the distributed key/value store.
package main

import (
	"flag"
	"log"
	"net"
	"strconv"
	"strings"

	"google.golang.org/grpc"

	"dfs"
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
	id := flag.String("id", "node1", "node ID")
	raftAddr := flag.String("raft", "", "raft bind address")
	grpcAddr := flag.String("grpc", "", "gRPC bind address")
	dataDir := flag.String("data", "data", "data directory")
	peers := flag.String("peers", "", "comma separated peer raft addresses")
	flag.Parse()

	const (
		defaultRaftPort = 12000
		defaultGRPCPort = 13000
	)

	rAddr := withDefaultPort(*raftAddr, defaultRaftPort)
	gAddr := withDefaultPort(*grpcAddr, defaultGRPCPort)
	peerStr := ""
	if *peers != "" {
		var ps []string
		for _, p := range strings.Split(*peers, ",") {
			ps = append(ps, withDefaultPort(p, defaultRaftPort))
		}
		peerStr = strings.Join(ps, ",")
	}

	n, err := node.New(*id, rAddr, *dataDir, peerStr)
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
	go dfsfs.Watch("/mnt/hostfs")

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
