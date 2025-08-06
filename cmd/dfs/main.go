package main

import (
    "flag"
    "log"
    "net"

    "google.golang.org/grpc"

    "dfs/internal/node"
    "dfs/internal/server"
    pb "dfs/proto"
)

func main() {
    id := flag.String("id", "node1", "node ID")
    raftAddr := flag.String("raft", ":12000", "raft bind address")
    grpcAddr := flag.String("grpc", ":13000", "gRPC bind address")
    dataDir := flag.String("data", "data", "data directory")
    peers := flag.String("peers", "", "comma separated peer raft addresses")
    flag.Parse()

    n, err := node.New(*id, *raftAddr, *dataDir, *peers)
    if err != nil {
        log.Fatalf("node: %v", err)
    }

    lis, err := net.Listen("tcp", *grpcAddr)
    if err != nil {
        log.Fatalf("listen: %v", err)
    }
    s := grpc.NewServer()
    pb.RegisterFileServiceServer(s, server.New(n))
    log.Printf("gRPC listening on %s", *grpcAddr)
    if err := s.Serve(lis); err != nil {
        log.Fatalf("serve: %v", err)
    }
}

