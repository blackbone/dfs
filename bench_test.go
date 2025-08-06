package dfs

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"dfs/internal/node"
	"dfs/internal/server"
	pb "dfs/proto"
)

// startNode creates a node and gRPC server listening on the given addresses.
func startNode(tb testing.TB, id, raftAddr, grpcAddr, peers, dataDir string) (*node.Node, func(), error) {
	tb.Helper()
	n, err := node.New(id, raftAddr, dataDir, peers)
	if err != nil {
		return nil, nil, err
	}
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		return nil, nil, err
	}
	s := grpc.NewServer()
	pb.RegisterFileServiceServer(s, server.New(n))
	go s.Serve(lis)
	cleanup := func() {
		s.Stop()
		lis.Close()
	}
	return n, cleanup, nil
}

// BenchmarkTwoNodes stores a value on one node and retrieves it from another.
const (
	raft1     = "127.0.0.1:12000"
	raft2     = "127.0.0.1:12001"
	grpc1     = "127.0.0.1:13000"
	grpc2     = "127.0.0.1:13001"
	dataVal   = "data"
	keyPrefix = "bench-"
)

func BenchmarkTwoNodes(b *testing.B) {
	dir1 := b.TempDir()
	dir2 := b.TempDir()

	_, stop1, err := startNode(b, raft1, raft1, grpc1, raft2, dir1)
	if err != nil {
		b.Fatalf("node1: %v", err)
	}
	defer stop1()
	n2, stop2, err := startNode(b, raft2, raft2, grpc2, raft1, dir2)
	if err != nil {
		b.Fatalf("node2: %v", err)
	}
	defer stop2()

	// Give the cluster time to elect a leader.
	time.Sleep(2 * time.Second)

	conn1, err := grpc.Dial(grpc1, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		b.Fatalf("dial1: %v", err)
	}
	defer conn1.Close()
	client1 := pb.NewFileServiceClient(conn1)

	conn2, err := grpc.Dial(grpc2, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		b.Fatalf("dial2: %v", err)
	}
	defer conn2.Close()
	client2 := pb.NewFileServiceClient(conn2)

	ctx := context.Background()

	// Determine which client is connected to the leader.
	leader := client1
	follower := client2
	if n2.IsLeader() {
		leader, follower = client2, client1
	}

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("%s%d", keyPrefix, i)
		if _, err := leader.Put(ctx, &pb.PutRequest{Key: key, Data: []byte(dataVal)}); err != nil {
			b.Fatalf("put: %v", err)
		}
		// Allow time for the entry to be replicated and applied on the peer.
		time.Sleep(200 * time.Millisecond)
		resp, err := follower.Get(ctx, &pb.GetRequest{Key: key})
		if err != nil {
			b.Fatalf("get: %v", err)
		}
		if string(resp.Data) != dataVal {
			b.Fatalf("unexpected data: %s", resp.Data)
		}
	}
}
