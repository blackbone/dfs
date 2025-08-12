package dfs

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"dfs/internal/node"
	"dfs/internal/server"
	pb "dfs/proto"
)

const (
	benchKeyFmt = "bench-%d"
	benchData   = "data"
	benchSleep  = 200 * time.Millisecond
	listenNet   = "tcp"
)

// startNode creates a node and gRPC server listening on a single address.
func startNode(tb testing.TB, id, addr, peers, dataDir string, bootstrap bool) (*node.Node, func(), error) {
	tb.Helper()
	lis, err := net.Listen(listenNet, addr)
	if err != nil {
		return nil, nil, err
	}
	mux := cmux.New(lis)
	grpcL := mux.Match(cmux.HTTP2())
	raftL := mux.Match(cmux.Any())

	n, err := node.NewWithListener(id, raftL, dataDir, peers, bootstrap)
	if err != nil {
		lis.Close()
		return nil, nil, err
	}
	s := grpc.NewServer()
	pb.RegisterFileServiceServer(s, server.New(n))
	go s.Serve(grpcL)
	go mux.Serve()
	cleanup := func() {
		s.Stop()
		lis.Close()
	}
	return n, cleanup, nil
}

// BenchmarkTwoNodes stores a value on one node and retrieves it from another.
func BenchmarkTwoNodes(b *testing.B) {
	dir1 := b.TempDir()
	dir2 := b.TempDir()

	_, stop1, err := startNode(b, "127.0.0.1:13000", "127.0.0.1:13000", "127.0.0.1:13001", dir1, true)
	if err != nil {
		b.Fatalf("node1: %v", err)
	}
	defer stop1()
	n2, stop2, err := startNode(b, "127.0.0.1:13001", "127.0.0.1:13001", "127.0.0.1:13000", dir2, true)
	if err != nil {
		b.Fatalf("node2: %v", err)
	}
	defer stop2()

	// Give the cluster time to elect a leader.
	time.Sleep(2 * time.Second)

	conn1, err := grpc.Dial("127.0.0.1:13000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		b.Fatalf("dial1: %v", err)
	}
	defer conn1.Close()
	client1 := pb.NewFileServiceClient(conn1)

	conn2, err := grpc.Dial("127.0.0.1:13001", grpc.WithTransportCredentials(insecure.NewCredentials()))
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
		key := fmt.Sprintf(benchKeyFmt, i)
		if _, err := leader.Put(ctx, &pb.PutRequest{Key: key, Data: []byte(benchData)}); err != nil {
			b.Fatalf("put: %v", err)
		}
		time.Sleep(benchSleep)
		resp, err := follower.Get(ctx, &pb.GetRequest{Key: key})
		if err != nil {
			b.Fatalf("get: %v", err)
		}
		if string(resp.Data) != benchData {
			b.Fatalf("unexpected data: %s", resp.Data)
		}
	}
}

func BenchmarkThreeNodes(b *testing.B) {
	dir1 := b.TempDir()
	dir2 := b.TempDir()
	dir3 := b.TempDir()

	_, stop1, err := startNode(b, "127.0.0.1:13010", "127.0.0.1:13010", "127.0.0.1:13011,127.0.0.1:13012", dir1, true)
	if err != nil {
		b.Fatalf("node1: %v", err)
	}
	defer stop1()
	n2, stop2, err := startNode(b, "127.0.0.1:13011", "127.0.0.1:13011", "127.0.0.1:13010,127.0.0.1:13012", dir2, true)
	if err != nil {
		b.Fatalf("node2: %v", err)
	}
	defer stop2()
	n3, stop3, err := startNode(b, "127.0.0.1:13012", "127.0.0.1:13012", "127.0.0.1:13010,127.0.0.1:13011", dir3, true)
	if err != nil {
		b.Fatalf("node3: %v", err)
	}
	defer stop3()

	time.Sleep(2 * time.Second)

	conn1, err := grpc.Dial("127.0.0.1:13010", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		b.Fatalf("dial1: %v", err)
	}
	defer conn1.Close()
	client1 := pb.NewFileServiceClient(conn1)

	conn2, err := grpc.Dial("127.0.0.1:13011", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		b.Fatalf("dial2: %v", err)
	}
	defer conn2.Close()
	client2 := pb.NewFileServiceClient(conn2)

	conn3, err := grpc.Dial("127.0.0.1:13012", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		b.Fatalf("dial3: %v", err)
	}
	defer conn3.Close()
	client3 := pb.NewFileServiceClient(conn3)

	ctx := context.Background()

	leader, f1, f2 := client1, client2, client3
	if n2.IsLeader() {
		leader, f1, f2 = client2, client1, client3
	}
	if n3.IsLeader() {
		leader, f1, f2 = client3, client1, client2
	}

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf(benchKeyFmt, i)
		if _, err := leader.Put(ctx, &pb.PutRequest{Key: key, Data: []byte(benchData)}); err != nil {
			b.Fatalf("put: %v", err)
		}
		time.Sleep(benchSleep)
		for _, follower := range []pb.FileServiceClient{f1, f2} {
			resp, err := follower.Get(ctx, &pb.GetRequest{Key: key})
			if err != nil {
				b.Fatalf("get: %v", err)
			}
			if string(resp.Data) != benchData {
				b.Fatalf("unexpected data: %s", resp.Data)
			}
		}
	}
}

func BenchmarkAddRemovePeer(b *testing.B) {
	addr1 := "127.0.0.1:15000"
	n1, err := node.New("leader", addr1, b.TempDir(), "", true)
	if err != nil {
		b.Fatalf("leader: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for !n1.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	for i := 0; i < b.N; i++ {
		addr2 := fmt.Sprintf("127.0.0.1:%d", 15001+i)
		id := fmt.Sprintf("n%d", i)
		n2, err := node.New(id, addr2, b.TempDir(), "", false)
		if err != nil {
			b.Fatalf("new: %v", err)
		}
		if err := n1.AddPeer(id, addr2); err != nil {
			b.Fatalf("add: %v", err)
		}
		if err := n1.RemovePeer(id); err != nil {
			b.Fatalf("remove: %v", err)
		}
		_ = n2
	}
}
