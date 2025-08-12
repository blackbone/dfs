package fusefs

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"

	"dfs"
	"dfs/internal/node"
	"dfs/internal/server"
	pb "dfs/proto"
)

const listenNet = "tcp"

// startNode creates a node and gRPC server using a shared address.
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

func TestWatchReplicatesFile(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	n1, stop1, err := startNode(t, "127.0.0.1:13010", "127.0.0.1:13010", "127.0.0.1:13011", dir1, true)
	if err != nil {
		t.Fatalf("node1: %v", err)
	}
	defer stop1()
	n2, stop2, err := startNode(t, "127.0.0.1:13011", "127.0.0.1:13011", "127.0.0.1:13010", dir2, true)
	if err != nil {
		t.Fatalf("node2: %v", err)
	}
	defer stop2()

	// allow leader election
	time.Sleep(2 * time.Second)

	leader, follower := n1, n2
	ldir := dir1
	if n2.IsLeader() {
		leader, follower = n2, n1
		ldir = dir2
	}
	dfs.SetNode(leader)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		if err := Watch(ctx, ldir); err != nil && err != context.Canceled {
			t.Errorf("watch: %v", err)
		}
	}()
	time.Sleep(100 * time.Millisecond)

	// create file on leader's cache
	data := []byte("replicate")
	if err := os.WriteFile(filepath.Join(ldir, "f.txt"), data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	// wait for watch to replicate
	deadline := time.Now().Add(2 * time.Second)
	for {
		if v, ok := leader.Get("f.txt"); ok && string(v) == string(data) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("leader missing data")
		}
		time.Sleep(50 * time.Millisecond)
	}
	deadline = time.Now().Add(2 * time.Second)
	for {
		if v, ok := follower.Get("f.txt"); ok && string(v) == string(data) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected file replicated")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
