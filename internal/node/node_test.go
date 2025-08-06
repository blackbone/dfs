package node_test

import (
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"

	"dfs/internal/node"
	"dfs/internal/server"
	"dfs/internal/store"
	pb "dfs/proto"
)

// getFreePort returns address on localhost with free TCP port.
func getFreePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

// waitLeader waits until one of the nodes reports itself leader.
func waitLeader(nodes ...*node.Node) *node.Node {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.IsLeader() {
				return n
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

func TestNewInvalidAddress(t *testing.T) {
	_, err := node.New("n1", "127.0.0.1:bad", t.TempDir(), "")
	if err == nil {
		t.Fatalf("expected error for bad address")
	}
}

func TestNodePutGetSingleNode(t *testing.T) {
	addr := getFreePort(t)
	n, err := node.New("n1", addr, t.TempDir(), "")
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer n.Shutdown()
	if waitLeader(n) != n {
		t.Fatalf("single node not elected leader")
	}

	if err := n.Put("foo", []byte("bar")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if v, ok := n.Get("foo"); !ok || string(v) != "bar" {
		t.Fatalf("get expected bar, got %q ok=%v", v, ok)
	}
	if _, ok := n.Get("missing"); ok {
		t.Fatalf("expected missing key to be absent")
	}
}

func TestNodeReplicationAndFollowerPut(t *testing.T) {
	addr1 := getFreePort(t)
	addr2 := getFreePort(t)
	n1, err := node.New(addr1, addr1, t.TempDir(), addr2)
	if err != nil {
		t.Fatalf("new n1: %v", err)
	}
	defer n1.Shutdown()
	n2, err := node.New(addr2, addr2, t.TempDir(), addr1)
	if err != nil {
		t.Fatalf("new n2: %v", err)
	}
	defer n2.Shutdown()

	leader := waitLeader(n1, n2)
	if leader == nil {
		t.Fatalf("no leader elected")
	}
	var follower *node.Node
	if leader == n1 {
		follower = n2
	} else {
		follower = n1
	}

	if err := follower.Put("k", []byte("v")); err == nil {
		t.Fatalf("follower put expected error")
	}
	if err := leader.Put("k", []byte("v")); err != nil {
		t.Fatalf("leader put: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if v, ok := follower.Get("k"); ok && string(v) == "v" {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("follower did not replicate value")
}

func TestNodePersistence(t *testing.T) {
	addr := getFreePort(t)
	dir := t.TempDir()
	n, err := node.New("n1", addr, dir, "")
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if waitLeader(n) == nil {
		t.Fatalf("no leader")
	}
	if err := n.Put("p", []byte("q")); err != nil {
		t.Fatalf("put: %v", err)
	}
	n.Shutdown()

	n2, err := node.New("n1", addr, dir, "")
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	if waitLeader(n2) == nil {
		t.Fatalf("no leader after restart")
	}
	if v, ok := n2.Get("p"); !ok || string(v) != "q" {
		t.Fatalf("expected persisted value, got %q ok=%v", v, ok)
	}
	n2.Shutdown()
}

func TestNodeRestore(t *testing.T) {
	addr1 := getFreePort(t)
	addr2 := getFreePort(t)
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	n1, err := node.New(addr1, addr1, dir1, addr2)
	if err != nil {
		t.Fatalf("n1: %v", err)
	}
	n2, err := node.New(addr2, addr2, dir2, addr1)
	if err != nil {
		t.Fatalf("n2: %v", err)
	}

	leader := waitLeader(n1, n2)
	if leader == nil {
		t.Fatalf("no leader")
	}
	var follower *node.Node
	if leader == n1 {
		follower = n2
	} else {
		follower = n1
	}
	cmd := store.Command{Op: store.OpPut, Key: store.S2B("x"), Data: []byte("y")}
	b, _ := json.Marshal(cmd)
	follower.Store.Apply(&raft.Log{Data: b})

	followerGRPC := getFreePort(t)
	lis, err := net.Listen("tcp", followerGRPC)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterFileServiceServer(srv, server.New(follower))
	go srv.Serve(lis)
	defer func() { srv.Stop(); lis.Close() }()

	if err := leader.Restore([]string{followerGRPC}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if v, ok := leader.Get("x"); !ok || string(v) != "y" {
		t.Fatalf("leader missing restored data")
	}
	leader.Shutdown()
	follower.Shutdown()
}
