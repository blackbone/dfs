package node

import (
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"
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
func waitLeader(nodes ...*Node) *Node {
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
	_, err := New("n1", "127.0.0.1:bad", t.TempDir(), "")
	if err == nil {
		t.Fatalf("expected error for bad address")
	}
}

func TestNodePutGetSingleNode(t *testing.T) {
	addr := getFreePort(t)
	n, err := New("n1", addr, t.TempDir(), "")
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
	n1, err := New(addr1, addr1, t.TempDir(), addr2)
	if err != nil {
		t.Fatalf("new n1: %v", err)
	}
	defer n1.Shutdown()
	n2, err := New(addr2, addr2, t.TempDir(), addr1)
	if err != nil {
		t.Fatalf("new n2: %v", err)
	}
	defer n2.Shutdown()

	leader := waitLeader(n1, n2)
	if leader == nil {
		t.Fatalf("no leader elected")
	}
	var follower *Node
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

func TestNodeJoinAndLeave(t *testing.T) {
	addr1 := getFreePort(t)
	n1, err := New("n1", addr1, t.TempDir(), "")
	if err != nil {
		t.Fatalf("new n1: %v", err)
	}
	defer n1.Shutdown()
	if waitLeader(n1) != n1 {
		t.Fatalf("n1 not leader")
	}

	addr2 := getFreePort(t)
	n2, err := New("n2", addr2, t.TempDir(), "")
	if err != nil {
		t.Fatalf("new n2: %v", err)
	}
	defer n2.Shutdown()

	if err := n1.Join("n2", addr2); err != nil {
		t.Fatalf("join: %v", err)
	}

	if err := n1.Put("k", []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if v, ok := n2.Get("k"); ok && string(v) == "v" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if v, ok := n2.Get("k"); !ok || string(v) != "v" {
		t.Fatalf("replication failed: %q %v", v, ok)
	}

	if err := n1.Leave("n2"); err != nil {
		t.Fatalf("leave: %v", err)
	}
	f := n1.raft.GetConfiguration()
	if err := f.Error(); err != nil {
		t.Fatalf("config: %v", err)
	}
	for _, s := range f.Configuration().Servers {
		if s.ID == raft.ServerID("n2") {
			t.Fatalf("n2 still in config")
		}
	}
}

func TestNodeJoinNotLeader(t *testing.T) {
	addr1 := getFreePort(t)
	addr2 := getFreePort(t)
	n1, err := New(addr1, addr1, t.TempDir(), addr2)
	if err != nil {
		t.Fatalf("n1: %v", err)
	}
	defer n1.Shutdown()
	n2, err := New(addr2, addr2, t.TempDir(), addr1)
	if err != nil {
		t.Fatalf("n2: %v", err)
	}
	defer n2.Shutdown()

	leader := waitLeader(n1, n2)
	if leader == nil {
		t.Fatalf("no leader")
	}
	follower := n1
	if leader == n1 {
		follower = n2
	}

	if err := follower.Join("n3", getFreePort(t)); err == nil {
		t.Fatalf("expected error from follower join")
	}

	if err := follower.Leave("n1"); err == nil {
		t.Fatalf("expected error from follower leave")
	}
}
