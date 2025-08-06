package node

import (
	"net"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

const (
	host    = "127.0.0.1"
	empty   = ""
	idA     = "n1"
	idB     = "n2"
	timeout = 5
	tmpPref = "file"
)

// getFreePort returns address on localhost with free TCP port.
func getFreePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", host+":0")
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
	defer n.raft.Shutdown()
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
	defer n1.raft.Shutdown()
	n2, err := New(addr2, addr2, t.TempDir(), addr1)
	if err != nil {
		t.Fatalf("new n2: %v", err)
	}
	defer n2.raft.Shutdown()

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

func TestNewSnapshotDirIsFile(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), tmpPref)
	if err != nil {
		t.Fatalf("temp: %v", err)
	}
	f.Close()
	addr := getFreePort(t)
	if _, err := New(idA, addr, f.Name(), empty); err == nil {
		t.Fatalf("expected error")
	}
}

func TestLeader(t *testing.T) {
	addr := getFreePort(t)
	n, err := New(idA, addr, t.TempDir(), empty)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer n.raft.Shutdown()
	if waitLeader(n) != n {
		t.Fatalf("no leader")
	}
	if n.Leader() == raft.ServerAddress(empty) {
		t.Fatalf("empty leader")
	}
}
