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
	_, err := New("n1", "127.0.0.1:bad", t.TempDir(), "", true)
	if err == nil {
		t.Fatalf("expected error for bad address")
	}
}

func TestNodePutGetSingleNode(t *testing.T) {
	addr := getFreePort(t)
	n, err := New("n1", addr, t.TempDir(), "", true)
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
	n1, err := New(addr1, addr1, t.TempDir(), addr2, true)
	if err != nil {
		t.Fatalf("new n1: %v", err)
	}
	defer n1.raft.Shutdown()
	n2, err := New(addr2, addr2, t.TempDir(), addr1, true)
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
	if _, err := New(idA, addr, f.Name(), empty, true); err == nil {
		t.Fatalf("expected error")
	}
}

func TestLeader(t *testing.T) {
	addr := getFreePort(t)
	n, err := New(idA, addr, t.TempDir(), empty, true)
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

func TestAddRemovePeer(t *testing.T) {
	addr1 := getFreePort(t)
	n1, err := New(idA, addr1, t.TempDir(), empty, true)
	if err != nil {
		t.Fatalf("n1: %v", err)
	}
	defer n1.raft.Shutdown()

	addr2 := getFreePort(t)
	n2, err := New(idB, addr2, t.TempDir(), empty, false)
	if err != nil {
		t.Fatalf("n2: %v", err)
	}
	defer n2.raft.Shutdown()

	if waitLeader(n1) != n1 {
		t.Fatalf("n1 not leader")
	}
	if err := n1.AddPeer(idB, addr2); err != nil {
		t.Fatalf("add peer: %v", err)
	}

	deadline := time.Now().Add(time.Duration(timeout) * time.Second)
	for time.Now().Before(deadline) {
		if n2.Leader() != raft.ServerAddress(empty) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err := n1.Put("k1", []byte("v1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	deadline = time.Now().Add(time.Duration(timeout) * time.Second)
	for time.Now().Before(deadline) {
		if v, ok := n2.Get("k1"); ok && string(v) == "v1" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err := n1.RemovePeer(idB); err != nil {
		t.Fatalf("remove: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	if err := n1.Put("k2", []byte("v2")); err != nil {
		t.Fatalf("put2: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	if _, ok := n2.Get("k2"); ok {
		t.Fatalf("expected no replication after removal")
	}
}

func TestAddPeerNotLeader(t *testing.T) {
	addr1 := getFreePort(t)
	addr2 := getFreePort(t)
	n1, err := New(addr1, addr1, t.TempDir(), addr2, true)
	if err != nil {
		t.Fatalf("n1: %v", err)
	}
	defer n1.raft.Shutdown()
	n2, err := New(addr2, addr2, t.TempDir(), addr1, true)
	if err != nil {
		t.Fatalf("n2: %v", err)
	}
	defer n2.raft.Shutdown()
	leader := waitLeader(n1, n2)
	if leader == nil {
		t.Fatalf("no leader")
	}
	follower := n2
	if leader == n2 {
		follower = n1
	}
	if err := follower.AddPeer("x", "127.0.0.1:9999"); err == nil {
		t.Fatalf("expected error from follower")
	}
}
