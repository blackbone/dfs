package node

import (
	"testing"
	"time"

	"dfs/internal/metastore"
)

func TestMetadataReplication(t *testing.T) {
	addr1 := getFreePort(t)
	addr2 := getFreePort(t)
	n1, err := New(addr1, addr1, t.TempDir(), addr2, true)
	if err != nil {
		t.Fatalf("new1: %v", err)
	}
	defer n1.raft.Shutdown()
	n2, err := New(addr2, addr2, t.TempDir(), addr1, true)
	if err != nil {
		t.Fatalf("new2: %v", err)
	}
	defer n2.raft.Shutdown()
	leader := waitLeader(n1, n2)
	if leader == nil {
		t.Fatalf("no leader")
	}
	follower := n1
	if leader == n1 {
		follower = n2
	}
	e := &metastore.Entry{Path: "m", Version: 1}
	if err := leader.SyncMeta(e); err != nil {
		t.Fatalf("sync: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fe, ok := follower.Meta.Get("m"); ok && fe.Version == 1 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("metadata not replicated")
}
