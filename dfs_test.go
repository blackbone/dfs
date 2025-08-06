package dfs

import (
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"dfs/internal/node"
)

const (
	addrHost    = "127.0.0.1"
	emptyString = ""
	sampleID    = "id"
	sampleKey   = "k"
	sampleVal   = "v"
	missingKey  = "m"
	waitSeconds = 5
	sleepMillis = 10
)

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", addrHost+":0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()
	return l.Addr().String()
}

func waitLeader(t *testing.T, n *node.Node) {
	t.Helper()
	deadline := time.Now().Add(time.Duration(waitSeconds) * time.Second)
	for time.Now().Before(deadline) {
		if n.IsLeader() {
			return
		}
		time.Sleep(time.Duration(sleepMillis) * time.Millisecond)
	}
	t.Fatalf("leader not elected")
}

func TestGetFileNoNode(t *testing.T) {
	if _, err := GetFile(sampleKey); err == nil {
		t.Fatalf("expected error")
	}
}

func TestPutFileNoNode(t *testing.T) {
	if err := PutFile(sampleKey, []byte(sampleVal)); err == nil {
		t.Fatalf("expected error")
	}
}

func TestSetNodeAndFileOps(t *testing.T) {
	addr := freePort(t)
	n, err := node.New(sampleID, addr, t.TempDir(), emptyString)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	SetNode(n)
	waitLeader(t, n)
	if err := PutFile(sampleKey, []byte(sampleVal)); err != nil {
		t.Fatalf("put: %v", err)
	}
	data, err := GetFile(sampleKey)
	if err != nil || string(data) != sampleVal {
		t.Fatalf("get: %v data=%q", err, data)
	}
	if _, err := GetFile(missingKey); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected not exist, got %v", err)
	}
}
