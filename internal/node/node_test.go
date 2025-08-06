package node

import (
	"net"
	"path/filepath"
	"testing"
	"time"
)

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func TestPutGetFileSingleNode(t *testing.T) {
	addr := freePort(t)
	host := t.TempDir()
	blob := filepath.Join(t.TempDir(), "blobs")
	n, err := New("n1", addr, host, blob, nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	// give server time to start
	time.Sleep(200 * time.Millisecond)
	if err := n.PutFile("file.txt", []byte("data")); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, err := n.GetFile("file.txt")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != "data" {
		t.Fatalf("unexpected data %q", got)
	}
}
