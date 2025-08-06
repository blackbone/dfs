package dfs

import (
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"dfs/internal/node"
)

func freePort(tb testing.TB) string {
	tb.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func startNode(tb testing.TB) *node.Node {
	addr := freePort(tb)
	host := tb.TempDir()
	blob := filepath.Join(tb.TempDir(), "blobs")
	n, err := node.New("n", addr, host, blob, nil)
	if err != nil {
		tb.Fatalf("node: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	return n
}

func BenchmarkPutGet(b *testing.B) {
	n := startNode(b)
	for i := 0; i < b.N; i++ {
		path := fmt.Sprintf("file-%d.txt", i)
		if err := n.PutFile(path, []byte("data")); err != nil {
			b.Fatalf("put: %v", err)
		}
		if _, err := n.GetFile(path); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}
