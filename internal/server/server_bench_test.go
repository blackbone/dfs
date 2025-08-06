package server

import (
	"context"
	"strconv"
	"testing"

	"dfs/internal/node"
	pb "dfs/proto"
)

// BenchmarkServerPut measures Put throughput on a single leader node.
func BenchmarkServerPut(b *testing.B) {
	addr := freeAddr(b)
	n, err := node.New("b1", addr, b.TempDir(), "")
	if err != nil {
		b.Fatalf("new node: %v", err)
	}
	if waitLeader(n) == nil {
		b.Fatalf("node not leader")
	}
	client, cleanup := startGRPC(b, n)
	defer cleanup()
	const prefix = "bench-"
	ctx := context.Background()
	data := []byte("v")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := prefix + strconv.Itoa(i)
		if _, err := client.Put(ctx, &pb.PutRequest{Key: key, Data: data}); err != nil {
			b.Fatalf("put: %v", err)
		}
	}
}
