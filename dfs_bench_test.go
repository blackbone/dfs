package dfs

import (
	"strconv"
	"testing"

	"dfs/internal/node"
)

const (
	benchVal       = "bench"
	benchKeyPrefix = "bench-"
)

func BenchmarkGetFile(b *testing.B) {
	addr := freePort(b)
	n, err := node.New("b1", addr, b.TempDir(), emptyString, true)
	if err != nil {
		b.Fatalf("new: %v", err)
	}
	SetNode(n)
	waitLeader(b, n)
	if err := PutFile(benchKeyPrefix, []byte(benchVal)); err != nil {
		b.Fatalf("put: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := GetFile(benchKeyPrefix); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

func BenchmarkPutFile(b *testing.B) {
	addr := freePort(b)
	n, err := node.New("b2", addr, b.TempDir(), emptyString, true)
	if err != nil {
		b.Fatalf("new: %v", err)
	}
	SetNode(n)
	waitLeader(b, n)
	data := []byte(benchVal)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := benchKeyPrefix + strconv.Itoa(i)
		if err := PutFile(key, data); err != nil {
			b.Fatalf("put: %v", err)
		}
	}
}
