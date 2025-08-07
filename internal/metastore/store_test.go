package metastore

import (
	"crypto/sha256"
	"fmt"
	"sync"
	"testing"
)

const (
	pathA           = "/a"
	pathB           = "/b"
	node1 ReplicaID = 1
	node2 ReplicaID = 2
	data1           = "data1"
	data2           = "data2"
)

func TestStorePutGetDelete(t *testing.T) {
	s := New()
	h1 := sha256.Sum256([]byte(data1))
	s.Sync(&Entry{Path: pathA, Version: 1, Hash: h1, Replicas: []ReplicaID{node1}})
	e, ok := s.Get(pathA)
	if !ok || e.Version != 1 || len(e.Replicas) != 1 || e.Replicas[0] != node1 {
		t.Fatalf("unexpected entry %+v ok=%v", e, ok)
	}
	s.Delete(pathA, 2)
	if _, ok := s.Get(pathA); ok {
		t.Fatalf("expected deleted")
	}
}

func TestStoreSyncVersion(t *testing.T) {
	s := New()
	h1 := sha256.Sum256([]byte(data1))
	h2 := sha256.Sum256([]byte(data2))
	s.Sync(&Entry{Path: pathB, Version: 1, Hash: h1})
	s.Sync(&Entry{Path: pathB, Version: 2, Hash: h2, Replicas: []ReplicaID{node1, node2}})
	s.Sync(&Entry{Path: pathB, Version: 1, Hash: h1})
	e, ok := s.Get(pathB)
	if !ok || e.Version != 2 || len(e.Replicas) != 2 {
		t.Fatalf("unexpected entry %+v ok=%v", e, ok)
	}
}

func TestStoreConcurrent(t *testing.T) {
	s := New()
	var wg sync.WaitGroup
	const goroutines = 8
	const items = 1000
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < items; i++ {
				p := fmt.Sprintf("/%d/%d", id, i)
				s.Sync(&Entry{Path: p, Version: 1})
				s.Get(p)
			}
		}(g)
	}
	wg.Wait()
	if n := len(s.List()); n != goroutines*items {
		t.Fatalf("expected %d items, got %d", goroutines*items, n)
	}
}

func TestStoreGC(t *testing.T) {
	s := New()
	s.Delete(pathA, 1)
	s.GC()
	if _, ok := s.data[pathA]; ok {
		t.Fatalf("expected entry removed")
	}
}
