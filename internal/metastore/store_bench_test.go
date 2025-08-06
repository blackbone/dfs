package metastore

import (
	"fmt"
	"testing"
)

func BenchmarkStoreGet(b *testing.B) {
	s := New()
	var h [hashSize]byte
	const total = 1024
	for i := 0; i < total; i++ {
		p := fmt.Sprintf("/f/%d", i)
		s.Sync(&Entry{Path: p, Version: 1, Hash: h})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Get("/f/512")
	}
}
