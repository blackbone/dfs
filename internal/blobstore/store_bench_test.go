package blobstore

import (
	"strconv"
	"testing"
)

const (
	benchName = "bench"
	benchVer  = 1
	benchSize = 1024
)

func BenchmarkStorePut(b *testing.B) {
	dir := b.TempDir()
	s := New(dir)
	data := make([]byte, benchSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := benchName + strconv.Itoa(i)
		if err := s.Put(n, benchVer, data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStoreGet(b *testing.B) {
	dir := b.TempDir()
	s := New(dir)
	data := make([]byte, benchSize)
	if err := s.Put(benchName, benchVer, data); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Get(benchName, benchVer); err != nil {
			b.Fatal(err)
		}
	}
}
