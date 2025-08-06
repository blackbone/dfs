package blobstore

import (
	"bytes"
	"strconv"
	"sync"
	"testing"
)

const (
	nameA      = "a.txt"
	nameB      = "b.txt"
	ver1       = 1
	dataA      = "hello"
	goroutines = 4
	items      = 128
)

func TestPutGet(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	if err := s.Put(nameA, ver1, []byte(dataA)); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	got, err := s.Get(nameA, ver1)
	if err != nil || !bytes.Equal(got, []byte(dataA)) {
		t.Fatalf("unexpected get %v %q", err, got)
	}
}

func TestGetMissing(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	if _, err := s.Get(nameB, ver1); err == nil {
		t.Fatalf("expected error")
	}
}

func TestConcurrent(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < items; i++ {
				n := nameA + strconv.Itoa(id) + strconv.Itoa(i)
				if err := s.Put(n, ver1, []byte(dataA)); err != nil {
					t.Errorf("put: %v", err)
					return
				}
				if _, err := s.Get(n, ver1); err != nil {
					t.Errorf("get: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}
