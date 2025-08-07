package fusefs

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"bazil.org/fuse"

	"dfs"
	"dfs/internal/metastore"
	"dfs/internal/node"
)

const (
	fileName = "f"
	oldData  = "old"
	newData  = "new"
	verNew   = 2
)

func prepNode() {
	nd := node.NewInmem()
	nd.Put(fileName, []byte(newData))
	hash := sha256.Sum256([]byte(newData))
	nd.Meta.Sync(&metastore.Entry{Path: fileName, Version: verNew, Hash: hash})
	dfs.SetNode(nd)
}

func TestScanUpdates(t *testing.T) {
	prepNode()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, fileName), []byte(oldData), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, fileName+verSuffix), []byte("1"), 0o644); err != nil {
		t.Fatalf("write ver: %v", err)
	}
	scan(dir)
	b, err := os.ReadFile(filepath.Join(dir, fileName))
	if err != nil || string(b) != newData {
		t.Fatalf("unexpected data %q err %v", b, err)
	}
	vb, err := os.ReadFile(filepath.Join(dir, fileName+verSuffix))
	if err != nil || string(vb) != "2" {
		t.Fatalf("unexpected ver %q err %v", vb, err)
	}
}

func TestScanDeletes(t *testing.T) {
	nd := node.NewInmem()
	dfs.SetNode(nd)
	dir := t.TempDir()
	f := filepath.Join(dir, fileName)
	if err := os.WriteFile(f, []byte(oldData), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.WriteFile(f+verSuffix, []byte("1"), 0o644); err != nil {
		t.Fatalf("write ver: %v", err)
	}
	scan(dir)
	if _, err := os.Stat(f); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("file still exists")
	}
	if _, err := os.Stat(f + verSuffix); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ver file still exists")
	}
}

func TestDirRemoveCacheOnly(t *testing.T) {
	prepNode()
	dir := t.TempDir()
	fs := New(dir)
	data := []byte(newData)
	if err := os.WriteFile(filepath.Join(dir, fileName), data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, fileName+verSuffix), []byte("2"), 0o644); err != nil {
		t.Fatalf("write ver: %v", err)
	}
	fs.mu.Lock()
	fs.mem[fileName] = cacheEntry{data: data, version: verNew}
	fs.mu.Unlock()
	d := &Dir{fs: fs, path: ""}
	if err := d.Remove(context.Background(), &fuse.RemoveRequest{Name: fileName}); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, ok := fs.mem[fileName]; ok {
		t.Fatalf("mem not cleared")
	}
	if _, err := os.Stat(filepath.Join(dir, fileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("file still exists")
	}
	if _, err := dfs.GetMetadata(fileName); err != nil {
		t.Fatalf("metadata missing: %v", err)
	}
}

func BenchmarkScan(b *testing.B) {
	prepNode()
	dir := b.TempDir()
	for i := 0; i < 100; i++ {
		name := filepath.Join(dir, strconv.Itoa(i))
		os.WriteFile(name, []byte(oldData), 0o644)
		os.WriteFile(name+verSuffix, []byte("1"), 0o644)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scan(dir)
	}
}
