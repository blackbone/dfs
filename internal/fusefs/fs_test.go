package fusefs

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"bazil.org/fuse"
	"github.com/hashicorp/raft"

	"dfs"
	"dfs/internal/metastore"
	"dfs/internal/node"
	"dfs/internal/store"
)

const (
	cacheFile = "file.dat"
	dfsFile   = "remote.dat"
	cacheDir  = "dir"
	dataValue = "data"
	waitMS    = 100
)

func TestFSEnsureAndNodes(t *testing.T) {
	dir := t.TempDir()
	fs := New(dir)
	meta := metastore.New()
	st := store.New()
	nd := &node.Node{Store: st, Meta: meta}
	dfs.SetNode(nd)

	meta.Sync(&metastore.Entry{Path: cacheFile, Version: 1})
	fs.mu.Lock()
	fs.mem[cacheFile] = cacheEntry{data: []byte(dataValue), version: 1}
	fs.mu.Unlock()
	if v, err := fs.ensure(cacheFile); err != nil || string(v) != dataValue {
		t.Fatalf("mem ensure: %v v=%q", err, v)
	}

	fs.mu.Lock()
	delete(fs.mem, cacheFile)
	fs.mu.Unlock()
	if err := os.WriteFile(filepath.Join(dir, cacheFile), []byte(dataValue), 0o644); err != nil {
		t.Fatalf("write cache: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, cacheFile+verSuffix), []byte("1"), 0o644); err != nil {
		t.Fatalf("write ver: %v", err)
	}
	if v, err := fs.ensure(cacheFile); err != nil || string(v) != dataValue {
		t.Fatalf("disk ensure: %v v=%q", err, v)
	}

	cmd := store.Command{Op: store.OpPut, Key: store.S2B(dfsFile), Data: []byte(dataValue)}
	b, _ := json.Marshal(cmd)
	st.Apply(&raft.Log{Data: b})
	meta.Sync(&metastore.Entry{Path: dfsFile, Version: 1})
	if v, err := fs.ensure(dfsFile); err != nil || string(v) != dataValue {
		t.Fatalf("dfs ensure: %v v=%q", err, v)
	}
	time.Sleep(time.Duration(waitMS) * time.Millisecond)
	if _, err := os.Stat(filepath.Join(dir, dfsFile)); err != nil {
		t.Fatalf("cache missing: %v", err)
	}
	if vb, err := os.ReadFile(filepath.Join(dir, dfsFile+verSuffix)); err != nil || string(vb) != "1" {
		t.Fatalf("version file: %v v=%q", err, vb)
	}

	root, err := fs.Root()
	if err != nil {
		t.Fatalf("root: %v", err)
	}
	d := root.(*Dir)
	if err := os.Mkdir(filepath.Join(dir, cacheDir), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if _, err := d.Lookup(nil, cacheDir); err != nil {
		t.Fatalf("lookup dir: %v", err)
	}
	if _, err := d.Lookup(nil, dfsFile); err != nil {
		t.Fatalf("lookup file: %v", err)
	}
	if entries, err := d.ReadDirAll(nil); err != nil || len(entries) == 0 {
		t.Fatalf("readdir: %v entries=%d", err, len(entries))
	}

	f := &File{fs: fs, path: dfsFile}
	var attr fuse.Attr
	if err := f.Attr(nil, &attr); err != nil || attr.Size == 0 {
		t.Fatalf("attr: %v size=%d", err, attr.Size)
	}
	if v, err := f.ReadAll(nil); err != nil || string(v) != dataValue {
		t.Fatalf("readall: %v v=%q", err, v)
	}
	dfs.SetNode(nil)
}

func TestEnsureVersionInvalidates(t *testing.T) {
	dir := t.TempDir()
	fs := New(dir)
	meta := metastore.New()
	st := store.New()
	nd := &node.Node{Store: st, Meta: meta}
	dfs.SetNode(nd)

	cmd1 := store.Command{Op: store.OpPut, Key: store.S2B(dfsFile), Data: []byte("v1")}
	b1, _ := json.Marshal(cmd1)
	st.Apply(&raft.Log{Data: b1})
	meta.Sync(&metastore.Entry{Path: dfsFile, Version: 1})
	if v, err := fs.ensure(dfsFile); err != nil || string(v) != "v1" {
		t.Fatalf("first ensure: %v v=%q", err, v)
	}
	time.Sleep(time.Duration(waitMS) * time.Millisecond)

	cmd2 := store.Command{Op: store.OpPut, Key: store.S2B(dfsFile), Data: []byte("v2")}
	b2, _ := json.Marshal(cmd2)
	st.Apply(&raft.Log{Data: b2})
	meta.Sync(&metastore.Entry{Path: dfsFile, Version: 2})
	if v, err := fs.ensure(dfsFile); err != nil || string(v) != "v2" {
		t.Fatalf("second ensure: %v v=%q", err, v)
	}
	time.Sleep(time.Duration(waitMS) * time.Millisecond)
	if vb, err := os.ReadFile(filepath.Join(dir, dfsFile+verSuffix)); err != nil || string(vb) != "2" {
		t.Fatalf("updated ver: %v v=%q", err, vb)
	}
	dfs.SetNode(nil)
}
