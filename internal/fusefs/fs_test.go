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

	fs.mu.Lock()
	fs.mem[cacheFile] = []byte(dataValue)
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
	if v, err := fs.ensure(cacheFile); err != nil || string(v) != dataValue {
		t.Fatalf("disk ensure: %v v=%q", err, v)
	}

	st := store.New()
	cmd := store.Command{Op: store.OpPut, Key: store.S2B(dfsFile), Data: []byte(dataValue)}
	b, _ := json.Marshal(cmd)
	st.Apply(&raft.Log{Data: b})
	dfs.SetNode(&node.Node{Store: st})
	if v, err := fs.ensure(dfsFile); err != nil || string(v) != dataValue {
		t.Fatalf("dfs ensure: %v v=%q", err, v)
	}
	time.Sleep(time.Duration(waitMS) * time.Millisecond)
	if _, err := os.Stat(filepath.Join(dir, dfsFile)); err != nil {
		t.Fatalf("cache missing: %v", err)
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
