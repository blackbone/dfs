package fusefs

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

const (
	dirName  = "sub"
	fileName = "a.txt"
)

func TestDirLookup(t *testing.T) {
	cacheDir := t.TempDir()
	if err := os.Mkdir(filepath.Join(cacheDir, dirName), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	fs := New(cacheDir)
	d := &Dir{fs: fs, path: ""}
	ctx := context.TODO()
	n, err := d.Lookup(ctx, dirName)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if _, ok := n.(*Dir); !ok {
		t.Fatalf("expected Dir node")
	}
}

func TestDirReadDirAll(t *testing.T) {
	cacheDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(cacheDir, fileName), []byte{}, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	fs := New(cacheDir)
	d := &Dir{fs: fs, path: ""}
	ctx := context.TODO()
	ents, err := d.ReadDirAll(ctx)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(ents) != 1 || ents[0].Name != fileName {
		t.Fatalf("unexpected entries: %v", ents)
	}
}
