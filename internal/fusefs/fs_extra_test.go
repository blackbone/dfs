package fusefs

import (
	"context"
	"testing"

	"bazil.org/fuse"
)

const fileData = "data"

func TestFileAttrAndReadAll(t *testing.T) {
	fs := New(t.TempDir())
	fs.mem[fileName] = []byte(fileData)
	f := &File{fs: fs, path: fileName}
	ctx := context.TODO()
	var a fuse.Attr
	if err := f.Attr(ctx, &a); err != nil {
		t.Fatalf("attr: %v", err)
	}
	if a.Size != uint64(len(fileData)) {
		t.Fatalf("size: %d", a.Size)
	}
	b, err := f.ReadAll(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(b) != fileData {
		t.Fatalf("data: %q", b)
	}
}
