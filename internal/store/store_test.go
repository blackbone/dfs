package store

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/hashicorp/raft"
)

// memSink implements raft.SnapshotSink in memory.
type memSink struct {
	bytes.Buffer
}

func (m *memSink) ID() string    { return "mem" }
func (m *memSink) Cancel() error { return nil }
func (m *memSink) Close() error  { return nil }

func TestStoreApplyAndGet(t *testing.T) {
	s := New()
	cmd := Command{Op: "put", Key: "foo", Data: []byte("bar")}
	b, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if res := s.Apply(&raft.Log{Data: b}); res != nil {
		t.Fatalf("apply put: %v", res)
	}
	v, ok := s.Get("foo")
	if !ok || string(v) != "bar" {
		t.Fatalf("expected bar, got %q ok=%v", v, ok)
	}
	cmd = Command{Op: "delete", Key: "foo"}
	b, _ = json.Marshal(cmd)
	if res := s.Apply(&raft.Log{Data: b}); res != nil {
		t.Fatalf("apply delete: %v", res)
	}
	if _, ok := s.Get("foo"); ok {
		t.Fatalf("expected key removed")
	}
}

func TestStoreSnapshotRestore(t *testing.T) {
	s := New()
	b, _ := json.Marshal(Command{Op: "put", Key: "foo", Data: []byte("bar")})
	s.Apply(&raft.Log{Data: b})

	snap, err := s.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	ms := &memSink{}
	if err := snap.Persist(ms); err != nil {
		t.Fatalf("persist: %v", err)
	}
	s2 := New()
	if err := s2.Restore(io.NopCloser(bytes.NewReader(ms.Bytes()))); err != nil {
		t.Fatalf("restore: %v", err)
	}
	v, ok := s2.Get("foo")
	if !ok || string(v) != "bar" {
		t.Fatalf("expected bar, got %q ok=%v", v, ok)
	}
}
