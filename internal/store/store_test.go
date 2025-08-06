package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
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
	cmd := Command{Op: OpPut, Key: S2B("foo"), Data: []byte("bar")}
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
	cmd = Command{Op: OpDelete, Key: S2B("foo")}
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
	b, _ := json.Marshal(Command{Op: OpPut, Key: S2B("foo"), Data: []byte("bar")})
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

func TestStoreRestoreInvalidData(t *testing.T) {
	s := New()
	if err := s.Restore(io.NopCloser(bytes.NewBufferString("bad"))); err == nil {
		t.Fatalf("expected error")
	}
}

type errSink struct {
	failWrite bool
	failClose bool
	canceled  bool
}

func (e *errSink) Write(p []byte) (int, error) {
	if e.failWrite {
		return 0, errors.New("write")
	}
	return len(p), nil
}

func (e *errSink) ID() string    { return "err" }
func (e *errSink) Cancel() error { e.canceled = true; return nil }
func (e *errSink) Close() error {
	if e.failClose {
		return errors.New("close")
	}
	return nil
}

func TestSnapshotPersistError(t *testing.T) {
	s := New()
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	es := &errSink{failWrite: true}
	if err := snap.Persist(es); err == nil || !es.canceled {
		t.Fatalf("expected write error and cancel")
	}
	es = &errSink{failClose: true}
	if err := snap.Persist(es); err == nil {
		t.Fatalf("expected close error")
	}
}

func TestStringByteConversion(t *testing.T) {
	sizes := []int{0, 1, 1 << 16}
	for _, n := range sizes {
		s := strings.Repeat(string(rune(97)), n)
		b := S2B(s)
		if got := B2S(b); got != s {
			t.Fatalf("mismatch size=%d", n)
		}
	}
}
