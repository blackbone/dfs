package store

import (
	"bytes"
	"errors"
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
	const (
		key = "foo"
		val = "bar"
	)
	cmd := Command{Op: OpPut, Key: S2B(key), Data: []byte(val)}
	if res := s.Apply(&raft.Log{Data: cmd.MarshalBinary()}); res != nil {
		t.Fatalf("apply put: %v", res)
	}
	v, ok := s.Get(key)
	if !ok || string(v) != val {
		t.Fatalf("expected %s, got %q ok=%v", val, v, ok)
	}
	cmd = Command{Op: OpDelete, Key: S2B(key)}
	if res := s.Apply(&raft.Log{Data: cmd.MarshalBinary()}); res != nil {
		t.Fatalf("apply delete: %v", res)
	}
	if _, ok := s.Get(key); ok {
		t.Fatalf("expected key removed")
	}
}

func TestStoreSnapshotRestore(t *testing.T) {
	s := New()
	const (
		key = "foo"
		val = "bar"
	)
	b := (&Command{Op: OpPut, Key: S2B(key), Data: []byte(val)}).MarshalBinary()
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
	v, ok := s2.Get(key)
	if !ok || string(v) != val {
		t.Fatalf("expected %s, got %q ok=%v", val, v, ok)
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

func TestStoreApplyEmptyKey(t *testing.T) {
	s := New()
	const emptyVal = "v"
	cmd := &Command{Op: OpPut, Key: nil, Data: []byte(emptyVal)}
	if res := s.Apply(&raft.Log{Data: cmd.MarshalBinary()}); res != nil {
		t.Fatalf("apply: %v", res)
	}
	if v, ok := s.Get(""); !ok || string(v) != emptyVal {
		t.Fatalf("unexpected value %q ok=%v", v, ok)
	}
}

func TestStoreApplyLargeData(t *testing.T) {
	s := New()
	const (
		bigKey = "big"
		filler = 'a'
	)
	big := bytes.Repeat([]byte{filler}, 1<<20)
	cmd := &Command{Op: OpPut, Key: S2B(bigKey), Data: big}
	if res := s.Apply(&raft.Log{Data: cmd.MarshalBinary()}); res != nil {
		t.Fatalf("apply: %v", res)
	}
	v, ok := s.Get(bigKey)
	if !ok || len(v) != len(big) {
		t.Fatalf("unexpected len %d ok=%v", len(v), ok)
	}
}
