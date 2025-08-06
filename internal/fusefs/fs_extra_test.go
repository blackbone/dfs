package fusefs

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"bazil.org/fuse"
	bazilfs "bazil.org/fuse/fs"
	"github.com/fsnotify/fsnotify"
	"github.com/hashicorp/raft"

	"dfs"
	"dfs/internal/node"
	"dfs/internal/store"
)

// fakeWatcher implements watcher for testing.
type fakeWatcher struct {
	events chan fsnotify.Event
	errors chan error
	addErr error
	once   sync.Once
}

func (f *fakeWatcher) Add(string) error { return f.addErr }
func (f *fakeWatcher) Close() error {
	f.once.Do(func() {
		if f.events != nil {
			close(f.events)
		}
		if f.errors != nil {
			close(f.errors)
		}
	})
	return nil
}
func (f *fakeWatcher) Events() <-chan fsnotify.Event { return f.events }
func (f *fakeWatcher) Errors() <-chan error          { return f.errors }

func TestFSEnsureErrorPaths(t *testing.T) {
	dir := t.TempDir()
	fs := New(dir)
	dfs.SetNode(nil)
	if _, err := fs.ensure("missing"); err == nil {
		t.Fatalf("expected error")
	}
	st := store.New()
	cmd := store.Command{Op: store.OpPut, Key: store.S2B("a/b"), Data: []byte(dataValue)}
	b, _ := json.Marshal(cmd)
	st.Apply(&raft.Log{Data: b})
	dfs.SetNode(&node.Node{Store: st})
	if err := os.WriteFile(filepath.Join(dir, "a"), []byte(dataValue), 0o644); err != nil {
		t.Fatalf("prep: %v", err)
	}
	if _, err := fs.ensure("a/b"); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	time.Sleep(time.Duration(waitMS) * time.Millisecond)
}

func TestDirAndFileErrorPaths(t *testing.T) {
	dir := t.TempDir()
	fs := New(dir)
	d := &Dir{fs: fs, path: "nope"}
	var a fuse.Attr
	if err := d.Attr(nil, &a); err != nil {
		t.Fatalf("attr: %v", err)
	}
	if _, err := d.ReadDirAll(nil); err == nil {
		t.Fatalf("expected readdir error")
	}
	dfs.SetNode(nil)
	f := &File{fs: fs, path: "nope"}
	if err := f.Attr(nil, &a); err == nil {
		t.Fatalf("expected attr error")
	}
	if _, err := f.ReadAll(nil); err == nil {
		t.Fatalf("expected readall error")
	}
}

func TestMountVariants(t *testing.T) {
	t.Run("mkdir fail", func(t *testing.T) {
		dir := t.TempDir()
		file := filepath.Join(dir, "f")
		if err := os.WriteFile(file, []byte(dataValue), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := Mount(filepath.Join(file, "m"), dir); err == nil {
			t.Fatalf("expected error")
		}
	})
	t.Run("mount fail", func(t *testing.T) {
		old := mountFn
		mErr := errors.New("mount")
		mountFn = func(string, ...fuse.MountOption) (*fuse.Conn, error) { return nil, mErr }
		defer func() { mountFn = old }()
		if err := Mount(filepath.Join(t.TempDir(), "m1"), t.TempDir()); !errors.Is(err, mErr) {
			t.Fatalf("unexpected err: %v", err)
		}
	})
	t.Run("success", func(t *testing.T) {
		oldM, oldS := mountFn, serveFn
		mountFn = func(string, ...fuse.MountOption) (*fuse.Conn, error) { return nil, nil }
		called := make(chan struct{})
		serveFn = func(*fuse.Conn, bazilfs.FS) error { close(called); return nil }
		defer func() { mountFn, serveFn = oldM, oldS }()
		if err := Mount(filepath.Join(t.TempDir(), "m2"), t.TempDir()); err != nil {
			t.Fatalf("mount: %v", err)
		}
		select {
		case <-called:
		case <-time.After(time.Duration(waitMS) * time.Millisecond):
			t.Fatalf("serve not called")
		}
	})
	t.Run("serve error", func(t *testing.T) {
		oldM, oldS := mountFn, serveFn
		mountFn = func(string, ...fuse.MountOption) (*fuse.Conn, error) { return nil, nil }
		sErr := errors.New("serve")
		serveFn = func(*fuse.Conn, bazilfs.FS) error { return sErr }
		defer func() { mountFn, serveFn = oldM, oldS }()
		if err := Mount(filepath.Join(t.TempDir(), "m3"), t.TempDir()); err != nil {
			t.Fatalf("mount: %v", err)
		}
		time.Sleep(time.Duration(waitMS) * time.Millisecond)
	})
}

func TestWatchNewWatcherError(t *testing.T) {
	wErr := errors.New("watch")
	old := watchFn
	watchFn = func() (watcher, error) { return nil, wErr }
	defer func() { watchFn = old }()
	if err := Watch(context.Background(), t.TempDir()); !errors.Is(err, wErr) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWatchFlows(t *testing.T) {
	dir := t.TempDir()
	fw := &fakeWatcher{events: make(chan fsnotify.Event, 5), errors: make(chan error, 2), addErr: errors.New("add")}
	oldW, oldP := watchFn, putFileFn
	watchFn = func() (watcher, error) { return fw, nil }
	putErr := errors.New("put")
	putFileFn = func(string, []byte) error { return putErr }
	defer func() { watchFn, putFileFn = oldW, oldP }()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- Watch(ctx, dir) }()
	newDir := filepath.Join(dir, "sub")
	if err := os.Mkdir(newDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	fw.events <- fsnotify.Event{Op: fsnotify.Create, Name: newDir}
	file := filepath.Join(dir, cacheFile)
	if err := os.WriteFile(file, []byte(dataValue), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	fw.events <- fsnotify.Event{Op: fsnotify.Write, Name: file}
	fw.errors <- errors.New("watcherr")
	time.Sleep(time.Duration(waitMS) * time.Millisecond)
	fw.Close()
	cancel()
	if err := <-done; err != nil && err != context.Canceled {
		t.Fatalf("watch: %v", err)
	}
}

func TestWatchContextDone(t *testing.T) {
	fw := &fakeWatcher{events: make(chan fsnotify.Event), errors: make(chan error)}
	old := watchFn
	watchFn = func() (watcher, error) { return fw, nil }
	defer func() { watchFn = old }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Watch(ctx, t.TempDir()); err != context.Canceled {
		t.Fatalf("expected canceled, got %v", err)
	}
}

func TestWatchEventsChannelClosed(t *testing.T) {
	fw := &fakeWatcher{events: make(chan fsnotify.Event)}
	fw.Close()
	old := watchFn
	watchFn = func() (watcher, error) { return fw, nil }
	defer func() { watchFn = old }()
	if err := Watch(context.Background(), t.TempDir()); err != nil {
		t.Fatalf("watch: %v", err)
	}
}

func TestWatchErrorsChannelClosed(t *testing.T) {
	fw := &fakeWatcher{errors: make(chan error)}
	fw.Close()
	old := watchFn
	watchFn = func() (watcher, error) { return fw, nil }
	defer func() { watchFn = old }()
	if err := Watch(context.Background(), t.TempDir()); err != nil {
		t.Fatalf("watch: %v", err)
	}
}
