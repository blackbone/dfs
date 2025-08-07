package fusefs

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"bazil.org/fuse"
	bazilfs "bazil.org/fuse/fs"
	"github.com/fsnotify/fsnotify"

	"dfs"
)

var (
	mountFn   = fuse.Mount
	serveFn   = bazilfs.Serve
	putFileFn = dfs.PutFile
	watchFn   = func() (watcher, error) {
		w, err := fsnotify.NewWatcher()
		return &fsWatcher{Watcher: w}, err
	}
)

const verSuffix = ".ver"

type watcher interface {
	Add(string) error
	Close() error
	Events() <-chan fsnotify.Event
	Errors() <-chan error
}

type fsWatcher struct{ *fsnotify.Watcher }

func (w *fsWatcher) Events() <-chan fsnotify.Event { return w.Watcher.Events }
func (w *fsWatcher) Errors() <-chan error          { return w.Watcher.Errors }

// FS implements a simple read-only FUSE filesystem backed by a cache
// directory and the DFS for missing files.
type cacheEntry struct {
	data    []byte
	version uint64
}

type FS struct {
	cacheDir string

	mu  sync.RWMutex
	mem map[string]cacheEntry
}

// New returns a new filesystem.
func New(cacheDir string) *FS {
	return &FS{cacheDir: cacheDir, mem: make(map[string]cacheEntry)}
}

// Root returns the root directory node.
func (f *FS) Root() (bazilfs.Node, error) {
	return &Dir{fs: f, path: ""}, nil
}

// ensure returns file data for the given path, loading it from the cache
// or DFS as needed.
func (f *FS) ensure(path string) ([]byte, error) {
	meta, err := dfs.GetMetadata(path)
	if err != nil {
		f.mu.Lock()
		delete(f.mem, path)
		f.mu.Unlock()
		diskPath := filepath.Join(f.cacheDir, path)
		os.Remove(diskPath)
		os.Remove(diskPath + verSuffix)
		return nil, err
	}
	f.mu.RLock()
	ce, ok := f.mem[path]
	f.mu.RUnlock()
	if ok && ce.version == meta.Version {
		return ce.data, nil
	}
	diskPath := filepath.Join(f.cacheDir, path)
	verPath := diskPath + verSuffix
	if data, err := os.ReadFile(diskPath); err == nil {
		if vb, err := os.ReadFile(verPath); err == nil {
			if v, err := strconv.ParseUint(string(vb), 10, 64); err == nil && v == meta.Version {
				f.mu.Lock()
				f.mem[path] = cacheEntry{data: data, version: v}
				f.mu.Unlock()
				return data, nil
			}
		}
		os.Remove(diskPath)
		os.Remove(verPath)
	}
	log.Printf("fetching %s from DFS", path)
	data, err := dfs.GetFile(path)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.mem[path] = cacheEntry{data: data, version: meta.Version}
	f.mu.Unlock()
	go func() {
		if err := os.MkdirAll(filepath.Dir(diskPath), 0o755); err != nil {
			return
		}
		_ = os.WriteFile(diskPath, data, 0o644)
		_ = os.WriteFile(verPath, []byte(strconv.FormatUint(meta.Version, 10)), 0o644)
	}()
	return data, nil
}

// Dir represents a directory.
type Dir struct {
	fs   *FS
	path string
}

// Attr sets the attributes for the directory.
func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0o555
	return nil
}

// Lookup looks up a specific entry in the receiver.
func (d *Dir) Lookup(ctx context.Context, name string) (bazilfs.Node, error) {
	full := filepath.Join(d.path, name)
	diskPath := filepath.Join(d.fs.cacheDir, full)
	if fi, err := os.Stat(diskPath); err == nil && fi.IsDir() {
		return &Dir{fs: d.fs, path: full}, nil
	}
	return &File{fs: d.fs, path: full}, nil
}

// ReadDirAll reads the contents of the directory from the cache only.
func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	dir := filepath.Join(d.fs.cacheDir, d.path)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var res []fuse.Dirent
	for _, e := range entries {
		de := fuse.Dirent{Name: e.Name()}
		if e.IsDir() {
			de.Type = fuse.DT_Dir
		} else {
			de.Type = fuse.DT_File
		}
		res = append(res, de)
	}
	return res, nil
}

// Remove deletes the local cache copy of the file.
func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	full := filepath.Join(d.path, req.Name)
	disk := filepath.Join(d.fs.cacheDir, full)
	d.fs.mu.Lock()
	delete(d.fs.mem, full)
	d.fs.mu.Unlock()
	os.Remove(disk)
	os.Remove(disk + verSuffix)
	return nil
}

// File represents a file node.
type File struct {
	fs   *FS
	path string
}

// Attr sets attributes for the file.
func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	data, err := f.fs.ensure(f.path)
	if err != nil {
		return err
	}
	a.Mode = 0o444
	a.Size = uint64(len(data))
	return nil
}

// ReadAll reads the file data.
func (f *File) ReadAll(ctx context.Context) ([]byte, error) {
	return f.fs.ensure(f.path)
}

// Mount mounts the filesystem at the given mount point.
func Mount(mountPoint, cacheDir string) error {
	if err := os.MkdirAll(mountPoint, 0o755); err != nil {
		return err
	}
	fs := New(cacheDir)
	c, err := mountFn(mountPoint, fuse.AllowOther())
	if err != nil {
		return err
	}
	go func() {
		if err := serveFn(c, fs); err != nil {
			log.Printf("serve: %v", err)
		}
	}()
	return nil
}

// Watch monitors cache directory changes and replicates new or modified files
// into the DFS. The watch runs until ctx is canceled.
func Watch(ctx context.Context, cacheDir string) error {
	watcher, err := watchFn()
	if err != nil {
		return err
	}
	defer watcher.Close()
	addDir := func(p string) {
		if err := watcher.Add(p); err != nil {
			log.Printf("watch add: %v", err)
		}
	}
	filepath.WalkDir(cacheDir, func(p string, d os.DirEntry, err error) error {
		if err == nil && d.IsDir() {
			addDir(p)
		}
		return nil
	})
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-watcher.Events():
			if !ok {
				return nil
			}
			if ev.Op&(fsnotify.Create|fsnotify.Write) != 0 {
				if fi, err := os.Stat(ev.Name); err == nil {
					if fi.IsDir() {
						addDir(ev.Name)
					} else {
						rel, err := filepath.Rel(cacheDir, ev.Name)
						if err == nil {
							if data, err := os.ReadFile(ev.Name); err == nil {
								if err := putFileFn(rel, data); err != nil {
									log.Printf("put file: %v", err)
								}
							}
						}
					}
				}
			}
		case err, ok := <-watcher.Errors():
			if !ok {
				return nil
			}
			log.Printf("watcher error: %v", err)
		}
	}
}
