package fusefs

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"sync"

	"bazil.org/fuse"
	bazilfs "bazil.org/fuse/fs"
	"github.com/fsnotify/fsnotify"

	"dfs"
)

// FS implements a simple read-only FUSE filesystem backed by a cache
// directory and the DFS for missing files.
type FS struct {
	cacheDir string

	mu  sync.RWMutex
	mem map[string][]byte
}

// New returns a new filesystem.
func New(cacheDir string) *FS {
	return &FS{cacheDir: cacheDir, mem: make(map[string][]byte)}
}

// Root returns the root directory node.
func (f *FS) Root() (bazilfs.Node, error) {
	return &Dir{fs: f, path: ""}, nil
}

// ensure returns file data for the given path, loading it from the cache
// or DFS as needed.
func (f *FS) ensure(path string) ([]byte, error) {
	f.mu.RLock()
	data, ok := f.mem[path]
	f.mu.RUnlock()
	if ok {
		return data, nil
	}

	// Check on-disk cache
	diskPath := filepath.Join(f.cacheDir, path)
	data, err := os.ReadFile(diskPath)
	if err == nil {
		f.mu.Lock()
		f.mem[path] = data
		f.mu.Unlock()
		return data, nil
	}

	// Fetch from DFS
	log.Printf("fetching %s from DFS", path)
	data, err = dfs.GetFile(path)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.mem[path] = data
	f.mu.Unlock()

	// Save to cache asynchronously
	go func() {
		if err := os.MkdirAll(filepath.Dir(diskPath), 0o755); err != nil {
			return
		}
		_ = os.WriteFile(diskPath, data, 0o644)
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
	c, err := fuse.Mount(mountPoint, fuse.AllowOther())
	if err != nil {
		return err
	}
	go func() {
		if err := bazilfs.Serve(c, fs); err != nil {
			log.Fatalf("serve: %v", err)
		}
	}()
	return nil
}

// Watch monitors cache directory changes and logs new files.
func Watch(cacheDir string) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Printf("watcher: %v", err)
		return
	}
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
		case ev, ok := <-watcher.Events:
			if !ok {
				return
			}
			if ev.Op&fsnotify.Create != 0 {
				log.Printf("cache file created: %s", ev.Name)
				if fi, err := os.Stat(ev.Name); err == nil && fi.IsDir() {
					addDir(ev.Name)
				}
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			log.Printf("watcher error: %v", err)
		}
	}
}
