package blobstore

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	dirPerm   = 0o755
	filePerm  = 0o644
	sep       = '@'
	verPrefix = 'v'
	emptyPath = ""
)

var errEmptyPath = errors.New("empty path")

// Store persists blobs on disk under root directory.
type Store struct{ root string }

// New creates a Store rooted at dir.
func New(dir string) *Store { return &Store{root: dir} }

// Put writes data for path and version.
func (s *Store) Put(path string, version uint64, data []byte) error {
	if path == emptyPath {
		return errEmptyPath
	}
	p := s.blobPath(path, version)
	if err := os.MkdirAll(filepath.Dir(p), dirPerm); err != nil {
		return err
	}
	return os.WriteFile(p, data, filePerm)
}

// Get reads data for path and version.
func (s *Store) Get(path string, version uint64) ([]byte, error) {
	if path == emptyPath {
		return nil, errEmptyPath
	}
	return os.ReadFile(s.blobPath(path, version))
}

// GC removes blob files not present in keep map.
func (s *Store) GC(keep map[string]uint64) {
	filepath.WalkDir(s.root, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(s.root, p)
		if err != nil {
			return nil
		}
		parts := strings.Split(rel, string(sep))
		if len(parts) != 2 {
			os.Remove(p)
			return nil
		}
		verStr := strings.TrimPrefix(parts[1], string(verPrefix))
		ver, err := strconv.ParseUint(verStr, 10, 64)
		if err != nil {
			os.Remove(p)
			return nil
		}
		if v, ok := keep[parts[0]]; !ok || v != ver {
			os.Remove(p)
		}
		return nil
	})
}

func (s *Store) blobPath(path string, version uint64) string {
	trimmed := strings.TrimLeft(path, string(os.PathSeparator))
	base := filepath.Join(s.root, trimmed)
	var b strings.Builder
	b.Grow(len(base) + 2 + 20)
	b.WriteString(base)
	b.WriteByte(sep)
	b.WriteByte(verPrefix)
	b.WriteString(strconv.FormatUint(version, 10))
	return b.String()
}
