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
