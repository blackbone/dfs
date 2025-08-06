package blobstore

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

type Store struct {
	root string
}

func New(root string) *Store { return &Store{root: root} }

func (s *Store) blobPath(path string, version int) string {
	base := filepath.Base(path)
	dir := filepath.Dir(path)
	name := fmt.Sprintf("%s@v%d", base, version)
	return filepath.Join(s.root, dir, name)
}

func (s *Store) Save(path string, version int, data []byte) error {
	p := s.blobPath(path, version)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	return ioutil.WriteFile(p, data, 0o644)
}

func (s *Store) Load(path string, version int) ([]byte, error) {
	p := s.blobPath(path, version)
	return ioutil.ReadFile(p)
}

func (s *Store) Delete(path string) {
	glob := filepath.Join(s.root, filepath.Dir(path), filepath.Base(path)+"@v*")
	matches, _ := filepath.Glob(glob)
	for _, m := range matches {
		_ = os.Remove(m)
	}
}
