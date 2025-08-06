package metadata

import (
	"encoding/json"
	"sync"
)

type FileMeta struct {
	Path     string   `json:"path"`
	Version  int      `json:"version"`
	Hash     string   `json:"hash"`
	Owner    string   `json:"owner"`
	Replicas []string `json:"replicas,omitempty"`
	Deleted  bool     `json:"deleted"`
}

type Store struct {
	mu    sync.RWMutex
	files map[string]*FileMeta
}

func New() *Store { return &Store{files: make(map[string]*FileMeta)} }

func (s *Store) Get(path string) (*FileMeta, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.files[path]
	if !ok || m.Deleted {
		return nil, false
	}
	c := *m
	return &c, true
}

func (s *Store) Put(m *FileMeta) {
	s.mu.Lock()
	s.files[m.Path] = m
	s.mu.Unlock()
}

func (s *Store) Delete(path string) {
	s.mu.Lock()
	if m, ok := s.files[path]; ok {
		m.Deleted = true
	} else {
		s.files[path] = &FileMeta{Path: path, Deleted: true}
	}
	s.mu.Unlock()
}

func (s *Store) Marshal() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.files)
}

func (s *Store) Unmarshal(b []byte) error {
	files := make(map[string]*FileMeta)
	if err := json.Unmarshal(b, &files); err != nil {
		return err
	}
	s.mu.Lock()
	s.files = files
	s.mu.Unlock()
	return nil
}
