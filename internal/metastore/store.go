package metastore

import "sync"

// ReplicaID identifies a node replica.
type ReplicaID uint64

const (
	// hashSize defines size of stored hashes (sha256).
	hashSize  = 32
	emptyPath = ""
)

// Entry describes file metadata.
type Entry struct {
	Path     string
	Version  uint64
	Hash     [hashSize]byte
	Replicas []ReplicaID
	Deleted  bool
}

// Store keeps file metadata in memory.
type Store struct {
	mu   sync.RWMutex
	data map[string]*Entry
}

// New returns empty Store.
func New() *Store { return &Store{data: make(map[string]*Entry)} }

// Sync merges metadata entry by version. Higher versions overwrite.
func (s *Store) Sync(e *Entry) {
	if e.Path == emptyPath {
		return
	}
	s.mu.Lock()
	cur, ok := s.data[e.Path]
	if !ok || cur.Version < e.Version {
		copyEntry := *e
		if len(e.Replicas) > 0 {
			copyEntry.Replicas = append([]ReplicaID(nil), e.Replicas...)
		}
		s.data[e.Path] = &copyEntry
	}
	s.mu.Unlock()
}

// Get returns metadata for path if present and not deleted.
func (s *Store) Get(path string) (Entry, bool) {
	if path == emptyPath {
		return Entry{}, false
	}
	s.mu.RLock()
	e, ok := s.data[path]
	if !ok || e.Deleted {
		s.mu.RUnlock()
		return Entry{}, false
	}
	res := *e
	if len(e.Replicas) > 0 {
		res.Replicas = append([]ReplicaID(nil), e.Replicas...)
	}
	s.mu.RUnlock()
	return res, true
}

// Delete marks path as deleted with given version.
func (s *Store) Delete(path string, version uint64) {
	if path == emptyPath {
		return
	}
	s.mu.Lock()
	cur, ok := s.data[path]
	if !ok || cur.Version < version {
		s.data[path] = &Entry{Path: path, Version: version, Deleted: true}
	}
	s.mu.Unlock()
}

// List returns copy of all non-deleted entries.
func (s *Store) List() []Entry {
	s.mu.RLock()
	res := make([]Entry, 0, len(s.data))
	for _, e := range s.data {
		if e.Deleted {
			continue
		}
		cp := *e
		if len(e.Replicas) > 0 {
			cp.Replicas = append([]ReplicaID(nil), e.Replicas...)
		}
		res = append(res, cp)
	}
	s.mu.RUnlock()
	return res
}
