// Package store provides a simple in-memory key/value store used as the
// Raft state machine.
package store

import (
	"encoding/json"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

// Command represents a replicated state machine command.
type Command struct {
	Op   string `json:"op"`
	Key  string `json:"key"`
	Data []byte `json:"data,omitempty"`
}

// Store is a simple in-memory key/value store implementing raft.FSM.
type Store struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func New() *Store {
	return &Store{data: make(map[string][]byte)}
}

// Apply applies a Raft log entry to the key/value store.
func (s *Store) Apply(log *raft.Log) interface{} {
	var c Command
	if err := json.Unmarshal(log.Data, &c); err != nil {
		return err
	}
	switch c.Op {
	case "put":
		s.mu.Lock()
		s.data[c.Key] = c.Data
		s.mu.Unlock()
	case "delete":
		s.mu.Lock()
		delete(s.data, c.Key)
		s.mu.Unlock()
	}
	return nil
}

// Snapshot returns a snapshot of the store.
func (s *Store) Snapshot() (raft.FSMSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	clone := make(map[string][]byte, len(s.data))
	for k, v := range s.data {
		clone[k] = v
	}
	return &snapshot{data: clone}, nil
}

// Restore stores the key/value data from a snapshot.
func (s *Store) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	data := make(map[string][]byte)
	if err := json.NewDecoder(rc).Decode(&data); err != nil {
		return err
	}
	s.mu.Lock()
	s.data = data
	s.mu.Unlock()
	return nil
}

// Get returns value for key.
func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

type snapshot struct {
	data map[string][]byte
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	b, err := json.Marshal(s.data)
	if err != nil {
		sink.Cancel()
		return err
	}
	if _, err := sink.Write(b); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {}
