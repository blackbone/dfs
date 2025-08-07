package node

import (
	"encoding/json"
	"io"
	"sync"

	"github.com/hashicorp/raft"

	"dfs/internal/metastore"
)

// op defines the state machine operation type.
type op uint8

const (
	opPut op = iota
	opDelete
	opMeta
)

// command encodes a replicated operation.
type command struct {
	Op   op              `json:"op"`
	Key  []byte          `json:"key,omitempty"`
	Data []byte          `json:"data,omitempty"`
	Meta metastore.Entry `json:"meta"`
}

// fsm implements raft.FSM and stores key/value data alongside metadata.
type fsm struct {
	mu   sync.RWMutex
	data map[string][]byte
	meta *metastore.Store
}

func newFSM(meta *metastore.Store) *fsm {
	return &fsm{data: make(map[string][]byte), meta: meta}
}

func (f *fsm) Apply(log *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(log.Data, &c); err != nil {
		return err
	}
	switch c.Op {
	case opPut:
		key := string(c.Key)
		f.mu.Lock()
		f.data[key] = c.Data
		f.mu.Unlock()
	case opDelete:
		key := string(c.Key)
		f.mu.Lock()
		delete(f.data, key)
		f.mu.Unlock()
	case opMeta:
		f.meta.Sync(&c.Meta)
	}
	return nil
}

type snap struct {
	Data map[string][]byte `json:"data"`
	Meta []metastore.Entry `json:"meta"`
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	data := make(map[string][]byte, len(f.data))
	for k, v := range f.data {
		data[k] = v
	}
	f.mu.RUnlock()
	return &fsmSnapshot{snap{Data: data, Meta: f.meta.List()}}, nil
}

func (f *fsm) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var s snap
	if err := json.NewDecoder(rc).Decode(&s); err != nil {
		return err
	}
	f.mu.Lock()
	f.data = s.Data
	f.mu.Unlock()
	f.meta = metastore.New()
	for i := range s.Meta {
		f.meta.Sync(&s.Meta[i])
	}
	return nil
}

type fsmSnapshot struct{ s snap }

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	b, err := json.Marshal(s.s)
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

func (s *fsmSnapshot) Release() {}

func (f *fsm) Get(key string) ([]byte, bool) {
	f.mu.RLock()
	v, ok := f.data[key]
	f.mu.RUnlock()
	return v, ok
}
