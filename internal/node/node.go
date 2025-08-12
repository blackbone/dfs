// Package node manages a single Raft instance and its state machine.
package node

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"dfs/internal/metastore"
)

const (
	networkTCP   = "tcp"
	sepComma     = ","
	emptyString  = ""
	maxPool      = 3
	dialTimeout  = 10 * time.Second
	applyTimeout = 5 * time.Second
)

// Node wraps a Raft instance and its finite state machine store.
type Node struct {
	raft *raft.Raft
	fsm  *fsm
	Meta *metastore.Store
}

// New creates a new Raft node bound to the given address. The peers
// argument is a comma separated list of other Raft server addresses
// that form the initial cluster configuration. If bootstrap is false
// the node starts unbootstrapped and must be added to the cluster via
// AddPeer.
func New(id, bind, dataDir, peers string, bootstrap bool) (*Node, error) {
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(id)
	addr, err := net.ResolveTCPAddr(networkTCP, bind)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(bind, addr, maxPool, dialTimeout, os.Stderr)
	if err != nil {
		return nil, err
	}
	return newWithTransport(cfg, dataDir, peers, bootstrap, transport)
}

// NewWithListener creates a new Raft node using an existing listener for all
// incoming connections.
func NewWithListener(id string, ln net.Listener, dataDir, peers string, bootstrap bool) (*Node, error) {
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(id)
	transport := raft.NewNetworkTransport(&streamLayer{ln}, maxPool, dialTimeout, os.Stderr)
	return newWithTransport(cfg, dataDir, peers, bootstrap, transport)
}

func newWithTransport(cfg *raft.Config, dataDir, peers string, bootstrap bool, transport raft.Transport) (*Node, error) {
	snap, err := raft.NewFileSnapshotStore(dataDir, 1, os.Stderr)
	if err != nil {
		return nil, err
	}
	logDB, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft-log.db"))
	if err != nil {
		return nil, err
	}
	stableDB, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft-stable.db"))
	if err != nil {
		return nil, err
	}
	meta := metastore.New()
	fsm := newFSM(meta)
	r, err := raft.NewRaft(cfg, fsm, logDB, stableDB, snap, transport)
	if err != nil {
		return nil, err
	}
	n := &Node{raft: r, fsm: fsm, Meta: meta}
	if bootstrap {
		configuration := raft.Configuration{}
		for _, p := range strings.Split(peers, sepComma) {
			if p == emptyString {
				continue
			}
			configuration.Servers = append(configuration.Servers, raft.Server{ID: raft.ServerID(p), Address: raft.ServerAddress(p)})
		}
		configuration.Servers = append(configuration.Servers, raft.Server{ID: cfg.LocalID, Address: transport.LocalAddr()})
		r.BootstrapCluster(configuration)
	}
	return n, nil
}

// NewInmem returns a Node backed by in-memory state without Raft.
func NewInmem() *Node {
	meta := metastore.New()
	return &Node{fsm: newFSM(meta), Meta: meta}
}

// Put replicates a key/value pair through Raft.
func (n *Node) Put(key string, data []byte) error {
	if n.raft == nil {
		n.fsm.mu.Lock()
		n.fsm.data[key] = append([]byte(nil), data...)
		n.fsm.mu.Unlock()
		return nil
	}
	c := &command{Op: opPut, Key: []byte(key), Data: data}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	f := n.raft.Apply(b, applyTimeout)
	return f.Error()
}

// Get returns value if present.
func (n *Node) Get(key string) ([]byte, bool) {
	return n.fsm.Get(key)
}

// Delete removes key through Raft.
func (n *Node) Delete(key string) error {
	if n.raft == nil {
		n.fsm.mu.Lock()
		delete(n.fsm.data, key)
		n.fsm.mu.Unlock()
		return nil
	}
	c := &command{Op: opDelete, Key: []byte(key)}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	f := n.raft.Apply(b, applyTimeout)
	return f.Error()
}

// SyncMeta replicates metadata entry through Raft.
func (n *Node) SyncMeta(e *metastore.Entry) error {
	if n.raft == nil {
		n.Meta.Sync(e)
		return nil
	}
	c := &command{Op: opMeta, Meta: *e}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	f := n.raft.Apply(b, applyTimeout)
	return f.Error()
}

// StartGC runs periodic garbage collection for metadata and blobs.
func (n *Node) StartGC(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			n.Meta.GC()
		}
	}()
}

// IsLeader reports whether this node is the cluster leader.
func (n *Node) IsLeader() bool { return n.raft.State() == raft.Leader }

// Leader returns the leader address.
func (n *Node) Leader() raft.ServerAddress { return n.raft.Leader() }

// AddPeer adds a voting peer to the cluster. Only the leader can
// perform membership changes.
func (n *Node) AddPeer(id, addr string) error {
	f := n.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 0)
	return f.Error()
}

// RemovePeer removes a peer from the cluster.
func (n *Node) RemovePeer(id string) error {
	f := n.raft.RemoveServer(raft.ServerID(id), 0, 0)
	return f.Error()
}
