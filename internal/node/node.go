// Package node manages a single Raft instance and its state machine.
package node

import (
	"encoding/json"
	"net"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/raft"

	"dfs/internal/store"
)

// Node wraps a Raft instance and its finite state machine store.
type Node struct {
	raft  *raft.Raft
	Store *store.Store
}

const (
	opTimeout = 5 * time.Second
	zeroIndex = 0
)

// New creates a new Raft node bound to the given address. The peers
// argument is a comma separated list of other Raft server addresses
// that form the initial cluster configuration.
func New(id, bind, dataDir, peers string) (*Node, error) {
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(id)

	addr, err := net.ResolveTCPAddr("tcp", bind)
	if err != nil {
		return nil, err
	}
	// Each node communicates with others over a TCP transport.
	transport, err := raft.NewTCPTransport(bind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}

	snap, err := raft.NewFileSnapshotStore(dataDir, 1, os.Stderr)
	if err != nil {
		return nil, err
	}
	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	fsm := store.New()
	// Create the Raft system with our in-memory log and stable stores.
	r, err := raft.NewRaft(cfg, fsm, logStore, stableStore, snap, transport)
	if err != nil {
		return nil, err
	}

	n := &Node{raft: r, Store: fsm}

	// Bootstrap cluster by configuring the known peers plus this node.
	configuration := raft.Configuration{}
	for _, p := range strings.Split(peers, ",") {
		if p == "" {
			continue
		}
		configuration.Servers = append(configuration.Servers, raft.Server{
			ID:      raft.ServerID(p),
			Address: raft.ServerAddress(p),
		})
	}
	configuration.Servers = append(configuration.Servers, raft.Server{ID: cfg.LocalID, Address: transport.LocalAddr()})
	r.BootstrapCluster(configuration)

	return n, nil
}

// Put replicates a key/value pair through Raft.
func (n *Node) Put(key string, data []byte) error {
	c := &store.Command{Op: store.OpPut, Key: store.S2B(key), Data: data}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	f := n.raft.Apply(b, opTimeout)
	return f.Error()
}

// Get returns value if present.
func (n *Node) Get(key string) ([]byte, bool) {
	return n.Store.Get(key)
}

// IsLeader reports whether this node is the cluster leader.
func (n *Node) IsLeader() bool { return n.raft.State() == raft.Leader }

// Leader returns the leader address.
func (n *Node) Leader() raft.ServerAddress { return n.raft.Leader() }

// Shutdown stops the Raft node.
func (n *Node) Shutdown() error { return n.raft.Shutdown().Error() }

// Join adds a voter to the Raft cluster.
func (n *Node) Join(id, addr string) error {
	f := n.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), zeroIndex, opTimeout)
	if err := f.Error(); err != nil {
		return err
	}
	return nil
}

// Leave removes a server from the Raft cluster.
func (n *Node) Leave(id string) error {
	f := n.raft.RemoveServer(raft.ServerID(id), zeroIndex, opTimeout)
	if err := f.Error(); err != nil {
		return err
	}
	return nil
}
