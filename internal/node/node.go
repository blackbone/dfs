// Package node manages a single Raft instance and its state machine.
package node

import (
	"encoding/json"
	"net"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/raft"

	"dfs/internal/metastore"
	"dfs/internal/store"
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
	raft  *raft.Raft
	Store *store.Store
	Meta  *metastore.Store
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
	// Each node communicates with others over a TCP transport.
	transport, err := raft.NewTCPTransport(bind, addr, maxPool, dialTimeout, os.Stderr)
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
	meta := metastore.New()
	// Create the Raft system with our in-memory log and stable stores.
	r, err := raft.NewRaft(cfg, fsm, logStore, stableStore, snap, transport)
	if err != nil {
		return nil, err
	}

	n := &Node{raft: r, Store: fsm, Meta: meta}

	if bootstrap {
		configuration := raft.Configuration{}
		for _, p := range strings.Split(peers, sepComma) {
			if p == emptyString {
				continue
			}
			configuration.Servers = append(configuration.Servers, raft.Server{
				ID:      raft.ServerID(p),
				Address: raft.ServerAddress(p),
			})
		}
		configuration.Servers = append(configuration.Servers, raft.Server{ID: cfg.LocalID, Address: transport.LocalAddr()})
		r.BootstrapCluster(configuration)
	}

	return n, nil
}

// Put replicates a key/value pair through Raft.
func (n *Node) Put(key string, data []byte) error {
	c := &store.Command{Op: store.OpPut, Key: store.S2B(key), Data: data}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	f := n.raft.Apply(b, applyTimeout)
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
