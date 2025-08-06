package dfs

import (
	"errors"
	"os"
	"sync"

	"dfs/internal/node"
)

var (
	nodeMu sync.RWMutex
	n      *node.Node
)

// SetNode registers the active DFS node.
func SetNode(nd *node.Node) {
	nodeMu.Lock()
	n = nd
	nodeMu.Unlock()
}

// GetFile returns the file contents for the given path.
func GetFile(path string) ([]byte, error) {
	nodeMu.RLock()
	nd := n
	nodeMu.RUnlock()
	if nd == nil {
		return nil, errors.New("node not initialized")
	}
	data, ok := nd.Get(path)
	if !ok {
		return nil, os.ErrNotExist
	}
	return data, nil
}
