package dfs

import (
	"errors"
	"os"
	"sync/atomic"

	"dfs/internal/node"
)

var (
	nodePtr               atomic.Pointer[node.Node]            // active DFS node
	errNodeNotInitialized = errors.New("node not initialized") // SetNode has not been called
)

// SetNode registers the active DFS node.
func SetNode(nd *node.Node) { nodePtr.Store(nd) }

// GetFile returns the file contents for the given path.
func GetFile(path string) ([]byte, error) {
	nd := nodePtr.Load()
	if nd == nil {
		return nil, errNodeNotInitialized
	}
	data, ok := nd.Get(path)
	if !ok {
		return nil, os.ErrNotExist
	}
	return data, nil
}

// PutFile stores the file contents for the given path through the active node.
func PutFile(path string, data []byte) error {
	nd := nodePtr.Load()
	if nd == nil {
		return errNodeNotInitialized
	}
	return nd.Put(path, data)
}
