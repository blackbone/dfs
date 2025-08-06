package dfs

import (
	"errors"
	"os"
	"sync"

	"dfs/internal/metadata"
	"dfs/internal/node"
)

var (
	nodeMu sync.RWMutex
	n      *node.Node
)

func SetNode(nd *node.Node) {
	nodeMu.Lock()
	n = nd
	nodeMu.Unlock()
}

func GetFile(path string) ([]byte, error) {
	nodeMu.RLock()
	nd := n
	nodeMu.RUnlock()
	if nd == nil {
		return nil, errors.New("node not initialized")
	}
	return nd.GetFile(path)
}

func GetMetadata(path string) (*metadata.FileMeta, error) {
	nodeMu.RLock()
	nd := n
	nodeMu.RUnlock()
	if nd == nil {
		return nil, errors.New("node not initialized")
	}
	m, ok := nd.GetMetadata(path)
	if !ok {
		return nil, os.ErrNotExist
	}
	return m, nil
}
