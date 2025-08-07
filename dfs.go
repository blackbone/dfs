package dfs

import (
	"crypto/sha256"
	"errors"
	"os"
	"sync/atomic"

	"dfs/internal/metastore"
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
	if err := nd.Put(path, data); err != nil {
		return err
	}
	var ver uint64
	if e, ok := nd.Meta.Get(path); ok {
		ver = e.Version
	}
	ver++
	hash := sha256.Sum256(data)
	nd.Meta.Sync(&metastore.Entry{Path: path, Version: ver, Hash: hash})
	return nil
}

// DeleteFile removes path from the store and marks its metadata deleted.
func DeleteFile(path string) error {
	nd := nodePtr.Load()
	if nd == nil {
		return errNodeNotInitialized
	}
	var ver uint64
	if e, ok := nd.Meta.Get(path); ok {
		ver = e.Version
	}
	ver++
	if err := nd.Delete(path); err != nil {
		return err
	}
	nd.Meta.Delete(path, ver)
	return nil
}

// GetMetadata returns metadata for path.
func GetMetadata(path string) (metastore.Entry, error) {
	nd := nodePtr.Load()
	if nd == nil {
		return metastore.Entry{}, errNodeNotInitialized
	}
	e, ok := nd.Meta.Get(path)
	if !ok {
		return metastore.Entry{}, os.ErrNotExist
	}
	return e, nil
}
