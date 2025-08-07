package dfs

import (
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"strings"
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
func cleanPath(p string) (string, error) {
	const empty = ""
	if p == empty {
		return empty, os.ErrNotExist
	}
	cp := filepath.Clean(p)
	cp = strings.TrimPrefix(cp, string(os.PathSeparator))
	if cp == "." || strings.Contains(cp, "..") {
		return empty, os.ErrNotExist
	}
	return cp, nil
}

func GetFile(path string) ([]byte, error) {
	p, err := cleanPath(path)
	if err != nil {
		return nil, err
	}
	nd := nodePtr.Load()
	if nd == nil {
		return nil, errNodeNotInitialized
	}
	data, ok := nd.Get(p)
	if !ok {
		return nil, os.ErrNotExist
	}
	return data, nil
}

// PutFile stores the file contents for the given path through the active node.
func PutFile(path string, data []byte) error {
	p, err := cleanPath(path)
	if err != nil {
		return err
	}
	nd := nodePtr.Load()
	if nd == nil {
		return errNodeNotInitialized
	}
	if err := nd.Put(p, data); err != nil {
		return err
	}
	var ver uint64
	if e, ok := nd.Meta.Get(p); ok {
		ver = e.Version
	}
	ver++
	hash := sha256.Sum256(data)
	return nd.SyncMeta(&metastore.Entry{Path: p, Version: ver, Hash: hash})
}

// DeleteFile removes path from the store and marks its metadata deleted.
func DeleteFile(path string) error {
	p, err := cleanPath(path)
	if err != nil {
		return err
	}
	nd := nodePtr.Load()
	if nd == nil {
		return errNodeNotInitialized
	}
	var ver uint64
	if e, ok := nd.Meta.Get(p); ok {
		ver = e.Version
	}
	ver++
	if err := nd.Delete(p); err != nil {
		return err
	}
	return nd.SyncMeta(&metastore.Entry{Path: p, Version: ver, Deleted: true})
}

// GetMetadata returns metadata for path.
func GetMetadata(path string) (metastore.Entry, error) {
	p, err := cleanPath(path)
	if err != nil {
		return metastore.Entry{}, err
	}
	nd := nodePtr.Load()
	if nd == nil {
		return metastore.Entry{}, errNodeNotInitialized
	}
	e, ok := nd.Meta.Get(p)
	if !ok {
		return metastore.Entry{}, os.ErrNotExist
	}
	return e, nil
}
