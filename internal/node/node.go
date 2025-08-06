package node

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dfs/internal/blobstore"
	"dfs/internal/metadata"
)

type Node struct {
	ID     string
	Addr   string
	peers  []string
	Meta   *metadata.Store
	Blobs  *blobstore.Store
	hostfs string
	client *http.Client
}

func New(id, addr, hostfs, blobDir string, peers []string) (*Node, error) {
	n := &Node{
		ID:     id,
		Addr:   addr,
		peers:  peers,
		Meta:   metadata.New(),
		Blobs:  blobstore.New(blobDir),
		hostfs: hostfs,
		client: &http.Client{Timeout: 10 * time.Second},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/blob", n.handleBlob)
	mux.HandleFunc("/update", n.handleUpdate)
	mux.HandleFunc("/delete", n.handleDelete)
	go http.ListenAndServe(addr, mux)
	return n, nil
}

func (n *Node) PutFile(path string, data []byte) error {
	m, ok := n.Meta.Get(path)
	version := 1
	if ok {
		version = m.Version + 1
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(data))
	if err := n.Blobs.Save(path, version, data); err != nil {
		return err
	}
	diskPath := filepath.Join(n.hostfs, path)
	if err := os.MkdirAll(filepath.Dir(diskPath), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(diskPath, data, 0o444); err != nil {
		return err
	}
	meta := &metadata.FileMeta{Path: path, Version: version, Hash: hash, Owner: n.Addr}
	n.Meta.Put(meta)
	n.notifyPeers(meta)
	return nil
}

func (n *Node) GetFile(path string) ([]byte, error) {
	m, ok := n.Meta.Get(path)
	if !ok {
		return nil, os.ErrNotExist
	}
	if m.Owner == n.Addr {
		return n.Blobs.Load(path, m.Version)
	}
	if data, err := n.Blobs.Load(path, m.Version); err == nil {
		return data, nil
	}
	url := fmt.Sprintf("http://%s/blob?path=%s&version=%d", m.Owner, path, m.Version)
	resp, err := n.client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, os.ErrNotExist
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	_ = n.Blobs.Save(path, m.Version, data)
	diskPath := filepath.Join(n.hostfs, path)
	_ = os.MkdirAll(filepath.Dir(diskPath), 0o755)
	_ = os.WriteFile(diskPath, data, 0o444)
	return data, nil
}

func (n *Node) DeleteFile(path string) {
	n.Meta.Delete(path)
	n.Blobs.Delete(path)
	os.Remove(filepath.Join(n.hostfs, path))
	n.notifyDelete(path)
}

func (n *Node) GetMetadata(path string) (*metadata.FileMeta, bool) {
	return n.Meta.Get(path)
}

func (n *Node) notifyPeers(meta *metadata.FileMeta) {
	b, _ := json.Marshal(meta)
	for _, p := range n.peers {
		if p == n.Addr {
			continue
		}
		_, _ = n.client.Post("http://"+p+"/update", "application/json", bytes.NewReader(b))
	}
}

func (n *Node) notifyDelete(path string) {
	for _, p := range n.peers {
		if p == n.Addr {
			continue
		}
		_, _ = n.client.Post("http://"+p+"/delete", "text/plain", strings.NewReader(path))
	}
}

func (n *Node) handleBlob(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	versionStr := r.URL.Query().Get("version")
	var version int
	fmt.Sscanf(versionStr, "%d", &version)
	data, err := n.Blobs.Load(path, version)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Write(data)
}

func (n *Node) handleUpdate(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var m metadata.FileMeta
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	n.Meta.Put(&m)
	w.WriteHeader(http.StatusOK)
}

func (n *Node) handleDelete(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	path := string(b)
	n.Meta.Delete(path)
	n.Blobs.Delete(path)
	os.Remove(filepath.Join(n.hostfs, path))
	w.WriteHeader(http.StatusOK)
}

func (n *Node) BackgroundCheck(interval time.Duration) {
	go func() {
		for {
			filepath.WalkDir(n.hostfs, func(p string, d os.DirEntry, err error) error {
				if err != nil || d.IsDir() {
					return nil
				}
				rel, _ := filepath.Rel(n.hostfs, p)
				meta, ok := n.Meta.Get(rel)
				if !ok {
					os.Remove(p)
					return nil
				}
				data, err := os.ReadFile(p)
				if err != nil {
					return nil
				}
				hash := fmt.Sprintf("%x", sha256.Sum256(data))
				if hash != meta.Hash {
					os.Remove(p)
				}
				return nil
			})
			time.Sleep(interval)
		}
	}()
}
