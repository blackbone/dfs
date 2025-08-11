//go:build stress

package fusefs

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"dfs"
	"dfs/internal/node"
)

const (
	hostAddr           = "127.0.0.1"
	baseRaftPort       = 12200
	baseGRPCPort       = 13200
	raftAddrFmt        = "%s:%d"
	filePrefix         = "stress"
	fileExt            = ".bin"
	electionWait       = 2 * time.Second
	replicationTimeout = 60 * time.Second
	pollInterval       = 100 * time.Millisecond
)

var (
	clusterSizes = []int{2, 3, 4}
	fileSizes    = []int{1 << 10, 1 << 20, 100 << 20}
)

func startCluster(t *testing.T, n int, offset int) ([]*node.Node, []string, []func()) {
	t.Helper()
	nodes := make([]*node.Node, 0, n)
	cacheDirs := make([]string, 0, n)
	stops := make([]func(), 0, n)
	for i := 0; i < n; i++ {
		raftAddr := fmt.Sprintf(raftAddrFmt, hostAddr, baseRaftPort+offset+i)
		grpcAddr := fmt.Sprintf(raftAddrFmt, hostAddr, baseGRPCPort+offset+i)
		dataDir := t.TempDir()
		cacheDir := t.TempDir()
		peerParts := make([]string, 0, n-1)
		for j := 0; j < n; j++ {
			if j == i {
				continue
			}
			peerParts = append(peerParts, fmt.Sprintf(raftAddrFmt, hostAddr, baseRaftPort+offset+j))
		}
		peers := strings.Join(peerParts, ",")
		nd, stop, err := startNode(t, raftAddr, raftAddr, grpcAddr, peers, dataDir, true)
		if err != nil {
			t.Fatalf("node %d: %v", i, err)
		}
		nodes = append(nodes, nd)
		cacheDirs = append(cacheDirs, cacheDir)
		stops = append(stops, stop)
	}
	return nodes, cacheDirs, stops
}

func waitLeader(t *testing.T, nodes []*node.Node) (int, *node.Node) {
	t.Helper()
	time.Sleep(electionWait)
	for i, nd := range nodes {
		if nd.IsLeader() {
			return i, nd
		}
	}
	t.Fatalf("no leader elected")
	return -1, nil
}

func waitReplication(t *testing.T, nodes []*node.Node, name string, data []byte) {
	t.Helper()
	deadline := time.Now().Add(replicationTimeout)
	for time.Now().Before(deadline) {
		ok := true
		for _, nd := range nodes {
			v, ok := nd.Get(name)
			if !ok || !bytes.Equal(v, data) {
				ok = false
				break
			}
			if _, ok := nd.Meta.Get(name); !ok {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(pollInterval)
	}
	t.Fatalf("replication timeout for %s", name)
}

func TestStressReplication(t *testing.T) {
	for idx, n := range clusterSizes {
		t.Run("nodes"+strconv.Itoa(n), func(t *testing.T) {
			nodes, cacheDirs, stops := startCluster(t, n, idx*10)
			defer func() {
				for _, s := range stops {
					s()
				}
			}()

			leaderIdx, leader := waitLeader(t, nodes)

			for _, sz := range fileSizes {
				t.Run("size"+strconv.Itoa(sz), func(t *testing.T) {
					dfs.SetNode(leader)
					ctx, cancel := context.WithCancel(context.Background())
					go func() { _ = Watch(ctx, cacheDirs[leaderIdx]) }()
					time.Sleep(pollInterval)
					name := fmt.Sprintf("%s_%d%s", filePrefix, sz, fileExt)
					data := bytes.Repeat([]byte{byte(sz % 251)}, sz)
					if err := os.WriteFile(filepath.Join(cacheDirs[leaderIdx], name), data, 0o644); err != nil {
						t.Fatalf("write: %v", err)
					}
					waitReplication(t, nodes, name, data)
					cancel()
					for j, nd := range nodes {
						if j == leaderIdx {
							continue
						}
						dfs.SetNode(nd)
						fs := New(cacheDirs[j])
						f := &File{fs: fs, path: name}
						got, err := f.ReadAll(context.Background())
						if err != nil {
							t.Fatalf("read: %v", err)
						}
						if !bytes.Equal(got, data) {
							t.Fatalf("data mismatch")
						}
					}
				})
			}
		})
	}
}
