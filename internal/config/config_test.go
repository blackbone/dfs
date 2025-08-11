package config

import (
	"strconv"
	"strings"
	"testing"
)

const (
	testID     = "env"
	testRaft   = "1.2.3.4:1"
	testGRPC   = "1.2.3.4:2"
	testData   = "/tmp/env"
	peerA      = "a"
	peerB      = "b"
	peerSepStr = ","
	joinTrue   = "true"
	joinBad    = "bad"
	bigPeers   = 1000
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv(EnvID, "")
	t.Setenv(EnvRaft, "")
	t.Setenv(EnvGRPC, "")
	t.Setenv(EnvData, "")
	t.Setenv(EnvPeers, "")
	t.Setenv(EnvJoin, "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.ID != DefaultID || cfg.Data != DefaultDataDir || cfg.Raft != "" || cfg.GRPC != "" || cfg.Join || cfg.Peers != nil {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestLoadEnv(t *testing.T) {
	t.Setenv(EnvID, testID)
	t.Setenv(EnvRaft, testRaft)
	t.Setenv(EnvGRPC, testGRPC)
	t.Setenv(EnvData, testData)
	t.Setenv(EnvPeers, peerA+peerSepStr+peerB)
	t.Setenv(EnvJoin, joinTrue)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.ID != testID || cfg.Raft != testRaft || cfg.GRPC != testGRPC || cfg.Data != testData || len(cfg.Peers) != 2 || cfg.Peers[0] != peerA || cfg.Peers[1] != peerB || !cfg.Join {
		t.Fatalf("unexpected cfg: %+v", cfg)
	}
}

func TestLoadPeersEmpty(t *testing.T) {
	t.Setenv(EnvPeers, "")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Peers != nil {
		t.Fatalf("expected nil peers: %+v", cfg.Peers)
	}
}

func TestLoadJoinInvalid(t *testing.T) {
	t.Setenv(EnvJoin, joinBad)
	if _, err := Load(); err == nil {
		t.Fatalf("expected error")
	}
}

func TestLoadPeersLarge(t *testing.T) {
	peers := make([]string, bigPeers)
	for i := 0; i < bigPeers; i++ {
		peers[i] = strconv.Itoa(i)
	}
	t.Setenv(EnvPeers, strings.Join(peers, peerSepStr))
	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(cfg.Peers) != bigPeers || cfg.Peers[0] != "0" || cfg.Peers[bigPeers-1] != strconv.Itoa(bigPeers-1) {
		t.Fatalf("unexpected peers length: %d", len(cfg.Peers))
	}
}
