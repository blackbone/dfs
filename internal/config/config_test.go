package config

import (
	"os"
	"path/filepath"
	"testing"
)

const (
	testYAML = "id: file\nraft: 1.2.3.4:1\ngrpc: 1.2.3.4:2\ndata: /tmp/file\npeers:\n  - a\n  - b\n"
)

func TestLoadYAML(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "cfg.yaml")
	if err := os.WriteFile(file, []byte(testYAML), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	cfg, err := Load(file)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.ID != "file" || cfg.Raft != "1.2.3.4:1" || cfg.GRPC != "1.2.3.4:2" || cfg.Data != "/tmp/file" || len(cfg.Peers) != 2 {
		t.Fatalf("unexpected cfg: %+v", cfg)
	}
}

func TestLoadEnvOverrides(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "cfg.yaml")
	if err := os.WriteFile(file, []byte(testYAML), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	const envID = "env"
	t.Setenv(EnvID, envID)
	cfg, err := Load(file)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.ID != envID {
		t.Fatalf("env override failed: %v", cfg.ID)
	}
}

func TestLoadMissingFile(t *testing.T) {
	cfg, err := Load(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.ID != DefaultID || cfg.Data != DefaultDataDir {
		t.Fatalf("defaults not applied: %+v", cfg)
	}
}
