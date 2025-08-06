package config

import (
	"os"
	"path/filepath"
	"testing"
)

const (
	testYAML      = "id: file\nraft: 1.2.3.4:1\ngrpc: 1.2.3.4:2\ndata: /tmp/file\npeers:\n  - a\n  - b\n"
	invalidYAML   = ":\n"
	unmarshalYAML = "peers:\n  key: val\n"
	cfgFile       = "cfg.yaml"
	badFile       = "bad.yaml"
	missingFile   = "missing.yaml"
	permUserRW    = 0o600
)

func TestLoadYAML(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, cfgFile)
	if err := os.WriteFile(file, []byte(testYAML), permUserRW); err != nil {
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
	file := filepath.Join(dir, cfgFile)
	if err := os.WriteFile(file, []byte(testYAML), permUserRW); err != nil {
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
	cfg, err := Load(filepath.Join(t.TempDir(), missingFile))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.ID != DefaultID || cfg.Data != DefaultDataDir {
		t.Fatalf("defaults not applied: %+v", cfg)
	}
}

func TestLoadEnvPath(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, cfgFile)
	if err := os.WriteFile(file, []byte(testYAML), permUserRW); err != nil {
		t.Fatalf("write: %v", err)
	}
	t.Setenv(EnvConfig, file)
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.ID != "file" {
		t.Fatalf("unexpected cfg: %+v", cfg)
	}
}

func TestLoadParseError(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, badFile)
	if err := os.WriteFile(file, []byte(invalidYAML), permUserRW); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := Load(file); err == nil {
		t.Fatalf("expected error")
	}
}

func TestLoadUnmarshalError(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, badFile)
	if err := os.WriteFile(file, []byte(unmarshalYAML), permUserRW); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := Load(file); err == nil {
		t.Fatalf("expected error")
	}
}

func TestLoadNoPath(t *testing.T) {
	t.Setenv(EnvConfig, "")
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.ID != DefaultID {
		t.Fatalf("unexpected cfg: %+v", cfg)
	}
}
