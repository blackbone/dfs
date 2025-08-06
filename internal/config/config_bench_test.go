package config

import (
	"os"
	"path/filepath"
	"testing"
)

const benchYAML = "id: bench\nraft: a\ngrpc: b\ndata: /tmp\npeers:\n  - x\n  - y\n"

func BenchmarkLoad(b *testing.B) {
	dir := b.TempDir()
	file := filepath.Join(dir, "cfg.yaml")
	if err := os.WriteFile(file, []byte(benchYAML), 0o600); err != nil {
		b.Fatalf("write: %v", err)
	}
	for i := 0; i < b.N; i++ {
		if _, err := Load(file); err != nil {
			b.Fatalf("load: %v", err)
		}
	}
}
