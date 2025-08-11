package config

import "testing"

const (
	benchID    = "bench"
	benchRaft  = "a"
	benchGRPC  = "b"
	benchData  = "/tmp"
	benchPeers = "x,y"
)

func BenchmarkLoad(b *testing.B) {
	b.Setenv(EnvID, benchID)
	b.Setenv(EnvRaft, benchRaft)
	b.Setenv(EnvGRPC, benchGRPC)
	b.Setenv(EnvData, benchData)
	b.Setenv(EnvPeers, benchPeers)
	for i := 0; i < b.N; i++ {
		if _, err := Load(); err != nil {
			b.Fatalf("load: %v", err)
		}
	}
}
