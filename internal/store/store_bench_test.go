package store

import (
	"testing"

	"github.com/hashicorp/raft"
)

const (
	benchKey = "bench"
	benchVal = "v"
)

var benchData = []byte(benchVal)

func BenchmarkStoreApply(b *testing.B) {
	s := New()
	cmd := &Command{Op: OpPut, Key: S2B(benchKey), Data: benchData}
	log := &raft.Log{Data: cmd.MarshalBinary()}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.Apply(log)
	}
}
