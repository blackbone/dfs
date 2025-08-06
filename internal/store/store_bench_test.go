package store

import "testing"

const benchStr = "bench-data"

func BenchmarkStringByteConversion(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if B2S(S2B(benchStr)) != benchStr {
			b.Fatalf("mismatch")
		}
	}
}
