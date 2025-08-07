package dfs

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"dfs/internal/node"
)

const (
	addrHost    = "127.0.0.1"
	emptyString = ""
	sampleID    = "id"
	sampleKey   = "k"
	sampleVal   = "v"
	missingKey  = "m"
	waitSeconds = 5
	sleepMillis = 10

	invalidPath = "../bad"
	emptyKey    = "zero"
	largeKey    = "dir/sub/large"
	bigFileSize = 1 << 20
	byteFill    = 7
)

func freePort(tb testing.TB) string {
	tb.Helper()
	l, err := net.Listen("tcp", addrHost+":0")
	if err != nil {
		tb.Fatalf("listen: %v", err)
	}
	defer l.Close()
	return l.Addr().String()
}

func waitLeader(tb testing.TB, n *node.Node) {
	tb.Helper()
	deadline := time.Now().Add(time.Duration(waitSeconds) * time.Second)
	for time.Now().Before(deadline) {
		if n.IsLeader() {
			return
		}
		time.Sleep(time.Duration(sleepMillis) * time.Millisecond)
	}
	tb.Fatalf("leader not elected")
}

func TestGetFileNoNode(t *testing.T) {
	if _, err := GetFile(sampleKey); !errors.Is(err, errNodeNotInitialized) {
		t.Fatalf("expected node not initialized error, got %v", err)
	}
}

func TestPutFileNoNode(t *testing.T) {
	if err := PutFile(sampleKey, []byte(sampleVal)); !errors.Is(err, errNodeNotInitialized) {
		t.Fatalf("expected node not initialized error, got %v", err)
	}
}

func TestSetNodeAndFileOps(t *testing.T) {
	addr := freePort(t)
	n, err := node.New(sampleID, addr, t.TempDir(), emptyString, true)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	SetNode(n)
	waitLeader(t, n)
	if err := PutFile(sampleKey, []byte(sampleVal)); err != nil {
		t.Fatalf("put: %v", err)
	}
	data, err := GetFile(sampleKey)
	if err != nil || string(data) != sampleVal {
		t.Fatalf("get: %v data=%q", err, data)
	}
	if _, err := GetFile(missingKey); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected not exist, got %v", err)
	}
}

func TestDeleteFile(t *testing.T) {
	addr := freePort(t)
	n, err := node.New(sampleID, addr, t.TempDir(), emptyString, true)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	SetNode(n)
	waitLeader(t, n)
	if err := PutFile(sampleKey, []byte(sampleVal)); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := DeleteFile(sampleKey); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := GetFile(sampleKey); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected not exist, got %v", err)
	}
	if _, err := GetMetadata(sampleKey); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected meta missing, got %v", err)
	}
}

func TestInvalidPaths(t *testing.T) {
	SetNode(nil)
	invalids := []string{invalidPath, emptyString}
	for _, p := range invalids {
		if err := PutFile(p, []byte(sampleVal)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected not exist for %q, got %v", p, err)
		}
		if _, err := GetFile(p); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected not exist for %q, got %v", p, err)
		}
	}
}

func TestPutAndGetEmptyFile(t *testing.T) {
	addr := freePort(t)
	n, err := node.New(sampleID, addr, t.TempDir(), emptyString, true)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	SetNode(n)
	t.Cleanup(func() { SetNode(nil) })
	waitLeader(t, n)
	if err := PutFile(emptyKey, nil); err != nil {
		t.Fatalf("put: %v", err)
	}
	data, err := GetFile(emptyKey)
	if err != nil || len(data) != 0 {
		t.Fatalf("get: %v len=%d", err, len(data))
	}
	meta, err := GetMetadata(emptyKey)
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	if meta.Hash != sha256.Sum256(nil) {
		t.Fatalf("hash mismatch")
	}
}

func TestPutGetLargeFile(t *testing.T) {
	addr := freePort(t)
	n, err := node.New(sampleID, addr, t.TempDir(), emptyString, true)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	SetNode(n)
	t.Cleanup(func() { SetNode(nil) })
	waitLeader(t, n)

	tmpIn := filepath.Join(t.TempDir(), "in.bin")
	data := bytes.Repeat([]byte{byteFill}, bigFileSize)
	if err := os.WriteFile(tmpIn, data, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	inData, err := os.ReadFile(tmpIn)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := PutFile(largeKey, inData); err != nil {
		t.Fatalf("put: %v", err)
	}

	got, err := GetFile(largeKey)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, inData) {
		t.Fatalf("data mismatch")
	}

	tmpOut := filepath.Join(t.TempDir(), "out.bin")
	if err := os.WriteFile(tmpOut, got, 0o600); err != nil {
		t.Fatalf("write out: %v", err)
	}
	outData, err := os.ReadFile(tmpOut)
	if err != nil {
		t.Fatalf("read out: %v", err)
	}
	if !bytes.Equal(outData, inData) {
		t.Fatalf("file mismatch")
	}

	meta, err := GetMetadata(largeKey)
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	wantHash := sha256.Sum256(inData)
	if meta.Hash != wantHash {
		t.Fatalf("hash mismatch")
	}
}
