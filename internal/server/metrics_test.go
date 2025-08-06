package server

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"dfs/internal/node"
	obs "dfs/internal/observability"
	pb "dfs/proto"
)

// TestMetricsPutGet verifies counters for successful operations.
func TestMetricsPutGet(t *testing.T) {
	addr := freeAddr(t)
	n, err := node.New("m1", addr, t.TempDir(), "")
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	if waitLeader(n) == nil {
		t.Fatalf("node not leader")
	}
	client, cleanup := startGRPC(t, n)
	defer cleanup()

	beforePut := testutil.ToFloat64(obs.PutCounter)
	beforeGet := testutil.ToFloat64(obs.GetCounter)

	ctx := context.Background()
	if _, err := client.Put(ctx, &pb.PutRequest{Key: "k", Data: []byte("v")}); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, err := client.Get(ctx, &pb.GetRequest{Key: "k"}); err != nil {
		t.Fatalf("get: %v", err)
	}

	afterPut := testutil.ToFloat64(obs.PutCounter)
	afterGet := testutil.ToFloat64(obs.GetCounter)
	if diff := afterPut - beforePut; diff != 1 {
		t.Fatalf("unexpected put count: %v", diff)
	}
	if diff := afterGet - beforeGet; diff != 1 {
		t.Fatalf("unexpected get count: %v", diff)
	}
}

// TestMetricsPutNotLeader ensures counter increments on failed Put.
func TestMetricsPutNotLeader(t *testing.T) {
	addr1 := freeAddr(t)
	addr2 := freeAddr(t)
	n1, err := node.New(addr1, addr1, t.TempDir(), addr2)
	if err != nil {
		t.Fatalf("n1: %v", err)
	}
	n2, err := node.New(addr2, addr2, t.TempDir(), addr1)
	if err != nil {
		t.Fatalf("n2: %v", err)
	}
	leader := waitLeader(n1, n2)
	if leader == nil {
		t.Fatalf("no leader")
	}
	var follower *node.Node
	if leader == n1 {
		follower = n2
	} else {
		follower = n1
	}

	client, cleanup := startGRPC(t, follower)
	defer cleanup()
	beforePut := testutil.ToFloat64(obs.PutCounter)
	ctx := context.Background()
	if _, err := client.Put(ctx, &pb.PutRequest{Key: "k", Data: []byte("v")}); err == nil {
		t.Fatalf("expected error")
	}
	afterPut := testutil.ToFloat64(obs.PutCounter)
	if diff := afterPut - beforePut; diff != 1 {
		t.Fatalf("put counter not incremented: %v", diff)
	}
}
