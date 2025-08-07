package server

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"dfs/internal/metastore"
	"dfs/internal/node"
	pb "dfs/proto"
)

const (
	bufSize = 1024 * 1024
	idA     = "n1"
	idB     = "n2"
	empty   = ""
)

func dialer(l *bufconn.Listener) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, s string) (net.Conn, error) {
		return l.Dial()
	}
}

func startGRPC(t *testing.T, n *node.Node) (pb.FileServiceClient, func()) {
	t.Helper()
	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	pb.RegisterFileServiceServer(srv, New(n))
	go srv.Serve(lis)
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "buf", grpc.WithContextDialer(dialer(lis)), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	client := pb.NewFileServiceClient(conn)
	cleanup := func() {
		conn.Close()
		srv.Stop()
		lis.Close()
	}
	return client, cleanup
}

func waitLeader(nodes ...*node.Node) *node.Node {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.IsLeader() {
				return n
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func TestServerPutGet(t *testing.T) {
	addr := freeAddr(t)
	n, err := node.New("n1", addr, t.TempDir(), "", true)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	if waitLeader(n) == nil {
		t.Fatalf("node not leader")
	}
	client, cleanup := startGRPC(t, n)
	defer cleanup()

	ctx := context.Background()
	if _, err := client.Put(ctx, &pb.PutRequest{Key: "foo", Data: []byte("bar")}); err != nil {
		t.Fatalf("put: %v", err)
	}
	resp, err := client.Get(ctx, &pb.GetRequest{Key: "foo"})
	if err != nil || string(resp.Data) != "bar" {
		t.Fatalf("get: %v resp=%q", err, resp.Data)
	}
	if _, err := client.Get(ctx, &pb.GetRequest{Key: "missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestServerDelete(t *testing.T) {
	addr := freeAddr(t)
	n, err := node.New("n1", addr, t.TempDir(), "", true)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	if waitLeader(n) == nil {
		t.Fatalf("node not leader")
	}
	client, cleanup := startGRPC(t, n)
	defer cleanup()
	ctx := context.Background()
	if _, err := client.Put(ctx, &pb.PutRequest{Key: "foo", Data: []byte("bar")}); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, err := client.Delete(ctx, &pb.DeleteRequest{Key: "foo"}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := client.Get(ctx, &pb.GetRequest{Key: "foo"}); status.Code(err) != codes.NotFound {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestServerPutNotLeader(t *testing.T) {
	addr1 := freeAddr(t)
	addr2 := freeAddr(t)
	n1, err := node.New(addr1, addr1, t.TempDir(), addr2, true)
	if err != nil {
		t.Fatalf("n1: %v", err)
	}
	n2, err := node.New(addr2, addr2, t.TempDir(), addr1, true)
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
	ctx := context.Background()
	if _, err := client.Put(ctx, &pb.PutRequest{Key: "k", Data: []byte("v")}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", err)
	}
}

func TestServerAddRemovePeer(t *testing.T) {
	addr1 := freeAddr(t)
	n1, err := node.New(idA, addr1, t.TempDir(), empty, true)
	if err != nil {
		t.Fatalf("n1: %v", err)
	}
	addr2 := freeAddr(t)
	n2, err := node.New(idB, addr2, t.TempDir(), empty, false)
	if err != nil {
		t.Fatalf("n2: %v", err)
	}
	_ = n2

	if waitLeader(n1) != n1 {
		t.Fatalf("n1 not leader")
	}
	client, cleanup := startGRPC(t, n1)
	defer cleanup()
	ctx := context.Background()
	if _, err := client.AddPeer(ctx, &pb.AddPeerRequest{Id: idB, Address: addr2}); err != nil {
		t.Fatalf("add peer: %v", err)
	}
	if _, err := client.RemovePeer(ctx, &pb.RemovePeerRequest{Id: idB}); err != nil {
		t.Fatalf("remove peer: %v", err)
	}
}

func TestServerSyncMetadata(t *testing.T) {
	n := &node.Node{Meta: metastore.New()}
	client, cleanup := startGRPC(t, n)
	defer cleanup()
	ctx := context.Background()
	req := &pb.SyncMetadataRequest{Meta: &pb.Metadata{
		Path:     "/m",
		Version:  1,
		Replicas: []uint64{1, 2},
	}}
	if _, err := client.SyncMetadata(ctx, req); err != nil {
		t.Fatalf("sync: %v", err)
	}
	e, ok := n.Meta.Get("/m")
	if !ok || e.Version != 1 || len(e.Replicas) != 2 {
		t.Fatalf("unexpected %+v ok=%v", e, ok)
	}
	req.Meta.Deleted = true
	req.Meta.Version = 2
	if _, err := client.SyncMetadata(ctx, req); err != nil {
		t.Fatalf("delete sync: %v", err)
	}
	if _, ok := n.Meta.Get("/m"); ok {
		t.Fatalf("expected deleted")
	}
}
