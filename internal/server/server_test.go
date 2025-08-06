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

	"dfs/internal/node"
	pb "dfs/proto"
)

const bufSize = 1024 * 1024

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
	n, err := node.New("n1", addr, t.TempDir(), "")
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

func TestServerPutNotLeader(t *testing.T) {
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
	ctx := context.Background()
	if _, err := client.Put(ctx, &pb.PutRequest{Key: "k", Data: []byte("v")}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", err)
	}
}

func TestServerReport(t *testing.T) {
	addr := freeAddr(t)
	n, err := node.New("n1", addr, t.TempDir(), "")
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	if waitLeader(n) == nil {
		t.Fatalf("no leader")
	}
	client, cleanup := startGRPC(t, n)
	defer cleanup()
	ctx := context.Background()
	if _, err := client.Put(ctx, &pb.PutRequest{Key: "k", Data: []byte("v")}); err != nil {
		t.Fatalf("put: %v", err)
	}
	resp, err := client.Report(ctx, &pb.ReportRequest{})
	if err != nil || len(resp.Entries) != 1 || resp.Entries[0].Key != "k" {
		t.Fatalf("report: %v resp=%v", err, resp)
	}
}
