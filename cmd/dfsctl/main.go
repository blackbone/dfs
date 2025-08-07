package main

import (
	"context"
	"flag"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "dfs/proto"
)

const (
	cmdAdd      = "add"
	cmdRemove   = "remove"
	cmdDelete   = "delete"
	flagGRPC    = "grpc"
	flagID      = "id"
	flagAddr    = "address"
	flagKey     = "key"
	defaultGRPC = ":13000"
	timeoutSec  = 5
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("usage: %s [add|remove|delete] [flags]", os.Args[0])
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	grpcAddr := fs.String(flagGRPC, defaultGRPC, "gRPC address")
	id := fs.String(flagID, "", "node id")
	addr := fs.String(flagAddr, "", "raft address")
	key := fs.String(flagKey, "", "file key")
	fs.Parse(os.Args[2:])

	ctx, cancel := context.WithTimeout(context.Background(), timeoutSec*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, *grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	client := pb.NewFileServiceClient(conn)
	switch cmd {
	case cmdAdd:
		if _, err := client.AddPeer(ctx, &pb.AddPeerRequest{Id: *id, Address: *addr}); err != nil {
			log.Fatalf("add: %v", err)
		}
	case cmdRemove:
		if _, err := client.RemovePeer(ctx, &pb.RemovePeerRequest{Id: *id}); err != nil {
			log.Fatalf("remove: %v", err)
		}
	case cmdDelete:
		if _, err := client.Delete(ctx, &pb.DeleteRequest{Key: *key}); err != nil {
			log.Fatalf("delete: %v", err)
		}
	default:
		log.Fatalf("unknown command %s", cmd)
	}
}
