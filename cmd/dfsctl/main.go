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
	cmdJoin    = "join"
	cmdLeave   = "leave"
	rpcTimeout = 5 * time.Second
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("usage: %s [join|leave]", os.Args[0])
	}
	switch os.Args[1] {
	case cmdJoin:
		joinCmd(os.Args[2:])
	case cmdLeave:
		leaveCmd(os.Args[2:])
	default:
		log.Fatalf("unknown command: %s", os.Args[1])
	}
}

func dial(addr string) (*grpc.ClientConn, pb.FileServiceClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return conn, pb.NewFileServiceClient(conn), nil
}

func joinCmd(args []string) {
	fs := flag.NewFlagSet(cmdJoin, flag.ExitOnError)
	leader := fs.String("leader", "", "leader gRPC address")
	id := fs.String("id", "", "node ID")
	raftAddr := fs.String("raft", "", "node raft address")
	fs.Parse(args)
	conn, client, err := dial(*leader)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	if _, err := client.Join(ctx, &pb.JoinRequest{Id: *id, RaftAddr: *raftAddr}); err != nil {
		log.Fatalf("join: %v", err)
	}
}

func leaveCmd(args []string) {
	fs := flag.NewFlagSet(cmdLeave, flag.ExitOnError)
	leader := fs.String("leader", "", "leader gRPC address")
	id := fs.String("id", "", "node ID")
	fs.Parse(args)
	conn, client, err := dial(*leader)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	if _, err := client.Leave(ctx, &pb.LeaveRequest{Id: *id}); err != nil {
		log.Fatalf("leave: %v", err)
	}
}
