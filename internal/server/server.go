// Package server exposes the gRPC API backed by the replicated store.
package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"dfs/internal/node"
	pb "dfs/proto"
)

const (
	errNotLeaderFmt = "not leader: %s"
	errNotFound     = "not found"
)

// Server implements the FileService gRPC interface. Each instance
// serves requests for a single Raft node.
type Server struct {
	pb.UnimplementedFileServiceServer
	node *node.Node
}

func New(n *node.Node) *Server { return &Server{node: n} }

// Put stores a key/value pair. Writes must go through the leader
// in order to be replicated via Raft.
func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if !s.node.IsLeader() {
		return nil, status.Errorf(codes.FailedPrecondition, errNotLeaderFmt, s.node.Leader())
	}
	if err := s.node.Put(req.Key, req.Data); err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &pb.PutResponse{}, nil
}

// Get returns the value for a key. Reads are served from the local
// state machine and therefore can be handled by any node.
func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	data, ok := s.node.Get(req.Key)
	if !ok {
		return nil, status.Errorf(codes.NotFound, errNotFound)
	}
	return &pb.GetResponse{Data: data}, nil
}

// Backup returns a serialized snapshot of the store.
func (s *Server) Backup(ctx context.Context, _ *pb.BackupRequest) (*pb.BackupResponse, error) {
	data, err := s.node.Backup()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &pb.BackupResponse{Data: data}, nil
}

// Restore loads serialized data into the store. Only the leader may restore.
func (s *Server) Restore(ctx context.Context, req *pb.RestoreRequest) (*pb.RestoreResponse, error) {
	if !s.node.IsLeader() {
		return nil, status.Errorf(codes.FailedPrecondition, errNotLeaderFmt, s.node.Leader())
	}
	if err := s.node.Restore(req.Data); err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &pb.RestoreResponse{}, nil
}
