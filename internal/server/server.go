// Package server exposes the gRPC API backed by the replicated store.
package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"dfs/internal/node"
	pb "dfs/proto"
)

// Server implements the FileService gRPC interface. Each instance
// serves requests for a single Raft node.
type Server struct {
	pb.UnimplementedFileServiceServer
	node *node.Node
}

func New(n *node.Node) *Server { return &Server{node: n} }

const (
	errNotLeaderFmt = "not leader: %s"
	errNotFound     = "not found"
)

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

// Join adds a node to the cluster.
func (s *Server) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	if !s.node.IsLeader() {
		return nil, status.Errorf(codes.FailedPrecondition, errNotLeaderFmt, s.node.Leader())
	}
	if err := s.node.Join(req.Id, req.RaftAddr); err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &pb.JoinResponse{}, nil
}

// Leave removes a node from the cluster.
func (s *Server) Leave(ctx context.Context, req *pb.LeaveRequest) (*pb.LeaveResponse, error) {
	if !s.node.IsLeader() {
		return nil, status.Errorf(codes.FailedPrecondition, errNotLeaderFmt, s.node.Leader())
	}
	if err := s.node.Leave(req.Id); err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &pb.LeaveResponse{}, nil
}
