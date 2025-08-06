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
	errNotLeader = "not leader: %s"
	errInternal  = "%v"
	errNotFound  = "not found"
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
		return nil, status.Errorf(codes.FailedPrecondition, errNotLeader, s.node.Leader())
	}
	if err := s.node.Put(req.Key, req.Data); err != nil {
		return nil, status.Errorf(codes.Internal, errInternal, err)
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

// AddPeer adds a node to the cluster.
func (s *Server) AddPeer(ctx context.Context, req *pb.AddPeerRequest) (*pb.AddPeerResponse, error) {
	if !s.node.IsLeader() {
		return nil, status.Errorf(codes.FailedPrecondition, errNotLeader, s.node.Leader())
	}
	if err := s.node.AddPeer(req.Id, req.Address); err != nil {
		return nil, status.Errorf(codes.Internal, errInternal, err)
	}
	return &pb.AddPeerResponse{}, nil
}

// RemovePeer removes a node from the cluster.
func (s *Server) RemovePeer(ctx context.Context, req *pb.RemovePeerRequest) (*pb.RemovePeerResponse, error) {
	if !s.node.IsLeader() {
		return nil, status.Errorf(codes.FailedPrecondition, errNotLeader, s.node.Leader())
	}
	if err := s.node.RemovePeer(req.Id); err != nil {
		return nil, status.Errorf(codes.Internal, errInternal, err)
	}
	return &pb.RemovePeerResponse{}, nil
}
