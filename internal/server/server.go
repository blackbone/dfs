package server

import (
    "context"

    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"

    pb "dfs/proto"
    "dfs/internal/node"
)

// Server implements the gRPC FileService.
type Server struct {
    pb.UnimplementedFileServiceServer
    node *node.Node
}

func New(n *node.Node) *Server { return &Server{node: n} }

// Put stores data under key using Raft for replication.
func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
    if !s.node.IsLeader() {
        return nil, status.Errorf(codes.FailedPrecondition, "not leader: %s", s.node.Leader())
    }
    if err := s.node.Put(req.Key, req.Data); err != nil {
        return nil, status.Errorf(codes.Internal, "%v", err)
    }
    return &pb.PutResponse{}, nil
}

// Get retrieves data for key from leader.
func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
    if !s.node.IsLeader() {
        return nil, status.Errorf(codes.FailedPrecondition, "not leader: %s", s.node.Leader())
    }
    data, ok := s.node.Get(req.Key)
    if !ok {
        return nil, status.Errorf(codes.NotFound, "not found")
    }
    return &pb.GetResponse{Data: data}, nil
}

