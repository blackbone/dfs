// Program dfs starts a single node of the distributed key/value store.
package main

import (
	"context"
	"flag"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	"dfs"
	dfsfs "dfs/internal/fusefs"
	"dfs/internal/node"
	obs "dfs/internal/observability"
	"dfs/internal/server"
	pb "dfs/proto"
)

// withDefaultPort ensures the address has a port. If missing, defaultPort
// is appended. An empty host is allowed and results in ":port".
func withDefaultPort(addr string, defaultPort int) string {
	if _, _, err := net.SplitHostPort(addr); err == nil {
		return addr
	}
	return net.JoinHostPort(addr, strconv.Itoa(defaultPort))
}

func main() {
	id := flag.String("id", "node1", "node ID")
	raftAddr := flag.String("raft", "", "raft bind address")
	grpcAddr := flag.String("grpc", "", "gRPC bind address")
	dataDir := flag.String("data", "data", "data directory")
	peers := flag.String("peers", "", "comma separated peer raft addresses")
	flag.Parse()

	const (
		defaultRaftPort = 12000
		defaultGRPCPort = 13000
		metricsAddr     = ":2112"

		msgNode   = "node"
		msgMount  = "mount"
		msgWatch  = "watch"
		msgListen = "listen"
		msgServe  = "serve"
		msgGRPC   = "grpc_listen"
		msgMetric = "metrics_listen"
	)

	rAddr := withDefaultPort(*raftAddr, defaultRaftPort)
	gAddr := withDefaultPort(*grpcAddr, defaultGRPCPort)
	peerStr := ""
	if *peers != "" {
		var ps []string
		for _, p := range strings.Split(*peers, ",") {
			ps = append(ps, withDefaultPort(p, defaultRaftPort))
		}
		peerStr = strings.Join(ps, ",")
	}

	n, err := node.New(*id, rAddr, *dataDir, peerStr)
	if err != nil {
		obs.Logger.Fatal().Err(err).Msg(msgNode)
	}
	dfs.SetNode(n)

	// Start FUSE filesystem and cache watcher.
	go func() {
		if err := dfsfs.Mount("/mnt/dfs", "/mnt/hostfs"); err != nil {
			obs.Logger.Fatal().Err(err).Msg(msgMount)
		}
	}()
	go func() {
		if err := dfsfs.Watch(context.Background(), "/mnt/hostfs"); err != nil {
			obs.Logger.Fatal().Err(err).Msg(msgWatch)
		}
	}()

	// Register metrics and start HTTP server.
	obs.Register()
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(metricsAddr, nil); err != nil {
			obs.Logger.Fatal().Err(err).Msg(msgMetric)
		}
	}()

	lis, err := net.Listen("tcp", gAddr)
	if err != nil {
		obs.Logger.Fatal().Err(err).Msg(msgListen)
	}
	s := grpc.NewServer()
	pb.RegisterFileServiceServer(s, server.New(n))
	obs.Logger.Info().Str(obs.FieldAddress, gAddr).Msg(msgGRPC)
	if err := s.Serve(lis); err != nil {
		obs.Logger.Fatal().Err(err).Msg(msgServe)
	}
}
