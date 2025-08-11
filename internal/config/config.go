package config

import (
	"os"
	"strconv"
	"strings"
)

const (
	EnvID    = "DFS_ID"
	EnvRaft  = "DFS_RAFT"
	EnvGRPC  = "DFS_GRPC"
	EnvData  = "DFS_DATA"
	EnvPeers = "DFS_PEERS"
	EnvJoin  = "DFS_JOIN"

	DefaultID      = "node1"
	DefaultDataDir = "data"

	commaSep = ','
)

type Config struct {
	ID    string
	Raft  string
	GRPC  string
	Data  string
	Peers []string
	Join  bool
}

// Load reads configuration from environment variables.
func Load() (Config, error) {
	cfg := Config{ID: DefaultID, Data: DefaultDataDir}

	if v, ok := os.LookupEnv(EnvID); ok && v != "" {
		cfg.ID = v
	}
	if v, ok := os.LookupEnv(EnvRaft); ok && v != "" {
		cfg.Raft = v
	}
	if v, ok := os.LookupEnv(EnvGRPC); ok && v != "" {
		cfg.GRPC = v
	}
	if v, ok := os.LookupEnv(EnvData); ok && v != "" {
		cfg.Data = v
	}
	if v, ok := os.LookupEnv(EnvPeers); ok {
		if v != "" {
			cfg.Peers = strings.Split(v, string(commaSep))
		} else {
			cfg.Peers = nil
		}
	}
	if v, ok := os.LookupEnv(EnvJoin); ok {
		if v != "" {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return cfg, err
			}
			cfg.Join = b
		}
	}

	return cfg, nil
}
