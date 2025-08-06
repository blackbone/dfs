package config

import (
	"errors"
	"os"

	"github.com/spf13/viper"
)

const (
	KeyID    = "id"
	KeyRaft  = "raft"
	KeyGRPC  = "grpc"
	KeyData  = "data"
	KeyPeers = "peers"

	EnvPrefix = "DFS"
	EnvConfig = "DFS_CONFIG"
	EnvID     = "DFS_ID"
	EnvRaft   = "DFS_RAFT"
	EnvGRPC   = "DFS_GRPC"
	EnvData   = "DFS_DATA"
	EnvPeers  = "DFS_PEERS"

	DefaultID         = "node1"
	DefaultDataDir    = "data"
	DefaultConfigName = "dfs"
	ConfigDir         = "."
)

type Config struct {
	ID    string   `mapstructure:"id"`
	Raft  string   `mapstructure:"raft"`
	GRPC  string   `mapstructure:"grpc"`
	Data  string   `mapstructure:"data"`
	Peers []string `mapstructure:"peers"`
}

func Load(path string) (Config, error) {
	var cfg Config
	v := viper.New()

	v.SetDefault(KeyID, DefaultID)
	v.SetDefault(KeyData, DefaultDataDir)

	v.SetEnvPrefix(EnvPrefix)
	v.AutomaticEnv()

	if path == "" {
		if envPath, ok := os.LookupEnv(EnvConfig); ok {
			path = envPath
		}
	}

	if path != "" {
		v.SetConfigFile(path)
	} else {
		v.SetConfigName(DefaultConfigName)
		v.AddConfigPath(ConfigDir)
	}

	if err := v.ReadInConfig(); err != nil {
		var nf viper.ConfigFileNotFoundError
		if !(errors.As(err, &nf) || errors.Is(err, os.ErrNotExist)) {
			return cfg, err
		}
	}

	if err := v.Unmarshal(&cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}
