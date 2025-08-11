# Config

The config package loads node configuration from files or environment variables using Viper.
It defines constant keys and environment variable names (e.g. `DFS_ID`, `DFS_RAFT`) and defaults
for node identity, data directories, and join behavior.

`Load(path string)` returns a `Config` struct with fields `ID`, `Raft`, `GRPC`, `Data`, `Peers`, and `Join`.
Command-line tools and servers call this function to obtain runtime settings.

**Data contracts**

- Each field of `Config` corresponds to a configuration option and may be populated from a config file or `DFS_*` env var.
- Empty `path` uses `${DFS_CONFIG}` or falls back to `dfs.yaml` in the current directory.
