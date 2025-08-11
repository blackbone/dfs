# Config

The config package loads node configuration from environment variables without
third-party dependencies. It defines constant environment variable names
(e.g. `DFS_ID`, `DFS_RAFT`) and defaults for node identity, data directories, and join
behavior.

`Load()` returns a `Config` struct with fields `ID`, `Raft`, `GRPC`, `Data`, `Peers`, and `Join`.
Command-line tools and servers call this function to obtain runtime settings.

**Data contracts**

- Each field of `Config` corresponds to a configuration option and may be populated from a `DFS_*` env var.
