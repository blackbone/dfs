# dfs

`dfs` is a tiny distributed key/value store built on top of
[Hashicorp Raft](https://github.com/hashicorp/raft) for consensus and
gRPC for the public API. It is intended as a learning example rather
than a production system. The project targets Unix-like environments
with FUSE; Windows platforms are unsupported.

## Building

```sh
go build ./cmd/dfs
```

## Running

Each process hosts a single node. Configuration is read from a file named
`dfs.yaml` in the working directory or from environment variables prefixed
with `DFS_`. Raft traffic defaults to port `12000` and the gRPC API to
`13000`.

```sh
# start first node
DFS_ID=node1 ./dfs

# start second node and join the first
DFS_ID=node2 DFS_PEERS=node1 ./dfs
```

Once a leader is elected you can store and retrieve data using any gRPC
client. The `USAGE.md` file shows examples with `grpcurl` and Docker
Compose for a three node cluster.

## Configuration

The `dfs` binary accepts the following flags:

* `-id` – node identifier (default `node1`).
* `-raft` – Raft bind address (default `:12000`).
* `-grpc` – gRPC bind address (default `:13000`).
* `-data` – data directory for Raft state (default `data`).
* `-peers` – comma-separated peer Raft addresses.

## API

The gRPC API exposes two methods defined in `proto/dfs.proto`:

* `Put` stores a key and opaque byte data.
* `Get` retrieves the data for a key.

Examples using `grpcurl` are available in `USAGE.md`.

## FUSE Filesystem

Each node mounts a read-only filesystem at `/mnt/dfs` backed by a cache
directory `/mnt/hostfs`. New or modified files written to the cache are
replicated into the DFS and become visible under the mount. See
[FUSE.md](FUSE.md) for details on mounting and watching the cache.

## Extending

The project is intentionally small:

* `cmd/dfs` contains the entry point and configuration loading.
* `internal/node` wraps a Raft instance.
* `internal/store` implements the replicated key/value state machine.
* `internal/server` exposes the gRPC `FileService` backed by the store.

New functionality can be added by extending the store and exposing new
RPC methods in the server package.

