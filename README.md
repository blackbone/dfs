# dfs

`dfs` is a tiny distributed key/value store built on top of
[Hashicorp Raft](https://github.com/hashicorp/raft) for consensus and
gRPC for the public API. It is intended as a learning example rather
than a production system.

## Building

```sh
go build ./cmd/dfs
```

## Running

Each process hosts a single node. Raft traffic defaults to port `12000`
and the gRPC API to `13000`, so they can be omitted from the flags.

```sh
# start first node
./dfs -id node1

# start second node and join the first
./dfs -id node2 -peers node1
```

Once a leader is elected you can store and retrieve data using any gRPC
client. The `USAGE.md` file shows examples with `grpcurl` and Docker
Compose for a three node cluster.

## Extending

The project is intentionally small:

* `cmd/dfs` contains the entry point and flag parsing.
* `internal/node` wraps a Raft instance.
* `internal/store` implements the replicated key/value state machine.
* `internal/server` exposes the gRPC `FileService` backed by the store.

New functionality can be added by extending the store and exposing new
RPC methods in the server package.

