# dfs

`dfs` is a tiny distributed file system written in Go. Each node runs a
FUSE filesystem mounted at `/mnt/dfs` and maintains a persistent cache in
`/mnt/hostfs`. Nodes exchange file metadata and blobs directly with each
other over HTTP; no external services are required.

## Building

```sh
go build ./cmd/dfs
```

## Running

Each process hosts a single node. The HTTP address defaults to
`127.0.0.1:9000` and peers can be specified as a comma separated list of
addresses.

```sh
# start first node
./dfs -id node1 -addr 127.0.0.1:9000

# start second node and connect to the first
./dfs -id node2 -addr 127.0.0.1:9001 -peers 127.0.0.1:9000
```

Files read through the FUSE mount are served from the local cache when
possible. When missing or stale the node fetches the blob from the owner
node, stores it locally and updates the cache.

## Project layout

The project is intentionally small:

* `cmd/dfs` contains the entry point and flag parsing.
* `internal/node` implements the DFS node including metadata and blob stores
  and the simple HTTP RPC service.
* `internal/metadata` provides the in-memory metadata store.
* `internal/blobstore` stores file contents on disk.
* `internal/fusefs` exposes the read-only FUSE filesystem backed by the
  cache and node RPC.

