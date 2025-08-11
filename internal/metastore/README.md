# Metastore

The metastore package maintains file metadata in memory. Each `Entry` records the path, version,
content hash, replica IDs and a deletion flag. Versions are monotonically increasing per path.

A `Store` offers concurrency-safe operations to `Sync` new entries, `Get` metadata, mark `Delete`,
`List` all entries, and perform `GC` to drop deleted records.

`node.Node` uses this package to replicate metadata through Raft and the gRPC server consults it
when serving metadata-related requests.

**Data contracts**

- `Entry` struct: `{Path string, Version uint64, Hash [32]byte, Replicas []ReplicaID, Deleted bool}`.
- `ReplicaID` uniquely identifies a node replica storing file data.
