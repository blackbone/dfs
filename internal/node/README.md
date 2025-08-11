# Node

The node package manages a single Hashicorp Raft instance and its associated finite state machine.
`Node` wraps the Raft `*raft.Raft`, an internal FSM for key/value data, and a `metastore.Store` for file metadata.

Responsibilities include configuring transports and storage, applying replicated commands (`Put`, `Delete`, `SyncMeta`),
managing cluster membership, exposing leadership information, and running periodic metadata garbage collection.

It is used by the gRPC server to serve client requests and by utilities that manipulate cluster state.

**Data contracts**

- `New(id, bind, dataDir, peers string, bootstrap bool)` constructs a disk-backed node.
- `NewInmem()` returns an in-memory node for tests.
- Commands applied through Raft encode an operation enum and key/data payload.
