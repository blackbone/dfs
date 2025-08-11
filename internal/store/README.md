# Store

The store package implements the in-memory key/value store that serves as the Raft finite state machine.
It defines an operation enum `Op` (`OpPut`, `OpDelete`) and a `Command` struct carrying the operation,
key, and optional data. Commands are JSON encoded in Raft log entries.

`Store` applies commands to a map, supports creating `Snapshot` objects, and restores state from
snapshots or JSON backups. It also provides helper functions to convert between strings and byte slices
without allocation.

The `node` package embeds this store to maintain replicated state.

**Data contracts**

- `Command` JSON schema: `{op: Op, key: []byte, data: []byte?}`.
- Snapshots and backups serialize the entire key/value map as JSON.
