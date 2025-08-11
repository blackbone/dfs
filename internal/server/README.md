# Server

The server package exposes the gRPC API defined in `proto` and is backed by a `node.Node`.
`Server` implements the `pb.FileServiceServer` interface, routing client requests to the underlying node.

Responsibilities:

- `Put` and `Delete` forward writes through the Raft leader.
- `Get` serves reads from the local state machine.
- `AddPeer` and `RemovePeer` modify cluster membership.
- `SyncMetadata` updates the local `metastore` with external metadata entries.

Other modules and external clients interact with this package over gRPC to manipulate or query the distributed store.

**Data contracts**

- Protobuf request/response messages in `proto` define the on-the-wire schema.
- Errors use gRPC status codes; writes return `FailedPrecondition` when invoked on followers.
