# Example Usage

This example shows how to run a three node cluster using Docker Compose and
interact with it using [`grpcurl`](https://github.com/fullstorydev/grpcurl).

## Start the cluster

```sh
docker compose up --build
```

Each node exposes a gRPC API on ports `13001`, `13002` and `13003` on the host.
After the cluster elects a leader, you can issue requests against the leader's
port.

## Store a value

```sh
# Replace 13001 with the leader's gRPC port if different
grpcurl -plaintext -d '{"key":"foo","data":"YmFy"}' localhost:13001 dfs.FileService/Put
```

## Retrieve a value

```sh
grpcurl -plaintext -d '{"key":"foo"}' localhost:13001 dfs.FileService/Get
```

The `data` field is base64 encoded. The responses will confirm the value has
been stored and retrieved through the distributed system.
