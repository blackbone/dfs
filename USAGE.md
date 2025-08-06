# Example Usage

This example shows how to run a three node cluster using Docker Compose and
interact with it using [`grpcurl`](https://github.com/fullstorydev/grpcurl).

The `dfs` binary defaults to Raft port `12000` and gRPC port `13000`, so
addresses in the compose file include ports only for clarity.

## Start the cluster

```sh
docker compose up --build
```

Each node exposes a gRPC API on ports `13001`, `13002` and `13003` on the host.
After the cluster elects a leader, `Put` requests must target the leader's
port. `Get` requests may be sent to any node.

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

## Access via FUSE

Each node exposes the replicated data as a read-only filesystem mounted at
`/mnt/dfs` and backed by a writable cache at `/mnt/hostfs`. Writing a file to
the cache replicates it to the cluster and makes it visible under the mount.

```sh
echo "hello" > /mnt/hostfs/greeting.txt
cat /mnt/dfs/greeting.txt
```
