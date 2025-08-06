# dfs

This repository contains a minimal distributed key/value store demonstrating
gRPC-based APIs with Raft leader election and log replication.

## Running

```
go build ./cmd/dfs
./dfs -id node1 -raft :12000 -grpc :13000 -data data1
./dfs -id node2 -raft :12001 -grpc :13001 -data data2 -peers :12000
```

After the cluster elects a leader, use a gRPC client to invoke `Put` and `Get`
requests against the leader's gRPC endpoint.

---

The content below is from the Google File System paper and left for reference.

The Google File System
Sanjay Ghemawat, Howard Gobioff, and Shun-Tak Leung
Google∗
ABSTRACT
We have designed and implemented the Google File Sys-
tem, a scalable distributed file system for large distributed
data-intensive applications. It provides fault tolerance while
running on inexpensive commodity hardware, and it delivers
high aggregate performance to a large number of clients.
While sharing many of the same goals as previous dis-
tributed file systems, our design has been driven by obser-
vations of our application workloads and technological envi-
ronment, both current and anticipated, that reflect a marked
departure from some earlier file system assumptions. This
has led us to reexamine traditional choices and explore rad-
