# FUSE Filesystem

The `fusefs` package provides a read-only FUSE filesystem backed by the
distributed store and a persistent on-disk cache. Files are served from
the cache when present and fetched from the DFS when missing.

## Mounting

Use `Mount(mountPoint, cacheDir)` to expose the filesystem. The binary
mounts `/mnt/dfs` by default and uses `/mnt/hostfs` as the cache.

```go
err := fusefs.Mount("/mnt/dfs", "/mnt/hostfs")
```

The mount point must exist and the process needs permission to access the
FUSE device.

## Watching the cache

`Watch(ctx, cacheDir)` monitors the cache directory and replicates new or
modified files into the DFS so they become available to all nodes.

```go
go fusefs.Watch(ctx, "/mnt/hostfs")
```

## Operational flow

1. Start one or more DFS nodes.
2. Mount the FUSE filesystem.
3. Write files into the cache directory; the watcher stores them in the DFS.
4. Read files from the mount point; missing files are fetched from the DFS.

The filesystem is read-only. Updates must be written to the cache
location rather than the mount point.

