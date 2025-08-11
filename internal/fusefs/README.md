# FuseFS

The fusefs package exposes a read-only FUSE filesystem backed by the DFS cluster and a local cache.
`FS` maintains an in-memory map of cached entries and a cache directory on disk. Missing files are fetched via
`dfs.GetFile` and stored alongside a version marker to keep the cache coherent.

Clients mount this filesystem to access distributed files through standard POSIX operations. The package interacts
with DFS metadata to ensure cached data matches recorded versions.

**Data contracts**

- `FS` struct: cache directory and in-memory `map[path]cacheEntry` holding file data and version.
- `Dir` and `File` types implement `bazil.org/fuse/fs` nodes for directory and file operations.
- Cached files store a companion `<name>.ver` file containing the version number.
