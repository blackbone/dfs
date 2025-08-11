# Blobstore

The blobstore package persists raw file data on disk under a root directory. Each blob is addressed by a path and a monotonically increasing version and stored as `<path>@v<version>`.

It provides `Put`, `Get`, and `GC` functions to write, read, and garbage collect blob files. Callers must supply non-empty paths and track versions externally.

Higher level components such as the distributed store use this package to durably store file contents separate from metadata. The garbage collector accepts a map of paths to versions and removes any blob file not listed.

**Data contracts**

- `Put(path string, version uint64, data []byte)` writes data for a specific path/version.
- `Get(path string, version uint64)` retrieves the blob identified by path/version.
- `GC(keep map[string]uint64)` deletes on-disk blobs missing from the keep set.
