//go:build !windows

package fusefs

import (
	"context"
	"crypto/sha256"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"dfs"
)

const (
	scanPerm = 0o644
)

// Check periodically validates cached files against metadata and refreshes stale entries.
func Check(ctx context.Context, cacheDir string, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			scan(cacheDir)
		}
	}
}

func scan(cacheDir string) {
	filepath.WalkDir(cacheDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(p) == verSuffix {
			return nil
		}
		rel, err := filepath.Rel(cacheDir, p)
		if err != nil {
			return nil
		}
		meta, err := dfs.GetMetadata(rel)
		if err != nil || meta.Deleted {
			os.Remove(p)
			os.Remove(p + verSuffix)
			return nil
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return nil
		}
		vb, err := os.ReadFile(p + verSuffix)
		if err != nil {
			vb = nil
		}
		v, _ := strconv.ParseUint(string(vb), 10, 64)
		h := sha256.Sum256(data)
		if v != meta.Version || h != meta.Hash {
			if data, err = dfs.GetFile(rel); err != nil {
				return nil
			}
			_ = os.WriteFile(p, data, scanPerm)
			_ = os.WriteFile(p+verSuffix, []byte(strconv.FormatUint(meta.Version, 10)), scanPerm)
		}
		return nil
	})
}
