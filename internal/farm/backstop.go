package farm

import (
	"context"
	"io/fs"
	"path/filepath"
	"time"
)

// DiscoverModified recursively finds the shallowest directories modified
// after cursor. Enqueueing one such directory is sufficient because queue jobs
// recurse below it. Root-level files are emitted directly; hidden/derivative
// trees are excluded by the same feedback-loop guard as keyspace events.
//
// This is a low-frequency backstop for Redis PubSub gaps, not the primary
// watcher. It deliberately performs no writes and is bounded by maxCandidates.
func DiscoverModified(ctx context.Context, mount string, cursor time.Time, maxCandidates int) ([]string, bool, error) {
	if maxCandidates <= 0 {
		maxCandidates = 2000
	}
	var paths []string
	saturated := false
	err := filepath.WalkDir(mount, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(mount, path)
		if err != nil {
			return nil
		}
		if entry.IsDir() {
			if rel == "." {
				return nil
			}
			if !WatchPathAllowed(filepath.ToSlash(rel)) {
				return filepath.SkipDir
			}
			info, err := entry.Info()
			if err == nil && info.ModTime().After(cursor) {
				paths = append(paths, filepath.ToSlash(rel))
				if len(paths) >= maxCandidates {
					saturated = true
					return filepath.SkipAll
				}
				return filepath.SkipDir
			}
			return nil
		}
		// A file created directly at mount root never generates an eligible
		// directory job (inode 1 is intentionally ignored), so include it.
		if filepath.Dir(rel) == "." && WatchPathAllowed(filepath.ToSlash(rel)) {
			if info, err := entry.Info(); err == nil && info.ModTime().After(cursor) {
				paths = append(paths, filepath.ToSlash(rel))
				if len(paths) >= maxCandidates {
					saturated = true
					return filepath.SkipAll
				}
			}
		}
		return nil
	})
	return paths, saturated, err
}
