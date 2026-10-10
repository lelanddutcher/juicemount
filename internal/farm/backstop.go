package farm

import (
	"context"
	"io/fs"
	"path/filepath"
	"time"
)

// DiscoverModified recursively finds the shallowest directories modified
// after cursor, or directories containing files modified after cursor. A file
// edit does not change its parent directory's mtime, so every eligible file
// must be checked even beneath old directories. Enqueueing a directory is
// sufficient because queue jobs recurse below it. Root-level files are
// emitted directly; hidden/derivative trees use the keyspace feedback guard.
//
// This is a low-frequency backstop for Redis PubSub gaps, not the primary
// watcher. It deliberately performs no writes and is bounded by maxCandidates.
func DiscoverModified(ctx context.Context, mount string, cursor time.Time, maxCandidates int) ([]string, bool, error) {
	if maxCandidates <= 0 {
		maxCandidates = 2000
	}
	var paths []string
	seen := make(map[string]struct{})
	saturated := false
	err := filepath.WalkDir(mount, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(mount, path)
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if rel == "." {
				return nil
			}
			if !WatchPathAllowed(filepath.ToSlash(rel)) {
				return filepath.SkipDir
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			if info.ModTime().After(cursor) {
				paths = append(paths, filepath.ToSlash(rel))
				if len(paths) >= maxCandidates {
					saturated = true
					return filepath.SkipAll
				}
				return filepath.SkipDir
			}
			return nil
		}
		if !WatchPathAllowed(filepath.ToSlash(rel)) {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.ModTime().After(cursor) {
			return nil
		}
		// The mount root cannot be enqueued by inode, while nested files are
		// represented by their recursive parent-directory job.
		target := rel
		if parent := filepath.Dir(rel); parent != "." {
			target = parent
		}
		target = filepath.ToSlash(target)
		if _, ok := seen[target]; !ok {
			seen[target] = struct{}{}
			paths = append(paths, target)
			if len(paths) >= maxCandidates {
				saturated = true
				return filepath.SkipAll
			}
		}
		return nil
	})
	return paths, saturated, err
}
