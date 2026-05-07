package verify

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// LocalTarget verifies files on a locally-mounted filesystem.
// Suitable for: USB drives, internal SSDs, NAS shares mounted via SMB/NFS,
// and the JuiceMount NFS volume itself (any path the OS treats as a folder).
type LocalTarget struct {
	root      string         // root directory (e.g. /Volumes/backup)
	skipFunc  func(p string) bool // optional: return true to skip a path
}

// NewLocalTarget creates a target rooted at the given directory.
// The path is verified at construction time but not at every Available() call.
func NewLocalTarget(root string) (*LocalTarget, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if info, err := os.Stat(abs); err != nil {
		return nil, err
	} else if !info.IsDir() {
		return nil, &fs.PathError{Op: "stat", Path: abs, Err: fs.ErrInvalid}
	}
	return &LocalTarget{root: abs}, nil
}

// SkipFunc registers a predicate that causes Walk to skip matching paths.
// Common uses: skip dotfiles, .DS_Store, ._resource forks, .git directories.
// The path passed in is relative to the target root.
func (t *LocalTarget) SkipFunc(fn func(string) bool) {
	t.skipFunc = fn
}

// Identifier returns "local:<root>" for stable manifest keying.
func (t *LocalTarget) Identifier() string {
	return commonPrefix("local", t.root)
}

// Available checks the root is still reachable. Survives unmounted USB drives
// and disconnected NAS shares — returns false rather than blocking.
func (t *LocalTarget) Available(ctx context.Context) bool {
	_, err := os.Stat(t.root)
	return err == nil
}

// Walk emits Entries for every file in the target's directory tree.
// Buffered channel of capacity 64 — enough to overlap walking with hashing
// without unbounded memory growth on huge libraries.
func (t *LocalTarget) Walk(ctx context.Context) (<-chan Entry, <-chan error) {
	entries := make(chan Entry, 64)
	errs := make(chan error, 1)

	go func() {
		defer close(entries)
		defer close(errs)

		err := filepath.WalkDir(t.root, func(path string, d fs.DirEntry, err error) error {
			// Honor cancellation
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if err != nil {
				// Don't abort the whole walk on a single permission error
				// — log via the error channel and keep going.
				select {
				case errs <- err:
				default: // drop if no consumer
				}
				return nil
			}
			if d.IsDir() {
				// Apply skipFunc to directories so we can prune (e.g. .git)
				rel := strings.TrimPrefix(strings.TrimPrefix(path, t.root), "/")
				if rel != "" && t.skipFunc != nil && t.skipFunc(rel) {
					return filepath.SkipDir
				}
				return nil
			}

			rel := strings.TrimPrefix(strings.TrimPrefix(path, t.root), "/")
			if t.skipFunc != nil && t.skipFunc(rel) {
				return nil
			}

			info, err := d.Info()
			if err != nil {
				return nil
			}

			e := Entry{
				RelPath: rel,
				Size:    info.Size(),
				ModTime: info.ModTime(),
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case entries <- e:
			}
			return nil
		})
		if err != nil && err != context.Canceled {
			select {
			case errs <- err:
			default:
			}
		}
	}()

	return entries, errs
}

// Hash streams the file at relPath through SHA-256 and returns the hex digest.
func (t *LocalTarget) Hash(ctx context.Context, relPath string) (string, error) {
	full := filepath.Join(t.root, relPath)
	f, err := os.Open(full)
	if err != nil {
		return "", err
	}
	defer f.Close()
	// Honor cancellation by spawning the hash in a goroutine — but for a
	// local file the Read syscall is cancelable via close(), so this works
	// even without context propagation in io.Copy.
	done := make(chan struct {
		h   string
		err error
	}, 1)
	go func() {
		h, err := hashReader(f)
		done <- struct {
			h   string
			err error
		}{h, err}
	}()
	select {
	case <-ctx.Done():
		f.Close() // cancels in-flight Read
		return "", ctx.Err()
	case r := <-done:
		return r.h, r.err
	}
}

// DefaultMacSkipFunc is a sensible skip filter for macOS-mounted targets.
// Skips Apple-Double files, .DS_Store, Spotlight + Time Machine bookkeeping.
func DefaultMacSkipFunc(rel string) bool {
	base := filepath.Base(rel)
	if strings.HasPrefix(base, "._") {
		return true
	}
	switch base {
	case ".DS_Store", ".Spotlight-V100", ".Trashes", ".fseventsd",
		".TemporaryItems", ".VolumeIcon.icns",
		".HFS+ Private Directory Data\r":
		return true
	}
	return false
}
