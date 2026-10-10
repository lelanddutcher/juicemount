//go:build darwin || linux

package metadata

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

// ErrStoreAlreadyOpen means another cooperating process owns this SQLite file.
var ErrStoreAlreadyOpen = errors.New("metadata store already open")

type storeFileLock struct {
	file *os.File
	once sync.Once
	err  error
}

// acquireStoreFileLock serializes on-disk Store lifetimes. The lock file is
// intentionally persistent: unlinking it could let a waiter lock a different
// inode from the process that currently owns it.
func acquireStoreFileLock(dbPath string) (*storeFileLock, string, error) {
	if dbPath == ":memory:" {
		return nil, "", nil
	}
	if strings.HasPrefix(dbPath, "file:") {
		return nil, "", fmt.Errorf("metadata store SQLite URI paths are unsupported: %q", dbPath)
	}
	if strings.ContainsAny(dbPath, "?#") {
		return nil, "", fmt.Errorf("metadata store paths containing '?' or '#' are unsupported: %q", dbPath)
	}
	if dbPath == "" {
		return nil, "", errors.New("metadata store path is empty")
	}

	abs, err := filepath.Abs(dbPath)
	if err != nil {
		return nil, "", fmt.Errorf("resolve metadata store path: %w", err)
	}
	canonical, err := canonicalStorePath(abs)
	if err != nil {
		return nil, "", fmt.Errorf("resolve metadata store path %q: %w", dbPath, err)
	}
	// A pathname lock cannot distinguish hard links to the same SQLite inode.
	// Reject every alias while the inode has multiple links, including aliases
	// created after an earlier Store acquired its lock.
	var stat unix.Stat_t
	if err := unix.Stat(canonical, &stat); err == nil {
		if stat.Mode&unix.S_IFMT == unix.S_IFREG && stat.Nlink > 1 {
			return nil, "", fmt.Errorf("metadata store database has %d hard links: %s", stat.Nlink, canonical)
		}
	} else if !errors.Is(err, unix.ENOENT) {
		return nil, "", fmt.Errorf("stat metadata store path %q: %w", canonical, err)
	}
	lockPath := canonical + ".juicemount-store.lock"
	fd, err := unix.Open(lockPath, unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, "", fmt.Errorf("open metadata store lock %q: %w", lockPath, err)
	}
	file := os.NewFile(uintptr(fd), lockPath)
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, "", fmt.Errorf("%w: %s", ErrStoreAlreadyOpen, canonical)
		}
		return nil, "", fmt.Errorf("lock metadata store %q: %w", canonical, err)
	}
	return &storeFileLock{file: file}, canonical, nil
}

func canonicalStorePath(abs string) (string, error) {
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		return filepath.Clean(resolved), nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if info, err := os.Lstat(abs); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("database path is a dangling or unresolved symlink")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	parent := filepath.Dir(abs)
	resolvedParent, err := filepath.EvalSymlinks(parent)
	if err != nil {
		return "", err
	}
	return filepath.Join(resolvedParent, filepath.Base(abs)), nil
}

func (l *storeFileLock) Close() error {
	if l == nil {
		return nil
	}
	l.once.Do(func() {
		if l.file != nil {
			l.err = l.file.Close()
		}
	})
	return l.err
}
