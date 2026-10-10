//go:build !darwin && !linux

package metadata

import (
	"errors"
	"fmt"
	"strings"
)

var ErrStoreAlreadyOpen = errors.New("metadata store already open")

type storeFileLock struct{}

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
	return nil, "", errors.New("on-disk metadata Store locking is unsupported on this platform")
}

func (l *storeFileLock) Close() error { return nil }
