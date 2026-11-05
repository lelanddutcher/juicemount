//go:build !linux

package manager

// refuseFUSEDir: only the mount-path check applies off Linux.
func refuseFUSEDir(int, string) error { return nil }
