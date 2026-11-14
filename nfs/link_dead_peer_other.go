//go:build !linux && !darwin

package nfs

import "syscall"

// linkSetUnackedTimeout: keepalive only on other systems.
func linkSetUnackedTimeout(string, string, syscall.RawConn) error { return nil }
