//go:build !linux && !darwin

/*
 * JuiceMount local source (not upstream JuiceFS). Apache-2.0.
 */

package object

import "syscall"

// jmSetUnackedTimeout: keepalive only on other systems (patch 0028).
func jmSetUnackedTimeout(string, string, syscall.RawConn) error { return nil }
