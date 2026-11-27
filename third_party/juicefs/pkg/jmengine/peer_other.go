//go:build !linux && !darwin

package jmengine

import (
	"errors"
	"net"
)

// peerCred is unavailable here: every connection is refused.
func peerCred(c *net.UnixConn) (uid, pid int, err error) {
	return 0, 0, errors.New("jm-engine: peer credentials are not supported on this system")
}
