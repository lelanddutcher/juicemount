//go:build linux

package jmengine

import (
	"net"

	"golang.org/x/sys/unix"
)

// peerCred returns the user and process ID on the other end of c.
func peerCred(c *net.UnixConn) (uid, pid int, err error) {
	raw, err := c.SyscallConn()
	if err != nil {
		return 0, 0, err
	}
	var cred *unix.Ucred
	var serr error
	if err := raw.Control(func(fd uintptr) {
		cred, serr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		return 0, 0, err
	}
	if serr != nil {
		return 0, 0, serr
	}
	return int(cred.Uid), int(cred.Pid), nil
}
