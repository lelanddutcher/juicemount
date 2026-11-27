//go:build darwin

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
	var serr error
	if err := raw.Control(func(fd uintptr) {
		var cred *unix.Xucred
		cred, serr = unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
		if serr != nil {
			return
		}
		uid = int(cred.Uid)
		pid, serr = unix.GetsockoptInt(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERPID)
	}); err != nil {
		return 0, 0, err
	}
	if serr != nil {
		return 0, 0, serr
	}
	return uid, pid, nil
}
