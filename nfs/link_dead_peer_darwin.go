package nfs

import (
	"syscall"

	"golang.org/x/sys/unix"
)

// linkSetUnackedTimeout sets TCP_RXT_CONNDROPTIME (seconds) on a new TCP
// socket (link_dead_peer.go).
func linkSetUnackedTimeout(network, _ string, c syscall.RawConn) error {
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil
	}
	var serr error
	if err := c.Control(func(fd uintptr) {
		serr = unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_RXT_CONNDROPTIME, int(linkUnackedTimeout.Seconds()))
	}); err != nil {
		return err
	}
	return serr
}
