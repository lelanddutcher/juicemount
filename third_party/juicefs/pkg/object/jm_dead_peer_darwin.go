/*
 * JuiceMount local source (not upstream JuiceFS). Apache-2.0.
 */

package object

import (
	"syscall"

	"golang.org/x/sys/unix"
)

// jmSetUnackedTimeout sets TCP_RXT_CONNDROPTIME (seconds: the time after
// which retransmission stops and the connection drops) on a new TCP socket
// (patch 0028, jm_dead_peer.go).
func jmSetUnackedTimeout(network, _ string, c syscall.RawConn) error {
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil
	}
	var serr error
	if err := c.Control(func(fd uintptr) {
		serr = unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_RXT_CONNDROPTIME, int(jmUnackedTimeout.Seconds()))
	}); err != nil {
		return err
	}
	return serr
}
