/*
 * JuiceMount local source (not upstream JuiceFS). Apache-2.0.
 */

package object

import (
	"syscall"

	"golang.org/x/sys/unix"
)

// jmSetUnackedTimeout sets TCP_USER_TIMEOUT (milliseconds) on a new TCP
// socket (patch 0028, jm_dead_peer.go).
func jmSetUnackedTimeout(network, _ string, c syscall.RawConn) error {
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil
	}
	var serr error
	if err := c.Control(func(fd uintptr) {
		serr = unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT, int(jmUnackedTimeout.Milliseconds()))
	}); err != nil {
		return err
	}
	return serr
}
