/*
 * JuiceMount local source (not upstream JuiceFS). Apache-2.0.
 */

package object

import (
	"net"
	"net/http"
	"testing"

	"golang.org/x/sys/unix"
)

// Patch 0029: every connection of the shared object-store client ends by
// itself when the peer stops answering (keepalive) or stops acknowledging
// (TCP_USER_TIMEOUT), instead of waiting for the whole get/put deadline.
func TestObjectClientConnectionsDetectDeadPeers(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			defer c.Close()
		}
	}()
	tr := GetHttpClient().Transport.(*http.Transport)
	conn, err := tr.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	raw, err := conn.(*net.TCPConn).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	get := func(level, opt int) int {
		var v int
		var gerr error
		if err := raw.Control(func(fd uintptr) { v, gerr = unix.GetsockoptInt(int(fd), level, opt) }); err != nil {
			t.Fatal(err)
		}
		if gerr != nil {
			t.Fatal(gerr)
		}
		return v
	}
	for _, c := range []struct {
		name       string
		level, opt int
		want       int
	}{
		{"SO_KEEPALIVE", unix.SOL_SOCKET, unix.SO_KEEPALIVE, 1},
		{"TCP_KEEPIDLE", unix.IPPROTO_TCP, unix.TCP_KEEPIDLE, int(jmKeepAliveIdle.Seconds())},
		{"TCP_KEEPINTVL", unix.IPPROTO_TCP, unix.TCP_KEEPINTVL, int(jmKeepAliveInterval.Seconds())},
		{"TCP_KEEPCNT", unix.IPPROTO_TCP, unix.TCP_KEEPCNT, jmKeepAliveCount},
		{"TCP_USER_TIMEOUT", unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT, int(jmUnackedTimeout.Milliseconds())},
	} {
		if got := get(c.level, c.opt); got != c.want {
			t.Errorf("%s = %d, want %d", c.name, got, c.want)
		}
	}
}
