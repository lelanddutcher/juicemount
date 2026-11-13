package nfs

import (
	"context"
	"net"
	"testing"

	"golang.org/x/sys/unix"
)

// The Link proxy's direct-LAN upstream ends by itself when the NAS stops
// answering (keepalive) or acknowledging (TCP_USER_TIMEOUT), like the
// object-store connections of bundled JuiceFS patch 0029.
func TestLinkDirectDialerDetectsDeadPeers(t *testing.T) {
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
	conn, err := linkDirectDialer().DialContext(context.Background(), "tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	raw, err := conn.(*net.TCPConn).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name       string
		level, opt int
		want       int
	}{
		{"SO_KEEPALIVE", unix.SOL_SOCKET, unix.SO_KEEPALIVE, 1},
		{"TCP_KEEPIDLE", unix.IPPROTO_TCP, unix.TCP_KEEPIDLE, int(linkKeepAliveIdle.Seconds())},
		{"TCP_KEEPINTVL", unix.IPPROTO_TCP, unix.TCP_KEEPINTVL, int(linkKeepAliveInterval.Seconds())},
		{"TCP_KEEPCNT", unix.IPPROTO_TCP, unix.TCP_KEEPCNT, linkKeepAliveCount},
		{"TCP_USER_TIMEOUT", unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT, int(linkUnackedTimeout.Milliseconds())},
	} {
		var v int
		var gerr error
		if err := raw.Control(func(fd uintptr) { v, gerr = unix.GetsockoptInt(int(fd), c.level, c.opt) }); err != nil || gerr != nil {
			t.Fatal(err, gerr)
		}
		if v != c.want {
			t.Errorf("%s = %d, want %d", c.name, v, c.want)
		}
	}
}
