package nfs

import (
	"net"
	"time"
)

// Dead-peer detection for the Link proxy's direct-LAN upstream (the same
// values as bundled JuiceFS patch 0029, pkg/object/jm_dead_peer.go).
//
// Through Link, JuiceFS reaches the NAS through a loopback proxy, so the
// patch's socket options land only on 127.0.0.1 sockets. The connection
// that dies when the Mac leaves the LAN is the proxy's own upstream: on the
// LAN it is a plain kernel TCP connection to the NAS (AdaptiveDialContext's
// direct path), which had a 30 s keepalive and no limit on unacknowledged
// data. The encrypted Link path runs inside the userspace node, where the
// WireGuard underlay follows the Mac to its new network by itself.
const (
	linkKeepAliveIdle     = 10 * time.Second
	linkKeepAliveInterval = 5 * time.Second
	linkKeepAliveCount    = 3
	linkUnackedTimeout    = 25 * time.Second
)

// linkDirectDialer dials the NAS directly on the LAN for the Link proxy.
func linkDirectDialer() *net.Dialer {
	return &net.Dialer{
		Timeout: 750 * time.Millisecond,
		KeepAliveConfig: net.KeepAliveConfig{
			Enable:   true,
			Idle:     linkKeepAliveIdle,
			Interval: linkKeepAliveInterval,
			Count:    linkKeepAliveCount,
		},
		Control: linkSetUnackedTimeout,
	}
}
