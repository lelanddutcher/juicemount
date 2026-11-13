/*
 * JuiceMount local source (not upstream JuiceFS). Apache-2.0.
 */

package object

import (
	"net"
	"time"
)

// Dead-peer detection for object-store connections (patch 0028).
//
// A Mac that changes networks (Wi-Fi to another Wi-Fi, to a hotspot, out of
// a VPN) keeps its open TCP connections, but their local address or path is
// gone: nothing sent on them is ever acknowledged and nothing arrives. The
// HTTP transport keeps idle connections for minutes and reuses them, and a
// GET that was streaming a block when the path died waits for more body
// bytes. Without help each such request waits for its whole get/put
// deadline (2 minutes on a slow link), so a playing file freezes for that
// long after the network is back (remote lab, 2026-11-13: 120 s).
//
// Two kernel timers end such a connection, so the request fails and is
// retried on a new connection over the new path:
//
//   - keepalive: a connection that receives nothing for jmKeepAliveIdle is
//     probed every jmKeepAliveInterval; jmKeepAliveCount unanswered probes
//     close it. This covers a request waiting for a reply or for body bytes
//     (no data of ours in flight) and idle pooled connections. A slow but
//     live server answers probes from its kernel, so a long server-side
//     wait (a disk spinning up) is not affected.
//   - unacknowledged data: data we sent that is not acknowledged within
//     jmUnackedTimeout closes the connection (TCP_USER_TIMEOUT on Linux,
//     TCP_RXT_CONNDROPTIME on macOS). This covers a request or upload sent
//     on a dead connection, where keepalive does not run. Any live path
//     acknowledges within seconds, even a congested cellular uplink.
//
// The probe traffic is one small packet pair per idle connection every
// jmKeepAliveIdle.
const (
	jmKeepAliveIdle     = 10 * time.Second
	jmKeepAliveInterval = 5 * time.Second
	jmKeepAliveCount    = 3
	jmUnackedTimeout    = 25 * time.Second
)

// jmObjectDialer is the dialer of the shared object-store HTTP client.
func jmObjectDialer() *net.Dialer {
	return &net.Dialer{
		Timeout: 10 * time.Second,
		KeepAliveConfig: net.KeepAliveConfig{
			Enable:   true,
			Idle:     jmKeepAliveIdle,
			Interval: jmKeepAliveInterval,
			Count:    jmKeepAliveCount,
		},
		Control: jmSetUnackedTimeout,
	}
}
