package health

import (
	"errors"
	"strings"
	"sync"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

// The tunnel ceiling: a backend reached through a tunnel is at best slow.
//
// A nearby tunnel endpoint can answer in 2 ms while the physical underlay is
// a phone hotspot, and Network.framework does not reliably mark every VPN as
// expensive. So when the kernel routes the backend through a tunnel
// interface, the automatic link class is capped at slow. A measured metered
// class stays metered, and an explicit force still wins (netprofile).
//
// Two processes choose a class from this rule: the NFS-serving process (its
// read rules and the live juicefs link follower) and the volume owner that
// mounts juicefs (the mount flags). Both call ApplyBackendRouteCeiling, so
// they cannot disagree about the same route. Before this was shared, only
// the serving process applied it: a mount made at home through WireGuard got
// lan flags (no --demand-first, buffer 4096 MiB) while the serving process
// said slow (laptop run 2026-11-13, B1).
//
// What counts as a tunnel:
//   - utunN: every macOS VPN, including WireGuard, the Tailscale app and
//     other "existing private network" clients;
//   - tailscale0: the Linux Tailscale interface.
//
// The embedded JuiceMount Link node is a userspace tunnel. Its traffic goes
// through a loopback proxy, never through a utun, so the route to the
// original backend address resolves to the physical interface, and neither
// process applies the ceiling. Both measure the link instead: the serving
// process takes the owner's Link proofs (startup and routine probes), and
// every owner mount takes one round trip through the Link node
// (FUSEConfig.LinkRTT), because its TCP dial to the loopback proxy says
// nothing. Before that, a mount whose profile held no sample (after an
// offline boot, or after a network-cost change reset it) got the medium
// --buffer-size/--prefetch abroad for its life (PR #90 review MEDIUM-3).
//
// A failed route lookup is handled the same way in both processes
// (RouteCeiling): the ceiling of the last successful lookup stays, and
// before any success the class is capped at slow. The serving process used
// to take the NetWatcher's default-route guess instead, so the two could
// disagree when DNS for a hostname target failed.

// BackendRouteTunneled reports whether iface, the interface the kernel
// routes the backend through, is a tunnel.
func BackendRouteTunneled(iface string) bool {
	return strings.HasPrefix(iface, "utun") || iface == "tailscale0"
}

// ApplyBackendRouteCeiling caps p at slow when iface is a tunnel and lifts
// the cap otherwise. It returns whether the cap is now set.
func ApplyBackendRouteCeiling(p *netprofile.Profile, iface string) bool {
	tunneled := BackendRouteTunneled(iface)
	if tunneled {
		ceiling := netprofile.ClassSlow
		p.CeilingClass(&ceiling)
	} else {
		p.CeilingClass(nil)
	}
	return tunneled
}

// ResolveBackendRoute asks resolver for the interface that carries target
// (host:port of the original backend, before any Link or debug rewrite).
// The production resolver is ResolveRouteInterface: a routing lookup that
// sends no packet.
func ResolveBackendRoute(target string, resolver func(string) (string, error)) (string, error) {
	if target == "" || resolver == nil {
		return "", errors.New("backend route target unavailable")
	}
	iface, err := resolver(target)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(iface) == "" {
		return "", errors.New("backend route interface unavailable")
	}
	return iface, nil
}

// RouteCeiling applies route lookup results to a profile with the shared
// fallback for a failed lookup. One RouteCeiling goes with one profile;
// DefaultRouteCeiling goes with netprofile.Default().
type RouteCeiling struct {
	mu       sync.Mutex
	decided  bool // a lookup has succeeded
	tunneled bool // the last successful lookup's answer
}

var defaultRouteCeiling RouteCeiling

// DefaultRouteCeiling is the RouteCeiling of netprofile.Default(), used by
// both the serving process and the mount.
func DefaultRouteCeiling() *RouteCeiling { return &defaultRouteCeiling }

// Apply applies one lookup result to p and returns whether the ceiling is
// set afterwards and whether it came from this lookup. A successful lookup
// (err nil, iface not empty) sets or lifts the cap by the interface. A
// failed one keeps the last successful lookup's ceiling, or caps at slow
// when no lookup has succeeded yet.
func (c *RouteCeiling) Apply(p *netprofile.Profile, iface string, err error) (tunneled, looked bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err == nil && strings.TrimSpace(iface) != "" {
		c.tunneled, c.decided = ApplyBackendRouteCeiling(p, iface), true
		return c.tunneled, true
	}
	if c.decided {
		return c.tunneled, false
	}
	ceiling := netprofile.ClassSlow
	p.CeilingClass(&ceiling)
	return true, false
}
