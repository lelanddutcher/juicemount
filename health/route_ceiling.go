package health

import (
	"errors"
	"strings"

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
// original backend address resolves to the physical interface. Through Link
// the class therefore comes from measurement alone, in both processes alike.

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
