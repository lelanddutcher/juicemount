package health

// Local-network dial-failure taxonomy (#106).
//
// macOS resets the app's "Local Network" privacy permission on every
// rebuild/re-sign/update. When that permission is denied, every dial to a
// LAN backend fails EHOSTUNREACH ("no route to host") — or occasionally an
// EPERM/EACCES-class error — while loopback keeps working and the network
// is otherwise perfectly fine. The app used to surface that as a generic
// "Redis unreachable" / offline state with zero explanation
// (project_local_network_permission_offline); users had to magically know
// to visit System Settings → Privacy & Security → Local Network.
//
// This file holds:
//
//   - DialErrorClass: the shared dial-error bucketing that bridge/diagnose.go
//     pioneered for the on-demand /diagnose route. It moved here (verbatim
//     semantics, plus the "denied" EACCES/EPERM class) so the periodic health
//     monitor and the /diagnose route share ONE taxonomy instead of two
//     drifting copies. bridge/diagnose.go now aliases these.
//
//   - IsLocalNetPermissionSignature: the conservative classifier the health
//     monitor runs on redis/minio dial failures. It reports the distinct
//     component status MsgLocalNetworkPermission only when the failure
//     pattern matches the permission denial and provably NOT ordinary
//     offline:
//
//       1. the error is EHOSTUNREACH/ENETUNREACH- or EACCES/EPERM-class;
//       2. the target is a PRIVATE (RFC1918 / link-local) IP, not loopback;
//       3. the machine's network is up (at least one non-loopback,
//          non-tunnel interface holds a unicast address — a machine with
//          Wi-Fi off / no cable has none, so genuine offline NEVER
//          classifies as a permission problem);
//       4. the target IP sits on a DIRECTLY-ATTACHED subnet of this machine.
//          macOS's Local Network permission gates same-link (ARP-level)
//          traffic; routed traffic through a gateway is not subject to it.
//          Requiring attachment therefore both matches how the permission
//          actually behaves AND kills the false positive of dialing the
//          NAS's 10.x address from a coffee-shop network (where the default
//          gateway may answer with ICMP host-unreachable).
//
// Known residual ambiguity (report-only, deliberate): a NAS that is powered
// off on the attached subnet also produces EHOSTUNREACH after the ARP
// timeout. The two cases are indistinguishable at the socket layer; the UI
// wording stays actionable for the far-more-common permission case while
// the /diagnose route remains the deeper on-demand tool.

import (
	"context"
	"errors"
	"net"
	"strings"
	"syscall"
)

// Dial-error classes returned by DialErrorClass. String values are frozen:
// bridge/diagnose.go aliases them and its tests assert on them.
const (
	DialClassOK      = ""
	DialClassNoRoute = "no-route"
	DialClassRefused = "refused"
	DialClassDNS     = "dns"
	DialClassTimeout = "timeout"
	// DialClassDenied is the EACCES/EPERM bucket (#106): macOS policy layers
	// (NECP / sandbox) occasionally surface a Local Network denial as
	// "operation not permitted" instead of EHOSTUNREACH.
	DialClassDenied = "denied"
	DialClassOther  = "other"
)

// MsgLocalNetworkPermission is the distinct component-status message the
// health monitor reports when a backend dial failure matches the macOS
// Local Network permission-denial signature (#106). The Swift menu-bar app
// keys on this exact string (ServerController.refreshCacheStatus) to render
// the "Local Network permission needed" remedy row, so treat it as API.
const MsgLocalNetworkPermission = "local-network-permission"

// DialErrorClass buckets a TCP dial error into the coarse classes the
// diagnosis and the health monitor care about. DNS is checked before the
// generic timeout so a resolver timeout reads as a name problem, not a
// route problem. (Moved from bridge/diagnose.go dialErrorClass — same
// semantics, plus the "denied" class.)
func DialErrorClass(err error) string {
	if err == nil {
		return DialClassOK
	}
	if errors.Is(err, syscall.EHOSTUNREACH) || errors.Is(err, syscall.ENETUNREACH) ||
		strings.Contains(err.Error(), "no route to host") {
		return DialClassNoRoute
	}
	if errors.Is(err, syscall.ECONNREFUSED) ||
		strings.Contains(err.Error(), "connection refused") {
		return DialClassRefused
	}
	if errors.Is(err, syscall.EACCES) || errors.Is(err, syscall.EPERM) ||
		strings.Contains(err.Error(), "operation not permitted") ||
		strings.Contains(err.Error(), "permission denied") {
		return DialClassDenied
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return DialClassDNS
	}
	var nerr net.Error
	if (errors.As(err, &nerr) && nerr.Timeout()) || errors.Is(err, context.DeadlineExceeded) {
		return DialClassTimeout
	}
	return DialClassOther
}

// LocalNetSnapshot is a point-in-time view of this machine's usable network
// attachments — the evidence the classifier weighs. Pure data so tests can
// fabricate it (the production source is CurrentLocalNetSnapshot).
type LocalNetSnapshot struct {
	// Up is true when at least one non-loopback, non-tunnel interface is up
	// and holds a unicast address. False == the machine is genuinely
	// offline (Wi-Fi off, no cable) — ordinary offline, never a permission
	// verdict.
	Up bool
	// Networks are the directly-attached subnets (interface address +
	// mask) of those interfaces.
	Networks []*net.IPNet
}

// localNetSnapshotFn indirection so monitor tests can inject a fabricated
// snapshot; production never reassigns it.
var localNetSnapshotFn = CurrentLocalNetSnapshot

// CurrentLocalNetSnapshot enumerates the kernel's interface table. Cheap
// (one sysctl walk, no traffic, no exec) and called only on the backend-
// dial FAILURE path — never on a healthy tick, never on an NFS RPC path.
func CurrentLocalNetSnapshot() LocalNetSnapshot {
	var snap LocalNetSnapshot
	ifaces, err := net.Interfaces()
	if err != nil {
		return snap // Up=false → conservative: classifier stays silent
	}
	for _, ifc := range ifaces {
		if ifc.Flags&net.FlagUp == 0 || ifc.Flags&net.FlagLoopback != 0 {
			continue
		}
		// Tunnels (utunN = Tailscale/VPN) don't prove LAN liveness and
		// their networks aren't ARP-level attachments the Local Network
		// permission governs.
		if strings.HasPrefix(ifc.Name, "utun") || strings.HasPrefix(ifc.Name, "tailscale") {
			continue
		}
		addrs, err := ifc.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			ipn, ok := a.(*net.IPNet)
			if !ok || ipn.IP == nil || ipn.IP.IsUnspecified() {
				continue
			}
			// IPv6 link-local (fe80::) exists on every up interface even
			// with no real network attached — not evidence of connectivity.
			// IPv4 link-local (169.254/16) IS kept: direct-attached 10GbE
			// NAS links without DHCP self-assign exactly that.
			if ipn.IP.To4() == nil && ipn.IP.IsLinkLocalUnicast() {
				continue
			}
			snap.Up = true
			snap.Networks = append(snap.Networks, ipn)
		}
	}
	return snap
}

// IsLocalNetPermissionSignature reports whether a backend dial failure
// matches the macOS Local Network permission-denial signature (#106). Pure
// with respect to snap — unit-testable without a network. Conservative by
// construction: any condition it cannot positively verify (hostname target,
// unreadable interface table, machine offline, target not attached) makes
// it answer false, leaving the generic unreachable report in place.
func IsLocalNetPermissionSignature(dialErr error, targetAddr string, snap LocalNetSnapshot) bool {
	switch DialErrorClass(dialErr) {
	case DialClassNoRoute, DialClassDenied:
		// candidate signature classes
	default:
		return false // refused/dns/timeout/other are NOT permission denials
	}
	host := targetAddr
	if h, _, err := net.SplitHostPort(targetAddr); err == nil {
		host = h
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false // hostname — cannot verify privateness; stay generic
	}
	if ip.IsLoopback() {
		return false // loopback is exempt from the Local Network permission
	}
	if !ip.IsPrivate() && !ip.IsLinkLocalUnicast() {
		return false // public target — not a Local Network matter
	}
	if !snap.Up {
		return false // genuinely offline (no Wi-Fi / no cable) — ordinary offline
	}
	for _, n := range snap.Networks {
		if n != nil && n.Contains(ip) {
			return true // attached-subnet private target: the signature
		}
	}
	return false // private but routed (not on-link) — permission doesn't apply
}
