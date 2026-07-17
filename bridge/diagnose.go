package main

// "Why is it slow?" self-diagnosis (INSTANT-NAV #14).
//
// GET /diagnose runs the six known silent-failure probes ON DEMAND (no
// periodic goroutine — the user asks, we answer) and returns a compact
// verdict the menu bar can render:
//
//	{
//	  "overall": "ok" | "degraded" | "broken",
//	  "checks": [
//	    { "id": "local-network", "title": "Backend reachability",
//	      "status": "ok" | "warn" | "fail",
//	      "detail": "…", "remedy": "…", "elapsed_ms": 3 },
//	    …
//	  ]
//	}
//
// The six checks, each mapping to a field-observed "it's slow/broken but
// nothing says why" class:
//
//	local-network       macOS Local Network permission silently denied after a
//	                    rebuild/re-sign/update → backend dials fail
//	                    EHOSTUNREACH ("no route to host") while loopback still
//	                    works, so the app looks "up but offline"
//	                    (project_local_network_permission_offline).
//	route-tunnel        NAS traffic riding a utun (Tailscale/VPN) instead of
//	                    the direct interface → WAN-like cold reads on a LAN
//	                    machine (project_tailscale_routing_gotcha).
//	fuse-mount          FUSE mountpoint absent/plain-dir/wedged — reuses the
//	                    pin package's identity gate verbatim (V2.3 G0); a bad
//	                    verdict here means drains are parked fail-closed.
//	backend-components  redis/minio/nfs down per the health monitor's cached
//	                    component status.
//	spool-backlog       drain backlog / stalled / failed rows / offline-full
//	                    from the live spool status (same source as /spool).
//	link-rtt            bounded TCP connect RTT to the backend → link class
//	                    ("navigation is instant off the local index; cold file
//	                    reads pay this RTT").
//
// Every check is time-bounded (diagnosePerCheckBudget) and they run
// concurrently under one overall budget (diagnoseOverallBudget), so the
// endpoint answers in ~worst-single-check time, never hangs the popover.
// Classification lives in small pure functions so the error-taxonomy logic
// is unit-testable without a network (diagnose_test.go).

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
	"github.com/lelanddutcher/juicemount/metadata"
	jmnfs "github.com/lelanddutcher/juicemount/nfs"
)

// Budgets. Vars (not consts) so the harness test can shrink them; production
// never mutates them.
var (
	// diagnoseOverallBudget caps the whole /diagnose request.
	diagnoseOverallBudget = 5 * time.Second
	// diagnosePerCheckBudget caps any single check; a check that doesn't
	// answer in time is reported as "did not finish" rather than blocking
	// the report (a wedged statfs or a black-holed dial must not hang the
	// popover that's trying to explain the wedge).
	diagnosePerCheckBudget = 3 * time.Second
	// diagnoseDialTimeout bounds the backend TCP connect inside the check
	// budget so a silent packet drop classifies as "timeout" instead of
	// tripping the harness cutoff.
	diagnoseDialTimeout = 2500 * time.Millisecond
	// diagnoseLoopbackDialTimeout bounds the loopback-works guard dial —
	// loopback answers in microseconds or something is deeply wrong.
	diagnoseLoopbackDialTimeout = 750 * time.Millisecond
)

// RTT classification thresholds for the link-rtt check (TCP connect time).
const (
	diagRTTLAN  = 3 * time.Millisecond  // wired LAN / same switch
	diagRTTFast = 15 * time.Millisecond // good WiFi / nearby WAN
	diagRTTWAN  = 50 * time.Millisecond // metro WAN; cold reads clearly feel it
)

// diagSpoolBacklogAge is how old the oldest pending spool row may be before
// a non-empty queue reads as "backlog" (drain slower than ingest) rather
// than "normal drain in progress".
const diagSpoolBacklogAge = 15 * time.Minute

// Check ids — stable strings the Swift side may key off.
const (
	diagIDLocalNetwork = "local-network"
	diagIDRouteTunnel  = "route-tunnel"
	diagIDFUSEMount    = "fuse-mount"
	diagIDComponents   = "backend-components"
	diagIDSpoolBacklog = "spool-backlog"
	diagIDLinkRTT      = "link-rtt"
)

// Statuses.
const (
	diagStatusOK   = "ok"
	diagStatusWarn = "warn"
	diagStatusFail = "fail"
)

// diagnoseCheck is one probe's verdict. Shape mirrored by Swift's
// NFSBridge.DiagnoseCheck — keep the JSON keys stable.
type diagnoseCheck struct {
	ID     string `json:"id"`
	Title  string `json:"title"`
	Status string `json:"status"` // ok | warn | fail
	Detail string `json:"detail"`
	Remedy string `json:"remedy"`
	// ElapsedMs is additive observability (how long the probe took) —
	// handy over curl when a check rides its budget.
	ElapsedMs int64 `json:"elapsed_ms"`
}

// diagnoseReport is the full /diagnose body.
type diagnoseReport struct {
	Overall string          `json:"overall"` // ok | degraded | broken
	Checks  []diagnoseCheck `json:"checks"`
}

// diagnoseDeps carries the one impure side effect (exec'ing route(8)) behind
// an injectable seam, mirroring the mountNowDeps idiom, so the route check is
// testable without shelling out.
var diagnoseDeps = struct {
	routeGet func(ctx context.Context, host string) (string, error)
}{
	routeGet: func(ctx context.Context, host string) (string, error) {
		// Absolute path: the c-archive runs inside the .app with a minimal
		// PATH that may not include /sbin. -n suppresses reverse-DNS of the
		// output (forward resolution of a hostname target still happens).
		out, err := exec.CommandContext(ctx, "/sbin/route", "-n", "get", host).CombinedOutput()
		return string(out), err
	},
}

// ----------------------------------------------------------------------------
// Pure classification (unit-tested in diagnose_test.go)
// ----------------------------------------------------------------------------

// Dial-error classes returned by dialErrorClass. The taxonomy moved to the
// health package (#106) so the periodic health monitor's local-network-
// permission classifier and this on-demand check share ONE bucketing
// instead of two drifting copies; these aliases keep every existing use
// (and test) in place. health adds a "denied" (EACCES/EPERM) class, which
// the switch in classifyBackendDial folds into its default arm — same
// verdict those errors always got here.
const (
	dialClassOK      = health.DialClassOK
	dialClassNoRoute = health.DialClassNoRoute
	dialClassRefused = health.DialClassRefused
	dialClassDNS     = health.DialClassDNS
	dialClassTimeout = health.DialClassTimeout
	dialClassOther   = health.DialClassOther
)

// dialErrorClass buckets a TCP dial error into the coarse classes the
// diagnosis cares about. Delegates to the shared health.DialErrorClass —
// see the note on the class aliases above.
func dialErrorClass(err error) string {
	return health.DialErrorClass(err)
}

// classifyBackendDial turns the backend dial outcome into the local-network
// check verdict. loopbackOK is the guard that separates "macOS is denying
// this app Local Network access" (loopback exempt → still works) from "the
// whole network stack is down".
func classifyBackendDial(addr string, dialErr error, loopbackOK bool) diagnoseCheck {
	c := diagnoseCheck{ID: diagIDLocalNetwork, Title: "Backend reachability"}
	if addr == "" {
		c.Status = diagStatusWarn
		c.Detail = "backend address unknown — the server has not started with a backend URL yet"
		c.Remedy = "Start JuiceMount (or check the Redis URL in Preferences), then diagnose again."
		return c
	}
	switch dialErrorClass(dialErr) {
	case dialClassOK:
		c.Status = diagStatusOK
		c.Detail = fmt.Sprintf("backend %s answers TCP", addr)
	case dialClassNoRoute:
		c.Status = diagStatusFail
		if loopbackOK {
			// The project_local_network_permission_offline signature: after a
			// rebuild/re-sign/update, macOS Local Network privacy resets and
			// every LAN dial fails EHOSTUNREACH while loopback (exempt) keeps
			// working — the app looks "up but offline" with a healthy network.
			c.Detail = fmt.Sprintf("dialing %s fails with \"no route to host\" while loopback works — the signature of macOS denying JuiceMount Local Network access (the permission silently resets after an app update or re-install)", addr)
			c.Remedy = "Open System Settings > Privacy & Security > Local Network, enable JuiceMount, then quit and relaunch the app. If it is already enabled, toggle it off and on, then relaunch."
		} else {
			c.Detail = fmt.Sprintf("dialing %s fails with \"no route to host\" and loopback is failing too — the network stack itself looks down", addr)
			c.Remedy = "Check this Mac's network connection (Wi-Fi/Ethernet), then diagnose again."
		}
	case dialClassRefused:
		c.Status = diagStatusFail
		c.Detail = fmt.Sprintf("host answers but %s refused the connection — the backend service (redis) is stopped, restarting, or on a different port", addr)
		c.Remedy = "Check the NAS: the JuiceMount backend containers must be running. Verify the Redis URL/port in Preferences."
	case dialClassDNS:
		c.Status = diagStatusFail
		c.Detail = fmt.Sprintf("the backend hostname in %s does not resolve", addr)
		c.Remedy = "Check the backend URL in Preferences (hostname spelling, VPN/Tailscale MagicDNS state)."
	case dialClassTimeout:
		c.Status = diagStatusFail
		c.Detail = fmt.Sprintf("dialing %s timed out after %s — the host is silently dropping packets (NAS asleep or off, firewall, or wrong IP)", addr, diagnoseDialTimeout)
		c.Remedy = "Verify the NAS is powered on and reachable (ping it), and that this Mac is on the right network."
	default:
		c.Status = diagStatusFail
		c.Detail = fmt.Sprintf("dialing %s failed: %v", addr, dialErr)
		c.Remedy = "Check the network path to the NAS, then diagnose again."
	}
	return c
}

// parseRouteInterface extracts the "interface:" value from `route -n get`
// output. Returns "" when the field is absent (unparseable / error text).
func parseRouteInterface(out string) string {
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if rest, ok := strings.CutPrefix(line, "interface:"); ok {
			return strings.TrimSpace(rest)
		}
	}
	return ""
}

// isTunnelInterface reports whether an interface name is a tunnel (macOS
// utunN — Tailscale/VPN — or Linux-style tailscaleN, for the CLI build).
func isTunnelInterface(iface string) bool {
	return strings.HasPrefix(iface, "utun") || strings.HasPrefix(iface, "tailscale")
}

// classifyRouteInterface turns the resolved route interface into the
// route-tunnel check verdict (project_tailscale_routing_gotcha: a NAS route
// via utunN on a LAN machine turns 10GbE into WAN).
func classifyRouteInterface(host, iface string, execErr error) diagnoseCheck {
	c := diagnoseCheck{ID: diagIDRouteTunnel, Title: "Network route"}
	switch {
	case execErr != nil:
		c.Status = diagStatusWarn
		c.Detail = fmt.Sprintf("could not determine the route to %s (route -n get failed: %v)", host, execErr)
		c.Remedy = fmt.Sprintf("Run `route -n get %s` in Terminal and check the interface line.", host)
	case iface == "":
		c.Status = diagStatusWarn
		c.Detail = fmt.Sprintf("route lookup for %s returned no interface", host)
		c.Remedy = fmt.Sprintf("Run `route -n get %s` in Terminal and check the interface line.", host)
	case iface == "lo0":
		c.Status = diagStatusOK
		c.Detail = "backend is local (loopback route)"
	case isTunnelInterface(iface):
		c.Status = diagStatusWarn
		c.Detail = fmt.Sprintf("traffic to %s is tunneling via %s (Tailscale/VPN), not a direct interface", host, iface)
		c.Remedy = "Expected when away from the NAS's LAN. On the LAN this makes a fast link feel like WAN: check `route get " + host + "` and your Tailscale settings (exit node / subnet routes) so NAS traffic uses the direct interface."
	default:
		c.Status = diagStatusOK
		c.Detail = fmt.Sprintf("direct interface %s carries backend traffic", iface)
	}
	return c
}

// classifyFUSE turns the pin package's FUSE identity verdict into the
// fuse-mount check. The gate is the single authority for "is a real
// filesystem mounted here" (see internal/cache/pin/fuseidentity.go) — this
// only maps its answer to a user-facing remedy.
func classifyFUSE(ok bool, reason string) diagnoseCheck {
	c := diagnoseCheck{ID: diagIDFUSEMount, Title: "FUSE mount"}
	if ok {
		c.Status = diagStatusOK
		switch {
		case strings.Contains(reason, "unconfigured"):
			c.Detail = "FUSE identity gate unconfigured (no FUSE mount managed by this process) — check skipped"
		case strings.Contains(reason, "disabled"):
			c.Detail = "FUSE identity gate disabled (" + reason + ") — check skipped"
		default:
			c.Detail = "a real filesystem is mounted at the FUSE mountpoint"
		}
		return c
	}
	c.Status = diagStatusFail
	c.Detail = reason
	if strings.Contains(reason, "wedged") || strings.Contains(reason, "timed out") {
		c.Remedy = "The FUSE mount is wedged. Use \"Force Eject Mount\" in the menu, then Stop everything and Start. In-flight writes are parked safely until the mount is real again."
	} else {
		c.Remedy = "The mountpoint has no filesystem (macFUSE not loaded or the mount is gone). Approve macFUSE under System Settings > Privacy & Security if prompted, then Stop everything and Start. Writes are parked — nothing is uploading to the wrong place."
	}
	return c
}

// classifyComponents summarizes the health monitor's cached component
// snapshot (redis / minio / fuse probe / nfs) for the backend-components
// check.
func classifyComponents(st health.HealthStatus) diagnoseCheck {
	c := diagnoseCheck{ID: diagIDComponents, Title: "Backend services"}
	type comp struct {
		name string
		s    health.ComponentStatus
	}
	comps := []comp{
		{"redis", st.Redis},
		{"minio", st.MinIO},
		{"fuse", st.FUSE},
		{"nfs", st.NFS},
	}
	var down []string
	for _, cp := range comps {
		if !cp.s.Healthy {
			msg := cp.s.Message
			if msg == "" {
				msg = "unhealthy"
			}
			down = append(down, cp.name+": "+msg)
		}
	}
	if len(down) == 0 {
		c.Status = diagStatusOK
		c.Detail = "redis, minio, fuse and nfs all report healthy"
		return c
	}
	c.Status = diagStatusFail
	c.Detail = strings.Join(down, "; ")
	c.Remedy = "Check the NAS/backend (containers running? network path?). If the backend is fine, Stop everything and Start."
	return c
}

// classifyLinkRTT maps a measured TCP connect RTT to a link class. Cold file
// reads pay this RTT per block fetch; navigation does not (it's served from
// the local metadata mirror), so the detail says which experiences are
// affected instead of a bare number.
func classifyLinkRTT(rtt time.Duration, dialErr error, snap netprofile.Snapshot) diagnoseCheck {
	c := diagnoseCheck{ID: diagIDLinkRTT, Title: "Link latency"}
	if dialErr != nil {
		c.Status = diagStatusWarn
		c.Detail = "could not measure — the backend dial failed (see Backend reachability)"
		return c
	}
	ms := float64(rtt.Microseconds()) / 1000.0
	switch {
	case rtt <= diagRTTLAN:
		c.Status = diagStatusOK
		c.Detail = fmt.Sprintf("%.1f ms to the backend — LAN-class link", ms)
	case rtt <= diagRTTFast:
		c.Status = diagStatusOK
		c.Detail = fmt.Sprintf("%.1f ms to the backend — fast link", ms)
	case rtt <= diagRTTWAN:
		c.Status = diagStatusWarn
		c.Detail = fmt.Sprintf("%.1f ms to the backend — WAN-class link; browsing stays instant (local index) but cold file reads will feel it", ms)
		c.Remedy = "Expected away from the NAS. On its LAN, check the route (see Network route) and Wi-Fi vs Ethernet."
	default:
		c.Status = diagStatusWarn
		c.Detail = fmt.Sprintf("%.1f ms to the backend — high-latency link (cellular/relay-class); cold reads and uploads will be slow", ms)
		c.Remedy = "Expected on cellular/remote links. Pin what you need in advance so it reads from the local cache."
	}
	if snap.HaveBW {
		c.Detail += fmt.Sprintf(" (smoothed link class: %s, %.1f MB/s)", snap.Class, snap.BytesPerSec/(1024*1024))
	} else if snap.HaveRTT {
		c.Detail += fmt.Sprintf(" (smoothed link class: %s)", snap.Class)
	}
	return c
}

// classifySpool turns the live spool status into the spool-backlog check.
// Mirrors the thresholds the popover's pending-uploads section implies: full
// offline buffer is a hard stop; stalled/failed rows have one-click recovery
// actions; an old-but-draining backlog is a warn; a fresh queue is normal.
func classifySpool(st jmnfs.SpoolStatusResponse, err error) diagnoseCheck {
	c := diagnoseCheck{ID: diagIDSpoolBacklog, Title: "Upload queue"}
	gb := func(b int64) string { return fmt.Sprintf("%.1f GB", float64(b)/(1<<30)) }
	switch {
	case err != nil:
		c.Status = diagStatusWarn
		c.Detail = fmt.Sprintf("spool status unavailable: %v", err)
		c.Remedy = "Stop everything and Start; if this repeats, export diagnostics and file an issue."
	case !st.Enabled:
		c.Status = diagStatusOK
		c.Detail = "write spool not enabled — writes go straight through"
	case st.OfflineBufferFull:
		c.Status = diagStatusFail
		c.Detail = fmt.Sprintf("offline buffer full — %d cop%s paused until the backend is reachable", st.StallWaiters, pluralIes(st.StallWaiters))
		c.Remedy = "Reconnect to the backend (or go back online); paused copies resume automatically."
	case st.StalledFiles > 0:
		c.Status = diagStatusWarn
		c.Detail = fmt.Sprintf("%d upload%s stalled (no progress without intervention)", st.StalledFiles, plural(st.StalledFiles))
		c.Remedy = "Open the popover's Pending uploads section and click \"Recover stalled\"."
	case st.FailedFiles > 0:
		c.Status = diagStatusWarn
		c.Detail = fmt.Sprintf("%d upload%s failed", st.FailedFiles, plural(st.FailedFiles))
		c.Remedy = "Open the popover's Pending uploads section and click \"Retry failed\"."
	case st.PendingFiles > 0 && st.Offline:
		c.Status = diagStatusWarn
		c.Detail = fmt.Sprintf("%d file%s (%s) queued while offline — they drain on reconnect", st.PendingFiles, plural(st.PendingFiles), gb(st.PendingBytes))
		c.Remedy = "Go back online (or restore the backend link) to drain the queue."
	case st.PendingFiles > 0 && time.Duration(st.OldestPendingAgeSec)*time.Second > diagSpoolBacklogAge:
		c.Status = diagStatusWarn
		c.Detail = fmt.Sprintf("upload backlog: %d file%s (%s) queued, oldest %s — draining slower than data is arriving", st.PendingFiles, plural(st.PendingFiles), gb(st.PendingBytes), (time.Duration(st.OldestPendingAgeSec) * time.Second).Round(time.Minute))
		c.Remedy = "Leave the app running so the queue drains; check Link latency / Network route if this persists on a fast network."
	case st.PendingFiles > 0:
		c.Status = diagStatusOK
		c.Detail = fmt.Sprintf("uploading %d file%s (%s) — normal drain in progress", st.PendingFiles, plural(st.PendingFiles), gb(st.PendingBytes))
	default:
		c.Status = diagStatusOK
		c.Detail = "no upload backlog"
	}
	return c
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

func pluralIes(n int) string {
	if n == 1 {
		return "y"
	}
	return "ies"
}

// diagnoseOverall folds check statuses into the report verdict: any fail →
// "broken", else any warn → "degraded", else "ok".
func diagnoseOverall(checks []diagnoseCheck) string {
	overall := "ok"
	for _, c := range checks {
		switch c.Status {
		case diagStatusFail:
			return "broken"
		case diagStatusWarn:
			overall = "degraded"
		}
	}
	return overall
}

// ----------------------------------------------------------------------------
// Bounded probes (impure, but each self-bounded)
// ----------------------------------------------------------------------------

// diagnoseDial is the shared bounded TCP dial. Returns the connect duration
// on success. DNS resolution happens OUTSIDE the timed window (same review
// fix as backendReachableRTT) so a cold resolver doesn't inflate the RTT.
func diagnoseDial(ctx context.Context, addr string, timeout time.Duration) (time.Duration, error) {
	resolved, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return 0, err
	}
	d := net.Dialer{Timeout: timeout}
	t0 := time.Now()
	conn, err := d.DialContext(ctx, "tcp", resolved.String())
	if err != nil {
		return 0, err
	}
	rtt := time.Since(t0)
	_ = conn.Close()
	return rtt, nil
}

// diagnoseBackendCheck runs the local-network probe: bounded dial to the
// backend; on failure, a loopback guard dial decides whether the failure
// pattern matches the Local Network permission signature.
func diagnoseBackendCheck(ctx context.Context, backendAddr, loopbackAddr string) diagnoseCheck {
	if backendAddr == "" {
		return classifyBackendDial("", nil, true)
	}
	_, dialErr := diagnoseDial(ctx, backendAddr, diagnoseDialTimeout)
	loopbackOK := true
	if dialErr != nil && loopbackAddr != "" {
		_, lErr := diagnoseDial(ctx, loopbackAddr, diagnoseLoopbackDialTimeout)
		loopbackOK = lErr == nil
	}
	return classifyBackendDial(backendAddr, dialErr, loopbackOK)
}

// diagnoseRouteCheck resolves which interface the kernel routes to the
// backend host via a bounded `route -n get`, then classifies tunnels.
func diagnoseRouteCheck(ctx context.Context, backendAddr string) diagnoseCheck {
	host := diagnoseHostOnly(backendAddr)
	if host == "" {
		return diagnoseCheck{
			ID: diagIDRouteTunnel, Title: "Network route",
			Status: diagStatusWarn,
			Detail: "backend address unknown — cannot inspect the route",
			Remedy: "Start JuiceMount with a backend configured, then diagnose again.",
		}
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return classifyRouteInterface(host, "lo0", nil)
	}
	out, err := diagnoseDeps.routeGet(ctx, host)
	return classifyRouteInterface(host, parseRouteInterface(out), err)
}

// diagnoseLinkRTTCheck measures one bounded TCP connect to the backend and
// classifies the latency. A successful sample is folded into netprofile
// (same as the boot probe) so the adaptive readahead sees it too.
func diagnoseLinkRTTCheck(ctx context.Context, backendAddr string) diagnoseCheck {
	if backendAddr == "" {
		return diagnoseCheck{
			ID: diagIDLinkRTT, Title: "Link latency",
			Status: diagStatusWarn,
			Detail: "backend address unknown — cannot measure",
		}
	}
	rtt, err := diagnoseDial(ctx, backendAddr, diagnoseDialTimeout)
	if err == nil {
		netprofile.Default().ObserveRTT(rtt)
	}
	return classifyLinkRTT(rtt, err, netprofile.Default().Snapshot())
}

// backendAddrFromRedisURL resolves the configured Redis URL to the backend
// "host:port" every network probe dials. Same parse as the boot probe
// (backendReachableRTT); false when the URL is empty or malformed.
func backendAddrFromRedisURL(redisURL string) (string, bool) {
	addr, _, err := metadata.ParseRedisURL(redisURL)
	if err != nil || addr == "" {
		return "", false
	}
	return addr, true
}

// diagnoseHostOnly strips the port from "host:port"; a bare host passes
// through unchanged.
func diagnoseHostOnly(addr string) string {
	if addr == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	return addr
}

// ----------------------------------------------------------------------------
// Harness + handler
// ----------------------------------------------------------------------------

// diagnoseRunner pairs a check's identity with its bounded probe.
type diagnoseRunner struct {
	id, title string
	run       func(ctx context.Context) diagnoseCheck
}

// runDiagnoseChecks executes every runner concurrently, each under
// diagnosePerCheckBudget (within the caller's overall ctx). A check that
// doesn't answer in time is reported as a warn — the report must always
// come back even when a probe is stuck against a wedged mount or a
// black-holed link. The straggler goroutine is abandoned (same accepted
// pattern as pin.checkFUSEIdentity); every probe it can be stuck in is
// itself bounded or single-flighted.
func runDiagnoseChecks(ctx context.Context, runners []diagnoseRunner) []diagnoseCheck {
	results := make([]diagnoseCheck, len(runners))
	var wg sync.WaitGroup
	for i, r := range runners {
		wg.Add(1)
		go func(i int, r diagnoseRunner) {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, diagnosePerCheckBudget)
			defer cancel()
			start := time.Now()
			done := make(chan diagnoseCheck, 1)
			go func() {
				defer func() {
					if p := recover(); p != nil {
						done <- diagnoseCheck{
							Status: diagStatusWarn,
							Detail: fmt.Sprintf("check panicked: %v", p),
						}
					}
				}()
				done <- r.run(cctx)
			}()
			var c diagnoseCheck
			select {
			case c = <-done:
			case <-cctx.Done():
				c = diagnoseCheck{
					Status: diagStatusWarn,
					Detail: fmt.Sprintf("check did not finish within %s", diagnosePerCheckBudget),
				}
			}
			c.ID, c.Title = r.id, r.title
			c.ElapsedMs = time.Since(start).Milliseconds()
			results[i] = c
		}(i, r)
	}
	wg.Wait()
	return results
}

// diagnoseInputs is the snapshot of process state the checks need — grabbed
// under globalMu once, then used lock-free.
type diagnoseInputs struct {
	backendAddr  string // metadata backend "host:port" ("" when unknown)
	loopbackAddr string // our own control plane, for the loopback-works guard
	monitor      *health.HealthMonitor
	spool        *jmnfs.SpoolStore
	drainer      *jmnfs.Drainer
}

// runDiagnosis assembles and runs the six checks, then folds the overall
// verdict.
func runDiagnosis(ctx context.Context, in diagnoseInputs) diagnoseReport {
	runners := []diagnoseRunner{
		{diagIDLocalNetwork, "Backend reachability", func(ctx context.Context) diagnoseCheck {
			return diagnoseBackendCheck(ctx, in.backendAddr, in.loopbackAddr)
		}},
		{diagIDRouteTunnel, "Network route", func(ctx context.Context) diagnoseCheck {
			return diagnoseRouteCheck(ctx, in.backendAddr)
		}},
		{diagIDFUSEMount, "FUSE mount", func(_ context.Context) diagnoseCheck {
			// FUSEIdentityState is internally bounded + single-flighted
			// (fuseIdentityStatfsTimeout); reused verbatim, never reimplemented.
			ok, reason := pin.FUSEIdentityState()
			return classifyFUSE(ok, reason)
		}},
		{diagIDComponents, "Backend services", func(_ context.Context) diagnoseCheck {
			if in.monitor == nil {
				return diagnoseCheck{
					Status: diagStatusWarn,
					Detail: "health monitor not running — the server is stopped or still starting",
					Remedy: "Start JuiceMount from the menu bar, then diagnose again.",
				}
			}
			return classifyComponents(in.monitor.Status())
		}},
		{diagIDSpoolBacklog, "Upload queue", func(_ context.Context) diagnoseCheck {
			if in.spool == nil {
				return classifySpool(jmnfs.SpoolStatusResponse{}, nil)
			}
			st, err := jmnfs.BuildSpoolStatus(in.spool, in.drainer)
			return classifySpool(st, err)
		}},
		{diagIDLinkRTT, "Link latency", func(ctx context.Context) diagnoseCheck {
			return diagnoseLinkRTTCheck(ctx, in.backendAddr)
		}},
	}
	checks := runDiagnoseChecks(ctx, runners)
	return diagnoseReport{Overall: diagnoseOverall(checks), Checks: checks}
}

// handleDiagnoseHTTP serves GET /diagnose — the on-demand "Why is it slow?"
// self-diagnosis (INSTANT-NAV #14). Always 200 with a JSON report; the
// report carries the verdict. Loopback-only like every control-plane route.
// Strictly on-demand: nothing here runs periodically.
func handleDiagnoseHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	globalMu.Lock()
	in := diagnoseInputs{
		loopbackAddr: globalMetricsAddr,
		monitor:      globalMonitor,
		spool:        globalSpool,
		drainer:      globalDrainer,
	}
	redisURL := globalRedisURL
	globalMu.Unlock()

	if redisURL != "" {
		if addr, ok := backendAddrFromRedisURL(redisURL); ok {
			in.backendAddr = addr
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), diagnoseOverallBudget)
	defer cancel()
	t0 := time.Now()
	report := runDiagnosis(ctx, in)
	jmlog.Info("diagnose ran",
		"overall", report.Overall,
		"elapsed_ms", time.Since(t0).Milliseconds())

	data, _ := json.Marshal(report)
	w.Write(data)
}
