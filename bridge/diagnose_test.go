package main

// Unit tests for the /diagnose ("Why is it slow?", INSTANT-NAV #14) pure
// classification logic + the bounded harness + the handler's JSON shape.
// No test here touches the real network or exec's route(8): dial errors are
// fabricated, route output is a fixture behind diagnoseDeps, and the handler
// smoke test zeroes the globals so every probe short-circuits.

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
	jmnfs "github.com/lelanddutcher/juicemount/nfs"
)

// diagnoseTestMu serializes tests that swap package-level diagnose state
// (deps, budgets, globals) — same convention as mountNowTestMu.
var diagnoseTestMu sync.Mutex

// fakeTimeoutErr implements net.Error with Timeout()=true.
type fakeTimeoutErr struct{}

func (fakeTimeoutErr) Error() string   { return "i/o timeout" }
func (fakeTimeoutErr) Timeout() bool   { return true }
func (fakeTimeoutErr) Temporary() bool { return true }

func opErr(inner error) error {
	return &net.OpError{Op: "dial", Net: "tcp", Err: inner}
}

func TestDialErrorClass(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, dialClassOK},
		{"ehostunreach errno chain",
			opErr(os.NewSyscallError("connect", syscall.EHOSTUNREACH)), dialClassNoRoute},
		{"enetunreach", opErr(os.NewSyscallError("connect", syscall.ENETUNREACH)), dialClassNoRoute},
		{"no route by string only",
			errors.New("dial tcp 192.168.0.4:6379: connect: no route to host"), dialClassNoRoute},
		{"econnrefused",
			opErr(os.NewSyscallError("connect", syscall.ECONNREFUSED)), dialClassRefused},
		{"refused by string", errors.New("dial tcp: connection refused"), dialClassRefused},
		{"dns not found",
			&net.DNSError{Err: "no such host", Name: "nas.local", IsNotFound: true}, dialClassDNS},
		// A resolver timeout must classify as DNS (name problem), not as a
		// generic timeout — order matters in dialErrorClass.
		{"dns timeout", &net.DNSError{Err: "lookup timed out", Name: "nas.local", IsTimeout: true}, dialClassDNS},
		{"net timeout", opErr(fakeTimeoutErr{}), dialClassTimeout},
		{"context deadline", context.DeadlineExceeded, dialClassTimeout},
		{"other", errors.New("something exotic"), dialClassOther},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := dialErrorClass(tc.err); got != tc.want {
				t.Fatalf("dialErrorClass(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

func TestClassifyBackendDialLocalNetworkSignature(t *testing.T) {
	noRoute := opErr(os.NewSyscallError("connect", syscall.EHOSTUNREACH))

	// The headline case: EHOSTUNREACH while loopback works = macOS Local
	// Network permission denial → fail + the System Settings remedy.
	c := classifyBackendDial("192.168.0.4:6379", noRoute, true)
	if c.Status != diagStatusFail {
		t.Fatalf("status = %q, want fail", c.Status)
	}
	if !strings.Contains(c.Detail, "no route to host") || !strings.Contains(c.Detail, "Local Network") {
		t.Fatalf("detail missing signature explanation: %q", c.Detail)
	}
	if !strings.Contains(c.Remedy, "System Settings") || !strings.Contains(c.Remedy, "Local Network") ||
		!strings.Contains(c.Remedy, "relaunch") {
		t.Fatalf("remedy is not the Local Network instruction: %q", c.Remedy)
	}

	// Same dial error but loopback is ALSO dead → not the permission
	// signature; must not send the user to System Settings.
	c = classifyBackendDial("192.168.0.4:6379", noRoute, false)
	if c.Status != diagStatusFail {
		t.Fatalf("status = %q, want fail", c.Status)
	}
	if strings.Contains(c.Remedy, "System Settings") {
		t.Fatalf("remedy wrongly blames Local Network when loopback is down: %q", c.Remedy)
	}
}

func TestClassifyBackendDialOtherClasses(t *testing.T) {
	if c := classifyBackendDial("192.168.0.4:6379", nil, true); c.Status != diagStatusOK {
		t.Fatalf("nil err: status = %q, want ok", c.Status)
	}
	if c := classifyBackendDial("", nil, true); c.Status != diagStatusWarn {
		t.Fatalf("empty addr: status = %q, want warn", c.Status)
	}
	c := classifyBackendDial("192.168.0.4:6379",
		opErr(os.NewSyscallError("connect", syscall.ECONNREFUSED)), true)
	if c.Status != diagStatusFail || !strings.Contains(c.Detail, "refused") {
		t.Fatalf("refused: got %+v", c)
	}
	c = classifyBackendDial("nas.local:6379",
		&net.DNSError{Err: "no such host", Name: "nas.local", IsNotFound: true}, true)
	if c.Status != diagStatusFail || !strings.Contains(c.Detail, "resolve") {
		t.Fatalf("dns: got %+v", c)
	}
	c = classifyBackendDial("192.168.0.4:6379", opErr(fakeTimeoutErr{}), true)
	if c.Status != diagStatusFail || !strings.Contains(c.Detail, "timed out") {
		t.Fatalf("timeout: got %+v", c)
	}
}

const routeOutDirect = `   route to: 192.168.0.42
destination: 192.168.0.0
       mask: 255.255.255.0
  interface: en21
      flags: <UP,DONE,CLONING>
 recvpipe  sendpipe  ssthresh  rtt,msec    rttvar  hopcount      mtu     expire
       0         0         0         0         0         0      1500         0
`

const routeOutTunnel = `   route to: 100.87.11.4
destination: 100.64.0.0
       mask: 255.192.0.0
  interface: utun4
      flags: <UP,DONE,CLONING>
`

func TestParseRouteInterface(t *testing.T) {
	if got := parseRouteInterface(routeOutDirect); got != "en21" {
		t.Fatalf("direct: got %q, want en21", got)
	}
	if got := parseRouteInterface(routeOutTunnel); got != "utun4" {
		t.Fatalf("tunnel: got %q, want utun4", got)
	}
	if got := parseRouteInterface("route: writing to routing socket: not in table\n"); got != "" {
		t.Fatalf("error output: got %q, want empty", got)
	}
	if got := parseRouteInterface(""); got != "" {
		t.Fatalf("empty output: got %q, want empty", got)
	}
}

func TestClassifyRouteInterface(t *testing.T) {
	c := classifyRouteInterface("192.168.0.4", "en21", nil)
	if c.Status != diagStatusOK || !strings.Contains(c.Detail, "en21") {
		t.Fatalf("direct: got %+v", c)
	}

	c = classifyRouteInterface("192.168.0.4", "utun4", nil)
	if c.Status != diagStatusWarn {
		t.Fatalf("tunnel: status = %q, want warn", c.Status)
	}
	if !strings.Contains(c.Detail, "utun4") || !strings.Contains(c.Detail, "tunnel") {
		t.Fatalf("tunnel: detail must name the interface: %q", c.Detail)
	}
	if !strings.Contains(c.Remedy, "Tailscale") {
		t.Fatalf("tunnel: remedy must mention Tailscale: %q", c.Remedy)
	}

	c = classifyRouteInterface("192.168.0.4", "tailscale0", nil)
	if c.Status != diagStatusWarn {
		t.Fatalf("tailscale0: status = %q, want warn", c.Status)
	}

	c = classifyRouteInterface("127.0.0.1", "lo0", nil)
	if c.Status != diagStatusOK {
		t.Fatalf("loopback: status = %q, want ok", c.Status)
	}

	c = classifyRouteInterface("192.168.0.4", "", errors.New("exec: not found"))
	if c.Status != diagStatusWarn || !strings.Contains(c.Detail, "route -n get failed") {
		t.Fatalf("exec error: got %+v", c)
	}

	c = classifyRouteInterface("192.168.0.4", "", nil)
	if c.Status != diagStatusWarn || !strings.Contains(c.Detail, "no interface") {
		t.Fatalf("no interface: got %+v", c)
	}
}

func TestDiagnoseRouteCheckInjectedDeps(t *testing.T) {
	diagnoseTestMu.Lock()
	defer diagnoseTestMu.Unlock()
	saved := diagnoseDeps.routeGet
	defer func() { diagnoseDeps.routeGet = saved }()

	var gotHost atomic.Value
	diagnoseDeps.routeGet = func(_ context.Context, host string) (string, error) {
		gotHost.Store(host)
		return routeOutTunnel, nil
	}
	c := diagnoseRouteCheck(context.Background(), "192.168.0.4:6379")
	if c.Status != diagStatusWarn || !strings.Contains(c.Detail, "utun4") {
		t.Fatalf("tunnel via deps: got %+v", c)
	}
	if h, _ := gotHost.Load().(string); h != "192.168.0.4" {
		t.Fatalf("routeGet called with host %q, want 192.168.0.4 (port stripped)", h)
	}

	// Loopback backend must short-circuit — no exec at all.
	called := atomic.Bool{}
	diagnoseDeps.routeGet = func(_ context.Context, _ string) (string, error) {
		called.Store(true)
		return "", nil
	}
	c = diagnoseRouteCheck(context.Background(), "127.0.0.1:6379")
	if c.Status != diagStatusOK {
		t.Fatalf("loopback backend: status = %q, want ok", c.Status)
	}
	if called.Load() {
		t.Fatal("routeGet must not be exec'd for a loopback backend")
	}

	// Unknown backend must short-circuit too.
	c = diagnoseRouteCheck(context.Background(), "")
	if c.Status != diagStatusWarn {
		t.Fatalf("empty backend: status = %q, want warn", c.Status)
	}
	if called.Load() {
		t.Fatal("routeGet must not be exec'd when the backend is unknown")
	}
}

func TestClassifyFUSE(t *testing.T) {
	if c := classifyFUSE(true, "ok"); c.Status != diagStatusOK {
		t.Fatalf("healthy: got %+v", c)
	}
	c := classifyFUSE(true, "gate unconfigured")
	if c.Status != diagStatusOK || !strings.Contains(c.Detail, "unconfigured") {
		t.Fatalf("unconfigured: got %+v", c)
	}

	c = classifyFUSE(false, "mountpoint is a plain directory — no filesystem mounted (macFUSE kext not loaded, or mount absent)")
	if c.Status != diagStatusFail {
		t.Fatalf("plain dir: status = %q, want fail", c.Status)
	}
	if !strings.Contains(c.Remedy, "macFUSE") {
		t.Fatalf("plain dir: remedy must mention macFUSE approval: %q", c.Remedy)
	}

	c = classifyFUSE(false, "statfs timed out — mount wedged")
	if c.Status != diagStatusFail || !strings.Contains(c.Remedy, "Force Eject") {
		t.Fatalf("wedged: got %+v", c)
	}
}

func TestClassifyComponents(t *testing.T) {
	healthy := health.ComponentStatus{Healthy: true, Message: "ok"}
	st := health.HealthStatus{Redis: healthy, MinIO: healthy, FUSE: healthy, NFS: healthy}
	if c := classifyComponents(st); c.Status != diagStatusOK {
		t.Fatalf("all healthy: got %+v", c)
	}

	st.Redis = health.ComponentStatus{Healthy: false, Message: "ping failed: connection refused"}
	c := classifyComponents(st)
	if c.Status != diagStatusFail {
		t.Fatalf("redis down: status = %q, want fail", c.Status)
	}
	if !strings.Contains(c.Detail, "redis: ping failed") {
		t.Fatalf("redis down: detail must carry the component message: %q", c.Detail)
	}

	// Empty message still names the component.
	st.Redis = health.ComponentStatus{Healthy: false}
	if c = classifyComponents(st); !strings.Contains(c.Detail, "redis: unhealthy") {
		t.Fatalf("empty message: got %q", c.Detail)
	}
}

func TestClassifyLinkRTT(t *testing.T) {
	none := netprofile.Snapshot{}
	if c := classifyLinkRTT(500*time.Microsecond, nil, none); c.Status != diagStatusOK || !strings.Contains(c.Detail, "LAN") {
		t.Fatalf("LAN: got %+v", c)
	}
	if c := classifyLinkRTT(10*time.Millisecond, nil, none); c.Status != diagStatusOK || !strings.Contains(c.Detail, "fast") {
		t.Fatalf("fast: got %+v", c)
	}
	if c := classifyLinkRTT(30*time.Millisecond, nil, none); c.Status != diagStatusWarn || !strings.Contains(c.Detail, "WAN") {
		t.Fatalf("WAN: got %+v", c)
	}
	if c := classifyLinkRTT(120*time.Millisecond, nil, none); c.Status != diagStatusWarn || !strings.Contains(c.Detail, "high-latency") {
		t.Fatalf("high latency: got %+v", c)
	}
	if c := classifyLinkRTT(0, errors.New("dial failed"), none); c.Status != diagStatusWarn || !strings.Contains(c.Detail, "could not measure") {
		t.Fatalf("dial error: got %+v", c)
	}

	withBW := netprofile.Snapshot{Class: netprofile.ClassMedium, HaveBW: true, BytesPerSec: 87 * 1024 * 1024}
	c := classifyLinkRTT(2*time.Millisecond, nil, withBW)
	if !strings.Contains(c.Detail, "smoothed link class: medium") {
		t.Fatalf("smoothed suffix missing: %q", c.Detail)
	}
}

func TestClassifySpool(t *testing.T) {
	base := jmnfs.SpoolStatusResponse{Enabled: true}

	c := classifySpool(jmnfs.SpoolStatusResponse{}, errors.New("sql: database is closed"))
	if c.Status != diagStatusWarn || !strings.Contains(c.Detail, "unavailable") {
		t.Fatalf("error: got %+v", c)
	}

	if c = classifySpool(jmnfs.SpoolStatusResponse{Enabled: false}, nil); c.Status != diagStatusOK {
		t.Fatalf("disabled: got %+v", c)
	}

	st := base
	st.OfflineBufferFull = true
	st.StallWaiters = 2
	c = classifySpool(st, nil)
	if c.Status != diagStatusFail || !strings.Contains(c.Detail, "2 copies paused") {
		t.Fatalf("offline full: got %+v", c)
	}

	st = base
	st.StalledFiles = 1
	c = classifySpool(st, nil)
	if c.Status != diagStatusWarn || !strings.Contains(c.Remedy, "Recover stalled") {
		t.Fatalf("stalled: got %+v", c)
	}

	st = base
	st.FailedFiles = 3
	c = classifySpool(st, nil)
	if c.Status != diagStatusWarn || !strings.Contains(c.Remedy, "Retry failed") {
		t.Fatalf("failed: got %+v", c)
	}

	st = base
	st.PendingFiles = 4
	st.PendingBytes = 2 << 30
	st.Offline = true
	c = classifySpool(st, nil)
	if c.Status != diagStatusWarn || !strings.Contains(c.Detail, "offline") {
		t.Fatalf("pending offline: got %+v", c)
	}

	st = base
	st.PendingFiles = 4
	st.PendingBytes = 2 << 30
	st.OldestPendingAgeSec = 3600
	c = classifySpool(st, nil)
	if c.Status != diagStatusWarn || !strings.Contains(c.Detail, "backlog") {
		t.Fatalf("old backlog: got %+v", c)
	}

	st = base
	st.PendingFiles = 4
	st.PendingBytes = 2 << 30
	st.OldestPendingAgeSec = 10
	c = classifySpool(st, nil)
	if c.Status != diagStatusOK || !strings.Contains(c.Detail, "normal drain") {
		t.Fatalf("fresh queue: got %+v", c)
	}

	if c = classifySpool(base, nil); c.Status != diagStatusOK || !strings.Contains(c.Detail, "no upload backlog") {
		t.Fatalf("empty: got %+v", c)
	}
}

func TestDiagnoseOverall(t *testing.T) {
	ok := diagnoseCheck{Status: diagStatusOK}
	warn := diagnoseCheck{Status: diagStatusWarn}
	fail := diagnoseCheck{Status: diagStatusFail}

	if got := diagnoseOverall([]diagnoseCheck{ok, ok}); got != "ok" {
		t.Fatalf("all ok: %q", got)
	}
	if got := diagnoseOverall([]diagnoseCheck{ok, warn, ok}); got != "degraded" {
		t.Fatalf("warn: %q", got)
	}
	if got := diagnoseOverall([]diagnoseCheck{ok, warn, fail}); got != "broken" {
		t.Fatalf("fail: %q", got)
	}
	if got := diagnoseOverall(nil); got != "ok" {
		t.Fatalf("empty: %q", got)
	}
}

func TestDiagnoseHostOnly(t *testing.T) {
	cases := map[string]string{
		"192.168.0.4:6379": "192.168.0.4",
		"nas.local:6379":   "nas.local",
		"nas.local":        "nas.local",
		"":                 "",
	}
	for in, want := range cases {
		if got := diagnoseHostOnly(in); got != want {
			t.Fatalf("diagnoseHostOnly(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRunDiagnoseChecksBounded(t *testing.T) {
	diagnoseTestMu.Lock()
	defer diagnoseTestMu.Unlock()
	savedBudget := diagnosePerCheckBudget
	diagnosePerCheckBudget = 50 * time.Millisecond
	defer func() { diagnosePerCheckBudget = savedBudget }()

	release := make(chan struct{})
	defer close(release) // let the abandoned straggler exit

	runners := []diagnoseRunner{
		{"fast", "Fast check", func(context.Context) diagnoseCheck {
			return diagnoseCheck{Status: diagStatusOK, Detail: "instant"}
		}},
		{"stuck", "Stuck check", func(context.Context) diagnoseCheck {
			<-release // simulates a wedged statfs / black-holed dial
			return diagnoseCheck{Status: diagStatusOK}
		}},
	}
	start := time.Now()
	checks := runDiagnoseChecks(context.Background(), runners)
	elapsed := time.Since(start)

	if len(checks) != 2 {
		t.Fatalf("len(checks) = %d, want 2", len(checks))
	}
	if checks[0].ID != "fast" || checks[0].Status != diagStatusOK {
		t.Fatalf("fast check: got %+v", checks[0])
	}
	if checks[1].ID != "stuck" || checks[1].Title != "Stuck check" {
		t.Fatalf("stuck check identity not stamped: %+v", checks[1])
	}
	if checks[1].Status != diagStatusWarn || !strings.Contains(checks[1].Detail, "did not finish") {
		t.Fatalf("stuck check must report the budget cutoff: %+v", checks[1])
	}
	// Concurrent: the whole run is bounded by the single stuck check's
	// budget (plus scheduling slack), not the sum of the checks.
	if elapsed > 2*time.Second {
		t.Fatalf("runDiagnoseChecks took %v — checks are not bounded/concurrent", elapsed)
	}
}

func TestRunDiagnoseChecksRecoversPanic(t *testing.T) {
	runners := []diagnoseRunner{
		{"boom", "Panicky check", func(context.Context) diagnoseCheck {
			panic("kaboom")
		}},
	}
	checks := runDiagnoseChecks(context.Background(), runners)
	if checks[0].Status != diagStatusWarn || !strings.Contains(checks[0].Detail, "panicked") {
		t.Fatalf("panic must degrade to warn: %+v", checks[0])
	}
}

// TestHandleDiagnoseHTTPShape zeroes the diagnose-relevant globals so every
// probe short-circuits (no network, no exec) and asserts the response
// contract: 200, the documented JSON shape, all six checks present in order.
func TestHandleDiagnoseHTTPShape(t *testing.T) {
	diagnoseTestMu.Lock()
	defer diagnoseTestMu.Unlock()

	globalMu.Lock()
	savedRedis, savedMetrics := globalRedisURL, globalMetricsAddr
	savedMonitor, savedSpool, savedDrainer := globalMonitor, globalSpool, globalDrainer
	globalRedisURL, globalMetricsAddr = "", ""
	globalMonitor, globalSpool, globalDrainer = nil, nil, nil
	globalMu.Unlock()
	defer func() {
		globalMu.Lock()
		globalRedisURL, globalMetricsAddr = savedRedis, savedMetrics
		globalMonitor, globalSpool, globalDrainer = savedMonitor, savedSpool, savedDrainer
		globalMu.Unlock()
	}()

	saved := diagnoseDeps.routeGet
	defer func() { diagnoseDeps.routeGet = saved }()
	execd := atomic.Bool{}
	diagnoseDeps.routeGet = func(context.Context, string) (string, error) {
		execd.Store(true)
		return "", nil
	}

	req := httptest.NewRequest(http.MethodGet, "/diagnose", nil)
	rec := httptest.NewRecorder()
	handleDiagnoseHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type = %q", ct)
	}
	var report diagnoseReport
	if err := json.Unmarshal(rec.Body.Bytes(), &report); err != nil {
		t.Fatalf("body is not valid JSON: %v\nbody: %s", err, rec.Body.String())
	}

	wantIDs := []string{
		diagIDLocalNetwork, diagIDRouteTunnel, diagIDFUSEMount,
		diagIDComponents, diagIDSpoolBacklog, diagIDLinkRTT,
	}
	if len(report.Checks) != len(wantIDs) {
		t.Fatalf("len(checks) = %d, want %d\nbody: %s", len(report.Checks), len(wantIDs), rec.Body.String())
	}
	for i, id := range wantIDs {
		c := report.Checks[i]
		if c.ID != id {
			t.Fatalf("checks[%d].id = %q, want %q", i, c.ID, id)
		}
		if c.Title == "" || c.Status == "" || c.Detail == "" {
			t.Fatalf("checks[%d] (%s) missing fields: %+v", i, id, c)
		}
		switch c.Status {
		case diagStatusOK, diagStatusWarn, diagStatusFail:
		default:
			t.Fatalf("checks[%d].status = %q — not in the vocabulary", i, c.Status)
		}
	}
	// With no backend configured / no monitor the verdict is degraded-not-
	// broken: warns explain the unknowns, nothing claims a hard failure.
	if report.Overall != "degraded" {
		t.Fatalf("overall = %q, want degraded\nbody: %s", report.Overall, rec.Body.String())
	}
	if execd.Load() {
		t.Fatal("route(8) must not be exec'd when the backend is unknown")
	}
}
