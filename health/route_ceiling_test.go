package health

import (
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
	"github.com/lelanddutcher/juicemount/nfs"
)

func TestBackendRouteTunneled(t *testing.T) {
	for iface, want := range map[string]bool{
		"utun0": true, "utun11": true, "tailscale0": true,
		"en0": false, "en7": false, "lo0": false, "": false,
	} {
		if got := BackendRouteTunneled(iface); got != want {
			t.Errorf("BackendRouteTunneled(%q) = %v, want %v", iface, got, want)
		}
	}
}

// The mount's link policy follows the route ceiling both ways, keeps the
// prior ceiling when the lookup fails, and always equals the live policy.
func TestMountLinkPolicyAppliesRouteCeiling(t *testing.T) {
	t.Setenv("JM_NET_CLASS", "")
	t.Setenv("JM_JFS_DEMAND_FIRST", "")
	t.Setenv("JM_JFS_OBJECT_TIMEOUTS", "")
	p := netprofile.New()
	rc := &RouteCeiling{}
	p.ObserveRTT(2 * time.Millisecond)
	lanClass := p.Class()
	if lanClass == netprofile.ClassSlow || lanClass == netprofile.ClassMetered {
		t.Fatalf("a 2 ms profile is already %s; the test would prove nothing", lanClass)
	}
	route := func(iface string, err error) func(string) (string, error) {
		return func(string) (string, error) { return iface, err }
	}
	check := func(step string, wantSlow bool, pol netprofile.JuiceFSPolicy, args []string) {
		t.Helper()
		if got := p.Class() == netprofile.ClassSlow; got != wantSlow {
			t.Fatalf("%s: class %s, want slow=%v", step, p.Class(), wantSlow)
		}
		if got := slices.Contains(args, "--demand-first"); got != (p.Class() != netprofile.ClassFast) {
			t.Fatalf("%s: class %s, args %v", step, p.Class(), args) // on every class but fast
		}
		if got := len(args) > 1; got != wantSlow { // slow adds the object deadlines
			t.Fatalf("%s: args %v, want slow deadlines=%v", step, args, wantSlow)
		}
		if pol != p.JuiceFS() {
			t.Fatalf("%s: policy %+v, profile says %+v", step, pol, p.JuiceFS())
		}
		if !reflect.DeepEqual(normDurations(t, args), normDurations(t, nfs.JFSLinkPolicyFlags(p))) {
			t.Fatalf("%s: mount args %v, live policy %v", step, args, nfs.JFSLinkPolicyFlags(p))
		}
	}
	pol, args := MountLinkPolicy(p, rc, "192.0.2.10:30179", route("utun11", nil))
	check("utun", true, pol, args)
	if pol.BufferSizeMB != 512 {
		t.Fatalf("tunneled mount buffer %d MiB, want the slow 512", pol.BufferSizeMB)
	}
	pol, args = MountLinkPolicy(p, rc, "192.0.2.10:30179", route("", errors.New("no route")))
	check("lookup failed", true, pol, args)
	pol, args = MountLinkPolicy(p, rc, "192.0.2.10:30179", route("en0", nil))
	check("en0", false, pol, args)
	if p.Class() != lanClass {
		t.Fatalf("lifting the ceiling left class %s, want %s", p.Class(), lanClass)
	}
	pol, args = MountLinkPolicy(p, rc, "", route("utun11", nil))
	check("no target", false, pol, args)
}

// The FUSE manager resolves FUSEConfig.RouteTarget, the original backend,
// not RedisURL: through Link or the debug proxy RedisURL is loopback.
func TestFUSEManagerMountPolicyResolvesRouteTarget(t *testing.T) {
	t.Setenv("JM_NET_CLASS", "")
	t.Setenv("JM_JFS_DEMAND_FIRST", "")
	defer netprofile.Default().CeilingClass(nil)
	fm := NewFUSEManager(FUSEConfig{RedisURL: "redis://127.0.0.1:1/1", RouteTarget: "192.0.2.10:30179"})
	var seen []string
	fm.routeResolver = func(target string) (string, error) {
		seen = append(seen, target)
		return "utun11", nil
	}
	_, args := fm.linkAwareJuiceFSPolicy()
	if !reflect.DeepEqual(seen, []string{"192.0.2.10:30179"}) {
		t.Fatalf("resolver saw %v, want the original backend once", seen)
	}
	if !slices.Contains(args, "--demand-first") || netprofile.Default().Class() != netprofile.ClassSlow {
		t.Fatalf("tunneled backend: args %v, class %s", args, netprofile.Default().Class())
	}
}

// PR #90 review LOW-1: a failed lookup keeps the last successful lookup's
// ceiling, and before any success it caps at slow.
func TestRouteCeilingFallbackOnFailedLookup(t *testing.T) {
	t.Setenv("JM_NET_CLASS", "")
	p := netprofile.New()
	p.ObserveRTT(2 * time.Millisecond)
	rc := &RouteCeiling{}
	fail := errors.New("no route")
	steps := []struct {
		iface      string
		err        error
		wantSlow   bool
		wantLooked bool
	}{
		{"", fail, true, false},     // no success yet: slow
		{"en0", nil, false, true},   // lifted
		{"", fail, false, false},    // keeps the lifted ceiling
		{"utun11", nil, true, true}, // tunnel
		{"", fail, true, false},     // keeps the tunnel ceiling
		{"tailscale0", nil, true, true},
	}
	for i, s := range steps {
		tunneled, looked := rc.Apply(p, s.iface, s.err)
		if tunneled != s.wantSlow || looked != s.wantLooked || (p.Class() == netprofile.ClassSlow) != s.wantSlow {
			t.Fatalf("step %d (%q, %v): tunneled %v looked %v class %s", i, s.iface, s.err, tunneled, looked, p.Class())
		}
	}
}

// The NetWatcher tells OnBackendRoute the lookup result itself (an error,
// not the default-route guess), and only when it changes.
func TestNetWatcherReportsBackendRouteResult(t *testing.T) {
	results := []struct {
		iface string
		err   error
	}{{"utun3", nil}, {"utun3", nil}, {"", errors.New("dns")}, {"", errors.New("dns again")}, {"", nil}, {"en0", nil}}
	i := 0
	w := NewNetWatcher(time.Hour, WithBackendTarget("nas.example:6379"))
	w.routeCacheFor = 0
	w.routeResolver = func(string) (string, error) { r := results[i]; i++; return r.iface, r.err }
	var got []string
	w.OnBackendRoute(func(iface string, err error) {
		if err != nil {
			got = append(got, "error")
		} else {
			got = append(got, iface)
		}
	})
	for range results {
		w.detect()
	}
	if want := []string{"utun3", "error", "en0"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("route callbacks %v, want %v", got, want)
	}
}
