package main

import (
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
	jmnfs "github.com/lelanddutcher/juicemount/nfs"
)

func TestRedisURLsAtConfigIngestProductionUnchanged(t *testing.T) {
	t.Setenv("JM_DEBUG_META_ADDR", "")
	const configured = "redis://192.0.2.10:30179/1"
	runtimeURL, routePolicyURL := redisURLsAtConfigIngest(configured)
	if runtimeURL != configured {
		t.Fatalf("ordinary runtime URL changed: got %q, want %q", runtimeURL, configured)
	}
	if routePolicyURL != configured {
		t.Fatalf("ordinary route-policy URL changed: got %q, want %q", routePolicyURL, configured)
	}
}

func TestRedisURLsAtConfigIngestKeepsRealRouteWhenRuntimeIsProxied(t *testing.T) {
	t.Setenv("JM_DEBUG_META_ADDR", "127.0.0.1:16379")
	const configured = "redis://user:secret@192.0.2.10:30179/1"
	runtimeURL, routePolicyURL := redisURLsAtConfigIngest(configured)
	if runtimeURL != "redis://user:secret@127.0.0.1:16379/1" {
		t.Fatalf("runtime URL = %q, want bounded loopback proxy", runtimeURL)
	}
	if routePolicyURL != configured {
		t.Fatalf("route-policy URL = %q, want original configured backend", routePolicyURL)
	}
	if got := backendRouteTarget(routePolicyURL); got != "192.0.2.10:30179" {
		t.Fatalf("route target = %q, want original numeric backend", got)
	}
	if got := backendRouteTarget(runtimeURL); got != "127.0.0.1:16379" {
		t.Fatalf("runtime dial target = %q, want accounting proxy", got)
	}
}

func TestBackendRouteTargetInvalidValuesFailClosed(t *testing.T) {
	for _, value := range []string{
		"",
		"redis://:6379/1",
		"redis://example.invalid:0/1",
		"redis://example.invalid:65536/1",
		"redis://example.invalid:not-a-port/1",
	} {
		if got := backendRouteTarget(value); got != "" {
			t.Errorf("backendRouteTarget(%q) = %q, want empty fail-closed result", value, got)
		}
	}
}

func TestOriginalBackendTargetReachesRouteCeiling(t *testing.T) {
	defer applyBackendRouteCeiling("en0")

	const target = "192.0.2.10:30179"
	seen := ""
	err := resolveAndApplyBackendRouteCeiling(target, func(got string) (string, error) {
		seen = got
		return "utun42", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if seen != target {
		t.Fatalf("resolver saw %q, want original backend %q", seen, target)
	}
	if !backendRoutePolicyState.tunneled.Load() {
		t.Fatal("sanitized route proof did not record tunneled backend")
	}
	if !backendRoutePolicyState.ceilingApplied.Load() {
		t.Fatal("tunneled backend did not reach applyBackendRouteCeiling")
	}
}

func TestBackendRouteResolutionFailurePreservesPriorCeiling(t *testing.T) {
	applyBackendRouteCeiling("utun42")
	defer applyBackendRouteCeiling("en0")
	err := resolveAndApplyBackendRouteCeiling("192.0.2.10:30179", func(string) (string, error) {
		return "", errors.New("test route miss")
	})
	if err == nil {
		t.Fatal("route resolver failure was ignored")
	}
	if !backendRoutePolicyState.tunneled.Load() || !backendRoutePolicyState.ceilingApplied.Load() {
		t.Fatal("failed route resolution cleared the prior conservative tunnel ceiling")
	}
}

// B1 (laptop run 2026-11-13): the volume owner mounted juicefs on lan flags
// through a WireGuard utun while the serving process capped the class at
// slow. Both processes now classify the route with one function. For each
// route, the serving side (resolveAndApplyBackendRouteCeiling on this
// process's profile) and the owner side (health.MountLinkPolicy on the owner
// process's own profile, same RTT sample) must reach the same classes, and
// the owner's mount args must equal what the serving process's live
// follower would send, so the follower has nothing to switch at mount.
func TestServingAndOwnerClassifyBackendRouteAlike(t *testing.T) {
	t.Setenv("JM_NET_CLASS", "")
	t.Setenv("JM_JFS_DEMAND_FIRST", "")
	t.Setenv("JM_JFS_OBJECT_TIMEOUTS", "")
	defer applyBackendRouteCeiling("en0")
	defer netprofile.Default().ResetForTransportChange()
	const target = "192.0.2.10:30179"
	const rtt = 2 * time.Millisecond // the owner's dial on the laptop lab
	for _, tc := range []struct {
		iface    string
		tunneled bool
	}{
		{"utun11", true},
		{"tailscale0", true},
		{"en0", false},
	} {
		resolver := func(got string) (string, error) {
			if got != target {
				t.Fatalf("resolver saw %q, want %q", got, target)
			}
			return tc.iface, nil
		}

		serving := netprofile.Default()
		serving.ResetForTransportChange()
		serving.ObserveRTT(rtt)
		if err := resolveAndApplyBackendRouteCeiling(target, resolver); err != nil {
			t.Fatal(err)
		}
		follower := jmnfs.JFSLinkPolicyFlags(serving)

		owner := netprofile.New()
		owner.ObserveRTT(rtt)
		pol, mountArgs := health.MountLinkPolicy(owner, &health.RouteCeiling{}, target, resolver)

		if owner.Class() != serving.Class() || owner.ReadPolicyClass() != serving.ReadPolicyClass() {
			t.Errorf("%s: owner class %s/%s, serving %s/%s", tc.iface,
				owner.Class(), owner.ReadPolicyClass(), serving.Class(), serving.ReadPolicyClass())
		}
		if pol != serving.JuiceFS() {
			t.Errorf("%s: owner prefetch policy %+v, serving %+v", tc.iface, pol, serving.JuiceFS())
		}
		if !reflect.DeepEqual(normLinkDurations(t, mountArgs), normLinkDurations(t, follower)) {
			t.Errorf("%s: mount args %v, follower would send %v", tc.iface, mountArgs, follower)
		}
		if tc.tunneled {
			if owner.Class() != netprofile.ClassSlow || !slices.Contains(mountArgs, "--demand-first") ||
				pol.BufferSizeMB != 512 {
				t.Errorf("%s: tunneled mount got class %s, args %v, buffer %d; want slow, --demand-first, 512",
					tc.iface, owner.Class(), mountArgs, pol.BufferSizeMB)
			}
		} else if owner.Class() == netprofile.ClassSlow {
			t.Errorf("%s: an untunneled 2 ms route was capped at slow", tc.iface)
		}
		if backendRoutePolicyState.tunneled.Load() != tc.tunneled {
			t.Errorf("%s: serving metrics tunneled = %v", tc.iface, !tc.tunneled)
		}
	}
}

// normLinkDurations writes "3m" and "3m0s" the same way.
func normLinkDurations(t *testing.T, args []string) []string {
	t.Helper()
	out := append([]string(nil), args...)
	for i := 1; i < len(out); i++ {
		if strings.HasSuffix(out[i-1], "-timeout") {
			d, err := time.ParseDuration(out[i])
			if err != nil {
				t.Fatal(err)
			}
			out[i] = d.String()
		}
	}
	return out
}

// PR #90 review LOW-1: after the same lookup history, a failed lookup leaves
// the serving process and the mount with the same ceiling. The serving
// process used to take the NetWatcher's default-route guess instead.
func TestServingAndOwnerTakeSameFallbackOnFailedLookup(t *testing.T) {
	t.Setenv("JM_NET_CLASS", "")
	defer applyBackendRouteCeiling("en0")
	const target = "192.0.2.10:30179"
	fail := func(string) (string, error) { return "", errors.New("test route miss") }
	defer netprofile.Default().ResetForTransportChange()
	netprofile.Default().ResetForTransportChange()
	netprofile.Default().ObserveRTT(2 * time.Millisecond)
	owner := netprofile.New()
	owner.ObserveRTT(2 * time.Millisecond)
	rc := &health.RouteCeiling{}
	for _, prior := range []string{"utun11", "en0"} {
		ok := func(string) (string, error) { return prior, nil }
		_ = resolveAndApplyBackendRouteCeiling(target, ok)
		health.MountLinkPolicy(owner, rc, target, ok)
		if err := resolveAndApplyBackendRouteCeiling(target, fail); err == nil {
			t.Fatal("a failed lookup reported no error")
		}
		_, ownerArgs := health.MountLinkPolicy(owner, rc, target, fail)
		serving := netprofile.Default()
		wantSlow := prior == "utun11"
		if (serving.Class() == netprofile.ClassSlow) != wantSlow || (owner.Class() == netprofile.ClassSlow) != wantSlow {
			t.Fatalf("after %s then a failed lookup: serving %s, owner %s; want slow=%v", prior, serving.Class(), owner.Class(), wantSlow)
		}
		if backendRoutePolicyState.tunneled.Load() != wantSlow {
			t.Fatalf("after %s then a failed lookup: serving status tunneled=%v", prior, !wantSlow)
		}
		if !reflect.DeepEqual(normLinkDurations(t, ownerArgs), normLinkDurations(t, jmnfs.JFSLinkPolicyFlags(serving))) {
			t.Fatalf("after %s: mount args %v, follower %v", prior, ownerArgs, jmnfs.JFSLinkPolicyFlags(serving))
		}
	}
}
