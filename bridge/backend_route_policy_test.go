package main

import (
	"errors"
	"testing"
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
	defer applyBackendRouteCeiling("")

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
	defer applyBackendRouteCeiling("")
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
