package main

import "testing"

// Review 4 N16: Start must record the endpoint it serves, or every desktop
// "enroll-verify" is refused as "not the endpoint this process serves". The
// recorded URL is the logical one: with a debug or Link rewrite the runtime
// URL is a loopback proxy, and enroll-verify must still accept the
// configured endpoint.
func TestH8BStartRecordsTheServingEndpoint(t *testing.T) {
	const configured = "redis://jmclient@192.0.2.10:6379/1"
	for _, debugAddr := range []string{"", "127.0.0.1:16379"} {
		t.Setenv("JM_DEBUG_META_ADDR", debugAddr)
		setServingRedisURL(t, "")
		runtimeURL, routePolicyURL := startRedisURLs(configured)
		got := servingRedisURL.Load()
		if got == nil || *got != configured {
			t.Fatalf("debug=%q: Start recorded %v as the serving endpoint, want %q", debugAddr, got, configured)
		}
		if routePolicyURL != configured || (debugAddr == "" && runtimeURL != configured) ||
			(debugAddr != "" && runtimeURL == configured) {
			t.Fatalf("debug=%q: runtime=%q route=%q", debugAddr, runtimeURL, routePolicyURL)
		}
	}
}
