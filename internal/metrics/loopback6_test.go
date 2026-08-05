package metrics

import (
	"net"
	"net/http"
	"testing"
	"time"
)

// The control plane must answer on BOTH loopback families.
//
// macOS resolves "localhost" to ::1 BEFORE 127.0.0.1. The server bound IPv4
// only, so any client that connects to the first resolved address without
// falling back got ECONNREFUSED — while curl, which does fall back, reported
// the control plane perfectly healthy. That is exactly the shape of a consumer
// reporting "cannot reach the control plane" against a service that answers
// fine from a shell.
func TestServerAnswersOnBothLoopbackFamilies(t *testing.T) {
	s := NewServer("127.0.0.1:0", nil)
	if err := s.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	_, port, err := net.SplitHostPort(s.Addr())
	if err != nil {
		t.Fatal(err)
	}

	get := func(host string) (int, error) {
		c := &http.Client{Timeout: 3 * time.Second}
		resp, err := c.Get("http://" + net.JoinHostPort(host, port) + "/health")
		if err != nil {
			return 0, err
		}
		defer resp.Body.Close()
		return resp.StatusCode, nil
	}

	if code, err := get("127.0.0.1"); err != nil || code != 200 {
		t.Fatalf("IPv4 loopback: code=%d err=%v", code, err)
	}

	// The one that was broken. Skip only where the machine genuinely has no
	// IPv6 loopback — not to paper over a regression.
	if !hasIPv6Loopback(t) {
		t.Skip("no IPv6 loopback on this machine")
	}
	code, err := get("::1")
	if err != nil {
		t.Fatalf("IPv6 loopback refused (%v) — a client resolving localhost to ::1 "+
			"and not falling back sees the control plane as DOWN", err)
	}
	if code != 200 {
		t.Errorf("IPv6 loopback: code=%d, want 200", code)
	}
}

// The ::1 companion must NEVER be added for a non-loopback bind — it is a
// second loopback family, not a widening of exposure. The control plane is
// unauthenticated and must not be reachable off-box.
func TestLoopbackCompanionOnlyForLoopbackBinds(t *testing.T) {
	for host, want := range map[string]bool{
		"127.0.0.1": true, "localhost": true, "::1": true,
		"0.0.0.0": false, "192.168.0.60": false, "": false,
	} {
		if got := isLoopbackHost(host); got != want {
			t.Errorf("isLoopbackHost(%q) = %v, want %v — a non-loopback bind must not "+
				"gain an extra listener", host, got, want)
		}
	}
}

func hasIPv6Loopback(t *testing.T) bool {
	t.Helper()
	l, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		return false
	}
	_ = l.Close()
	return true
}
