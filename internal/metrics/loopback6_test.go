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

	if code, err := get("127.0.0.1"); err != nil || code == 0 {
		t.Fatalf("IPv4 loopback: code=%d err=%v", code, err)
	}
	// F1: bare server returns 503 'starting' (no provider attached) —
	// any HTTP response proves the listener works on both families.

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
	// F1: bare server returns 503 'starting' (no provider attached) —
	// any valid HTTP response on ::1 proves dual-family listening works.
	if code != http.StatusServiceUnavailable {
		t.Logf("IPv6 loopback /health returned %d (expected 503 starting)", code)
	}
}

func TestLoopbackHostRequiresIPLiteral(t *testing.T) {
	for host, want := range map[string]bool{
		"127.0.0.1": true, "127.0.0.2": true, "::1": true,
		"localhost": false, "0.0.0.0": false, "192.168.0.60": false,
		"::": false, "fe80::1%lo0": false, "::ffff:127.0.0.1": false, "": false,
	} {
		if got := isLoopbackHost(host); got != want {
			t.Errorf("isLoopbackHost(%q) = %v, want %v — a non-loopback bind must not "+
				"gain an extra listener", host, got, want)
		}
	}
}

func TestServerRejectsNonLoopbackListenAddressBeforeBind(t *testing.T) {
	for _, addr := range []string{
		"", ":0", "0.0.0.0:0", "[::]:0", "192.168.0.60:0",
		"localhost:0", "example.test:0", "127.0.0.1", "127.0.0.1:http",
		"127.0.0.1:-1", "127.0.0.1:65536", "[fe80::1%lo0]:0",
		"[::ffff:127.0.0.1]:0",
	} {
		t.Run(addr, func(t *testing.T) {
			s := NewServer(addr, nil)
			if err := s.Start(); err == nil {
				s.Stop()
				t.Fatal("non-loopback or malformed address accepted")
			}
			if s.listener != nil {
				t.Fatal("listener opened before address validation")
			}
		})
	}
}

func TestServerAcceptsExplicitLoopbackLiterals(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:0", "[::1]:0"} {
		t.Run(addr, func(t *testing.T) {
			s := NewServer(addr, nil)
			if err := s.Start(); err != nil {
				if addr == "[::1]:0" && !hasIPv6Loopback(t) {
					t.Skip("no IPv6 loopback on this machine")
				}
				t.Fatalf("loopback bind rejected: %v", err)
			}
			defer s.Stop()
			if !s.listener.Addr().(*net.TCPAddr).IP.IsLoopback() {
				t.Fatalf("bound outside loopback: %s", s.Addr())
			}
		})
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
