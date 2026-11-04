package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"
)

func TestNASRouteTCPHandlerScopesAndForwards(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	backendDone := make(chan error, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			backendDone <- acceptErr
			return
		}
		defer conn.Close()
		line, readErr := bufio.NewReader(conn).ReadString('\n')
		if readErr != nil {
			backendDone <- readErr
			return
		}
		_, writeErr := io.WriteString(conn, "echo:"+line)
		backendDone <- writeErr
	}()

	dialer := &net.Dialer{Timeout: time.Second}
	handlerFor := nasRouteTCPHandler(netip.MustParsePrefix("127.0.0.0/8"), dialer.DialContext)
	outside := netip.MustParseAddrPort("192.0.2.1:9000")
	if handler, intercept := handlerFor(netip.AddrPort{}, outside); intercept || handler != nil {
		t.Fatalf("outside route was intercepted: handler=%v intercept=%v", handler != nil, intercept)
	}

	backendAddr := listener.Addr().(*net.TCPAddr).AddrPort()
	handler, intercept := handlerFor(netip.AddrPort{}, backendAddr)
	if !intercept || handler == nil {
		t.Fatalf("inside route was not intercepted: handler=%v intercept=%v", handler != nil, intercept)
	}
	client, routed := net.Pipe()
	proxyDone := make(chan struct{})
	go func() {
		handler(routed)
		close(proxyDone)
	}()

	if _, err := io.WriteString(client, "juice\n"); err != nil {
		t.Fatal(err)
	}
	got, err := bufio.NewReader(client).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if want := "echo:juice\n"; got != want {
		t.Fatalf("forwarded response = %q, want %q", got, want)
	}
	_ = client.Close()

	select {
	case err := <-backendDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("backend did not finish")
	}
	select {
	case <-proxyDone:
	case <-time.After(2 * time.Second):
		t.Fatal("route proxy did not finish")
	}
}

func TestNASRouteTCPHandlerPropagatesDialFailure(t *testing.T) {
	wantErr := fmt.Errorf("backend down")
	dial := func(context.Context, string, string) (net.Conn, error) { return nil, wantErr }
	handlerFor := nasRouteTCPHandler(netip.MustParsePrefix("192.0.2.0/24"), dial)
	handler, intercept := handlerFor(netip.AddrPort{}, netip.MustParseAddrPort("192.0.2.5:6379"))
	if !intercept || handler == nil {
		t.Fatal("expected in-route handler")
	}
	client, routed := net.Pipe()
	done := make(chan struct{})
	go func() {
		handler(routed)
		close(done)
	}()
	defer client.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("dial failure did not terminate handler")
	}
}

// TestNASPeerAddrHandlerKeysEachNodeOnce: a Mac reaching the in-process
// Manager over either of its tailnet addresses is seen at its IPv4 address,
// so per-address limits count it once; an unidentified peer is refused.
func TestNASPeerAddrHandlerKeysEachNodeOnce(t *testing.T) {
	mac := []netip.Prefix{netip.MustParsePrefix("fd7a:115c:a1e0::2/128"), netip.MustParsePrefix("100.64.0.2/32")}
	whois := func(_ context.Context, remote string) ([]netip.Prefix, error) {
		switch remote {
		case "100.64.0.2:4000", "[fd7a:115c:a1e0::2]:4001":
			return mac, nil
		case "[fd7a:115c:a1e0::9]:4002":
			return []netip.Prefix{netip.MustParsePrefix("fd7a:115c:a1e0::9/128")}, nil
		}
		return nil, errors.New("unknown peer")
	}
	var seen string
	h := nasPeerAddrHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { seen = r.RemoteAddr }), whois)
	for remote, want := range map[string]string{
		"100.64.0.2:4000":          "100.64.0.2:4000",
		"[fd7a:115c:a1e0::2]:4001": "100.64.0.2:4001",
		"[fd7a:115c:a1e0::9]:4002": "[fd7a:115c:a1e0::9]:4002",
	} {
		seen = ""
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = remote
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK || seen != want {
			t.Errorf("from %s: status %d, handler saw %q, want %q", remote, rec.Code, seen, want)
		}
	}
	seen = ""
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "100.64.0.77:4003"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable || seen != "" {
		t.Fatalf("unidentified peer: status %d, handler saw %q", rec.Code, seen)
	}
}
