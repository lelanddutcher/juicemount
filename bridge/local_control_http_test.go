//go:build !serviceclient

package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/lelanddutcher/juicemount/metadata"
)

func TestLocalOperatorActionsRejectHeaderlessRequestsOnRealListener(t *testing.T) {
	paths := []string{"/pin", "/unpin", "/sync", "/spool-recover", "/verify-pins", "/reclaim", "/force-eject", "/mount-now", "/stop", "/cache-clear", "/prewarm", "/self-test", "/offline"}
	s := metrics.NewServer("127.0.0.1:0", metrics.NewRegistry())
	s.ExtraRoutes = controlPlaneRoutes()
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	client := &http.Client{Timeout: 3 * time.Second}
	defer client.CloseIdleConnections()
	for _, path := range paths {
		query := ""
		switch path {
		case "/offline":
			query = "?on=true"
		case "/prewarm":
			query = "?run=1"
		case "/spool-recover":
			query = "?action=retry-failed"
		}
		for _, tc := range []struct {
			method string
			want   int
		}{
			{http.MethodGet, http.StatusMethodNotAllowed},
			{http.MethodPost, http.StatusForbidden},
		} {
			if path == "/self-test" && tc.method == http.MethodGet {
				continue // Cached result is deliberately read-only.
			}
			t.Run(path+"/"+tc.method, func(t *testing.T) {
				req, err := http.NewRequest(tc.method, "http://"+s.Addr()+path+query, nil)
				if err != nil {
					t.Fatal(err)
				}
				resp, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				resp.Body.Close()
				if resp.StatusCode != tc.want {
					t.Fatalf("status = %d, want %d", resp.StatusCode, tc.want)
				}
			})
		}
	}
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		req, err := http.NewRequest(method, "http://"+s.Addr()+"/spool-recover?action=clear-failed&confirm=true", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		want := http.StatusForbidden
		if method == http.MethodGet {
			want = http.StatusMethodNotAllowed
		}
		if resp.StatusCode != want {
			t.Fatalf("headerless %s clear-failed status = %d, want %d", method, resp.StatusCode, want)
		}
	}
	// A native control request reaches the handler. There is no NFS server in
	// this test, so the handler returns 503 without mounting anything.
	req, err := http.NewRequest(http.MethodPost, "http://"+s.Addr()+"/mount-now", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(localControlHeader, "1")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("native POST status = %d, want 503", resp.StatusCode)
	}
	// jmctl sync must reach a registered handler, and a stopped server must
	// report unavailable rather than the old 404 for the missing route.
	req, err = http.NewRequest(http.MethodPost, "http://"+s.Addr()+"/sync", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(localControlHeader, "1")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("native sync POST status = %d, want 503 while stopped", resp.StatusCode)
	}
}

func TestSyncHTTPAcceptsNativeRequestWithoutWaitingForReconcile(t *testing.T) {
	rc, err := metadata.NewRedisClientDeferred("redis://127.0.0.1:6379/1", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Stop()
	globalMu.Lock()
	previous := globalRC
	globalRC = rc
	globalMu.Unlock()
	defer func() {
		globalMu.Lock()
		globalRC = previous
		globalMu.Unlock()
	}()

	req := httptest.NewRequest(http.MethodPost, "/sync", nil)
	req.Header.Set(localControlHeader, "1")
	rec := httptest.NewRecorder()
	handleSyncHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("sync request status = %d, want 202: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("sync response Content-Type = %q, want application/json", got)
	}
}
