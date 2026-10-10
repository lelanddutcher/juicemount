package metrics

import (
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"testing"
)

func TestControlRequestGuardOnRealListenerAndLateRoutes(t *testing.T) {
	var mutations atomic.Int32
	mutate := func(w http.ResponseWriter, _ *http.Request) {
		mutations.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}
	s := NewServer("127.0.0.1:0", NewRegistry())
	s.ExtraRoutes = map[string]http.HandlerFunc{"/early": mutate}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	s.RegisterRoutes(map[string]http.HandlerFunc{"/late": mutate})

	_, port, err := net.SplitHostPort(s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	boundPort, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: &http.Transport{Proxy: nil}}
	tests := []struct {
		name       string
		path       string
		host       string
		origin     string
		fetchSite  string
		wantStatus int
		wantCall   bool
	}{
		{"native early", "/early", "127.0.0.1:" + port, "", "", http.StatusNoContent, true},
		{"native late", "/late", "localhost:" + port, "", "", http.StatusNoContent, true},
		{"same-origin late", "/late", "127.0.0.1:" + port, "http://127.0.0.1:" + port, "same-origin", http.StatusNoContent, true},
		{"same-origin localhost", "/early", "localhost:" + port, "http://localhost:" + port, "same-origin", http.StatusNoContent, true},
		{"dns rebinding host", "/late", "attacker.example:" + port, "", "", http.StatusForbidden, false},
		{"lan host", "/early", "192.168.1.5:" + port, "", "", http.StatusForbidden, false},
		{"wrong port", "/late", "127.0.0.1:" + strconv.Itoa(boundPort+1), "", "", http.StatusForbidden, false},
		{"unbracketed ipv6", "/early", "::1:" + port, "", "", http.StatusForbidden, false},
		{"cross-origin post", "/late", "127.0.0.1:" + port, "https://attacker.example", "", http.StatusForbidden, false},
		{"loopback alias cross-origin", "/early", "127.0.0.1:" + port, "http://localhost:" + port, "", http.StatusForbidden, false},
		{"null origin", "/late", "127.0.0.1:" + port, "null", "", http.StatusForbidden, false},
		{"origin wrong port", "/early", "127.0.0.1:" + port, "http://127.0.0.1:" + strconv.Itoa(boundPort+1), "", http.StatusForbidden, false},
		{"cross-site fetch", "/late", "127.0.0.1:" + port, "", "cross-site", http.StatusForbidden, false},
		{"same-site fetch", "/late", "127.0.0.1:" + port, "", "same-site", http.StatusForbidden, false},
		{"cross-site even with origin", "/early", "127.0.0.1:" + port, "http://127.0.0.1:" + port, "cross-site", http.StatusForbidden, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, "http://"+s.Addr()+tc.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Host = tc.host
			if tc.origin != "" {
				req.Header.Set("Origin", tc.origin)
			}
			if tc.fetchSite != "" {
				req.Header.Set("Sec-Fetch-Site", tc.fetchSite)
			}
			before := mutations.Load()
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.wantStatus)
			}
			if got := mutations.Load() != before; got != tc.wantCall {
				t.Fatalf("mutation called = %t, want %t", got, tc.wantCall)
			}
			if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "" {
				t.Fatalf("unexpected permissive CORS header %q", got)
			}
		})
	}

	for _, host := range []string{"attacker.example:" + port, "0.0.0.0:" + port} {
		req, err := http.NewRequest(http.MethodGet, "http://"+s.Addr()+"/health", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Host = host
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("built-in route with Host %q returned %d", host, resp.StatusCode)
		}
	}
}
