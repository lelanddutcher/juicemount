package enrollclient

// Regression tests for the 2026-10-06 adversarial review of the Mac
// enrollment client (findings 3, 4 and 5), adapted from the reviewer's probes.

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// noRedirectClient is the client the bridge uses (bridge/enrollment.go
// newEnrollHTTPClient): it returns a 3xx instead of following it.
func noRedirectClient() *http.Client {
	return &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

// A Manager answer that redirects is a failure, and the redirect target
// never sees a request (so never the secret).
func TestEnrollRedirectIsAFailureAndNeverForwardsTheSecret(t *testing.T) {
	var mu sync.Mutex
	var forwarded []string
	evil := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		forwarded = append(forwarded, string(b))
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"pending","principal":"` + principalA + `","fingerprint":"AAAA-BBBB","poll_token":"` + strings.Repeat("a", 64) + `"}`))
	}))
	defer evil.Close()
	for _, code := range []int{http.StatusMovedPermanently, http.StatusFound, http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		mgr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, evil.URL+"/steal", code)
		}))
		id := Identity{InstanceID: instanceA, Host: "probe"}
		r := Step(context.Background(), noRedirectClient(), mgr.URL, id, State{})
		r = Step(context.Background(), noRedirectClient(), mgr.URL, id, r.State)
		mgr.Close()
		if r.Phase != Failed || !strings.Contains(r.Message, "redirect") {
			t.Fatalf("HTTP %d: phase %s %q, want Failed naming the redirect", code, r.Phase, r.Message)
		}
		if r.Persist || r.State.PollToken != "" || r.State.Principal != "" {
			t.Fatalf("HTTP %d: a redirect changed the saved state: %+v", code, r.State)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(forwarded) != 0 {
		t.Fatalf("the redirect target received %d request(s)", len(forwarded))
	}
}

// The Manager's answer must name this Mac's own principal.
func TestEnrollRefusesAPrincipalThatIsNotThisMacs(t *testing.T) {
	for _, name := range []string{"admin", "farm", "mac-000000000000", "mac-4f1c2a9e7b3", "MAC-4F1C2A9E7B3D", principalA + " "} {
		mgr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"status":"approved","principal":"` + name + `","fingerprint":"AAAA-BBBB","poll_token":"` + strings.Repeat("b", 64) + `","object_access_key":"jm-client","object_secret_key":"clientsecret0123"}`))
		}))
		id := Identity{InstanceID: instanceA, Host: "probe"}
		r := Step(context.Background(), mgr.Client(), mgr.URL, id, State{})
		start := r.State
		r = Step(context.Background(), mgr.Client(), mgr.URL, id, r.State)
		mgr.Close()
		if r.Phase != Failed || r.ObjectAccessKey != "" || r.ObjectSecretKey != "" || r.State.Principal != "" || r.Persist {
			t.Fatalf("principal %q adopted: phase %s state %+v", name, r.Phase, r.State)
		}
		if r.State.Secret != start.Secret || r.State.PollToken != "" {
			t.Fatalf("principal %q: refused answer changed the state", name)
		}
	}
	if got := PrincipalFor(instanceA); got != principalA {
		t.Fatalf("PrincipalFor(%s) = %q, want %q", instanceA, got, principalA)
	}
	for _, bad := range []string{"", "4F1C2A9E", "ZZZZZZZZ-ZZZZ-ZZZZ-ZZZZ-ZZZZZZZZZZZZ"} {
		if got := PrincipalFor(bad); got != "" {
			t.Fatalf("PrincipalFor(%q) = %q, want none", bad, got)
		}
	}
}

// A malformed object key is refused as a whole; a well-formed one and no key
// at all are accepted.
func TestEnrollRefusesAMalformedObjectKey(t *testing.T) {
	cases := []struct {
		ak, sk string
		ok     bool
	}{
		{"jm-client", "clientsecret0123", true},
		{"", "", true},
		{"jm-client", "", false},
		{"", "clientsecret0123", false},
		{"jm client", "clientsecret0123", false},
		{"jm-client", "short", false},
		{"ak", "clientsecret0123", false},
		{"jm-client", "client\nsecret0123", false},
		{"jm-client", "clientsecret0123é", false},
		{strings.Repeat("a", 129), "clientsecret0123", false},
	}
	for _, c := range cases {
		body := `{"status":"approved","principal":"` + principalA + `","fingerprint":"AAAA-BBBB","poll_token":"` + strings.Repeat("c", 64) +
			`","object_access_key":` + jsonString(c.ak) + `,"object_secret_key":` + jsonString(c.sk) + `}`
		mgr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) }))
		id := Identity{InstanceID: instanceA, Host: "probe"}
		r := Step(context.Background(), mgr.Client(), mgr.URL, id, State{})
		r = Step(context.Background(), mgr.Client(), mgr.URL, id, r.State)
		mgr.Close()
		if c.ok && r.Phase != Approved {
			t.Fatalf("key %q/%q: phase %s %q, want approved", c.ak, c.sk, r.Phase, r.Message)
		}
		if !c.ok && (r.Phase != Failed || r.ObjectAccessKey != "" || r.ObjectSecretKey != "" || r.State.Principal != "") {
			t.Fatalf("key %q/%q adopted: phase %s", c.ak, c.sk, r.Phase)
		}
	}
}

func jsonString(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '\n':
			b.WriteString(`\n`)
		case '"', '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}

// Respelling the same Manager's address (trailing slash, default port, case,
// trailing dot) keeps the secret: no new request and no Conflict. A
// genuinely different Manager still starts over, and so does another
// loopback listener (it can be a tunnel to another NAS).
func TestEnrollManagerRespellingKeepsTheSecret(t *testing.T) {
	srv, _ := realManager(t)
	ctx := context.Background()
	id := Identity{InstanceID: instanceA, Host: "mac", DeviceDigest: digestMac}
	r := Step(ctx, srv.Client(), srv.URL, id, State{})
	r = Step(ctx, srv.Client(), srv.URL, id, r.State)
	admin(t, srv, "approve", principalA)
	r = Step(ctx, srv.Client(), srv.URL, id, r.State)
	if r.Phase != Approved {
		t.Fatalf("phase %s %s", r.Phase, r.Message)
	}
	orig := r.State.Secret
	for _, alt := range []string{
		srv.URL + "/",
		strings.ToUpper(srv.URL[:4]) + srv.URL[4:],
		srv.URL + "//",
	} {
		r2 := Step(ctx, srv.Client(), alt, id, r.State)
		if r2.State.Secret != orig {
			t.Fatalf("%s: respelling minted a new secret (phase %s)", alt, r2.Phase)
		}
		if r2.Phase != Approved {
			t.Fatalf("%s: phase %s %q, want approved", alt, r2.Phase, r2.Message)
		}
	}
	if r.State.ManagerID == "" {
		t.Fatal("the enrollment is not bound to the Manager's ID")
	}
	// Bound to a Manager ID: an address that cannot be asked for its ID is
	// retried, the secret is kept and nothing is sent.
	other := Step(ctx, srv.Client(), "http://nas-two.example:30190", id, r.State)
	if other.State.Secret != orig || other.Persist || other.Phase != Failed {
		t.Fatalf("an unreachable new address changed the bound enrollment: %+v", other)
	}
	// Unbound (a Manager that sends no ID): the address decides, as before.
	unbound := r.State
	unbound.ManagerID = ""
	other = Step(ctx, srv.Client(), "http://nas-two.example:30190", id, unbound)
	if other.State.Secret == orig || !other.Persist || other.Phase != NotEnrolled {
		t.Fatalf("a different Manager kept the old secret: %+v", other)
	}
}

// Two loopback tunnels on one port but different loopback addresses (or
// spellings) are two Managers: the second never gets the first one's
// enrollment, and never the first one's secret, not even after it answers
// 403 to the first one's poll token (review 2 finding F4, the reviewer's
// probe-K).
func TestEnrollLoopbackTunnelsDoNotShareEnrollment(t *testing.T) {
	first, _ := realManager(t) // 127.0.0.1:<port>
	ctx := context.Background()
	id := Identity{InstanceID: instanceA, Host: "mac", DeviceDigest: digestMac}
	r := Step(ctx, first.Client(), first.URL, id, State{})
	r = Step(ctx, first.Client(), first.URL, id, r.State)
	if r.Phase != Waiting || r.State.PollToken == "" {
		t.Fatalf("first NAS: phase %s %q", r.Phase, r.Message)
	}
	orig := r.State.Secret
	port := first.URL[strings.LastIndex(first.URL, ":")+1:]

	// The second NAS's tunnel listens on [::1] with the same port. It
	// records every secret it is sent and refuses unknown poll tokens.
	var mu sync.Mutex
	var seen []string
	second := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var body enrollRequest
		_ = json.NewDecoder(req.Body).Decode(&body)
		mu.Lock()
		if body.Secret != "" {
			seen = append(seen, body.Secret)
		}
		mu.Unlock()
		w.WriteHeader(http.StatusForbidden)
	}))
	ln, err := net.Listen("tcp", "[::1]:"+port)
	if err != nil {
		t.Skipf("cannot listen on [::1]:%s: %v", port, err)
	}
	second.Listener = ln
	second.Start()
	defer second.Close()

	if r.State.ManagerID == "" {
		t.Fatal("the first NAS's enrollment is not bound to its Manager ID")
	}
	// Bound to the first Manager's ID. The second tunnel answers without that
	// ID: a fresh enrollment. No listener: retried, nothing sent.
	if st := Step(ctx, second.Client(), "http://[::1]:"+port, id, r.State); st.State.Secret == orig || !st.Persist || st.Phase != NotEnrolled {
		t.Fatalf("[::1]: the second tunnel kept the first NAS's enrollment: %+v", st)
	}
	sctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	if st := Step(sctx, second.Client(), "http://127.0.0.2:"+port, id, r.State); st.Persist || st.Phase != Failed {
		t.Fatalf("127.0.0.2 (no listener): %+v", st)
	}
	cancel()

	// Unbound (an old Manager that sends no ID): the address rule (F4).
	unbound := r.State
	unbound.ManagerID = ""
	for _, alt := range []string{
		"http://[::1]:" + port,
		"http://localhost:" + port,
		"http://127.0.0.2:" + port, // no listener: the key alone must differ
	} {
		st := unbound
		for i := 0; i < 3; i++ {
			sctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			next := Step(sctx, second.Client(), alt, id, st)
			cancel()
			if next.Persist {
				st = next.State
			}
		}
		if st.Secret == orig {
			t.Fatalf("%s: the second tunnel kept the first NAS's enrollment (secret)", alt)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for _, s := range seen {
		if s == orig {
			t.Fatal("the first NAS's secret was sent through the second tunnel")
		}
	}
}

func TestManagerKey(t *testing.T) {
	same := [][]string{
		{"http://127.0.0.1:30190", "http://127.0.0.1:30190/", "HTTP://127.0.0.1:30190//"},
		{"http://localhost:30190", "http://LOCALHOST:30190/", "http://localhost.:30190"},
		{"http://[::1]:30190", "http://[0:0:0:0:0:0:0:1]:30190"},
		{"http://nas.local:30190", "HTTP://NAS.local:30190/", "http://nas.local.:30190", "  http://nas.local:30190//  "},
		{"http://nas.local", "http://nas.local:80", "http://nas.local/"},
		{"https://manager.example", "https://manager.example:443/"},
		{"http://[fe80::0001]:30190", "http://[FE80::1]:30190"},
		{"http://nas.local:30190/jm", "http://nas.local:30190/jm/"},
	}
	for _, group := range same {
		k0, err := ManagerKey(group[0])
		if err != nil {
			t.Fatalf("%s: %v", group[0], err)
		}
		for _, s := range group[1:] {
			if k, err := ManagerKey(s); err != nil || k != k0 {
				t.Fatalf("ManagerKey(%q) = %q, %v; want %q (same as %q)", s, k, err, k0, group[0])
			}
		}
	}
	different := [][2]string{
		{"http://nas.local:30190", "http://nas.local:30191"},
		{"http://nas.local:30190", "https://nas.local:30190"},
		{"http://nas.local:30190", "http://192.168.1.10:30190"},
		{"http://nas.local", "http://nas.local:443"},
		{"http://nas.local:30190/a", "http://nas.local:30190/b"},
		// Each loopback address and spelling can be a tunnel to another NAS.
		{"http://127.0.0.1:30190", "http://localhost:30190"},
		{"http://127.0.0.1:30190", "http://[::1]:30190"},
		{"http://127.0.0.1:30190", "http://127.0.0.2:30190"},
		{"http://localhost:30190", "http://[::1]:30190"},
	}
	for _, d := range different {
		a, _ := ManagerKey(d[0])
		b, _ := ManagerKey(d[1])
		if a == b {
			t.Fatalf("%q and %q map to the same key %q", d[0], d[1], a)
		}
	}
	if _, err := ManagerKey("ftp://nas.local"); err == nil {
		t.Fatal("ManagerKey accepted a non-http scheme")
	}
}
