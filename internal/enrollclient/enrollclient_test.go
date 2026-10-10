package enrollclient

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/manager"
)

const (
	adminKey   = "0123456789abcdef0123456789abcdef-admin"
	instanceA  = "4F1C2A9E-7B3D-4C1A-9E2B-0A1B2C3D4E5F"
	instanceB  = "9A8B7C6D-5E4F-4A3B-8C2D-1E0F9A8B7C6D"
	digestMac  = "0123456789abcdef0123456789abcdef"
	clientAK   = "jm-client"
	clientSK   = "clientsecret0123456789abcdef0123456789ab"
	principalA = "mac-4f1c2a9e7b3d"
)

// realManager runs the real Manager HTTP API (enrollment store + proxy
// principals file) on a loopback test server, with a client object key.
func realManager(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	mux, proxyDir := realManagerMux(t)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, proxyDir
}

// realManagerMux is realManager's handler, for serving one Manager on
// several addresses or swapping Managers behind one address.
func realManagerMux(t *testing.T) (*http.ServeMux, string) {
	t.Helper()
	dir := t.TempDir()
	proxyDir := filepath.Join(dir, "respproxy")
	if err := os.MkdirAll(proxyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("JM_RESPPROXY_DIR", proxyDir)
	t.Setenv("JM_META_ADMIN_PASSWORD", "")
	t.Setenv("JM_OBJ_CLIENT_ACCESS_KEY", clientAK)
	t.Setenv("JM_OBJ_CLIENT_SECRET_KEY", clientSK)
	mux := http.NewServeMux()
	mgr := manager.Register(mux, "", manager.Config{
		JuiceFSBin: "/dev/null", FUSEMount: filepath.Join(dir, "jfs"), SourceRoots: []string{"/sources"},
		DestMount: "/jfs", AdminKey: adminKey, StateFile: filepath.Join(dir, "state.json"),
	})
	t.Cleanup(mgr.StopAll)
	startFakeProxyAck(t, proxyDir)
	return mux, proxyDir
}

// startFakeProxyAck models the applied-credential handshake: a request is
// acknowledged only after its digest matches the current principals file.
// The Manager requires reload.applied and a current heartbeat for approval.
func startFakeProxyAck(t *testing.T, dir string) {
	t.Helper()
	stop, done := make(chan struct{}), make(chan struct{})
	last := ""
	if b, err := os.ReadFile(filepath.Join(dir, "reload.request")); err == nil {
		last = string(b) // a newly started proxy cannot attest an earlier request
	}
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
			}
			req, err := os.ReadFile(filepath.Join(dir, "reload.request"))
			if err != nil || string(req) == last {
				_ = os.WriteFile(filepath.Join(dir, "reload.heartbeat"), []byte("1\n"), 0o600)
				continue
			}
			fields := strings.Fields(string(req))
			body, err := os.ReadFile(filepath.Join(dir, "principals.json"))
			if len(fields) != 3 || err != nil {
				_ = os.WriteFile(filepath.Join(dir, "reload.heartbeat"), []byte("1\n"), 0o600)
				continue
			}
			digest := sha256.Sum256(body)
			if hex.EncodeToString(digest[:]) != fields[0] {
				_ = os.WriteFile(filepath.Join(dir, "reload.heartbeat"), []byte("1\n"), 0o600)
				continue
			}
			applied := []byte(strings.Join(fields, " ") + "\n")
			if os.WriteFile(filepath.Join(dir, "reload.applied"), applied, 0o600) == nil {
				_ = os.WriteFile(filepath.Join(dir, "reload.ack"), applied, 0o600)
				last = string(req)
			}
			_ = os.WriteFile(filepath.Join(dir, "reload.heartbeat"), []byte("1\n"), 0o600)
		}
	}()
	t.Cleanup(func() { close(stop); <-done })
}

func admin(t *testing.T, srv *httptest.Server, op, name string) {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"name": name})
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/principals/"+op, bytes.NewReader(body))
	req.Header.Set("X-JuiceMount-Admin-Key", adminKey)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("admin %s %s: %d %s", op, name, resp.StatusCode, b)
	}
}

func listPrincipals(t *testing.T, srv *httptest.Server) manager.PrincipalsSnapshot {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/principals", nil)
	req.Header.Set("X-JuiceMount-Admin-Key", adminKey)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var snap manager.PrincipalsSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatal(err)
	}
	return snap
}

// recorder wraps a Doer and keeps every request body and URL.
type recorder struct {
	mu     sync.Mutex
	inner  Doer
	bodies []string
	urls   []string
}

func (r *recorder) Do(req *http.Request) (*http.Response, error) {
	b, _ := io.ReadAll(req.Body)
	req.Body = io.NopCloser(bytes.NewReader(b))
	r.mu.Lock()
	r.bodies = append(r.bodies, string(b))
	r.urls = append(r.urls, req.URL.String())
	r.mu.Unlock()
	return r.inner.Do(req)
}

func (r *recorder) count() int { r.mu.Lock(); defer r.mu.Unlock(); return len(r.bodies) }

func (r *recorder) last() map[string]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var m map[string]string
	_ = json.Unmarshal([]byte(r.bodies[len(r.bodies)-1]), &m)
	return m
}

// assertNoSecretInMessage: no user-visible text ever carries a secret.
func assertNoSecretInMessage(t *testing.T, r Result, secrets ...string) {
	t.Helper()
	for _, s := range secrets {
		if s != "" && strings.Contains(r.Message, s) {
			t.Fatalf("Result.Message carries a secret: %q", r.Message)
		}
	}
}

// TestEnrollPendingApproveRevoke drives the client against the real Manager:
// the first step only makes a secret; the request is pending until an admin
// approves it; only then does the client learn the object key; a revoke is
// noticed on the next poll.
func TestEnrollPendingApproveRevoke(t *testing.T) {
	srv, _ := realManager(t)
	rec := &recorder{inner: srv.Client()}
	id := Identity{InstanceID: instanceA, Host: "Edit-Bay-2", Label: "Edit Bay 2", DeviceDigest: digestMac}
	ctx := context.Background()

	// 1. No secret: one is made and must be saved first. Nothing is sent.
	r := Step(ctx, rec, srv.URL+"/", id, State{})
	if rec.count() != 0 || !r.Persist || r.State.Secret == "" || r.Phase != NotEnrolled || r.RetryAfterMS != 0 {
		t.Fatalf("first step sent %d requests / %+v; want only a persisted secret", rec.count(), r.Phase)
	}
	if r.State.ManagerURL != srv.URL {
		t.Fatalf("manager URL not normalized: %q", r.State.ManagerURL)
	}
	secret := r.State.Secret

	// 2. Enroll: pending, principal and fingerprint shown, poll token kept.
	r = Step(ctx, rec, srv.URL, id, r.State)
	if r.Phase != Waiting || r.State.Principal != principalA || r.State.Fingerprint == "" || r.State.PollToken == "" || !r.Persist {
		t.Fatalf("enroll: %+v", r)
	}
	if got := rec.last(); got["secret"] != secret || got["host"] != "Edit-Bay-2" || got["device_digest"] != digestMac {
		t.Fatalf("enroll body = %v", got)
	}
	if r.ObjectSecretKey != "" || r.RetryAfterMS != PendingPoll.Milliseconds() {
		t.Fatalf("pending result: object key %q, retry %d", r.ObjectSecretKey, r.RetryAfterMS)
	}
	token := r.State.PollToken

	// 3. Poll while pending: token only, the secret never crosses again.
	r = Step(ctx, rec, srv.URL, id, r.State)
	if r.Phase != Waiting || r.Persist {
		t.Fatalf("pending poll: %+v", r)
	}
	if got := rec.last(); got["secret"] != "" || got["poll_token"] != token {
		t.Fatalf("poll body carries the secret or lacks the token: %v", rec.bodies[len(rec.bodies)-1] != "")
	}

	// 4. Admin approves: the next poll is approved and carries the key.
	admin(t, srv, "approve", principalA)
	r = Step(ctx, rec, srv.URL, id, r.State)
	if r.Phase != Approved || r.ObjectAccessKey != clientAK || r.ObjectSecretKey != clientSK || !r.Persist {
		t.Fatalf("approved poll: phase %s key %q", r.Phase, r.ObjectAccessKey)
	}
	if r.ClearCloneNotice {
		t.Fatal("no clone notice was pending, yet the result asks to clear one")
	}
	if r.RetryAfterMS != ApprovedPoll.Milliseconds() {
		t.Fatalf("approved retry = %d", r.RetryAfterMS)
	}

	// 5. Admin revokes: the next poll says so and carries no key.
	admin(t, srv, "revoke", principalA)
	r = Step(ctx, rec, srv.URL, id, r.State)
	if r.Phase != Revoked || r.ObjectSecretKey != "" || r.RetryAfterMS != SettledPoll.Milliseconds() {
		t.Fatalf("revoked poll: %+v", r.Phase)
	}
	assertNoSecretInMessage(t, r, secret, token, clientSK)
}

// TestEnrollCloneReEnroll: after a clone the instance ID is rotated. The
// client drops the old request (it belongs to the other Mac's principal),
// enrolls the new ID with the previous ID and device digest, and on approval
// asks the caller to clear the clone notice.
func TestEnrollCloneReEnroll(t *testing.T) {
	srv, _ := realManager(t)
	rec := &recorder{inner: srv.Client()}
	ctx := context.Background()
	old := Identity{InstanceID: instanceA, Host: "Edit-Bay-2", DeviceDigest: digestMac}
	r := Step(ctx, rec, srv.URL, old, State{})
	r = Step(ctx, rec, srv.URL, old, r.State)
	admin(t, srv, "approve", principalA)
	r = Step(ctx, rec, srv.URL, old, r.State)
	if r.Phase != Approved {
		t.Fatalf("original Mac not approved: %s", r.Phase)
	}
	copied := r.State // the clone carries this state over (Keychain copy)

	cloned := Identity{InstanceID: instanceB, Host: "Edit-Bay-2", DeviceDigest: "fedcba9876543210fedcba9876543210", PreviousInstanceID: instanceA}
	r = Step(ctx, rec, srv.URL, cloned, copied)
	if !r.Persist || r.State.Secret == "" || r.State.Secret == copied.Secret || r.State.PollToken != "" || r.State.Principal != "" {
		t.Fatal("the rotated identity reused the other Mac's secret or token")
	}
	sent := rec.count()
	if r = Step(ctx, rec, srv.URL, cloned, r.State); r.Phase != Waiting || r.State.Principal == principalA {
		t.Fatalf("clone enroll: %s as %s", r.Phase, r.State.Principal)
	}
	body := rec.last()
	if rec.count() != sent+1 || body["previous_instance_id"] != instanceA || body["instance_id"] != instanceB || body["device_digest"] == "" {
		t.Fatalf("clone enroll body = %v", body)
	}
	p := listPrincipals(t, srv)
	var rec2 *manager.MacPrincipal
	for i := range p.Principals {
		if p.Principals[i].Name == r.State.Principal {
			rec2 = &p.Principals[i]
		}
	}
	if rec2 == nil || rec2.PreviousInstanceID != instanceA {
		t.Fatalf("Manager did not record the clone origin: %+v", rec2)
	}
	if r.ClearCloneNotice {
		t.Fatal("clone notice cleared before approval")
	}
	admin(t, srv, "approve", r.State.Principal)
	r = Step(ctx, rec, srv.URL, cloned, r.State)
	if r.Phase != Approved || !r.ClearCloneNotice {
		t.Fatalf("clone approval: phase %s clear=%v", r.Phase, r.ClearCloneNotice)
	}
}

// fakeManager answers with a scripted status code.
type fakeManager struct {
	mu      sync.Mutex
	codes   []int
	headers []http.Header
	bodies  []string
	seen    []map[string]string
}

func (f *fakeManager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var m map[string]string
	_ = json.NewDecoder(r.Body).Decode(&m)
	f.seen = append(f.seen, m)
	i := len(f.seen) - 1
	if i >= len(f.codes) {
		i = len(f.codes) - 1
	}
	for k, v := range f.headers[i] {
		w.Header()[k] = v
	}
	w.WriteHeader(f.codes[i])
	_, _ = io.WriteString(w, f.bodies[i])
}

func TestEnrollRateLimitedAndBusyBackOffBounded(t *testing.T) {
	for _, tc := range []struct {
		code   int
		header http.Header
		want   time.Duration
	}{
		{http.StatusTooManyRequests, http.Header{"Retry-After": {"30"}}, 30 * time.Second},
		{http.StatusServiceUnavailable, http.Header{"Retry-After": {"5"}}, ErrorBase},
		{http.StatusTooManyRequests, http.Header{"Retry-After": {"86400"}}, MaxBackoff},
		{http.StatusTooManyRequests, nil, ErrorBase},
		{http.StatusInternalServerError, nil, ErrorBase},
	} {
		f := &fakeManager{codes: []int{tc.code}, headers: []http.Header{tc.header}, bodies: []string{"secret-must-not-echo " + clientSK}}
		srv := httptest.NewServer(f)
		st := State{ManagerURL: srv.URL, InstanceID: instanceA, Secret: "s3cret-0123456789abcdef0123456789abcdef", PollToken: strings.Repeat("a", 64)}
		r := Step(context.Background(), srv.Client(), srv.URL, Identity{InstanceID: instanceA, Host: "h"}, st)
		srv.Close()
		if r.Phase != Failed || time.Duration(r.RetryAfterMS)*time.Millisecond != tc.want {
			t.Errorf("%d %v: phase %s retry %dms, want %v", tc.code, tc.header, r.Phase, r.RetryAfterMS, tc.want)
		}
		if r.State.Secret != st.Secret || r.State.PollToken != st.PollToken {
			t.Errorf("%d: a transient failure dropped the secret or poll token", tc.code)
		}
		assertNoSecretInMessage(t, r, st.Secret, st.PollToken, clientSK, "secret-must-not-echo")
	}

	// Repeated failures back off exponentially, never beyond MaxBackoff.
	srv := httptest.NewServer(&fakeManager{codes: []int{503}, headers: []http.Header{nil}, bodies: []string{""}})
	defer srv.Close()
	st := State{ManagerURL: srv.URL, InstanceID: instanceA, Secret: "s3cret-0123456789abcdef0123456789abcdef"}
	var waits []time.Duration
	for range 10 {
		r := Step(context.Background(), srv.Client(), srv.URL, Identity{InstanceID: instanceA, Host: "h"}, st)
		st = r.State
		waits = append(waits, time.Duration(r.RetryAfterMS)*time.Millisecond)
	}
	if waits[0] != ErrorBase || waits[1] != 2*ErrorBase || waits[9] != MaxBackoff {
		t.Fatalf("backoff sequence %v", waits)
	}
	for i := 1; i < len(waits); i++ {
		if waits[i] < waits[i-1] || waits[i] > MaxBackoff {
			t.Fatalf("backoff not monotone and bounded: %v", waits)
		}
	}

	// Unreachable Manager: Failed with backoff, message names the address only.
	dead := httptest.NewServer(http.NotFoundHandler())
	url := dead.URL
	dead.Close()
	r := Step(context.Background(), http.DefaultClient, url, Identity{InstanceID: instanceA, Host: "h"}, State{ManagerURL: url, InstanceID: instanceA, Secret: "s3cret-0123456789abcdef0123456789abcdef"})
	if r.Phase != Failed || r.RetryAfterMS != ErrorBase.Milliseconds() {
		t.Fatalf("unreachable: %+v", r)
	}
}

// A lost poll token (403) falls back to the secret at once; a conflict (409)
// stops automatic retries and tells the admin what to do.
func TestEnrollLostTokenAndConflict(t *testing.T) {
	f := &fakeManager{
		codes:   []int{http.StatusForbidden, http.StatusAccepted, http.StatusConflict},
		headers: []http.Header{nil, nil, nil},
		bodies:  []string{"", `{"status":"pending","principal":"mac-4f1c2a9e7b3d","fingerprint":"ABCD-1234","poll_token":"` + strings.Repeat("b", 64) + `"}`, ""},
	}
	srv := httptest.NewServer(f)
	defer srv.Close()
	id := Identity{InstanceID: instanceA, Host: "h"}
	st := State{ManagerURL: srv.URL, InstanceID: instanceA, Secret: "s3cret-0123456789abcdef0123456789abcdef", PollToken: strings.Repeat("a", 64), Status: "pending", Principal: principalA}
	r := Step(context.Background(), srv.Client(), srv.URL, id, st)
	if r.State.PollToken != "" || r.RetryAfterMS != 0 || !r.Persist || r.State.Secret != st.Secret {
		t.Fatalf("403 on a token poll: %+v", r)
	}
	r = Step(context.Background(), srv.Client(), srv.URL, id, r.State)
	if f.seen[1]["secret"] != st.Secret || r.Phase != Waiting || r.State.PollToken != strings.Repeat("b", 64) {
		t.Fatalf("re-enroll with the secret: %v / %+v", f.seen[1]["secret"] != "", r.Phase)
	}
	r.State.PollToken = ""
	r = Step(context.Background(), srv.Client(), srv.URL, id, r.State)
	if r.Phase != Conflict || r.RetryAfterMS != SettledPoll.Milliseconds() || !strings.Contains(r.Message, principalA) {
		t.Fatalf("409: %+v", r)
	}
	assertNoSecretInMessage(t, r, st.Secret)
}

func TestNormalizeManagerURL(t *testing.T) {
	for in, want := range map[string]string{
		" http://nas.local:30190/ ": "http://nas.local:30190",
		"https://nas.example":       "https://nas.example",
	} {
		if got, err := NormalizeManagerURL(in); err != nil || got != want {
			t.Errorf("%q -> %q %v", in, got, err)
		}
	}
	for _, bad := range []string{"", "nas:30190", "ftp://nas", "http://u:p@nas:30190", "http://nas?x=1", "redis://nas:6379"} {
		if _, err := NormalizeManagerURL(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestNewSecretMatchesManagerFormat(t *testing.T) {
	a, err := NewSecret()
	if err != nil {
		t.Fatal(err)
	}
	b, _ := NewSecret()
	if a == b || len(a) < 32 || strings.Trim(a, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._~-") != "" {
		t.Fatalf("secret %q is not a fresh URL-safe value", a)
	}
}

// A state that names this Manager and instance but holds no secret (the
// stored record lost it) never sends a request: a secret is made and must be
// saved first.
func TestEnrollNeverSendsWithoutSavedSecret(t *testing.T) {
	f := &fakeManager{codes: []int{http.StatusAccepted}, headers: []http.Header{nil}, bodies: []string{`{"status":"pending","principal":"mac-4f1c2a9e7b3d"}`}}
	srv := httptest.NewServer(f)
	defer srv.Close()
	st := State{ManagerURL: srv.URL, InstanceID: instanceA, PollToken: strings.Repeat("a", 64)}
	r := Step(context.Background(), srv.Client(), srv.URL, Identity{InstanceID: instanceA, Host: "h"}, st)
	if len(f.seen) != 0 || !r.Persist || r.State.Secret == "" || r.State.PollToken != "" {
		t.Fatalf("a request went out before a secret was saved (%d requests, persist=%v)", len(f.seen), r.Persist)
	}
}
