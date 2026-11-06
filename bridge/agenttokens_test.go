package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/agentmcp"
	"github.com/lelanddutcher/juicemount/internal/credstore"
)

const (
	agentTestPoll   = "poll-token-value-not-for-output-0123456789"
	agentTestID     = "fedcba9876543210"
	agentTestSecret = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
	agentTestToken  = "jma_" + agentTestID + "_" + agentTestSecret
)

type agentReq struct {
	key    string
	header http.Header
	body   map[string]any
}

type agentAnswer struct {
	status int
	body   any
	drop   bool
}

// agentManager is a fake Manager: answers queued per "METHOD /path".
type agentManager struct {
	mu      sync.Mutex
	answers map[string][]agentAnswer
	got     []agentReq
	srv     *httptest.Server
}

func newAgentManager(t *testing.T) *agentManager {
	f := &agentManager{answers: map[string][]agentAnswer{}}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := agentReq{key: r.Method + " " + r.URL.Path, header: r.Header.Clone()}
		if b, _ := io.ReadAll(r.Body); len(b) > 0 {
			_ = json.Unmarshal(b, &req.body)
		}
		f.mu.Lock()
		f.got = append(f.got, req)
		a := agentAnswer{status: 404, body: map[string]string{"error": "no such route"}}
		if q := f.answers[req.key]; len(q) > 0 {
			a, f.answers[req.key] = q[0], q[1:]
		}
		f.mu.Unlock()
		if a.drop {
			if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
				conn.Close()
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(a.status)
		_ = json.NewEncoder(w).Encode(a.body)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *agentManager) on(key string, a agentAnswer) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.answers[key] = append(f.answers[key], a)
}

func (f *agentManager) requests() []agentReq {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]agentReq(nil), f.got...)
}

// agentTokenEnv keeps the agent token in a private file under a temporary
// config directory (the off-darwin form of the item serve reads) and
// returns a fake Manager.
func agentTokenEnv(t *testing.T) *agentManager {
	t.Helper()
	if runtime.GOOS == "darwin" {
		t.Skip("the agent token is kept in the Keychain on darwin")
	}
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)
	t.Setenv("HOME", dir)
	t.Setenv("JM_APP_ISOLATION", "")
	t.Setenv("JM_APP_ISOLATION_ROOT", "")
	os.Unsetenv("JM_APP_ISOLATION")
	os.Unsetenv("JM_APP_ISOLATION_ROOT")
	return newAgentManager(t)
}

func agentInput(t *testing.T, f *agentManager, status string, extra map[string]any) string {
	t.Helper()
	in := map[string]any{"state": map[string]any{"manager_url": f.srv.URL, "manager_id": "m1", "instance_id": "5A000000-7B3D-4C1A-9E2B-0A1B2C3D4E5F",
		"secret": "enroll-secret-not-for-output", "poll_token": agentTestPoll, "principal": "mac-5a0000007b3d", "status": status}}
	for k, v := range extra {
		in[k] = v
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func mintedAnswer(id, token string) agentAnswer {
	return agentAnswer{status: 200, body: map[string]any{"token": token, "agent_token": map[string]any{"id": id, "name": "Claude Desktop",
		"person": "p1", "person_name": "Ann", "mac": "mac-5a0000007b3d", "created_at_ms": 1_790_000_000_000, "expires_at_ms": 1_797_776_000_000}}}
}

func decodeAgentOutput(t *testing.T, raw string) agentTokenOutput {
	t.Helper()
	for _, secret := range []string{agentTestSecret, agentTestPoll, "enroll-secret-not-for-output"} {
		if strings.Contains(raw, secret) {
			t.Fatalf("output holds a secret: %s", raw)
		}
	}
	var out agentTokenOutput
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatalf("output %q: %v", raw, err)
	}
	return out
}

func storedRecord(t *testing.T) (agentmcp.TokenRecord, error) {
	t.Helper()
	raw, err := credstore.LoadAgentToken() // what juicemount-mcp serve reads
	if err != nil {
		return agentmcp.TokenRecord{}, err
	}
	return agentmcp.ParseTokenRecord(raw)
}

// The app mints with its enrollment record, as this Mac, and the token goes
// into the item juicemount-mcp serve reads; the output never holds it.
func TestAgentTokenCreateStoresForServe(t *testing.T) {
	f := agentTokenEnv(t)
	f.on("POST /api/agent-tokens", mintedAnswer(agentTestID, agentTestToken))
	out := decodeAgentOutput(t, agentTokenCreate(agentInput(t, f, "approved", map[string]any{"name": " Claude Desktop ", "days": 30})))
	if !out.OK || out.Token == nil || out.Token.ID != agentTestID || out.StoredID != agentTestID || out.ReplacedID != "" || out.Warning != "" {
		t.Fatalf("output %+v", out)
	}
	got := f.requests()
	if len(got) != 1 || got[0].body["name"] != "Claude Desktop" || got[0].body["expires_in_days"] != 30.0 {
		t.Fatalf("mint request %+v", got)
	}
	if h := got[0].header; h.Get("X-JuiceMount-Mac-Instance") != "5A000000-7B3D-4C1A-9E2B-0A1B2C3D4E5F" ||
		h.Get("X-JuiceMount-Mac-Token") != agentTestPoll || h.Get("Authorization") != "" {
		t.Fatalf("not sent as this Mac: %v", h)
	}
	rec, err := storedRecord(t)
	if err != nil || rec.Token != agentTestToken || rec.ID != agentTestID || rec.ManagerURL != f.srv.URL || rec.Name != "Claude Desktop" {
		t.Fatalf("stored %+v %v", rec, err)
	}
	// Minting again replaces it here and names the one replaced.
	const id2 = "0123456789abcdef"
	f.on("POST /api/agent-tokens", mintedAnswer(id2, "jma_"+id2+"_"+strings.Repeat("ab", 32)))
	out = decodeAgentOutput(t, agentTokenCreate(agentInput(t, f, "approved", map[string]any{"name": "Claude Code"})))
	if !out.OK || out.ReplacedID != agentTestID || out.StoredID != id2 {
		t.Fatalf("second mint %+v", out)
	}
	if _, ok := f.requests()[1].body["expires_in_days"]; ok {
		t.Fatal("the default lifetime was sent explicitly")
	}
	if rec, err := storedRecord(t); err != nil || rec.ID != id2 {
		t.Fatalf("stored after the second mint %+v %v", rec, err)
	}
}

func TestAgentTokenCreateRefusals(t *testing.T) {
	f := agentTokenEnv(t)
	for _, c := range []struct {
		name, status string
		extra        map[string]any
		want         string
	}{
		{"pending enrollment", "pending", map[string]any{"name": "x"}, agentErrNotEnrolled},
		{"no name", "approved", map[string]any{"name": "  "}, agentErrInvalid},
		{"91 days", "approved", map[string]any{"name": "x", "days": 91}, agentErrInvalid},
	} {
		if out := decodeAgentOutput(t, agentTokenCreate(agentInput(t, f, c.status, c.extra))); out.OK || out.Error != c.want {
			t.Errorf("%s: %+v", c.name, out)
		}
	}
	if out := decodeAgentOutput(t, agentTokenCreate(`{"name":"x"}`)); out.Error != agentErrNotEnrolled {
		t.Errorf("no state: %+v", out)
	}
	if out := decodeAgentOutput(t, agentTokenCreate(`{`)); out.Error != agentErrInvalid {
		t.Errorf("bad JSON: %+v", out)
	}
	if n := len(f.requests()); n != 0 {
		t.Fatalf("%d requests sent for refused input", n)
	}
	for _, c := range []struct {
		answer agentAnswer
		want   string
	}{
		{agentAnswer{status: 403, body: map[string]string{"error": "this Mac belongs to no person"}}, agentErrRefused},
		{agentAnswer{status: 404, body: map[string]string{"error": "continuity is off (JM_CONTINUITY)"}}, agentErrContinuityOff},
		{agentAnswer{status: 409, body: map[string]string{"error": "Ann already has 10 agent tokens"}}, agentErrManager},
		{agentAnswer{status: 502, body: map[string]string{"error": "bad gateway"}}, agentErrMaybeMinted},
		{agentAnswer{drop: true}, agentErrMaybeMinted},
	} {
		f.on("POST /api/agent-tokens", c.answer)
		out := decodeAgentOutput(t, agentTokenCreate(agentInput(t, f, "approved", map[string]any{"name": "x"})))
		if out.OK || out.Error != c.want {
			t.Errorf("%+v: %+v", c.answer, out)
		}
		if c.want == agentErrManager && out.Detail != "Ann already has 10 agent tokens" {
			t.Errorf("detail %q", out.Detail)
		}
	}
	if _, err := storedRecord(t); !errors.Is(err, credstore.ErrNotFound) {
		t.Fatalf("something was stored: %v", err)
	}
}

type failingStore struct{ prev *agentmcp.TokenRecord }

func (s failingStore) Save(agentmcp.TokenRecord) error {
	if s.prev != nil {
		return agentmcp.ErrPreviousTokenRemoved
	}
	return errors.New("keychain locked")
}

func (s failingStore) Load() (agentmcp.TokenRecord, error) {
	if s.prev != nil {
		return *s.prev, nil
	}
	return agentmcp.TokenRecord{}, agentmcp.ErrNoToken
}

func (failingStore) Delete() error { return nil }

// A token that cannot be stored is revoked, so nobody holds a live token.
func TestAgentTokenCreateUnstoredIsRevoked(t *testing.T) {
	f := agentTokenEnv(t)
	prevStore := agentTokenStore
	t.Cleanup(func() { agentTokenStore = prevStore })
	agentTokenStore = func() agentmcp.TokenStore { return failingStore{} }
	f.on("POST /api/agent-tokens", mintedAnswer(agentTestID, agentTestToken))
	f.on("POST /api/agent-tokens/"+agentTestID+"/revoke", agentAnswer{status: 200, body: map[string]string{"revoked": agentTestID}})
	out := decodeAgentOutput(t, agentTokenCreate(agentInput(t, f, "approved", map[string]any{"name": "x"})))
	if out.OK || out.Error != agentErrStoreFailed || out.Token != nil {
		t.Fatalf("output %+v", out)
	}
	if got := f.requests(); len(got) != 2 || got[1].key != "POST /api/agent-tokens/"+agentTestID+"/revoke" {
		t.Fatalf("requests %+v", got)
	}
	// The revoke fails too: the output names the live token to revoke, and
	// the token stored before, which the failed store removed.
	prev := &agentmcp.TokenRecord{ID: "1111111111111111"}
	agentTokenStore = func() agentmcp.TokenStore { return failingStore{prev: prev} }
	f.on("POST /api/agent-tokens", mintedAnswer(agentTestID, agentTestToken))
	f.on("POST /api/agent-tokens/"+agentTestID+"/revoke", agentAnswer{status: 500, body: map[string]string{"error": "boom"}})
	out = decodeAgentOutput(t, agentTokenCreate(agentInput(t, f, "approved", map[string]any{"name": "x"})))
	if out.OK || out.Error != agentErrStoreLive || out.Token == nil || out.Token.ID != agentTestID || out.ReplacedID != prev.ID || !out.PreviousRemoved {
		t.Fatalf("revoke failed too: %+v", out)
	}
	// The previous token could not be read first (the person denied the
	// prompt) but the store removed it: the output still says so.
	agentTokenStore = func() agentmcp.TokenStore { return unreadableRemovingStore{} }
	f.on("POST /api/agent-tokens", mintedAnswer(agentTestID, agentTestToken))
	f.on("POST /api/agent-tokens/"+agentTestID+"/revoke", agentAnswer{status: 200, body: map[string]string{"revoked": agentTestID}})
	out = decodeAgentOutput(t, agentTokenCreate(agentInput(t, f, "approved", map[string]any{"name": "x"})))
	if out.OK || out.Error != agentErrStoreFailed || !out.PreviousRemoved || out.ReplacedID != "" {
		t.Fatalf("unread previous removed: %+v", out)
	}
}

// unreadableRemovingStore could not read the token it held, then removed
// it and failed to store the new one.
type unreadableRemovingStore struct{}

func (unreadableRemovingStore) Save(agentmcp.TokenRecord) error {
	return agentmcp.ErrPreviousTokenRemoved
}
func (unreadableRemovingStore) Load() (agentmcp.TokenRecord, error) {
	return agentmcp.TokenRecord{}, errors.New("keychain read refused (OSStatus -128)")
}
func (unreadableRemovingStore) Delete() error { return nil }

// unreadStore is the app's store with reading the token made a failure:
// the app tells the stored token by the ID kept beside it, so listing,
// creating and revoking never read it (no Keychain prompt).
type unreadStore struct {
	appTokenStore
	t *testing.T
}

func (s unreadStore) Load() (agentmcp.TokenRecord, error) {
	s.t.Error("the stored token was read")
	return agentmcp.TokenRecord{}, errors.New("read")
}

func TestAgentTokenAppNeverReadsTheToken(t *testing.T) {
	f := agentTokenEnv(t)
	prevStore := agentTokenStore
	t.Cleanup(func() { agentTokenStore = prevStore })
	agentTokenStore = func() agentmcp.TokenStore { return unreadStore{appTokenStore{credstore.AppAgentToken{}}, t} }
	f.on("POST /api/agent-tokens", mintedAnswer(agentTestID, agentTestToken))
	if out := decodeAgentOutput(t, agentTokenCreate(agentInput(t, f, "approved", map[string]any{"name": "x"}))); !out.OK || out.StoredID != agentTestID {
		t.Fatalf("create %+v", out)
	}
	const id2 = "0123456789abcdef"
	f.on("POST /api/agent-tokens", mintedAnswer(id2, "jma_"+id2+"_"+strings.Repeat("ab", 32)))
	if out := decodeAgentOutput(t, agentTokenCreate(agentInput(t, f, "approved", map[string]any{"name": "y"}))); !out.OK || out.ReplacedID != agentTestID {
		t.Fatalf("second create %+v", out)
	}
	f.on("GET /api/agent-tokens", agentAnswer{status: 200, body: map[string]any{"agent_tokens": []any{}}})
	if out := decodeAgentOutput(t, agentTokenList(agentInput(t, f, "approved", nil))); !out.OK || out.StoredID != id2 {
		t.Fatalf("list %+v", out)
	}
	f.on("POST /api/agent-tokens/"+id2+"/revoke", agentAnswer{status: 200, body: map[string]string{"revoked": id2}})
	if out := decodeAgentOutput(t, agentTokenRevoke(agentInput(t, f, "approved", map[string]any{"id": id2}))); !out.OK || !out.Deleted {
		t.Fatalf("revoke %+v", out)
	}
	if _, err := storedRecord(t); !errors.Is(err, credstore.ErrNotFound) {
		t.Fatalf("still stored: %v", err)
	}
	// A token stored before its ID was kept beside it is unknown: listed as
	// not on this Mac, never read, and not deleted on a guess.
	if err := credstore.SaveAgentToken(agentmcp.TokenRecord{ManagerURL: f.srv.URL, Token: agentTestToken, ID: agentTestID}.Encode(), ""); err != nil {
		t.Fatal(err)
	}
	f.on("GET /api/agent-tokens", agentAnswer{status: 200, body: map[string]any{"agent_tokens": []any{}}})
	if out := decodeAgentOutput(t, agentTokenList(agentInput(t, f, "approved", nil))); !out.OK || out.StoredID != "" {
		t.Fatalf("legacy list %+v", out)
	}
	f.on("POST /api/agent-tokens/"+agentTestID+"/revoke", agentAnswer{status: 200, body: map[string]string{"revoked": agentTestID}})
	if out := decodeAgentOutput(t, agentTokenRevoke(agentInput(t, f, "approved", map[string]any{"id": agentTestID}))); !out.OK || out.Deleted {
		t.Fatalf("legacy revoke %+v", out)
	}
}

// Names and errors the Manager supplies reach the app without control or
// format characters (bidi overrides, zero-width).
func TestAgentTokenOutputCleansManagerText(t *testing.T) {
	f := agentTokenEnv(t)
	f.on("GET /api/agent-tokens", agentAnswer{status: 200, body: map[string]any{"agent_tokens": []any{
		map[string]any{"id": "aaaaaaaaaaaaaaaa", "name": "cl\u202eaude\u200b\u0007", "person_name": "A\u2066nn"},
	}}})
	raw := agentTokenList(agentInput(t, f, "approved", nil))
	out := decodeAgentOutput(t, raw)
	if !out.OK || len(out.Tokens) != 1 || out.Tokens[0].Name != "claude " || out.Tokens[0].PersonName != "Ann" {
		t.Fatalf("list %q", raw)
	}
	f.on("POST /api/agent-tokens", agentAnswer{status: 409, body: map[string]string{"error": "too \u202emany"}})
	if out := decodeAgentOutput(t, agentTokenCreate(agentInput(t, f, "approved", map[string]any{"name": "x"}))); out.Detail != "too many" {
		t.Fatalf("detail %q", out.Detail)
	}
}

// Only what minting needs of the enrollment record is used: a record
// without its secret mints.
func TestAgentTokenStateNeedsNoSecret(t *testing.T) {
	f := agentTokenEnv(t)
	f.on("POST /api/agent-tokens", mintedAnswer(agentTestID, agentTestToken))
	in := `{"name":"x","state":{"manager_url":"` + f.srv.URL + `","instance_id":"I","poll_token":"` + agentTestPoll + `","status":"approved"}}`
	if out := decodeAgentOutput(t, agentTokenCreate(in)); !out.OK {
		t.Fatalf("create %+v", out)
	}
}

// The list says whether the Manager has agent tokens on: a 404 is
// "continuity off", not an error.
func TestAgentTokenListContinuity(t *testing.T) {
	f := agentTokenEnv(t)
	f.on("GET /api/agent-tokens", agentAnswer{status: 404, body: map[string]string{"error": "continuity is off (JM_CONTINUITY)"}})
	if out := decodeAgentOutput(t, agentTokenList(agentInput(t, f, "approved", nil))); !out.OK || out.Continuity || len(out.Tokens) != 0 {
		t.Fatalf("continuity off: %+v", out)
	}
	f.on("POST /api/agent-tokens", mintedAnswer(agentTestID, agentTestToken))
	if out := decodeAgentOutput(t, agentTokenCreate(agentInput(t, f, "approved", map[string]any{"name": "x"}))); !out.OK {
		t.Fatalf("mint %+v", out)
	}
	f.on("GET /api/agent-tokens", agentAnswer{status: 200, body: map[string]any{"agent_tokens": []any{
		mintedAnswer(agentTestID, "").body.(map[string]any)["agent_token"], map[string]any{"id": "aaaaaaaaaaaaaaaa", "name": "cli", "created_at_ms": 1, "expires_at_ms": 2},
	}}})
	out := decodeAgentOutput(t, agentTokenList(agentInput(t, f, "approved", nil)))
	if !out.OK || !out.Continuity || len(out.Tokens) != 2 || out.Tokens[1].Name != "cli" || out.StoredID != agentTestID {
		t.Fatalf("list %+v", out)
	}
	f.on("GET /api/agent-tokens", agentAnswer{status: 403, body: map[string]string{"error": "this Mac belongs to no person"}})
	if out := decodeAgentOutput(t, agentTokenList(agentInput(t, f, "approved", nil))); out.OK || out.Error != agentErrRefused {
		t.Fatalf("403: %+v", out)
	}
	if out := decodeAgentOutput(t, agentTokenList(agentInput(t, f, "waiting", nil))); out.OK || out.Error != agentErrNotEnrolled {
		t.Fatalf("not approved: %+v", out)
	}
	f.srv.Close()
	if out := decodeAgentOutput(t, agentTokenList(agentInput(t, f, "approved", nil))); out.OK || out.Error != agentErrUnreachable {
		t.Fatalf("unreachable: %+v", out)
	}
}

// Revoking the token stored here deletes it here too, so serve stops using
// it; revoking another keeps it.
func TestAgentTokenRevoke(t *testing.T) {
	f := agentTokenEnv(t)
	f.on("POST /api/agent-tokens", mintedAnswer(agentTestID, agentTestToken))
	if out := decodeAgentOutput(t, agentTokenCreate(agentInput(t, f, "approved", map[string]any{"name": "x"}))); !out.OK {
		t.Fatalf("mint %+v", out)
	}
	f.on("POST /api/agent-tokens/aaaaaaaaaaaaaaaa/revoke", agentAnswer{status: 200, body: map[string]string{"revoked": "aaaaaaaaaaaaaaaa"}})
	out := decodeAgentOutput(t, agentTokenRevoke(agentInput(t, f, "approved", map[string]any{"id": "aaaaaaaaaaaaaaaa"})))
	if !out.OK || out.Deleted || out.Gone {
		t.Fatalf("revoke other: %+v", out)
	}
	if _, err := storedRecord(t); err != nil {
		t.Fatalf("revoking another token removed the stored one: %v", err)
	}
	// A Manager failure deletes nothing.
	f.on("POST /api/agent-tokens/"+agentTestID+"/revoke", agentAnswer{status: 500, body: map[string]string{"error": "boom"}})
	if out := decodeAgentOutput(t, agentTokenRevoke(agentInput(t, f, "approved", map[string]any{"id": agentTestID}))); out.OK || out.Error != agentErrManager {
		t.Fatalf("500: %+v", out)
	}
	if _, err := storedRecord(t); err != nil {
		t.Fatalf("a failed revoke removed the stored token: %v", err)
	}
	// Already gone on the Manager (404) still deletes it here.
	f.on("POST /api/agent-tokens/"+agentTestID+"/revoke", agentAnswer{status: 404, body: map[string]string{"error": "no such agent token"}})
	out = decodeAgentOutput(t, agentTokenRevoke(agentInput(t, f, "approved", map[string]any{"id": agentTestID})))
	if !out.OK || !out.Gone || !out.Deleted {
		t.Fatalf("revoke stored: %+v", out)
	}
	if _, err := storedRecord(t); !errors.Is(err, credstore.ErrNotFound) {
		t.Fatalf("still stored: %v", err)
	}
	n := len(f.requests())
	if out := decodeAgentOutput(t, agentTokenRevoke(agentInput(t, f, "approved", map[string]any{"id": "../x"}))); out.OK || out.Error != agentErrInvalid {
		t.Fatalf("bad ID: %+v", out)
	}
	if len(f.requests()) != n {
		t.Fatal("a bad ID was sent")
	}
}
