package agentmcp

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"strings"
	"testing"
)

// memStore is a TokenStore in memory.
type memStore struct {
	rec     *TokenRecord
	saveErr error
}

func (m *memStore) Save(r TokenRecord) error {
	if m.saveErr != nil {
		return m.saveErr
	}
	m.rec = &r
	return nil
}

func (m *memStore) Load() (TokenRecord, error) {
	if m.rec == nil {
		return TokenRecord{}, ErrNoToken
	}
	return *m.rec, nil
}

func (m *memStore) Delete() error { m.rec = nil; return nil }

const (
	testPollToken = "poll-token-value-not-for-logs-0123456789"
	mintedID      = "fedcba9876543210"
	mintedToken   = "jma_" + mintedID + "_" + "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
	mintedSecret  = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
)

type tokenEnv struct {
	f        *fakeManager
	store    *memStore
	tc       *TokenCommands
	out, err *bytes.Buffer
	logs     *bytes.Buffer
}

func newTokenEnv(t *testing.T) *tokenEnv {
	f, srv := newFakeManager(t)
	e := &tokenEnv{f: f, store: &memStore{}, out: &bytes.Buffer{}, err: &bytes.Buffer{}, logs: &bytes.Buffer{}}
	// Anything the package logs goes to the captured log.
	prev, prevFlags := log.Writer(), log.Flags()
	log.SetOutput(e.logs)
	t.Cleanup(func() { log.SetOutput(prev); log.SetFlags(prevFlags) })
	e.tc = &TokenCommands{Store: e.store, HTTP: NewHTTPClient(), Out: e.out, Err: e.err, Mac: func() (MacCredentials, error) {
		return MacCredentials{ManagerURL: srv.URL + "/manager", InstanceID: "5A000000-7B3D-4C1A-9E2B-0A1B2C3D4E5F", PollToken: testPollToken, Device: "0123456789abcdef0123456789abcdef"}, nil
	}}
	return e
}

// noSecrets fails when the token's secret or the poll token is in stderr or
// the log (and, unless allowed, stdout).
func (e *tokenEnv) noSecrets(t *testing.T, stdoutMayHold bool) {
	t.Helper()
	for name, b := range map[string]string{"stderr": e.err.String(), "log": e.logs.String(), "stdout": e.out.String()} {
		if name == "stdout" && stdoutMayHold {
			continue
		}
		if strings.Contains(b, mintedSecret) || strings.Contains(b, testPollToken) {
			t.Fatalf("%s holds a secret: %q", name, b)
		}
	}
}

func mintAnswer() map[string]any {
	return map[string]any{"token": mintedToken, "agent_token": map[string]any{"id": mintedID, "name": "Claude Desktop", "person": "p1",
		"person_name": "Ann", "mac": "mac-5a0000007b3d", "created_at_ms": 1_790_000_000_000, "expires_at_ms": 1_797_776_000_000}}
}

func (e *tokenEnv) checkMacAuth(t *testing.T, s seen) {
	t.Helper()
	if s.Header.Get("X-JuiceMount-Mac-Instance") != "5A000000-7B3D-4C1A-9E2B-0A1B2C3D4E5F" || s.Header.Get("X-JuiceMount-Mac-Token") != testPollToken ||
		s.Header.Get("X-JuiceMount-Mac-Device") != "0123456789abcdef0123456789abcdef" || s.Auth != "" {
		t.Fatalf("Mac credentials not sent as on the enrollment poll: %v", s.Header)
	}
}

func TestTokenCreateStoresWithoutPrinting(t *testing.T) {
	e := newTokenEnv(t)
	e.store.rec = &TokenRecord{ID: "1111111111111111", Token: "jma_1111111111111111_" + strings.Repeat("1", 64), ManagerURL: "http://old"}
	e.f.on("POST /manager/api/agent-tokens", 200, mintAnswer())
	if err := e.tc.Create(context.Background(), " Claude Desktop ", 30, false); err != nil {
		t.Fatal(err)
	}
	got := e.f.requests()
	if len(got) != 1 || got[0].Body["name"] != "Claude Desktop" || got[0].Body["expires_in_days"] != 30.0 {
		t.Fatalf("mint request: %+v", got)
	}
	e.checkMacAuth(t, got[0])
	r := e.store.rec
	if r == nil || r.Token != mintedToken || r.ID != mintedID || !strings.HasSuffix(r.ManagerURL, "/manager") || r.Name != "Claude Desktop" {
		t.Fatalf("stored %+v", r)
	}
	if !strings.Contains(e.out.String(), mintedID) || !strings.Contains(e.out.String(), "replaces token 1111111111111111") {
		t.Fatalf("stdout: %q", e.out)
	}
	e.noSecrets(t, false)
	// The stored record round-trips.
	if back, err := ParseTokenRecord(r.Encode()); err != nil || back != *r {
		t.Fatalf("round trip: %+v %v", back, err)
	}
}

func TestTokenCreatePrintOnlyToStdout(t *testing.T) {
	e := newTokenEnv(t)
	e.f.on("POST /manager/api/agent-tokens", 200, mintAnswer())
	if err := e.tc.Create(context.Background(), "headless", 0, true); err != nil {
		t.Fatal(err)
	}
	if e.out.String() != mintedToken+"\n" || e.store.rec != nil {
		t.Fatalf("stdout %q, stored %+v", e.out, e.store.rec)
	}
	if _, ok := e.f.requests()[0].Body["expires_in_days"]; ok {
		t.Fatal("default expiry sent explicitly")
	}
	e.noSecrets(t, true)
}

func TestTokenCreateUnstoredIsRevoked(t *testing.T) {
	e := newTokenEnv(t)
	e.store.saveErr = errors.New("keychain locked")
	e.f.on("POST /manager/api/agent-tokens", 200, mintAnswer())
	e.f.on("POST /manager/api/agent-tokens/"+mintedID+"/revoke", 200, map[string]string{"revoked": mintedID})
	err := e.tc.Create(context.Background(), "x", 0, false)
	if err == nil || !strings.Contains(err.Error(), "revoked on the Manager") || strings.Contains(err.Error(), mintedSecret) {
		t.Fatalf("err %v", err)
	}
	if got := e.f.requests(); len(got) != 2 || got[1].Path != "/manager/api/agent-tokens/"+mintedID+"/revoke" {
		t.Fatalf("requests %+v", got)
	}
	e.noSecrets(t, false)
}

func TestTokenCreateRefusals(t *testing.T) {
	e := newTokenEnv(t)
	e.f.on("POST /manager/api/agent-tokens", 403, map[string]string{"error": "this Mac belongs to no person"})
	err := e.tc.Create(context.Background(), "x", 0, false)
	if err == nil || !strings.Contains(err.Error(), "belongs to no person") || e.store.rec != nil {
		t.Fatalf("403: %v", err)
	}
	if err := e.tc.Create(context.Background(), "", 0, false); err == nil {
		t.Fatal("no name accepted")
	}
	if err := e.tc.Create(context.Background(), "x", 91, false); err == nil {
		t.Fatal("91 days accepted")
	}
	// A malformed token in the answer is refused.
	bad := mintAnswer()
	bad["token"] = "jma_0000000000000000_" + mintedSecret
	e.f.on("POST /manager/api/agent-tokens", 200, bad)
	if err := e.tc.Create(context.Background(), "x", 0, false); err == nil || e.store.rec != nil {
		t.Fatalf("mismatched ID: %v", err)
	}
	e.noSecrets(t, false)
}

func TestTokenListAndRevoke(t *testing.T) {
	e := newTokenEnv(t)
	e.store.rec = &TokenRecord{ID: mintedID, Token: mintedToken, ManagerURL: "http://nas.example:30190"}
	e.f.on("GET /manager/api/agent-tokens", 200, map[string]any{"agent_tokens": []any{
		mintAnswer()["agent_token"], map[string]any{"id": "aaaaaaaaaaaaaaaa", "name": "cli", "person_name": "Ann"},
	}})
	if err := e.tc.List(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.checkMacAuth(t, e.f.requests()[0])
	lines := strings.Split(e.out.String(), "\n")
	if !strings.HasPrefix(lines[1], "*") || !strings.Contains(lines[1], mintedID) || strings.HasPrefix(lines[2], "*") || !strings.Contains(e.out.String(), "stored on this Mac") {
		t.Fatalf("list:\n%s", e.out)
	}
	// Revoking another token keeps the stored one.
	e.f.on("POST /manager/api/agent-tokens/aaaaaaaaaaaaaaaa/revoke", 200, map[string]string{"revoked": "aaaaaaaaaaaaaaaa"})
	if err := e.tc.Revoke(context.Background(), "aaaaaaaaaaaaaaaa", false); err != nil || e.store.rec == nil {
		t.Fatalf("revoke other: %v %+v", err, e.store.rec)
	}
	// Revoking the stored one deletes it here too; a 404 (already gone on
	// the Manager) still deletes it.
	e.f.on("POST /manager/api/agent-tokens/"+mintedID+"/revoke", 404, map[string]string{"error": "no such agent token"})
	if err := e.tc.Revoke(context.Background(), mintedID, false); err != nil || e.store.rec != nil {
		t.Fatalf("revoke stored: %v %+v", err, e.store.rec)
	}
	if !strings.Contains(e.out.String(), "not on the Manager") || !strings.Contains(e.out.String(), "Deleted token "+mintedID) {
		t.Fatalf("stdout: %s", e.out)
	}
	e.checkMacAuth(t, e.f.requests()[2])
	// A Manager failure stops a revoke before anything is deleted.
	e.store.rec = &TokenRecord{ID: mintedID, Token: mintedToken}
	e.f.on("POST /manager/api/agent-tokens/"+mintedID+"/revoke", 500, map[string]string{"error": "boom"})
	if err := e.tc.Revoke(context.Background(), mintedID, false); err == nil || e.store.rec == nil {
		t.Fatalf("500: %v %+v", err, e.store.rec)
	}
	// --local-only needs no Manager or Mac.
	n := len(e.f.requests())
	e.tc.Mac = func() (MacCredentials, error) { return MacCredentials{}, errors.New("no enrollment") }
	if err := e.tc.Revoke(context.Background(), mintedID, true); err != nil || e.store.rec != nil || len(e.f.requests()) != n {
		t.Fatalf("local-only: %v", err)
	}
	if err := e.tc.Revoke(context.Background(), "../x", false); err == nil {
		t.Fatal("a bad ID was accepted")
	}
	e.noSecrets(t, false)
}

func TestTokenImport(t *testing.T) {
	e := newTokenEnv(t)
	base := e.f // the import checks the token with the Manager
	base.on("GET /manager/api/collections", 401, map[string]string{"error": "unknown or revoked agent token"})
	mgr := e.mustMac(t).ManagerURL + "/"
	if err := e.tc.Import(context.Background(), strings.NewReader(mintedToken+"\n"), mgr); err == nil || e.store.rec != nil {
		t.Fatalf("refused token stored: %v", err)
	}
	base.on("GET /manager/api/collections", 200, map[string]any{"collections": []any{}})
	if err := e.tc.Import(context.Background(), strings.NewReader("  "+mintedToken+"\r\n"), mgr); err != nil {
		t.Fatal(err)
	}
	got := base.requests()
	if got[1].Auth != "Bearer "+mintedToken || got[1].Query.Get("limit") != "1" || got[1].Header.Get("X-JuiceMount-Mac-Token") != "" {
		t.Fatalf("check request: %+v", got[1])
	}
	if r := e.store.rec; r == nil || r.Token != mintedToken || r.ID != mintedID || r.ManagerURL != strings.TrimSuffix(mgr, "/") {
		t.Fatalf("stored %+v", r)
	}
	for _, in := range []string{"", "jma_x", mintedToken + "x", strings.Repeat("a", 5000)} {
		if err := e.tc.Import(context.Background(), strings.NewReader(in), mgr); err == nil {
			t.Errorf("imported %.20q", in)
		}
	}
	if err := e.tc.Import(context.Background(), strings.NewReader(mintedToken), "ftp://x"); err == nil {
		t.Fatal("bad Manager address accepted")
	}
	e.noSecrets(t, false)
}

func (e *tokenEnv) mustMac(t *testing.T) MacCredentials {
	m, err := e.tc.Mac()
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestMacCredentialsFromState(t *testing.T) {
	ok := `{"manager_url":"http://nas.example:30190/","instance_id":"I","secret":"s","poll_token":"P","status":"approved"}`
	m, err := MacCredentialsFromState(ok, "D")
	if err != nil || m != (MacCredentials{ManagerURL: "http://nas.example:30190", InstanceID: "I", PollToken: "P", Device: "D"}) {
		t.Fatalf("%+v %v", m, err)
	}
	for _, bad := range []string{
		`{`,
		`{"manager_url":"http://nas.example:30190","instance_id":"I","poll_token":"P","status":"pending"}`,
		`{"manager_url":"http://nas.example:30190","instance_id":"I","status":"approved"}`,
		`{"manager_url":"nas","instance_id":"I","poll_token":"P","status":"approved"}`,
	} {
		if _, err := MacCredentialsFromState(bad, ""); err == nil || strings.Contains(err.Error(), "P\"") {
			t.Errorf("%s: %v", bad, err)
		}
	}
}

func TestTokenCreateLostAnswer(t *testing.T) {
	e := newTokenEnv(t)
	e.f.dropNext("POST /manager/api/agent-tokens")
	err := e.tc.Create(context.Background(), "x", 0, false)
	if err == nil || !strings.Contains(err.Error(), "A token may have been created: run juicemount-mcp token list and revoke any you don't recognise") || e.store.rec != nil {
		t.Fatalf("lost answer: %v", err)
	}
	e.f.on("POST /manager/api/agent-tokens", 502, map[string]string{"error": "bad gateway"})
	if err := e.tc.Create(context.Background(), "x", 0, false); err == nil || !strings.Contains(err.Error(), "token list") {
		t.Fatalf("502: %v", err)
	}
	// A refusal created nothing.
	e.f.on("POST /manager/api/agent-tokens", 409, map[string]string{"error": "Ann already has 10 agent tokens"})
	if err := e.tc.Create(context.Background(), "x", 0, false); err == nil || strings.Contains(err.Error(), "may have been created") {
		t.Fatalf("409: %v", err)
	}
	e.noSecrets(t, false)
}

func TestCleartextWarning(t *testing.T) {
	for base, warn := range map[string]bool{
		"http://nas.example:30190": true, "http://192.0.2.7:30190": true, "https://nas.example:30190": false,
		"http://127.0.0.1:30190": false, "http://localhost:30190": false, "http://[::1]:30190": false,
	} {
		if got := CleartextWarning(base) != ""; got != warn {
			t.Errorf("%s: warning %v", base, got)
		}
	}
	// token create warns on stderr (here: loopback, so not) and goes on.
	e := newTokenEnv(t)
	e.f.on("POST /manager/api/agent-tokens", 200, mintAnswer())
	if err := e.tc.Create(context.Background(), "x", 0, false); err != nil || strings.Contains(e.err.String(), "plain http") {
		t.Fatalf("loopback: %v %q", err, e.err)
	}
	// Import over plain http to another host warns too (no network: the
	// dial fails at once, and the token is stored with a warning).
	e.tc.HTTP = &http.Client{Transport: &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("no network in this test")
	}}}
	if err := e.tc.Import(context.Background(), strings.NewReader(mintedToken), "http://nas.invalid:1"); err != nil || !strings.Contains(e.err.String(), "plain http") {
		t.Fatalf("import over http: %v %q", err, e.err)
	}
}
