package manager

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/auditlog"
	"github.com/lelanddutcher/juicemount/internal/continuity/binder"
)

// newAgentEnv is newCollectionsEnv with every route the collections API
// serves: queries, discovery and agent tokens.
func newAgentEnv(t *testing.T) *collectionsEnv {
	t.Helper()
	e := newCollectionsEnv(t)
	for _, p := range []string{"/api/queries", "/api/queries/"} {
		e.mux.HandleFunc("/manager"+p, e.h.serveQueries)
	}
	e.mux.HandleFunc("/manager/api/continuity/find", e.h.serveFind)
	e.mux.HandleFunc("/manager/api/continuity/asset/", e.h.serveAsset)
	e.mux.HandleFunc("/manager/api/agent-tokens", e.h.serveAgentTokens)
	e.mux.HandleFunc("/manager/api/agent-tokens/", e.h.serveAgentTokens)
	e.h.roots = func() []string { return []string{"Recents", "Collections", "Queries"} }
	t.Cleanup(e.h.audit.Close)
	return e
}

type mintReply struct {
	Token      string         `json:"token"`
	AgentToken AgentTokenView `json:"agent_token"`
}

// mint mints a token from who's Mac.
func (e *collectionsEnv) mint(who string, body map[string]any) mintReply {
	e.t.Helper()
	rec := e.do(who, http.MethodPost, "/api/agent-tokens", body)
	if rec.Code != http.StatusOK {
		e.t.Fatalf("%s mint %v: %d %s", who, body, rec.Code, rec.Body)
	}
	var out mintReply
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		e.t.Fatal(err)
	}
	return out
}

func (e *collectionsEnv) tokenList(who string) []AgentTokenView {
	e.t.Helper()
	rec := e.do(who, http.MethodGet, "/api/agent-tokens", nil)
	var out struct {
		Tokens []AgentTokenView `json:"agent_tokens"`
	}
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &out) != nil {
		e.t.Fatalf("%s list: %d %s", who, rec.Code, rec.Body)
	}
	return out.Tokens
}

// auditRecords reads the catalog audit log, oldest first.
func (e *collectionsEnv) auditRecords() []auditlog.Record {
	e.t.Helper()
	if w := e.h.audit.writer(); w != nil {
		if err := w.Sync(); err != nil {
			e.t.Fatal(err)
		}
	}
	page, err := auditlog.Query(e.h.audit.dir, auditlog.Filter{}, 0, 1000)
	if err != nil {
		e.t.Fatal(err)
	}
	out := page.Records
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

func TestAgentTokenMintListRevoke(t *testing.T) {
	e := newAgentEnv(t)
	m := e.mint("Ann", map[string]any{"name": "Claude Desktop"})
	if !agentTokenRe.MatchString(m.Token) || m.AgentToken.Person != e.ids["Ann"] || m.AgentToken.Mac != macPrincipalName(e.macs["Ann"][0]) {
		t.Fatalf("minted: %+v", m)
	}
	if life := time.Duration(m.AgentToken.ExpiresAt-m.AgentToken.CreatedAt) * time.Millisecond; life != 90*24*time.Hour {
		t.Fatalf("default life %v", life)
	}
	// Only a hash is stored; the token is never in a list.
	disk, err := os.ReadFile(e.people.opts.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	secret := m.Token[len("jma_")+17:]
	if bytes.Contains(disk, []byte(secret)) || !bytes.Contains(disk, []byte(agentTokenHash(m.Token))) {
		t.Fatal("the store holds the token, or not its hash")
	}
	if rec := e.do("Ann", http.MethodGet, "/api/agent-tokens", nil); strings.Contains(rec.Body.String(), secret) || strings.Contains(rec.Body.String(), "sha256") {
		t.Fatalf("list shows the secret: %s", rec.Body)
	}
	// The token works as Ann.
	if rec := e.do(m.Token, http.MethodGet, "/api/collections", nil); rec.Code != 200 {
		t.Fatalf("agent list: %d %s", rec.Code, rec.Body)
	}
	// Survives a restart of the store.
	again, err := NewPrincipalStore(e.people.opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, p, _, err := again.agentPerson(m.Token); err != nil || p.ID != e.ids["Ann"] {
		t.Fatalf("after reload: %v %v", p.ID, err)
	}
	if got := e.tokenList("Ann"); len(got) != 1 || got[0].ID != m.AgentToken.ID {
		t.Fatalf("Ann's list: %+v", got)
	}
	if got := e.tokenList("Bob"); len(got) != 0 {
		t.Fatalf("Bob sees: %+v", got)
	}
	bobs := e.mint("Bob", map[string]any{"name": "cli", "expires_in_days": 7})
	if life := time.Duration(bobs.AgentToken.ExpiresAt-bobs.AgentToken.CreatedAt) * time.Millisecond; life != 7*24*time.Hour {
		t.Fatalf("7-day life %v", life)
	}
	if got := e.tokenList("admin"); len(got) != 2 {
		t.Fatalf("admin sees: %+v", got)
	}
	// Input, who may mint, and agents never manage tokens.
	for _, c := range []struct {
		who  string
		body map[string]any
		code int
	}{
		{"Ann", map[string]any{"name": ""}, 400},
		{"Ann", map[string]any{"name": "x", "expires_in_days": 91}, 400},
		{"Ann", map[string]any{"name": "x", "expires_in_days": -1}, 400},
		{"Ann", map[string]any{"name": "x", "scope": "all"}, 400},
		{"admin", map[string]any{"name": "x"}, 403},
		{m.Token, map[string]any{"name": "x"}, 403},
		{"Nobody", map[string]any{"name": "x"}, 403},
	} {
		if rec := e.do(c.who, http.MethodPost, "/api/agent-tokens", c.body); rec.Code != c.code {
			t.Errorf("mint %v as %.8s: %d %s, want %d", c.body, c.who, rec.Code, rec.Body, c.code)
		}
	}
	if rec := e.do(m.Token, http.MethodGet, "/api/agent-tokens", nil); rec.Code != 403 {
		t.Errorf("agent lists tokens: %d", rec.Code)
	}
	// Bob cannot revoke Ann's token; Ann can; then it is refused.
	if rec := e.do("Bob", http.MethodPost, "/api/agent-tokens/"+m.AgentToken.ID+"/revoke", nil); rec.Code != 404 {
		t.Fatalf("Bob revokes Ann's: %d %s", rec.Code, rec.Body)
	}
	if rec := e.do(m.Token, http.MethodPost, "/api/agent-tokens/"+m.AgentToken.ID+"/revoke", nil); rec.Code != 403 {
		t.Fatalf("an agent revokes: %d %s", rec.Code, rec.Body)
	}
	if rec := e.do("Ann", http.MethodPost, "/api/agent-tokens/"+m.AgentToken.ID+"/revoke", nil); rec.Code != 200 {
		t.Fatalf("Ann revokes: %d %s", rec.Code, rec.Body)
	}
	if rec := e.do(m.Token, http.MethodGet, "/api/collections", nil); rec.Code != 401 {
		t.Fatalf("revoked token: %d %s", rec.Code, rec.Body)
	}
	// The admin revokes any token.
	if rec := e.do("admin", http.MethodDelete, "/api/agent-tokens/"+bobs.AgentToken.ID, nil); rec.Code != 200 {
		t.Fatalf("admin revokes: %d %s", rec.Code, rec.Body)
	}
	if rec := e.do(bobs.Token, http.MethodGet, "/api/collections", nil); rec.Code != 401 {
		t.Fatalf("admin-revoked token: %d", rec.Code)
	}
}

func TestAgentTokenCapPerPerson(t *testing.T) {
	e := newAgentEnv(t)
	for i := 0; i < agentTokensPerPerson; i++ {
		e.mint("Ann", map[string]any{"name": "a"})
	}
	if rec := e.do("Ann", http.MethodPost, "/api/agent-tokens", map[string]any{"name": "one too many"}); rec.Code != 409 {
		t.Fatalf("over the cap: %d %s", rec.Code, rec.Body)
	}
	e.mint("Bob", map[string]any{"name": "b"}) // per person
	id := e.tokenList("Ann")[0].ID
	e.do("Ann", http.MethodDelete, "/api/agent-tokens/"+id, nil)
	e.mint("Ann", map[string]any{"name": "after a revoke"})
}

// Each way a token stops working: refused at once, and never back.
func TestAgentTokenRefusals(t *testing.T) {
	ok := func(e *collectionsEnv, tok string) int {
		return e.do(tok, http.MethodGet, "/api/collections", nil).Code
	}
	for _, c := range []struct {
		name   string
		change func(e *collectionsEnv)
		after  func(e *collectionsEnv) // after the refusal: must stay refused
	}{
		{"expired", func(e *collectionsEnv) {
			e.people.opts.Now = func() time.Time { return time.Now().Add(91 * 24 * time.Hour) }
		}, nil},
		{"revoked", func(e *collectionsEnv) {
			id := e.tokenList("Ann")[0].ID
			e.do("Ann", http.MethodPost, "/api/agent-tokens/"+id+"/revoke", nil)
		}, nil},
		{"person removed", func(e *collectionsEnv) {
			if err := e.people.DeletePerson(e.ids["Ann"]); err != nil {
				e.t.Fatal(err)
			}
		}, nil},
		{"Mac revoked", func(e *collectionsEnv) {
			if err := e.people.Revoke(macPrincipalName(e.macs["Ann"][0])); err != nil {
				e.t.Fatal(err)
			}
		}, func(e *collectionsEnv) {
			// Approved again (a new approval): the token stays dead.
			if err := e.people.Approve(macPrincipalName(e.macs["Ann"][0])); err != nil {
				e.t.Fatal(err)
			}
			if err := e.people.AssignDevice(macPrincipalName(e.macs["Ann"][0]), e.ids["Ann"]); err != nil {
				e.t.Fatal(err)
			}
		}},
		{"Mac given to another person", func(e *collectionsEnv) {
			if err := e.people.AssignDevice(macPrincipalName(e.macs["Ann"][0]), e.ids["Bob"]); err != nil {
				e.t.Fatal(err)
			}
		}, func(e *collectionsEnv) {
			if err := e.people.AssignDevice(macPrincipalName(e.macs["Ann"][0]), e.ids["Ann"]); err != nil {
				e.t.Fatal(err)
			}
		}},
		{"Mac unassigned", func(e *collectionsEnv) {
			if err := e.people.AssignDevice(macPrincipalName(e.macs["Ann"][0]), ""); err != nil {
				e.t.Fatal(err)
			}
		}, func(e *collectionsEnv) {
			if err := e.people.AssignDevice(macPrincipalName(e.macs["Ann"][0]), e.ids["Ann"]); err != nil {
				e.t.Fatal(err)
			}
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := newAgentEnv(t)
			tok := e.mint("Ann", map[string]any{"name": "agent"}).Token
			if code := ok(e, tok); code != 200 {
				t.Fatalf("before: %d", code)
			}
			var buf bytes.Buffer
			log.SetOutput(&buf)
			defer log.SetOutput(os.Stderr)
			c.change(e)
			if code := ok(e, tok); code != 401 {
				t.Fatalf("after %s: %d", c.name, code)
			}
			if c.after != nil {
				c.after(e)
				if code := ok(e, tok); code != 401 {
					t.Fatalf("after %s and back: %d", c.name, code)
				}
			}
			// A refusal's log line never holds the token.
			if strings.Contains(buf.String(), tok[len("jma_")+17:]) {
				t.Fatalf("the log holds the token:\n%s", buf.String())
			}
		})
	}
}

// The token is refused when its Mac is flagged as cloned, and a token of an
// unknown ID or a wrong secret is refused in the same words.
func TestAgentTokenForgedAndClonedMac(t *testing.T) {
	e := newAgentEnv(t)
	tok := e.mint("Ann", map[string]any{"name": "agent"}).Token
	forged := tok[:len(tok)-1] + map[bool]string{true: "0", false: "1"}[tok[len(tok)-1] != '0']
	other := "jma_0123456789abcdef_" + strings.Repeat("ab", 32)
	var codes []int
	var bodies []string
	for _, x := range []string{forged, other, "jma_short"} {
		rec := e.do(x, http.MethodGet, "/api/collections", nil)
		codes, bodies = append(codes, rec.Code), append(bodies, rec.Body.String())
	}
	if codes[0] != 401 || codes[1] != 401 || codes[2] != 401 || bodies[0] != bodies[1] {
		t.Fatalf("forged/unknown: %v %v", codes, bodies)
	}
	// A device clash on the Mac.
	mac := e.macs["Ann"]
	if _, _, err := e.people.macPerson(mac[0], mac[1], strings.Repeat("e", 32)); err == nil {
		t.Fatal("a different digest passed")
	}
	if rec := e.do(tok, http.MethodGet, "/api/collections", nil); rec.Code != 401 {
		t.Fatalf("cloned Mac's token: %d", rec.Code)
	}
	// Mixed credentials are refused.
	req := httptest.NewRequest(http.MethodGet, "/manager/api/collections", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("X-JuiceMount-Admin-Key", collectionsAdminKey)
	rec := httptest.NewRecorder()
	e.mux.ServeHTTP(rec, req)
	if rec.Code != 400 {
		t.Fatalf("token plus admin key: %d", rec.Code)
	}
}

// An agent is never the open admin path: with no admin key configured, a
// bad token is still refused.
func TestAgentTokenNotOpenAdmin(t *testing.T) {
	e := newAgentEnv(t)
	e.a.adminKey = ""
	if rec := e.do("jma_0123456789abcdef_"+strings.Repeat("ab", 32), http.MethodGet, "/api/collections", nil); rec.Code != 401 {
		t.Fatalf("bad token in LAN mode: %d", rec.Code)
	}
}

// Within the allowed subtrees only the listed requests pass, on canonical
// paths only.
func TestAgentAllowlistWithinRoutes(t *testing.T) {
	e := newAgentEnv(t)
	tok := e.mint("Ann", map[string]any{"name": "agent"}).Token
	col := string(e.write("admin", "/api/collections", map[string]any{"name": "Mine", "members": pathRefs("Clients/A/one.mov")}, 200).Collection.ID)
	qry := string(e.queryWrite("admin", "/api/queries", map[string]any{"name": "Q", "folders": []string{"Clients/A"}}, 200).Query.ID)
	asset := string(binder.AssetID(e.vol.VolumeUUID(), 42))
	allowed := []struct{ method, path string }{
		{"GET", "/api/collections"},
		{"GET", "/api/collections/" + col},
		{"GET", "/api/collections/" + col + "/members"},
		{"POST", "/api/collections/" + col + "/rename"},
		{"POST", "/api/collections/" + col + "/members/add"},
		{"POST", "/api/collections/" + col + "/members/remove"},
		{"POST", "/api/collections"},
		{"GET", "/api/queries"},
		{"GET", "/api/queries/" + qry},
		{"GET", "/api/queries/" + qry + "/results"},
		{"GET", "/api/continuity/find"},
		{"GET", "/api/continuity/asset/" + asset},
	}
	refused := []struct{ method, path string }{
		{"DELETE", "/api/collections/" + col},
		{"POST", "/api/collections/" + col + "/delete"},
		{"PUT", "/api/collections"},
		{"POST", "/api/collections/" + col + "/members"},
		{"POST", "/api/queries"},
		{"DELETE", "/api/queries/" + qry},
		{"POST", "/api/queries/" + qry + "/rename"},
		{"POST", "/api/queries/" + qry + "/delete"},
		{"POST", "/api/continuity/find"},
		{"GET", "/api/collections/" + col + "/members/add/x"},
		{"GET", "/api/agent-tokens"},
		{"POST", "/api/agent-tokens"},
	}
	bodyFor := func(p string) any {
		switch {
		case strings.HasSuffix(p, "/rename"):
			return map[string]any{"name": "Renamed"}
		case strings.HasSuffix(p, "/add"), strings.HasSuffix(p, "/remove"):
			return map[string]any{"members": pathRefs("Clients/A/two.mov")}
		case p == "/api/collections":
			return map[string]any{"name": "New"}
		}
		return nil
	}
	for _, r := range allowed {
		if rec := e.do(tok, r.method, r.path, bodyFor(r.path)); rec.Code != 200 {
			t.Errorf("allowed %s %s: %d %s", r.method, r.path, rec.Code, rec.Body)
		}
	}
	for _, r := range refused {
		if rec := e.do(tok, r.method, r.path, bodyFor(r.path)); rec.Code != 403 {
			t.Errorf("refused %s %s: %d %s", r.method, r.path, rec.Code, rec.Body)
		}
	}
	// Path variants (routeParts, collections.go): one trailing slash, or an
	// encoded one, is the same request; an empty segment (a doubled or
	// encoded slash inside) is 404. A refused request is never served under
	// any variant.
	for i, r := range append(allowed[:len(allowed):len(allowed)], refused...) {
		isAllowed := i < len(allowed)
		segs := strings.Split(r.path, "/")
		last := segs[len(segs)-1]
		for j, v := range []string{
			r.path + "/",
			r.path + "%2F",
			r.path + "%2F%2F",
			strings.TrimSuffix(r.path, last) + "%2F" + last,
		} {
			rec := e.do(tok, r.method, v, bodyFor(r.path))
			same := isAllowed && j < 2
			switch {
			case rec.Code >= 200 && rec.Code < 300 && !same:
				t.Errorf("variant %s %s served: %d %s", r.method, v, rec.Code, rec.Body)
			case same && rec.Code != 200 && rec.Code != 404:
				t.Errorf("variant %s %s: %d %s", r.method, v, rec.Code, rec.Body)
			}
		}
	}
	// A slot-taking route under an encoded slash takes no slot: refused
	// before the slot, so a held slot does not matter.
	e.h.inflight[e.ids["Ann"]] = true
	if rec := e.do(tok, "GET", "/api/queries/"+qry+"/results%2F%2F", nil); rec.Code != 404 {
		t.Fatalf("encoded results: %d %s", rec.Code, rec.Body)
	}
	delete(e.h.inflight, e.ids["Ann"])
}

// Every route Register wires refuses an agent unless it is on the
// allowlist, and a route added later is refused by default.
func TestAgentRouteWalk(t *testing.T) {
	var patterns []string
	routesRegistered = func(p []string) { patterns = append([]string(nil), p...) }
	defer func() { routesRegistered = nil }()
	mux, _, _, _ := newH8BAPINoProxy(t)
	if len(patterns) < 50 {
		t.Fatalf("only %d routes recorded", len(patterns))
	}
	tok := "jma_0123456789abcdef_" + strings.Repeat("ab", 32)
	send := func(method, path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(`{}`))
		req.Header.Set("Authorization", "Bearer "+tok)
		// With the admin key too: the refusal is the guard's, not a
		// missing credential.
		req.Header.Set("X-JuiceMount-Admin-Key", h8bTestAdminKey)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}
	allowed := map[string]bool{}
	for _, p := range agentPatterns {
		allowed[p] = true
	}
	seen := map[string]bool{}
	for _, p := range patterns {
		seen[p] = true
		path := p
		if strings.HasSuffix(path, "/") {
			path += "x"
		}
		for _, method := range []string{"GET", "POST", "DELETE"} {
			for _, v := range []string{path, path + "/", path + "%2Fx"} {
				rec := send(method, v)
				guard := rec.Code == 403 && strings.Contains(rec.Body.String(), "agent tokens may not use this route")
				switch {
				case !allowed[p] && v == path && !guard:
					t.Errorf("%s %s (pattern %q): %d %s, want the guard's 403", method, v, p, rec.Code, rec.Body)
				case rec.Code >= 200 && rec.Code < 300:
					t.Errorf("%s %s (pattern %q): %d for an agent", method, v, p, rec.Code)
				case allowed[p] && v == path && guard:
					t.Errorf("%s %s: allowlisted pattern %q refused by the guard", method, v, p)
				}
			}
		}
	}
	for p := range allowed {
		if !seen[p] {
			t.Errorf("allowlisted pattern %q is not registered", p)
		}
	}
	// A new route is refused for agents by default.
	m := newAgentRouteMux(http.NewServeMux(), "/m")
	m.HandleFunc("/m/api/new-thing", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	for _, auth := range []string{"Bearer " + tok, "bearer x", "Bearer"} {
		req := httptest.NewRequest("GET", "/m/api/new-thing", nil)
		req.Header.Set("Authorization", auth)
		rec := httptest.NewRecorder()
		m.mux.ServeHTTP(rec, req)
		if rec.Code != 403 {
			t.Fatalf("new route with %q: %d", auth, rec.Code)
		}
	}
	rec := httptest.NewRecorder()
	m.mux.ServeHTTP(rec, httptest.NewRequest("GET", "/m/api/new-thing", nil))
	if rec.Code != 200 {
		t.Fatalf("new route without a token: %d", rec.Code)
	}
}

// An agent edits only within its person's grants, and never deletes.
func TestAgentEditsWithinGrants(t *testing.T) {
	e := newAgentEnv(t)
	tok := e.mint("Ann", map[string]any{"name": "agent"}).Token
	mine := string(e.write(tok, "/api/collections", map[string]any{"name": "Agent picks", "members": pathRefs("Clients/A/one.mov")}, 200).Collection.ID)
	e.write(tok, "/api/collections/"+mine+"/members/add", map[string]any{"members": pathRefs("Clients/B/three.mov")}, 403)
	e.write(tok, "/api/collections", map[string]any{"name": "B", "members": pathRefs("Clients/B/three.mov")}, 403)
	mixed := string(e.write("admin", "/api/collections", map[string]any{"name": "Mixed", "members": pathRefs("Clients/A/one.mov", "Clients/B/three.mov")}, 200).Collection.ID)
	e.write(tok, "/api/collections/"+mixed+"/rename", map[string]any{"name": "x"}, 403)
	e.write(tok, "/api/collections/"+mixed+"/members/remove", map[string]any{"members": pathRefs("Clients/B/three.mov")}, 403)
	e.write(tok, "/api/collections/"+mixed+"/members/remove", map[string]any{"members": pathRefs("Clients/A/one.mov")}, 200)
	e.write(tok, "/api/collections/"+mine+"/delete", map[string]any{}, 403)
	// Nia has no write access: her agent writes nothing.
	nia := e.mint("Nia", map[string]any{"name": "agent"}).Token
	e.write(nia, "/api/collections", map[string]any{"name": "x"}, 403)
	// Acting as Ann, the agent's own retries are its own: Ann's Mac reusing
	// its operation_id gets a new operation.
	a := e.write(tok, "/api/collections", map[string]any{"name": "Retry", "operation_id": "k1"}, 200)
	b := e.write("Ann", "/api/collections", map[string]any{"name": "Retry", "operation_id": "k1"}, 200)
	if a.Collection.ID == b.Collection.ID || a.Operation.Actor != e.ids["Ann"] || b.Operation.Actor != e.ids["Ann"] {
		t.Fatalf("agent %+v Mac %+v", a, b)
	}
	if again := e.write(tok, "/api/collections", map[string]any{"name": "Retry", "operation_id": "k1"}, 200); again.Operation.ID != a.Operation.ID {
		t.Fatalf("agent retry: %+v vs %+v", again, a)
	}
}

// An agent shares its person's slot with the person's Macs.
func TestAgentSharesPersonSlot(t *testing.T) {
	e := newAgentEnv(t)
	saved := collectionBusyWait
	collectionBusyWait = 50 * time.Millisecond
	defer func() { collectionBusyWait = saved }()
	tok := e.mint("Ann", map[string]any{"name": "agent"}).Token
	release, err := e.h.acquire(t.Context(), collectionActor{person: &Person{ID: e.ids["Ann"]}})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"/api/continuity/find", "/api/continuity/asset/" + string(binder.AssetID(e.vol.VolumeUUID(), 42))} {
		if rec := e.do(tok, http.MethodGet, p, nil); rec.Code != 503 || !strings.Contains(rec.Body.String(), "still being checked") {
			t.Fatalf("agent while Ann's Mac holds the slot, %s: %d %s", p, rec.Code, rec.Body)
		}
	}
	if rec := e.do("Bob", http.MethodGet, "/api/continuity/find", nil); rec.Code != 200 {
		t.Fatalf("Bob: %d", rec.Code)
	}
	release()
	if rec := e.do(tok, http.MethodGet, "/api/continuity/find", nil); rec.Code != 200 {
		t.Fatalf("after: %d %s", rec.Code, rec.Body)
	}
}

// Mints, revokes and every agent edit are in the catalog audit log with the
// person, the minting Mac and the token; refusals too. With the log failing,
// an agent edit and a mint are refused and change nothing; a revoke still
// happens.
func TestAgentAudit(t *testing.T) {
	e := newAgentEnv(t)
	m := e.mint("Ann", map[string]any{"name": "Claude Desktop"})
	mac := macPrincipalName(e.macs["Ann"][0])
	col := e.write(m.Token, "/api/collections", map[string]any{"name": "Picks", "members": pathRefs("Clients/A/one.mov")}, 200).Collection.ID
	e.write(m.Token, "/api/collections/"+string(col)+"/members/add", map[string]any{"members": pathRefs("Clients/A/two.mov")}, 200)
	e.write(m.Token, "/api/collections/"+string(col)+"/members/add", map[string]any{"members": pathRefs("Clients/B/three.mov")}, 403)
	e.write("Ann", "/api/collections/"+string(col)+"/rename", map[string]any{"name": "By hand"}, 200) // not an agent: not here
	e.do("Ann", http.MethodDelete, "/api/agent-tokens/"+m.AgentToken.ID, nil)
	recs := e.auditRecords()
	var actions []string
	outcomes := map[uint64]string{}
	for _, r := range recs {
		if r.Type == auditlog.TypeOutcome {
			outcomes[r.Unit] = r.Outcome
		}
	}
	for _, r := range recs {
		switch r.Type {
		case auditlog.TypeIntent, auditlog.TypeRefused:
		default:
			continue
		}
		actions = append(actions, r.Type+":"+r.Action)
		if r.Person != e.ids["Ann"] || r.PersonName != "Ann" || r.Principal != mac {
			t.Errorf("%s %s: who %+v", r.Type, r.Action, r)
		}
		if strings.HasPrefix(r.Action, "collection") || r.Type == auditlog.TypeRefused {
			if r.Kind != "agent" || r.Agent != m.AgentToken.ID || r.AgentName != "Claude Desktop" {
				t.Errorf("%s: agent attribution %+v", r.Action, r)
			}
		}
		if r.Type == auditlog.TypeIntent && outcomes[r.Seq] != auditlog.OutcomeCommitted {
			t.Errorf("%s: outcome %q", r.Action, outcomes[r.Seq])
		}
	}
	want := "intent:agent-token-mint intent:collection-create intent:collection-add-members refused:refused intent:agent-token-revoke"
	if got := strings.Join(actions, " "); got != want {
		t.Fatalf("audit:\n got %s\nwant %s", got, want)
	}
	for _, r := range recs {
		if r.Action == "collection-add-members" && !strings.Contains(r.Detail, string(binder.AssetID(e.vol.VolumeUUID(), 43))) {
			t.Errorf("add detail: %q", r.Detail)
		}
	}
	// Read through /api/audit?log=catalog.
	pa := &peopleAPI{catalogAuditDir: e.h.audit.dir}
	rec := httptest.NewRecorder()
	pa.handleAudit(rec, httptest.NewRequest(http.MethodGet, "/api/audit?log=catalog&person="+e.ids["Ann"], nil))
	var page auditlog.Page
	byAgent := 0
	if rec.Code == 200 && json.Unmarshal(rec.Body.Bytes(), &page) == nil {
		for _, r := range page.Records {
			if r.Agent == m.AgentToken.ID {
				byAgent++
			}
		}
	}
	// Two intents, two outcomes and the refusal.
	if byAgent != 5 {
		t.Fatalf("audit API: %d %s", rec.Code, rec.Body)
	}

	// The log fails: the agent's edit and a mint are refused.
	tok := e.mint("Ann", map[string]any{"name": "second"})
	e.h.audit.Close()
	failing, _ := auditlog.Open(t.TempDir(), auditlog.Options{BeforeWrite: func() error { return errors.New("disk gone") }, RetryEvery: time.Hour})
	e.h.audit.mu.Lock()
	e.h.audit.w = failing
	e.h.audit.mu.Unlock()
	before := e.members(col, "")
	if rec := e.do(tok.Token, http.MethodPost, "/api/collections/"+string(col)+"/members/remove", map[string]any{"members": pathRefs("Clients/A/one.mov")}); rec.Code != 503 {
		t.Fatalf("unaudited edit: %d %s", rec.Code, rec.Body)
	}
	if after := e.members(col, ""); len(after.Members) != len(before.Members) {
		t.Fatalf("members changed: %d -> %d", len(before.Members), len(after.Members))
	}
	if rec := e.do("Ann", http.MethodPost, "/api/agent-tokens", map[string]any{"name": "x"}); rec.Code != 503 {
		t.Fatalf("unaudited mint: %d %s", rec.Code, rec.Body)
	}
	if n := len(e.tokenList("Ann")); n != 1 {
		t.Fatalf("tokens after a refused mint: %d", n)
	}
	if rec := e.do("Ann", http.MethodDelete, "/api/agent-tokens/"+tok.AgentToken.ID, nil); rec.Code != 200 {
		t.Fatalf("revoke with the log down: %d %s", rec.Code, rec.Body)
	}
	if rec := e.do(tok.Token, http.MethodGet, "/api/collections", nil); rec.Code != 401 {
		t.Fatalf("revoked with the log down: %d", rec.Code)
	}
}
