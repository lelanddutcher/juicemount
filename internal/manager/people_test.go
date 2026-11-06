package manager

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/auditlog"
	"github.com/lelanddutcher/juicemount/internal/respproxy"
)

// startRealProxyReloader runs the proxy's own reloader (what jm-respproxy
// runs with reload_handshake) on dir: it applies the exact requested bytes
// with the proxy's parser and writes a heartbeat naming the formats it
// reads.
func startRealProxyReloader(t *testing.T, dir string) (stop func()) {
	t.Helper()
	r, err := respproxy.NewPrincipalReloader(filepath.Join(dir, proxyPrincipalsName), respproxy.NewAuthenticator(nil))
	if err != nil {
		t.Fatal(err)
	}
	quit, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			_ = r.Tick() // a refused file stays unacknowledged
			select {
			case <-quit:
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
	}()
	var stopped bool
	stop = func() {
		if !stopped {
			stopped = true
			close(quit)
			<-done
		}
	}
	t.Cleanup(stop)
	return stop
}

// oldProxyParse is the parser of a proxy image from before format 2.
func oldProxyParse(b []byte) error {
	var f struct {
		Principals []respproxy.PrincipalEntry `json:"principals"`
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	return dec.Decode(&f)
}

func approvedMac(t *testing.T, s *PrincipalStore, instance string) string {
	t.Helper()
	if _, err := s.Enroll(EnrollRequest{InstanceID: instance, Host: "edit-bay", Secret: testMacSecret}, "192.0.2.7"); err != nil {
		t.Fatal(err)
	}
	name := macPrincipalName(instance)
	if err := s.Approve(name); err != nil {
		t.Fatal(err)
	}
	return name
}

// waitApplied waits until the proxy applied the latest request.
// waitHeartbeatFormat waits until the proxy directory's fresh heartbeat
// advertises format want (the previous proxy's heartbeat stays fresh for
// proxyHeartbeatMaxAge, so a fixed sleep races a slow first tick).
func waitHeartbeatFormat(t *testing.T, dir string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if f, ok := heartbeatFormat(dir); ok && f == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no fresh heartbeat with format %d", want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitApplied(t *testing.T, s *PrincipalStore) PeopleSnapshot {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		snap := s.PeopleSnapshot()
		if snap.GrantsApplied || time.Now().After(deadline) {
			return snap
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func proxyFile(t *testing.T, dir string) (raw []byte, ps *respproxy.Principals) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, proxyPrincipalsName))
	if err != nil {
		t.Fatal(err)
	}
	ps, err = respproxy.ParsePrincipals(raw)
	if err != nil {
		t.Fatalf("the proxy refuses the Manager's file: %v\n%s", err, raw)
	}
	return raw, ps
}

// TestPeopleReachTheProxy: with a proxy that reads format 2, people,
// device assignments and grants go into the principals file, the proxy's
// reloader applies it, and revocation and access changes follow.
func TestPeopleReachTheProxy(t *testing.T) {
	dir := t.TempDir()
	s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: filepath.Join(dir, "store.json"), ProxyDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	startRealProxyReloader(t, dir)
	mac := approvedMac(t, s, testInstanceID)
	john, err := s.CreatePerson("John")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AssignDevice(mac, john.ID); err != nil {
		t.Fatal(err)
	}
	// access "all": a person, no restriction.
	_, ps := proxyFile(t, dir)
	if p := ps.PersonOf(mac); p == nil || p.ID != john.ID || p.Name != "John" || p.Scoped || ps.GrantsInForce() {
		t.Fatalf("person of %s = %+v", mac, p)
	}
	if err := s.AddGrant(john.ID, "/Clients/A/"); err != nil {
		t.Fatal(err)
	}
	if snap := waitApplied(t, s); !snap.GrantsSent || !snap.GrantsApplied || snap.Problem != "" || snap.ProxyFormat != 2 {
		t.Fatalf("snapshot after grant: %+v", snap)
	}
	raw, ps := proxyFile(t, dir)
	if p := ps.PersonOf(mac); p == nil || !p.Scoped || len(p.Grants) != 1 || p.Grants[0] != "/Clients/A" {
		t.Fatalf("grant not in the proxy file: %+v\n%s", p, raw)
	}
	if oldProxyParse(raw) == nil {
		t.Fatal("an old proxy would accept the version-2 file")
	}
	// Revoking the only grant leaves John with no write access (still
	// scoped), not with access everywhere.
	if err := s.RemoveGrant(john.ID, "/Clients/A"); err != nil {
		t.Fatal(err)
	}
	_, ps = proxyFile(t, dir)
	if p := ps.PersonOf(mac); p == nil || !p.Scoped || len(p.Grants) != 0 {
		t.Fatalf("after revoking the only grant: %+v", p)
	}
	all := personAccessAll
	if err := s.UpdatePerson(john.ID, nil, &all); err != nil {
		t.Fatal(err)
	}
	if _, ps = proxyFile(t, dir); ps.PersonOf(mac).Scoped {
		t.Fatal("access all still scoped")
	}
	// Deleting the last person leaves the Mac with no person and grants
	// still in force (restrict_unassigned, on since the first grant): the
	// Mac may not write, rather than writing everywhere.
	if err := s.DeletePerson(john.ID); err != nil {
		t.Fatal(err)
	}
	raw, ps = proxyFile(t, dir)
	if ps.PersonOf(mac) != nil || !ps.GrantsInForce() {
		t.Fatalf("after the last person left: %s", raw)
	}
	if snap := s.PeopleSnapshot(); !snap.GrantsSent || !snap.GrantsInForce || snap.UnassignedMacs != 1 ||
		len(snap.Devices) != 1 || !snap.Devices[0].ReadOnly {
		t.Fatalf("snapshot after the last person left: %+v", snap)
	}
	// Only the admin lifts it: then the file is version 1 again.
	if err := s.SetRestrictUnassigned(false); err != nil {
		t.Fatal(err)
	}
	raw, ps = proxyFile(t, dir)
	if ps.GrantsInForce() || bytes.Contains(raw, []byte(`"version"`)) || oldProxyParse(raw) != nil {
		t.Fatalf("after lifting the restriction: %s", raw)
	}
	if snap := s.PeopleSnapshot(); snap.GrantsSent || snap.GrantsInForce || snap.Devices[0].ReadOnly {
		t.Fatalf("grants still sent: %+v", snap)
	}
}

// TestPeopleFormatFollowsProxyImage: an older proxy image (heartbeat
// without the format token) gets version-1 files it can read, and the
// admin is told grants are not enforced; an upgrade gets the people within
// one sync; a later downgrade gets version 1 again. A stale heartbeat
// (the proxy restarting) keeps the last format, so grants are never
// dropped by a restart.
func TestPeopleFormatFollowsProxyImage(t *testing.T) {
	dir := t.TempDir()
	s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: filepath.Join(dir, "store.json"), ProxyDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	stopOld := startFakeProxyAck(t, dir) // heartbeat "1": an image before format 2
	mac := approvedMac(t, s, testInstanceID)
	john, _ := s.CreatePerson("John")
	if err := s.AssignDevice(mac, john.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.AddGrant(john.ID, "/Clients/A"); err != nil {
		t.Fatal(err)
	}
	raw, ps := proxyFile(t, dir)
	if oldProxyParse(raw) != nil || ps.PersonOf(mac) != nil {
		t.Fatalf("an old proxy got a file it cannot read:\n%s", raw)
	}
	if snap := s.PeopleSnapshot(); snap.GrantsSent || snap.ProxyFormat != 1 || !strings.Contains(snap.Problem, "does not support") {
		t.Fatalf("old proxy snapshot: %+v", snap)
	}

	// Upgrade.
	stopOld()
	stopNew := startRealProxyReloader(t, dir)
	waitHeartbeatFormat(t, dir, 2) // a fresh heartbeat with the token
	s.mu.Lock()
	err = s.syncPeopleLocked()
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if snap := waitApplied(t, s); !snap.GrantsApplied {
		t.Fatalf("after upgrade: %+v", snap)
	}
	if _, ps = proxyFile(t, dir); ps.PersonOf(mac) == nil || !ps.GrantsInForce() {
		t.Fatal("grants not sent after the upgrade")
	}

	// The proxy restarts: no fresh heartbeat. A change made meanwhile
	// keeps the people.
	old := proxyHeartbeatMaxAge
	proxyHeartbeatMaxAge = 0
	if err := s.AddGrant(john.ID, "/Clients/B"); err != nil {
		t.Fatal(err)
	}
	proxyHeartbeatMaxAge = old
	if _, ps = proxyFile(t, dir); ps.PersonOf(mac) == nil || len(ps.PersonOf(mac).Grants) != 2 {
		t.Fatal("a change during a proxy restart dropped the grants")
	}

	// Downgrade: a fresh heartbeat without the token.
	stopNew()
	startFakeProxyAck(t, dir)
	waitHeartbeatFormat(t, dir, 1)
	s.mu.Lock()
	err = s.syncPeopleLocked()
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if raw, _ = proxyFile(t, dir); oldProxyParse(raw) != nil {
		t.Fatalf("after a downgrade the file is not version 1:\n%s", raw)
	}
	if snap := s.PeopleSnapshot(); snap.GrantsSent || !strings.Contains(snap.Problem, "does not support person grants") {
		t.Fatalf("after a downgrade the grants are lifted without a problem: %+v", snap)
	}
	// With people but no grant, the downgrade stops the per-person log.
	all := personAccessAll
	if err := s.UpdatePerson(john.ID, nil, &all); err != nil {
		t.Fatal(err)
	}
	if err := s.SetRestrictUnassigned(false); err != nil {
		t.Fatal(err)
	}
	if snap := s.PeopleSnapshot(); snap.GrantsInForce || !strings.Contains(snap.Problem, "does not support people") {
		t.Fatalf("downgraded proxy with unscoped people: %+v", snap)
	}
}

// TestProxyFileUnchangedWithoutPeople: an install that never made a person
// writes exactly the version-1 bytes of before.
func TestProxyFileUnchangedWithoutPeople(t *testing.T) {
	dir := t.TempDir()
	s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: filepath.Join(dir, "store.json"), ProxyDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	startRealProxyReloader(t, dir)
	mac := approvedMac(t, s, testInstanceID)
	raw, _ := proxyFile(t, dir)
	type oldEntry struct {
		Name     string `json:"name"`
		Kind     string `json:"kind"`
		Host     string `json:"host,omitempty"`
		Argon2id string `json:"argon2id"`
		Disabled bool   `json:"disabled,omitempty"`
	}
	var old struct {
		Principals []oldEntry `json:"principals"`
	}
	if err := json.Unmarshal(raw, &old); err != nil || len(old.Principals) != 1 || old.Principals[0].Name != mac {
		t.Fatalf("file: %v\n%s", err, raw)
	}
	want, _ := json.MarshalIndent(old, "", "  ")
	if !bytes.Equal(raw, append(want, '\n')) {
		t.Fatalf("version-1 file changed:\n%s\nwant\n%s", raw, want)
	}
}

func TestPeoplePersistAndValidate(t *testing.T) {
	dir := t.TempDir()
	opts := PrincipalStoreOptions{StatePath: filepath.Join(dir, "store.json"), ProxyDir: dir}
	s, err := NewPrincipalStore(opts)
	if err != nil {
		t.Fatal(err)
	}
	startRealProxyReloader(t, dir)
	mac := approvedMac(t, s, testInstanceID)
	jane, _ := s.CreatePerson("Jane")
	if _, err := s.CreatePerson(" jane "); !errors.Is(err, ErrPersonInvalid) {
		t.Fatalf("duplicate name: %v", err)
	}
	for _, bad := range []string{"Clients", "/", "/a/b/c", "/a/../b", "/a//b", "/a/\x01"} {
		if err := s.AddGrant(jane.ID, bad); !errors.Is(err, ErrPersonInvalid) {
			t.Errorf("grant %q: %v, want refused", bad, err)
		}
	}
	if err := s.AssignDevice("mac-000000000000", jane.ID); !errors.Is(err, ErrPrincipalNotFound) {
		t.Fatalf("unknown device: %v", err)
	}
	if err := s.AssignDevice(mac, "p-nobody"); !errors.Is(err, ErrPersonNotFound) {
		t.Fatalf("unknown person: %v", err)
	}
	if err := s.AssignDevice(mac, jane.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.AddGrant(jane.ID, "/Clients"); err != nil {
		t.Fatal(err)
	}
	s2, err := NewPrincipalStore(opts)
	if err != nil {
		t.Fatal(err)
	}
	snap := s2.PeopleSnapshot()
	if len(snap.People) != 1 || snap.People[0].Name != "Jane" || snap.People[0].Access != personAccessGranted ||
		len(snap.People[0].Grants) != 1 || len(snap.People[0].Devices) != 1 || snap.People[0].Devices[0] != mac {
		t.Fatalf("after reopen: %+v", snap.People)
	}
	// The reopened store rewrote the proxy file at start: people kept.
	if _, ps := proxyFile(t, dir); ps.PersonOf(mac) == nil {
		t.Fatal("restart dropped the people from the proxy file")
	}
}

// TestPeopleAndAuditAPI exercises the routes behind the admin key.
func TestPeopleAndAuditAPI(t *testing.T) {
	mux, proxyDir, _, _ := newH8BAPI(t)
	key := h8bTestAdminKey
	if rr := doJSON(t, mux, http.MethodGet, "/api/people", "", nil); rr.Code != http.StatusUnauthorized {
		t.Fatalf("GET /api/people without the key: %d", rr.Code)
	}
	if rr := doJSON(t, mux, http.MethodGet, "/api/audit", "", nil); rr.Code != http.StatusUnauthorized {
		t.Fatalf("GET /api/audit without the key: %d", rr.Code)
	}
	rr := doJSON(t, mux, http.MethodPost, "/api/people/create", key, map[string]string{"name": "John"})
	if rr.Code != http.StatusOK {
		t.Fatalf("create: %d %s", rr.Code, rr.Body)
	}
	var snap PeopleSnapshot
	json.Unmarshal(rr.Body.Bytes(), &snap)
	if len(snap.People) != 1 || snap.People[0].Access != personAccessAll {
		t.Fatalf("snapshot: %s", rr.Body)
	}
	id := snap.People[0].ID
	if rr := doJSON(t, mux, http.MethodPost, "/api/people/grant", key, map[string]string{"id": id, "path": "relative"}); rr.Code != http.StatusBadRequest {
		t.Fatalf("bad grant: %d", rr.Code)
	}
	// This Manager has no metadata URL, so it cannot check the volume for
	// quotas above the project folders: grants are not put in force
	// (quotascope.go; quotascope_test.go covers a Manager that can check).
	if rr := doJSON(t, mux, http.MethodPost, "/api/people/grant", key, map[string]string{"id": id, "path": "/Clients/A"}); rr.Code != http.StatusBadRequest ||
		!strings.Contains(rr.Body.String(), "no metadata URL") {
		t.Fatalf("grant without a metadata URL: %d %s", rr.Code, rr.Body)
	}
	if rr := doJSON(t, mux, http.MethodPost, "/api/people/assign", key, map[string]string{"device": "mac-000000000000", "person": id}); rr.Code != http.StatusNotFound {
		t.Fatalf("assign unknown Mac: %d", rr.Code)
	}
	if rr := doJSON(t, mux, http.MethodPost, "/api/people/delete", key, map[string]string{"id": "p-none"}); rr.Code != http.StatusNotFound {
		t.Fatalf("delete unknown person: %d", rr.Code)
	}

	// The audit routes read <JM_RESPPROXY_DIR>/audit.
	w, err := auditlog.Open(filepath.Join(proxyDir, "audit"), auditlog.Options{Logf: func(string, ...any) {}})
	if err != nil {
		t.Fatal(err)
	}
	w.Append(&auditlog.Record{Type: auditlog.TypeIntent, Person: id, Principal: "mac-1", Projects: []auditlog.Project{{Ino: 5, Path: "/Clients/A"}}})
	w.Append(&auditlog.Record{Type: auditlog.TypeRefused, Person: "p-other", Principal: "mac-2", Reason: "NOT_GRANTED"})
	w.Close()
	rr = doJSON(t, mux, http.MethodGet, "/api/audit?person="+id+"&project=/Clients", key, nil)
	var page auditlog.Page
	if err := json.Unmarshal(rr.Body.Bytes(), &page); rr.Code != http.StatusOK || err != nil || len(page.Records) != 1 || page.Records[0].Principal != "mac-1" {
		t.Fatalf("audit query: %d %s", rr.Code, rr.Body)
	}
	if rr := doJSON(t, mux, http.MethodGet, "/api/audit?since=yesterday", key, nil); rr.Code != http.StatusBadRequest {
		t.Fatalf("bad since: %d", rr.Code)
	}
	rr = doJSON(t, mux, http.MethodGet, "/api/audit/verify?full=1", key, nil)
	var rep auditlog.Report
	if err := json.Unmarshal(rr.Body.Bytes(), &rep); rr.Code != http.StatusOK || err != nil || !rep.OK || rep.Records != 3 {
		t.Fatalf("verify: %d %s", rr.Code, rr.Body)
	}
	if rr := doJSON(t, mux, http.MethodGet, "/api/audit/verify", key, nil); rr.Code != http.StatusOK {
		t.Fatalf("verify (anchor state): %d %s", rr.Code, rr.Body)
	}
	rr = doJSON(t, mux, http.MethodGet, "/api/audit/status", key, nil)
	var st AuditStatus
	if err := json.Unmarshal(rr.Body.Bytes(), &st); rr.Code != http.StatusOK || err != nil || !st.Configured {
		t.Fatalf("audit status: %d %s", rr.Code, rr.Body)
	}
	if rr := doJSON(t, mux, http.MethodPost, "/api/people/policy", key, map[string]any{}); rr.Code != http.StatusBadRequest {
		t.Fatalf("policy without a value: %d", rr.Code)
	}
	if rr := doJSON(t, mux, http.MethodPost, "/api/people/policy", key, map[string]any{"restrict_unassigned": false}); rr.Code != http.StatusOK {
		t.Fatalf("policy: %d %s", rr.Code, rr.Body)
	}
	// The page is served.
	rr = doJSON(t, mux, http.MethodGet, "/people.html", "", nil)
	if b, _ := io.ReadAll(rr.Body); rr.Code != http.StatusOK || !bytes.Contains(b, []byte("People and access")) {
		t.Fatalf("people.html: %d", rr.Code)
	}
}
