package manager

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/crypto/argon2"
)

const h8bTestAdminKey = "0123456789abcdef0123456789abcdef-admin"

// readProxyPrincipals parses the proxy's principals file the way
// jm-respproxy does (unknown fields refused).
func readProxyPrincipals(t *testing.T, path string) []proxyPrincipalEntry {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read principals file: %v", err)
	}
	var f proxyPrincipalsFile
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		t.Fatalf("principals file is not in the proxy's format: %v\n%s", err, b)
	}
	return f.Principals
}

func findProxyPrincipal(es []proxyPrincipalEntry, name string) (proxyPrincipalEntry, bool) {
	for _, e := range es {
		if e.Name == name {
			return e, true
		}
	}
	return proxyPrincipalEntry{}, false
}

// verifyPHC checks pass against a PHC argon2id string in the exact format
// internal/respproxy/principals.go parses.
func verifyPHC(t *testing.T, phc, pass string) bool {
	t.Helper()
	h, err := parsePHCArgon2id(phc)
	if err != nil {
		t.Fatalf("hash %q is not a PHC argon2id string the proxy accepts: %v", phc, err)
	}
	got := argon2.IDKey([]byte(pass), h.salt, h.time, h.memory, h.threads, uint32(len(h.key)))
	return bytes.Equal(got, h.key)
}

// newH8BAPI is the Manager with a live proxy (startFakeProxyAck answers
// reload requests the way the respproxy entrypoint does).
func newH8BAPI(t *testing.T) (*http.ServeMux, string, string, *JobManager) {
	t.Helper()
	mux, proxyDir, dir, mgr := newH8BAPINoProxy(t)
	startFakeProxyAck(t, proxyDir)
	return mux, proxyDir, dir, mgr
}

// newH8BAPINoProxy is the Manager before the proxy cutover: nothing
// acknowledges reload.request.
func newH8BAPINoProxy(t *testing.T) (*http.ServeMux, string, string, *JobManager) {
	t.Helper()
	dir := t.TempDir()
	proxyDir := filepath.Join(dir, "respproxy")
	if err := os.MkdirAll(proxyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("JM_RESPPROXY_DIR", proxyDir)
	t.Setenv("JM_META_ADMIN_PASSWORD", "")
	mux := http.NewServeMux()
	mgr := Register(mux, "", Config{
		JuiceFSBin:  "/dev/null",
		FUSEMount:   filepath.Join(dir, "jfs"),
		SourceRoots: []string{"/sources"},
		DestMount:   "/jfs",
		AdminKey:    h8bTestAdminKey,
		StateFile:   filepath.Join(dir, "state.json"),
	})
	t.Cleanup(mgr.StopAll)
	return mux, proxyDir, dir, mgr
}

func doJSON(t *testing.T, mux http.Handler, method, path, key string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	req.RemoteAddr = "192.0.2.10:51234"
	if key != "" {
		req.Header.Set("X-JuiceMount-Admin-Key", key)
	}
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	return rr
}

const testInstanceID = "4F1C2A9E-7B3D-4C1A-9E2B-0A1B2C3D4E5F"
const testMacSecret = "macsecret-0123456789abcdefghijklmnopqrstuvwxyz"

// TestH8BEnrollRequiresApproval: a Mac's enrollment request never yields a
// working proxy credential until an admin approves it (outside an explicit
// auto-approve window), and nothing an unauthenticated caller does can
// approve it, steal it or swap its secret.
func TestH8BEnrollRequiresApproval(t *testing.T) {
	mux, proxyDir, _, _ := newH8BAPI(t)
	principalsPath := filepath.Join(proxyDir, "principals.json")
	enroll := map[string]string{
		"instance_id": testInstanceID, "host": "Edit-Bay-2", "label": "Edit Bay 2", "secret": testMacSecret,
	}

	// 1. The Mac enrolls without the admin key (it does not have it).
	rr := doJSON(t, mux, http.MethodPost, "/api/principals/enroll", "", enroll)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("enroll: got %d %s, want 202 pending", rr.Code, rr.Body)
	}
	var er EnrollResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &er); err != nil {
		t.Fatal(err)
	}
	if er.Status != PrincipalPending {
		t.Fatalf("enroll status = %q, want pending (no admin approved it)", er.Status)
	}
	if er.Principal == "" || !strings.HasPrefix(er.Principal, "mac-") || er.Fingerprint == "" {
		t.Fatalf("enroll response lacks principal name or fingerprint: %+v", er)
	}
	if strings.Contains(rr.Body.String(), testMacSecret) {
		t.Fatal("enroll response echoes the secret")
	}
	// Pending: the proxy must not accept this Mac yet.
	if _, err := os.Stat(principalsPath); err == nil {
		if _, ok := findProxyPrincipal(readProxyPrincipals(t, principalsPath), er.Principal); ok {
			t.Fatal("a PENDING Mac is already in the proxy's principals file")
		}
	}

	// 2. Approval needs the admin key.
	rr = doJSON(t, mux, http.MethodPost, "/api/principals/approve", "", map[string]string{"name": er.Principal})
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("approve without admin key: got %d, want 401", rr.Code)
	}
	// A different secret for the same instance must not replace the request.
	evil := map[string]string{"instance_id": testInstanceID, "host": "Edit-Bay-2", "secret": "attacker-secret-0123456789abcdefghijklmnop"}
	rr = doJSON(t, mux, http.MethodPost, "/api/principals/enroll", "", evil)
	if rr.Code != http.StatusConflict {
		t.Fatalf("re-enroll with a different secret: got %d %s, want 409", rr.Code, rr.Body)
	}

	// 3. Admin approves; the proxy file now authenticates the Mac's secret.
	rr = doJSON(t, mux, http.MethodPost, "/api/principals/approve", h8bTestAdminKey, map[string]string{"name": er.Principal})
	if rr.Code != http.StatusOK {
		t.Fatalf("approve: got %d %s", rr.Code, rr.Body)
	}
	entries := readProxyPrincipals(t, principalsPath)
	e, ok := findProxyPrincipal(entries, er.Principal)
	if !ok {
		t.Fatalf("approved Mac %s missing from the proxy principals file", er.Principal)
	}
	if e.Kind != "mac" || e.Host != "Edit-Bay-2" {
		t.Fatalf("approved entry = %+v, want kind mac host Edit-Bay-2", e)
	}
	if !verifyPHC(t, e.Argon2id, testMacSecret) {
		t.Fatal("approved entry's hash does not verify the Mac's secret")
	}
	if verifyPHC(t, e.Argon2id, "attacker-secret-0123456789abcdefghijklmnop") {
		t.Fatal("attacker secret verifies")
	}
	// The Mac's poll (same secret) now reports approved.
	rr = doJSON(t, mux, http.MethodPost, "/api/principals/enroll", "", enroll)
	if rr.Code != http.StatusOK {
		t.Fatalf("poll after approval: got %d %s", rr.Code, rr.Body)
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &er)
	if er.Status != PrincipalApproved {
		t.Fatalf("poll status = %q, want approved", er.Status)
	}

	// 4. Revoke removes the credential from the proxy file.
	rr = doJSON(t, mux, http.MethodPost, "/api/principals/revoke", h8bTestAdminKey, map[string]string{"name": er.Principal})
	if rr.Code != http.StatusOK {
		t.Fatalf("revoke: got %d %s", rr.Code, rr.Body)
	}
	if _, ok := findProxyPrincipal(readProxyPrincipals(t, principalsPath), er.Principal); ok {
		t.Fatal("revoked Mac still in the proxy principals file")
	}
	// A revoked Mac cannot re-approve itself by enrolling again.
	rr = doJSON(t, mux, http.MethodPost, "/api/principals/enroll", "", enroll)
	if rr.Code == http.StatusOK || rr.Code == http.StatusAccepted {
		var again EnrollResponse
		_ = json.Unmarshal(rr.Body.Bytes(), &again)
		if again.Status != PrincipalRevoked {
			t.Fatalf("revoked Mac re-enrolled into status %q", again.Status)
		}
	}

	// 5. The auto-approve window (admin-only) approves new Macs at once,
	// and only while it is open.
	rr = doJSON(t, mux, http.MethodPost, "/api/principals/auto-approve", "", map[string]int{"hours": 24})
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("auto-approve without admin key: got %d, want 401", rr.Code)
	}
	rr = doJSON(t, mux, http.MethodPost, "/api/principals/auto-approve", h8bTestAdminKey, map[string]int{"hours": 24})
	if rr.Code != http.StatusOK {
		t.Fatalf("open auto-approve: %d %s", rr.Code, rr.Body)
	}
	second := map[string]string{"instance_id": "0A0B0C0D-1111-4222-8333-444455556666", "host": "Laptop", "secret": testMacSecret + "-2"}
	rr = doJSON(t, mux, http.MethodPost, "/api/principals/enroll", "", second)
	var er2 EnrollResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &er2)
	if rr.Code != http.StatusOK || er2.Status != PrincipalApproved {
		t.Fatalf("enroll inside the window: %d %+v, want approved", rr.Code, er2)
	}
	if _, ok := findProxyPrincipal(readProxyPrincipals(t, principalsPath), er2.Principal); !ok {
		t.Fatal("auto-approved Mac missing from the proxy principals file")
	}
	rr = doJSON(t, mux, http.MethodPost, "/api/principals/auto-approve", h8bTestAdminKey, map[string]int{"hours": 0})
	if rr.Code != http.StatusOK {
		t.Fatalf("close auto-approve: %d %s", rr.Code, rr.Body)
	}
	third := map[string]string{"instance_id": "99999999-1111-4222-8333-444455556666", "host": "Late", "secret": testMacSecret + "-3"}
	rr = doJSON(t, mux, http.MethodPost, "/api/principals/enroll", "", third)
	var er3 EnrollResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &er3)
	if er3.Status != PrincipalPending {
		t.Fatalf("enroll after the window closed: %+v, want pending", er3)
	}
}

// TestH8BEnrollWindowExpires: the auto-approve window ends by itself.
func TestH8BEnrollWindowExpires(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 11, 3, 9, 0, 0, 0, time.UTC)
	s, err := NewPrincipalStore(PrincipalStoreOptions{
		StatePath: filepath.Join(dir, "store.json"),
		ProxyDir:  dir,
		Now:       func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	startFakeProxyAck(t, dir)
	if _, err := s.SetAutoApproveWindow(time.Hour); err != nil {
		t.Fatal(err)
	}
	now = now.Add(61 * time.Minute)
	r, err := s.Enroll(EnrollRequest{InstanceID: testInstanceID, Host: "h", Secret: testMacSecret}, "192.0.2.1")
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != PrincipalPending {
		t.Fatalf("enroll after window expiry = %q, want pending", r.Status)
	}
}

// TestH8BPrincipalsFileWrittenAtomically0600: the proxy's principals file
// holds every credential the volume accepts. It is always 0600, it is
// replaced atomically (a failed write leaves the previous table intact and
// no partial or temporary file behind), the server principals the proxy
// entrypoint generated are preserved, and the proxy is signalled.
func TestH8BPrincipalsFileWrittenAtomically0600(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "principals.json")
	// The entrypoint wrote the server principals at boot, world-readable
	// by mistake: the manager must not inherit that mode.
	serverHash := "$argon2id$v=19$m=19456,t=2,p=1$c2FsdHNhbHRzYWx0c2FsdA$aGFzaGhhc2hoYXNoaGFzaGhhc2hoYXNoaGFzaGhhc2g"
	seed := `{"principals":[{"name":"farm","kind":"farm","argon2id":"` + serverHash + `"}]}`
	if err := os.WriteFile(path, []byte(seed), 0o644); err != nil {
		t.Fatal(err)
	}
	old := syscall.Umask(0)
	defer syscall.Umask(old)

	// The proxy is live: it acknowledged a request of this Manager and its
	// heartbeat is fresh (this test's signaler writes no files, so the
	// handshake is written directly and the line recorded as issued below).
	for _, f := range []string{reloadRequestName, reloadAckName, reloadHeartbeatName} {
		if err := os.WriteFile(filepath.Join(dir, f), []byte("live 1\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	sig := &recordingSignaler{}
	s, err := NewPrincipalStore(PrincipalStoreOptions{
		StatePath: filepath.Join(dir, "store.json"),
		ProxyDir:  dir,
		Signal:    sig,
	})
	if err != nil {
		t.Fatal(err)
	}
	s.issued = append(s.issued, issuedRequest{line: "live 1", at: time.Now().Add(-time.Minute)}) // written before the heartbeat
	r, err := s.Enroll(EnrollRequest{InstanceID: testInstanceID, Host: "Edit-Bay-2", Secret: testMacSecret}, "192.0.2.1")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Approve(r.Principal); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o600 {
		t.Fatalf("principals file mode = %o, want 600", st.Mode().Perm())
	}
	for _, aux := range []string{"rollback-macs.acl", "store.json"} {
		if st, err := os.Stat(filepath.Join(dir, aux)); err != nil || st.Mode().Perm() != 0o600 {
			t.Fatalf("%s: mode %v err %v, want 0600", aux, st, err)
		}
	}
	entries := readProxyPrincipals(t, path)
	if f, ok := findProxyPrincipal(entries, "farm"); !ok || f.Argon2id != serverHash || f.Kind != "farm" {
		t.Fatalf("server principal not preserved verbatim: %+v", entries)
	}
	if _, ok := findProxyPrincipal(entries, r.Principal); !ok {
		t.Fatal("approved Mac missing")
	}
	if len(sig.shas) == 0 || sig.shas[len(sig.shas)-1] != sha256File(t, path) {
		t.Fatalf("proxy not signalled with the written file's sha256 (got %v)", sig.shas)
	}

	// A write that dies half way (disk full, crash) must leave the previous
	// table byte-identical: the proxy may reload at any instant.
	before, _ := os.ReadFile(path)
	// Fail the principals file itself, half way through its body.
	atomicWriteHook = func(f *os.File, data []byte) error {
		if !strings.Contains(filepath.Base(f.Name()), "principals.json") {
			_, err := f.Write(data)
			return err
		}
		_, _ = f.Write(data[:len(data)/2])
		return errors.New("injected: disk full")
	}
	defer func() { atomicWriteHook = nil }()
	signalsBefore := len(sig.shas)
	if err := s.Revoke(r.Principal); err == nil {
		t.Fatal("revoke reported success although the principals file write failed")
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatalf("failed write changed the live principals file:\nbefore %s\nafter  %s", before, after)
	}
	if len(sig.shas) != signalsBefore {
		t.Fatal("proxy was signalled for a write that failed")
	}
	left, _ := filepath.Glob(filepath.Join(dir, "*.tmp*"))
	left2, _ := filepath.Glob(filepath.Join(dir, ".principals*"))
	for _, l := range append(left, left2...) {
		if !strings.HasSuffix(l, ".lock") {
			t.Fatalf("temporary file left behind: %s", l)
		}
	}
	// The failed revoke must not be half-applied in the store either: the
	// Mac is still approved, so a retry is honest.
	if got := s.Snapshot().byName(r.Principal); got == nil || got.Status != PrincipalApproved {
		t.Fatalf("store state after failed write = %+v, want still approved", got)
	}
	// A failure of the rollback-ACL fragment (written after the principals
	// file) must put the principals file back: the two never disagree.
	atomicWriteHook = func(f *os.File, data []byte) error {
		if strings.Contains(filepath.Base(f.Name()), "rollback-macs.acl") {
			return errors.New("injected: fragment write failed")
		}
		_, err := f.Write(data)
		return err
	}
	if err := s.Revoke(r.Principal); err == nil {
		t.Fatal("revoke reported success although the rollback fragment write failed")
	}
	if after, _ := os.ReadFile(path); !bytes.Equal(before, after) {
		t.Fatalf("failed fragment write left a changed principals file:\nbefore %s\nafter  %s", before, after)
	}
	atomicWriteHook = nil
	if err := s.Revoke(r.Principal); err != nil {
		t.Fatal(err)
	}
	if _, ok := findProxyPrincipal(readProxyPrincipals(t, path), r.Principal); ok {
		t.Fatal("retried revoke did not remove the Mac")
	}
}

// TestH8BRollbackACLMirrorsPrincipals: plan §3 rollback step 3 loads plain
// Redis ACL users with the same names and secrets as the proxy principals.
func TestH8BRollbackACLMirrorsPrincipals(t *testing.T) {
	dir := t.TempDir()
	s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: filepath.Join(dir, "store.json"), ProxyDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	startFakeProxyAck(t, dir)
	r, err := s.Enroll(EnrollRequest{InstanceID: testInstanceID, Host: "Edit-Bay-2", Secret: testMacSecret}, "192.0.2.1")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Approve(r.Principal); err != nil {
		t.Fatal(err)
	}
	pending, err := s.Enroll(EnrollRequest{InstanceID: "0A0B0C0D-1111-4222-8333-444455556666", Host: "p", Secret: testMacSecret + "-p"}, "192.0.2.2")
	if err != nil {
		t.Fatal(err)
	}
	acl := RenderRollbackACL([]RollbackUser{{Name: "farm", Secret: "farm-secret-0123456789abcdef0123456789"}}, s.RollbackSnapshot())
	want := "user " + r.Principal + " on #" + sha256Hex(testMacSecret) + " ~* &* +@all"
	if !strings.Contains(acl, want) {
		t.Fatalf("rollback ACL lacks the mirrored Mac user line %q:\n%s", want, acl)
	}
	if !strings.Contains(acl, "user farm on #"+sha256Hex("farm-secret-0123456789abcdef0123456789")+" ~* &* +@all") {
		t.Fatalf("rollback ACL lacks the server principal:\n%s", acl)
	}
	if strings.Contains(acl, pending.Principal) {
		t.Fatal("a pending Mac is in the rollback ACL")
	}
	if strings.Contains(acl, testMacSecret) || strings.Contains(acl, "nopass") || strings.Contains(acl, ">") {
		t.Fatalf("rollback ACL carries a plaintext secret or a passwordless user:\n%s", acl)
	}
	frag, err := os.ReadFile(filepath.Join(dir, "rollback-macs.acl"))
	if err != nil || !strings.Contains(string(frag), want) {
		t.Fatalf("rollback-macs.acl fragment missing the Mac line: %v\n%s", err, frag)
	}
}

func countHashes(t *testing.T) *int {
	t.Helper()
	n := new(int)
	var mu sync.Mutex
	origH, origV := hashSecret, verifySecret
	hashSecret = func(s string) (string, error) { mu.Lock(); *n++; mu.Unlock(); return origH(s) }
	verifySecret = func(h, s string) bool { mu.Lock(); *n++; mu.Unlock(); return origV(h, s) }
	t.Cleanup(func() { hashSecret, verifySecret = origH, origV })
	return n
}

func instanceN(i int) string { return fmt.Sprintf("%08X-1111-4222-8333-444455556666", i) }

func newTestStore(t *testing.T, maxPending int) *PrincipalStore {
	t.Helper()
	dir := t.TempDir()
	s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: filepath.Join(dir, "store.json"), ProxyDir: dir, MaxPending: maxPending})
	if err != nil {
		t.Fatal(err)
	}
	startFakeProxyAck(t, dir)
	return s
}

// TestH8BEnrollAbuseBounds: the unauthenticated enroll route cannot be
// turned into memory exhaustion or a flood of requests (review 2026-10-06:
// 200 concurrent argon2id polls = 3.8 GiB).
func TestH8BEnrollAbuseBounds(t *testing.T) {
	t.Run("pending caps are checked before any hashing", func(t *testing.T) {
		s := newTestStore(t, 1)
		n := countHashes(t)
		if _, err := s.Enroll(EnrollRequest{InstanceID: instanceN(1), Host: "a", Secret: testMacSecret}, "192.0.2.1"); err != nil {
			t.Fatal(err)
		}
		before := *n
		_, err := s.Enroll(EnrollRequest{InstanceID: instanceN(2), Host: "b", Secret: testMacSecret}, "192.0.2.2")
		if !errors.Is(err, ErrEnrollTooMany) {
			t.Fatalf("over the global pending cap: err = %v", err)
		}
		if *n != before {
			t.Fatal("an enrollment over the pending cap still ran argon2id")
		}
	})
	t.Run("per-address pending cap", func(t *testing.T) {
		s := newTestStore(t, 64)
		for i := 0; i < maxPendingPerIP; i++ {
			if _, err := s.Enroll(EnrollRequest{InstanceID: instanceN(10 + i), Host: "h", Secret: testMacSecret}, "192.0.2.9"); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.Enroll(EnrollRequest{InstanceID: instanceN(99), Host: "h", Secret: testMacSecret}, "192.0.2.9"); !errors.Is(err, ErrEnrollTooMany) {
			t.Fatalf("pending request %d from one address: err = %v", maxPendingPerIP+1, err)
		}
	})
	t.Run("per-address request rate", func(t *testing.T) {
		s := newTestStore(t, 64)
		var limited bool
		for i := 0; i < 40; i++ {
			_, err := s.Enroll(EnrollRequest{InstanceID: instanceN(1), PollToken: strings.Repeat("a", 64)}, "192.0.2.3")
			if errors.Is(err, ErrEnrollRateLimited) {
				limited = true
				break
			}
		}
		if !limited {
			t.Fatal("40 requests in a burst from one address were never rate-limited")
		}
		// Another address is unaffected.
		if _, err := s.Enroll(EnrollRequest{InstanceID: instanceN(1), PollToken: strings.Repeat("a", 64)}, "192.0.2.4"); !errors.Is(err, ErrEnrollBadToken) {
			t.Fatalf("other address: err = %v", err)
		}
	})
	t.Run("concurrent hashing is bounded", func(t *testing.T) {
		s := newTestStore(t, 64)
		old := hashWaitTimeout
		hashWaitTimeout = 50 * time.Millisecond
		defer func() { hashWaitTimeout = old }()
		for i := 0; i < cap(s.hashSem); i++ {
			s.hashSem <- struct{}{} // every slot busy
		}
		_, err := s.Enroll(EnrollRequest{InstanceID: instanceN(5), Host: "h", Secret: testMacSecret}, "192.0.2.5")
		if !errors.Is(err, ErrEnrollBusy) {
			t.Fatalf("enroll with every hashing slot busy: err = %v, want ErrEnrollBusy", err)
		}
	})
	t.Run("polls use the token, never argon2id, and the secret crosses once", func(t *testing.T) {
		s := newTestStore(t, 64)
		r, err := s.Enroll(EnrollRequest{InstanceID: instanceN(7), Host: "h", Secret: testMacSecret}, "192.0.2.7")
		if err != nil || len(r.PollToken) != 64 {
			t.Fatalf("first enroll: %+v %v, want a poll token", r, err)
		}
		n := countHashes(t)
		p, err := s.Enroll(EnrollRequest{InstanceID: instanceN(7), PollToken: r.PollToken}, "192.0.2.7")
		if err != nil || p.Status != PrincipalPending || p.PollToken != "" {
			t.Fatalf("token poll: %+v %v", p, err)
		}
		if *n != 0 {
			t.Fatal("a token poll ran argon2id")
		}
		if _, err := s.Enroll(EnrollRequest{InstanceID: instanceN(7), PollToken: strings.Repeat("0", 64)}, "192.0.2.7"); !errors.Is(err, ErrEnrollBadToken) {
			t.Fatalf("wrong token: err = %v", err)
		}
		if strings.Contains(fmt.Sprint(s.Snapshot()), r.PollToken) || s.Snapshot().byName(r.Principal).PollTokenSHA256 != "" {
			t.Fatal("the admin view exposes the poll token or its hash")
		}
	})
}

// TestH8BAutoApproveWindowCapped: the cutover window approves at most its
// cap in total and two per source address; everything it approved stays on
// the review list until an admin confirms or revokes it.
func TestH8BAutoApproveWindowCapped(t *testing.T) {
	s := newTestStore(t, 64)
	if _, err := s.SetAutoApproveWindow(time.Hour, 3); err != nil {
		t.Fatal(err)
	}
	status := func(i int, ip string) PrincipalStatus {
		r, err := s.Enroll(EnrollRequest{InstanceID: instanceN(i), Host: "h", Secret: testMacSecret}, ip)
		if err != nil {
			t.Fatal(err)
		}
		return r.Status
	}
	if status(1, "192.0.2.1") != PrincipalApproved || status(2, "192.0.2.1") != PrincipalApproved {
		t.Fatal("first two from one address not auto-approved")
	}
	if got := status(3, "192.0.2.1"); got != PrincipalPending {
		t.Fatalf("third from one address = %s, want pending (per-address cap)", got)
	}
	if status(4, "192.0.2.2") != PrincipalApproved {
		t.Fatal("first from a second address not auto-approved")
	}
	if got := status(5, "192.0.2.3"); got != PrincipalPending {
		t.Fatalf("fourth approval = %s, want pending (window cap 3)", got)
	}
	snap := s.Snapshot()
	if snap.AutoApproveUsed != 3 || snap.AutoApproveMax != 3 || snap.NeedsReview != 3 {
		t.Fatalf("snapshot used=%d max=%d review=%d, want 3/3/3", snap.AutoApproveUsed, snap.AutoApproveMax, snap.NeedsReview)
	}
	if err := s.Approve(macPrincipalName(instanceN(1))); err != nil {
		t.Fatal(err)
	}
	if err := s.Revoke(macPrincipalName(instanceN(2))); err != nil {
		t.Fatal(err)
	}
	if got := s.Snapshot().NeedsReview; got != 1 {
		t.Fatalf("after confirming one and revoking one, needs_review = %d, want 1", got)
	}
}

// TestH8BAdminRoutesRequireKey: every H-8B admin route refuses a request
// without the Manager admin key (only /api/principals/enroll is open).
func TestH8BAdminRoutesRequireKey(t *testing.T) {
	mux, _, _, _ := newH8BAPI(t)
	for _, rt := range []struct{ method, path string }{
		{http.MethodGet, "/api/principals"},
		{http.MethodPost, "/api/principals/approve"},
		{http.MethodPost, "/api/principals/revoke"},
		{http.MethodPost, "/api/principals/forget"},
		{http.MethodPost, "/api/principals/auto-approve"},
		{http.MethodGet, "/api/leases"},
		{http.MethodPost, "/api/leases/break"},
		{http.MethodGet, "/api/leases/mode"},
		{http.MethodPost, "/api/leases/mode"},
	} {
		rr := doJSON(t, mux, rt.method, rt.path, "", map[string]string{"name": "mac-000000000000", "mode": "passthrough", "project": "ino:1", "confirm": "ino:1"})
		if rr.Code != http.StatusUnauthorized {
			t.Errorf("%s %s without the admin key: %d, want 401", rt.method, rt.path, rr.Code)
		}
		rr = doJSON(t, mux, rt.method, rt.path, "wrong-key-0123456789abcdef0123456789", nil)
		if rr.Code != http.StatusUnauthorized {
			t.Errorf("%s %s with a wrong key: %d, want 401", rt.method, rt.path, rr.Code)
		}
	}
}
