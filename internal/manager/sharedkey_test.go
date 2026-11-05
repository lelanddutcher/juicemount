package manager

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// sharedKeyRig is a shared-key-mode store (JM_OBJ_PER_MAC off) whose
// configured jm-client key the test can change, as a Manager recreated
// with a new server/.env would see it.
type sharedKeyRig struct {
	t         *testing.T
	statePath string
	proxyDir  string
	ak, sk    string
	s         *PrincipalStore
}

func newSharedKeyRig(t *testing.T) *sharedKeyRig {
	t.Helper()
	dir := t.TempDir()
	r := &sharedKeyRig{t: t, statePath: filepath.Join(dir, "store", "principals-store.json"),
		proxyDir: filepath.Join(dir, "respproxy"), ak: testClientAK, sk: testClientSK}
	if err := os.MkdirAll(r.proxyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	startFakeProxyAck(t, r.proxyDir)
	r.open()
	return r
}

// open (re)starts the Manager's store with the key configured now.
func (r *sharedKeyRig) open() {
	r.t.Helper()
	ak, sk := r.ak, r.sk
	s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: r.statePath, ProxyDir: r.proxyDir,
		ObjectKey: func() (string, string) { return ak, sk }})
	if err != nil {
		r.t.Fatal(err)
	}
	r.s = s
}

// approved enrolls Mac i and approves it; it returns its name and poll token.
func (r *sharedKeyRig) approved(i int) (string, string) {
	r.t.Helper()
	name, tok := r.enroll(i)
	if err := r.s.Approve(name); err != nil {
		r.t.Fatalf("approve %s: %v", name, err)
	}
	return name, tok
}

func (r *sharedKeyRig) enroll(i int) (string, string) {
	r.t.Helper()
	objRigIP++
	resp, err := r.s.Enroll(EnrollRequest{InstanceID: instanceN(i), Host: "Mac-" + string(rune('A'+i)),
		Secret: strings.Repeat(string(rune('a'+i%26)), 40)}, "10.77.0."+string(rune('0'+i)))
	if err != nil {
		r.t.Fatalf("enroll %d: %v", i, err)
	}
	return resp.Principal, resp.PollToken
}

func (r *sharedKeyRig) poll(i int, token string) EnrollResponse {
	r.t.Helper()
	objRigIP++
	resp, err := r.s.Enroll(EnrollRequest{InstanceID: instanceN(i), PollToken: token}, "10.78.0."+string(rune('0'+i)))
	if err != nil {
		r.t.Fatalf("poll %d: %v", i, err)
	}
	return resp
}

func stillValidNames(s PrincipalsSnapshot) []string {
	var out []string
	for _, h := range s.SharedKeyStillValid {
		out = append(out, h.Name)
	}
	return out
}

// Revoking a Mac that holds the shared jm-client key says plainly that the
// key still works for it, and records the pending rotation durably; a Mac
// rejected while pending was never given the key.
func TestRevokeSharedKeyReportsStillValid(t *testing.T) {
	r := newSharedKeyRig(t)
	a, aTok := r.approved(1)
	if got := r.poll(1, aTok); got.ObjectAccessKey != testClientAK || got.ObjectSecretKey != testClientSK {
		t.Fatalf("approved poll = %+v, want the shared key", got)
	}
	if !r.s.Snapshot().byName(a).SharedKeyHeld {
		t.Fatal("approved Mac on the shared key: shared_key_held not set (the revoke confirmation relies on it)")
	}
	pending, _ := r.enroll(2)
	if r.s.Snapshot().byName(pending).SharedKeyHeld {
		t.Fatal("pending Mac marked as holding the shared key")
	}

	res, err := r.s.RevokeWithResult(a)
	if err != nil {
		t.Fatal(err)
	}
	if res.ObjectKey != RevokeObjectKeyStillValid || !strings.Contains(res.Message, "STILL WORKS") {
		t.Fatalf("revoke result = %+v, want still_valid with a plain warning", res)
	}
	if strings.Contains(res.Message, testClientSK) {
		t.Fatal("revoke result carries the shared secret")
	}
	if got := stillValidNames(r.s.Snapshot()); len(got) != 1 || got[0] != a {
		t.Fatalf("pending rotation lists %v, want [%s]", got, a)
	}
	// The revoked Mac's poll carries no key.
	if got := r.poll(1, aTok); got.Status != PrincipalRevoked || got.ObjectAccessKey != "" || got.ObjectSecretKey != "" {
		t.Fatalf("revoked poll = %+v, want revoked with no key", got)
	}
	// Revoking again reports the same state.
	if res, err := r.s.RevokeWithResult(a); err != nil || res.ObjectKey != RevokeObjectKeyStillValid {
		t.Fatalf("second revoke = %+v, %v; want still_valid", res, err)
	}

	res, err = r.s.RevokeWithResult(pending)
	if err != nil {
		t.Fatal(err)
	}
	if res.ObjectKey != RevokeObjectKeyNone {
		t.Fatalf("reject of a pending Mac = %+v, want none", res)
	}

	// Durable: a Manager restart (same key) still lists it, Forget does not
	// hide it, and the store holds a check value, never the secret.
	r.open()
	if err := r.s.Forget(a); err != nil {
		t.Fatal(err)
	}
	if got := stillValidNames(r.s.Snapshot()); len(got) != 1 || got[0] != a {
		t.Fatalf("after restart and Forget: pending rotation lists %v, want [%s]", got, a)
	}
	raw, err := os.ReadFile(r.statePath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), testClientSK) {
		t.Fatal("the principal store holds the shared secret")
	}
	if !strings.Contains(string(raw), `"shared_key_holders"`) {
		t.Fatal("the pending rotation is not in the principal store")
	}
}

// After the operator rotates JM_OBJ_CLIENT_SECRET_KEY and recreates the
// Manager, the pending rotation clears, a remaining approved Mac gets the
// new key at its next poll, and the revoked Mac gets nothing.
func TestRevokeSharedKeyClearsAfterRotation(t *testing.T) {
	r := newSharedKeyRig(t)
	a, aTok := r.approved(1)
	b, bTok := r.approved(2)
	if _, err := r.s.RevokeWithResult(a); err != nil {
		t.Fatal(err)
	}
	if got := stillValidNames(r.s.Snapshot()); len(got) != 1 {
		t.Fatalf("pending rotation lists %v", got)
	}

	const rotated = "rotatedsecret0123456789abcdef0123456789"
	r.sk = rotated
	r.open()
	if got := r.s.Snapshot().SharedKeyStillValid; len(got) != 0 {
		t.Fatalf("after rotation the page still lists %+v", got)
	}
	if got := r.poll(2, bTok); got.Status != PrincipalApproved || got.ObjectAccessKey != testClientAK || got.ObjectSecretKey != rotated {
		t.Fatalf("remaining Mac %s poll = %+v, want the rotated key", b, got)
	}
	if got := r.poll(1, aTok); got.ObjectAccessKey != "" || got.ObjectSecretKey != "" {
		t.Fatalf("revoked Mac poll = %+v, want no key", got)
	}
	// A revoke after the rotation reports the rotated key as still valid
	// for that Mac (it was given the new one).
	if res, err := r.s.RevokeWithResult(b); err != nil || res.ObjectKey != RevokeObjectKeyStillValid {
		t.Fatalf("revoke after rotation = %+v, %v; want still_valid", res, err)
	}
	if got := stillValidNames(r.s.Snapshot()); len(got) != 1 || got[0] != b {
		t.Fatalf("pending rotation lists %v, want [%s]", got, b)
	}
	// Back to the old key (an older server/.env restored, so minio-keys
	// sets the old secret again): a's copy works again and is listed again;
	// b was given only the rotated key, which no longer works.
	r.sk = testClientSK
	r.open()
	if got := stillValidNames(r.s.Snapshot()); len(got) != 1 || got[0] != a {
		t.Fatalf("under the old key the page lists %v, want [%s]", got, a)
	}
}

// Approving a revoked Mac again clears its entry: it holds the key
// legitimately again.
func TestRevokeSharedKeyReapproveClears(t *testing.T) {
	r := newSharedKeyRig(t)
	a, _ := r.approved(1)
	if err := r.s.Revoke(a); err != nil {
		t.Fatal(err)
	}
	if err := r.s.Approve(a); err != nil {
		t.Fatal(err)
	}
	if got := r.s.Snapshot().SharedKeyStillValid; len(got) != 0 {
		t.Fatalf("re-approved Mac still listed: %+v", got)
	}
	r.open()
	if got := r.s.Snapshot().SharedKeyStillValid; len(got) != 0 {
		t.Fatalf("after restart, re-approved Mac listed: %+v", got)
	}
}

// A revoke whose store write fails changes nothing: the Mac stays approved
// and no pending rotation is recorded in memory.
func TestRevokeSharedKeyStoreFailureRollsBack(t *testing.T) {
	r := newSharedKeyRig(t)
	a, _ := r.approved(1)
	// The store path becomes a non-empty directory: the atomic rename fails.
	if err := os.Remove(r.statePath); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(r.statePath, "x"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := r.s.RevokeWithResult(a); err == nil {
		t.Fatal("revoke succeeded with an unwritable store")
	}
	snap := r.s.Snapshot()
	if p := snap.byName(a); p == nil || p.Status != PrincipalApproved {
		t.Fatalf("after a failed revoke: %+v, want still approved", p)
	}
	r.s.mu.Lock()
	held := len(r.s.sharedHolders)
	r.s.mu.Unlock()
	if len(snap.SharedKeyStillValid) != 0 || held != 0 {
		t.Fatalf("a failed revoke recorded %+v (%d held)", snap.SharedKeyStillValid, held)
	}
}

// Without a configured client key no Mac was given one: nothing to rotate.
func TestRevokeSharedKeyNoClientKey(t *testing.T) {
	r := newSharedKeyRig(t)
	r.ak, r.sk = "", ""
	r.open()
	a, _ := r.approved(1)
	res, err := r.s.RevokeWithResult(a)
	if err != nil || res.ObjectKey != RevokeObjectKeyNone {
		t.Fatalf("revoke = %+v, %v; want none", res, err)
	}
	if got := r.s.Snapshot().SharedKeyStillValid; len(got) != 0 {
		t.Fatalf("listed %+v with no client key configured", got)
	}
}

// Per-Mac keys on: a Mac with its own active key is cut off by removing that
// key; a Mac approved before the switch that is still on the shared key is
// reported like shared mode.
func TestRevokePerMacKeyVersusLegacyShared(t *testing.T) {
	setObjectKeyEnv(t)
	r := newObjRig(t)
	off, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: r.statePath, ProxyDir: r.proxyDir, ObjectKey: clientObjectKey})
	if err != nil {
		t.Fatal(err)
	}
	r.s = off
	legacy, _ := r.enroll(5)
	if err := off.Approve(legacy); err != nil {
		t.Fatal(err)
	}
	r.open() // per-Mac keys on, keyd down: the legacy Mac stays on the shared key
	if !r.s.Snapshot().byName(legacy).SharedKeyHeld {
		t.Fatal("legacy Mac still on the shared key: shared_key_held not set")
	}
	res, err := r.s.RevokeWithResult(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if res.ObjectKey != RevokeObjectKeyStillValid {
		t.Fatalf("legacy revoke = %+v, want still_valid", res)
	}

	own, _ := r.enroll(6)
	r.keydUp()
	if err := r.s.Approve(own); err != nil {
		t.Fatal(err)
	}
	if r.s.Snapshot().byName(own).SharedKeyHeld {
		t.Fatal("Mac with its own key marked as holding the shared key")
	}
	res, err = r.s.RevokeWithResult(own)
	if err != nil {
		t.Fatal(err)
	}
	if res.ObjectKey != RevokeObjectKeyRemoving {
		t.Fatalf("own-key revoke = %+v, want removing", res)
	}
	if got := stillValidNames(r.s.Snapshot()); len(got) != 1 || got[0] != legacy {
		t.Fatalf("pending rotation lists %v, want only [%s]", got, legacy)
	}
}

// The revoke route answers with the result and the pending rotation; the
// Principals page renders both and warns before the revoke.
func TestRevokeSharedKeyHTTPAndPage(t *testing.T) {
	setObjectKeyEnv(t)
	mux, _, _, _ := newH8BAPI(t)
	enroll := map[string]string{"instance_id": testInstanceID, "host": "Edit-Bay-2", "secret": testMacSecret}
	rr := doJSON(t, mux, http.MethodPost, "/api/principals/enroll", "", enroll)
	name := decodeEnroll(t, rr.Body.Bytes()).Principal
	if rr = doJSON(t, mux, http.MethodPost, "/api/principals/approve", h8bTestAdminKey, map[string]string{"name": name}); rr.Code != http.StatusOK {
		t.Fatalf("approve: %d %s", rr.Code, rr.Body)
	}
	rr = doJSON(t, mux, http.MethodPost, "/api/principals/revoke", h8bTestAdminKey, map[string]string{"name": name})
	if rr.Code != http.StatusOK {
		t.Fatalf("revoke: %d %s", rr.Code, rr.Body)
	}
	var snap PrincipalsSnapshot
	if err := json.Unmarshal(rr.Body.Bytes(), &snap); err != nil {
		t.Fatal(err)
	}
	if snap.Revoked == nil || snap.Revoked.Name != name || snap.Revoked.ObjectKey != RevokeObjectKeyStillValid {
		t.Fatalf("revoke answer: revoked = %+v, want %s still_valid", snap.Revoked, name)
	}
	if got := stillValidNames(snap); len(got) != 1 || got[0] != name {
		t.Fatalf("revoke answer lists %v", got)
	}
	if strings.Contains(rr.Body.String(), testClientSK) || strings.Contains(rr.Body.String(), "key_check") {
		t.Fatalf("revoke answer leaks the key or its check value: %s", rr.Body)
	}
	rr = doJSON(t, mux, http.MethodGet, "/api/principals", h8bTestAdminKey, nil)
	if !strings.Contains(rr.Body.String(), `"shared_key_still_valid"`) || strings.Contains(rr.Body.String(), `"revoked":{`) {
		t.Fatalf("list: %s", rr.Body)
	}

	raw, err := staticFS.ReadFile("static/principals.html")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"shared_key_still_valid", "shared_key_held", "r.revoked.object_key === 'still_valid'",
		"Its storage key will STILL WORK", "Rotate the shared storage key", "Rotating the shared object key"} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("principals.html does not render %q", want)
		}
	}
}
