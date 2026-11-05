package manager

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

// fakeMinIO answers ObjectStoreProbe like MinIO: one secret per user.
type fakeMinIO struct {
	mu    sync.Mutex
	users map[string]string
	down  bool
}

func (m *fakeMinIO) set(ak, sk string) { m.mu.Lock(); m.users[ak] = sk; m.mu.Unlock() }
func (m *fakeMinIO) del(ak string)     { m.mu.Lock(); delete(m.users, ak); m.mu.Unlock() }

func (m *fakeMinIO) probe(_ context.Context, ak, sk string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down {
		return "", errors.New("connection refused")
	}
	want, ok := m.users[ak]
	switch {
	case !ok:
		return "InvalidAccessKeyId", nil
	case want != sk:
		return "SignatureDoesNotMatch", nil
	}
	return "", nil
}

// sharedKeyRig is a shared-key-mode store (JM_OBJ_PER_MAC off) whose
// configured jm-client key the test can change, as a Manager recreated
// with a new server/.env would see it.
type sharedKeyRig struct {
	t         *testing.T
	statePath string
	proxyDir  string
	ak, sk    string
	minio     *fakeMinIO
	s         *PrincipalStore
}

func newSharedKeyRig(t *testing.T) *sharedKeyRig {
	t.Helper()
	dir := t.TempDir()
	r := &sharedKeyRig{t: t, statePath: filepath.Join(dir, "store", "principals-store.json"),
		proxyDir: filepath.Join(dir, "respproxy"), ak: testClientAK, sk: testClientSK,
		minio: &fakeMinIO{users: map[string]string{testClientAK: testClientSK}}}
	if err := os.MkdirAll(r.proxyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	startFakeProxyAck(t, r.proxyDir)
	r.open()
	return r
}

// open (re)starts the Manager's store. Its key is the rig's ak/sk (the
// Manager's environment; changing it without open is a test shortcut).
func (r *sharedKeyRig) open() {
	r.t.Helper()
	s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: r.statePath, ProxyDir: r.proxyDir,
		ObjectKey: func() (string, string) { return r.ak, r.sk }, ObjectStoreProbe: r.minio.probe})
	if err != nil {
		r.t.Fatal(err)
	}
	r.s = s
}

// verify runs one rotation check and returns the page's list.
func (r *sharedKeyRig) verify() PrincipalsSnapshot {
	r.t.Helper()
	r.s.VerifySharedKeyRotation(context.Background())
	return r.s.Snapshot()
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

// A new secret in the Manager's environment clears nothing: only the object
// store's own answer does (a signed request with the new key succeeds, so
// its one secret for the user is no longer the old one). A remaining
// approved Mac gets the new key at its next poll; the revoked Mac nothing.
func TestRevokeSharedKeyClearsOnlyOnObjectStoreProof(t *testing.T) {
	r := newSharedKeyRig(t)
	a, aTok := r.approved(1)
	b, bTok := r.approved(2)
	if _, err := r.s.RevokeWithResult(a); err != nil {
		t.Fatal(err)
	}

	// Step order gone wrong: the Manager runs with the new secret, MinIO
	// still has the old one.
	const rotated = "rotatedsecret0123456789abcdef0123456789"
	r.sk = rotated
	r.open()
	snap := r.verify()
	if got := stillValidNames(snap); len(got) != 1 || got[0] != a {
		t.Fatalf("Manager key changed, MinIO not: page lists %v, want [%s]", got, a)
	}
	if !strings.Contains(snap.SharedKeyProblem, "SignatureDoesNotMatch") {
		t.Fatalf("problem = %q, want the object store's refusal", snap.SharedKeyProblem)
	}
	// MinIO changed but unreachable: still no proof.
	r.minio.set(testClientAK, rotated)
	r.minio.down = true
	if got := stillValidNames(r.verify()); len(got) != 1 {
		t.Fatalf("object store unreachable: page lists %v, want [%s]", got, a)
	}
	r.minio.down = false
	snap = r.verify()
	if len(snap.SharedKeyStillValid) != 0 || snap.SharedKeyProblem != "" {
		t.Fatalf("after a confirmed rotation the page lists %+v (%q)", snap.SharedKeyStillValid, snap.SharedKeyProblem)
	}
	if got := r.poll(2, bTok); got.Status != PrincipalApproved || got.ObjectAccessKey != testClientAK || got.ObjectSecretKey != rotated {
		t.Fatalf("remaining Mac %s poll = %+v, want the rotated key", b, got)
	}
	if got := r.poll(1, aTok); got.ObjectAccessKey != "" || got.ObjectSecretKey != "" {
		t.Fatalf("revoked Mac poll = %+v, want no key", got)
	}
	// The old secret put back at MinIO only: a's copy works again.
	r.minio.set(testClientAK, testClientSK)
	if got := stillValidNames(r.verify()); len(got) != 1 || got[0] != a {
		t.Fatalf("old secret back at MinIO: page lists %v, want [%s]", got, a)
	}
}

// Renaming the shared access key proves nothing (minio-keys keeps the old
// user): the Mac stays listed until the object store reports the old access
// key unknown.
func TestRevokeSharedKeyAccessKeyRename(t *testing.T) {
	r := newSharedKeyRig(t)
	a, _ := r.approved(1)
	if _, err := r.s.RevokeWithResult(a); err != nil {
		t.Fatal(err)
	}
	r.ak, r.sk = "jm-client2", "renamedsecret0123456789abcdef0123456789"
	r.minio.set(r.ak, r.sk)
	r.open()
	snap := r.verify()
	if got := stillValidNames(snap); len(got) != 1 || got[0] != a || snap.SharedKeyStillValid[0].AccessKey != testClientAK {
		t.Fatalf("renamed key: page lists %+v, want %s on %s", snap.SharedKeyStillValid, a, testClientAK)
	}
	if !strings.Contains(snap.SharedKeyProblem, "old access key "+testClientAK) {
		t.Fatalf("problem = %q", snap.SharedKeyProblem)
	}
	// The old user is gone, but the current key is refused too (a wrong
	// address or a MinIO without the new user): no proof.
	r.minio.del(testClientAK)
	r.minio.del(r.ak)
	if got := stillValidNames(r.verify()); len(got) != 1 {
		t.Fatalf("old user unknown but the current key refused: page lists %v, want [%s]", got, a)
	}
	r.minio.set(r.ak, r.sk)
	if got := r.verify().SharedKeyStillValid; len(got) != 0 {
		t.Fatalf("old user removed: page lists %+v", got)
	}
}

// A Mac approved while the Manager handed out the shared key still knows it
// after it got its own per-Mac key.
func TestRevokeSharedKeyIssuedSurvivesPerMacKey(t *testing.T) {
	setObjectKeyEnv(t)
	r := newObjRig(t)
	off, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: r.statePath, ProxyDir: r.proxyDir, ObjectKey: clientObjectKey})
	if err != nil {
		t.Fatal(err)
	}
	r.s = off
	name, tok := r.enroll(7)
	if err := off.Approve(name); err != nil {
		t.Fatal(err)
	}
	if got := r.poll(7, tok); got.ObjectAccessKey != testClientAK {
		t.Fatalf("shared mode poll = %+v", got)
	}
	r.open()
	r.keydUp()
	r.waitFor("backfill", func() bool { return r.rec(name).ObjKeyState == ObjKeyActive })
	res, err := r.s.RevokeWithResult(name)
	if err != nil {
		t.Fatal(err)
	}
	if res.ObjectKey != RevokeObjectKeyStillValid {
		t.Fatalf("revoke of a Mac given the shared key, now on its own key = %+v, want still_valid", res)
	}
}

// The shared key is never handed out before its issue is recorded.
func TestSharedKeyNotHandedOutUnrecorded(t *testing.T) {
	r := newSharedKeyRig(t)
	r.ak, r.sk = "", ""
	a, tok := r.approved(1)
	r.ak, r.sk = testClientAK, testClientSK // configured without a restart
	if err := os.Remove(r.statePath); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(r.statePath, "x"), 0o700); err != nil {
		t.Fatal(err)
	}
	if got := r.poll(1, tok); got.ObjectAccessKey != "" || got.ObjectSecretKey != "" {
		t.Fatalf("poll with an unwritable store = %+v, want no key", got)
	}
	if err := os.RemoveAll(r.statePath); err != nil {
		t.Fatal(err)
	}
	if got := r.poll(1, tok); got.ObjectSecretKey != testClientSK {
		t.Fatalf("poll = %+v, want the key once recorded", got)
	}
	if res, err := r.s.RevokeWithResult(a); err != nil || res.ObjectKey != RevokeObjectKeyStillValid {
		t.Fatalf("revoke = %+v, %v; want still_valid", res, err)
	}
}

// Macs revoked by an earlier Manager (no holder records) are listed once,
// as legacy entries; later revokes of pending requests are not.
func TestRevokeSharedKeyLegacyRevokes(t *testing.T) {
	r := newSharedKeyRig(t)
	a, _ := r.approved(1)
	b, _ := r.enroll(2)
	for _, n := range []string{a, b} {
		if err := r.s.Revoke(n); err != nil {
			t.Fatal(err)
		}
	}
	// What an earlier Manager would have stored: no holders, no marks.
	raw, err := os.ReadFile(r.statePath)
	if err != nil {
		t.Fatal(err)
	}
	var f map[string]any
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	delete(f, "shared_key_holders")
	delete(f, "shared_key_tracking")
	for _, p := range f["principals"].([]any) {
		m := p.(map[string]any)
		delete(m, "shared_key_issued")
		if m["name"] == b {
			m["obj_key_gen"] = 1 // given a per-Mac key at some point: still counted
		}
	}
	raw, _ = json.Marshal(f)
	if err := os.WriteFile(r.statePath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	r.open()
	snap := r.s.Snapshot()
	if len(snap.SharedKeyStillValid) != 2 || !snap.SharedKeyStillValid[0].Legacy || !snap.SharedKeyStillValid[1].Legacy {
		t.Fatalf("legacy revokes: page lists %+v, want both as legacy", snap.SharedKeyStillValid)
	}
	r.open()
	if got := r.s.Snapshot().SharedKeyStillValid; len(got) != 2 {
		t.Fatalf("after a second start: %+v, want the same two", got)
	}
	// A pending request rejected by this Manager is accounted for: not listed.
	c, _ := r.enroll(3)
	if err := r.s.Revoke(c); err != nil {
		t.Fatal(err)
	}
	r.open()
	if got := r.s.Snapshot().SharedKeyStillValid; len(got) != 2 {
		t.Fatalf("a rejected request was listed: %+v", got)
	}
}

// The probe signs with AWS signature V4 and reports the S3 error code.
func TestObjectStoreProbeSignsAndParses(t *testing.T) {
	users := map[string]string{"jm-client": "probesecret0123456789abcdef0123456789ab"}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		auth := req.Header.Get("Authorization")
		_, rest, _ := strings.Cut(auth, "Credential=")
		ak, _, _ := strings.Cut(rest, "/")
		sk, ok := users[ak]
		if req.URL.Path != "/zpool" || req.URL.Query().Get("list-type") != "2" {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`<Error><Code>NoSuchBucket</Code></Error>`))
			return
		}
		if !ok {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`<?xml version="1.0"?><Error><Code>InvalidAccessKeyId</Code></Error>`))
			return
		}
		at, err := time.Parse("20060102T150405Z", req.Header.Get("X-Amz-Date"))
		if err != nil {
			t.Errorf("X-Amz-Date: %v", err)
		}
		again, _ := http.NewRequest(req.Method, "http://"+req.Host+req.URL.RequestURI(), nil)
		again.Header.Set("X-Amz-Content-Sha256", req.Header.Get("X-Amz-Content-Sha256"))
		_ = v4.NewSigner().SignHTTP(context.Background(), aws.Credentials{AccessKeyID: ak, SecretAccessKey: sk}, again,
			emptyPayloadSHA256, "s3", "us-east-1", at)
		if again.Header.Get("Authorization") != auth {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`<Error><Code>SignatureDoesNotMatch</Code></Error>`))
			return
		}
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>zpool</Name></ListBucketResult>`))
	}))
	defer srv.Close()
	probe := NewObjectStoreProbe(srv.URL+"/zpool", "")
	for _, c := range []struct{ ak, sk, want string }{
		{"jm-client", users["jm-client"], ""},
		{"jm-client", "wrongsecret0123456789abcdef0123456789ab", "SignatureDoesNotMatch"},
		{"jm-gone", "x", "InvalidAccessKeyId"},
	} {
		code, err := probe(context.Background(), c.ak, c.sk)
		if err != nil || code != c.want {
			t.Fatalf("probe(%s) = %q, %v; want %q", c.ak, code, err, c.want)
		}
	}
	if _, err := NewObjectStoreProbe("http://127.0.0.1:1/zpool", "")(context.Background(), "jm-client", "x"); err == nil {
		t.Fatal("unreachable object store: no error")
	}
	// A 200 that is not a listing of this bucket proves nothing.
	for _, body := range []string{`<html>login</html>`, `<ListBucketResult><Name>other</Name></ListBucketResult>`, ``} {
		other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(body)) }))
		code, err := NewObjectStoreProbe(other.URL+"/zpool", "")(context.Background(), "jm-client", "x")
		other.Close()
		if err == nil {
			t.Fatalf("200 with %q counted as accepted (code %q)", body, code)
		}
	}
}

// A Mac that an earlier Manager approved in shared mode and that later got
// its own per-Mac key is still counted as holding the shared key.
func TestSharedKeyAdoptionCountsPerMacKeyedMacs(t *testing.T) {
	setObjectKeyEnv(t)
	r := newObjRig(t)
	r.keydUp()
	name, _ := r.enroll(8)
	if err := r.s.Approve(name); err != nil {
		t.Fatal(err)
	}
	// What an earlier Manager left: an approved Mac with its own key and
	// no shared-key tracking at all.
	raw, err := os.ReadFile(r.statePath)
	if err != nil {
		t.Fatal(err)
	}
	var f map[string]any
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	delete(f, "shared_key_tracking")
	raw, _ = json.Marshal(f)
	if err := os.WriteFile(r.statePath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	r.open()
	if !r.rec(name).SharedKeyIssued {
		t.Fatal("adoption did not mark the approved per-Mac-keyed Mac")
	}
	if res, err := r.s.RevokeWithResult(name); err != nil || res.ObjectKey != RevokeObjectKeyStillValid {
		t.Fatalf("revoke = %+v, %v; want still_valid", res, err)
	}
	// Adopted once: a Mac approved now, under per-Mac keys, is not marked.
	other, _ := r.enroll(9)
	if err := r.s.Approve(other); err != nil {
		t.Fatal(err)
	}
	r.open()
	if r.rec(other).SharedKeyIssued {
		t.Fatal("a Mac approved under per-Mac keys after adoption was marked")
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
	if strings.Contains(rr.Body.String(), testClientSK) || strings.Contains(rr.Body.String(), "secret_check") {
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
		"Its storage key will STILL WORK", "Rotate the shared storage key", "Rotating the shared object key",
		"s.shared_key_problem", "sharedKeyLegacy", "Restarting the Manager with a new key alone clears nothing", "does not prove that no other credential"} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("principals.html does not render %q", want)
		}
	}
}

// The background verifier confirms a rotation without an admin action; a
// Manager without an object-store address says why nothing clears.
func TestSharedKeyVerifierAndWiring(t *testing.T) {
	r := newSharedKeyRig(t)
	a, _ := r.approved(1)
	if err := r.s.Revoke(a); err != nil {
		t.Fatal(err)
	}
	const rotated = "rotatedsecret0123456789abcdef0123456789"
	r.sk = rotated
	r.minio.set(testClientAK, rotated)
	r.open()
	stop := r.s.StartSharedKeyVerifier()
	defer stop()
	deadline := time.Now().Add(5 * time.Second)
	for len(r.s.Snapshot().SharedKeyStillValid) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the verifier did not confirm the rotation")
		}
		time.Sleep(10 * time.Millisecond)
	}

	noProbe, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: filepath.Join(t.TempDir(), "s.json"),
		ObjectKey: func() (string, string) { return r.ak, r.sk }})
	if err != nil {
		t.Fatal(err)
	}
	noProbe.mu.Lock()
	noProbe.sharedHolders = []sharedKeyHolder{{Name: a, AccessKey: testClientAK, SecretCheck: newSharedKeyCheck(testClientSK)}}
	noProbe.mu.Unlock()
	if snap := noProbe.Snapshot(); len(snap.SharedKeyStillValid) != 1 || snap.SharedKeyProblem != noProbeProblem {
		t.Fatalf("no probe: %+v %q", snap.SharedKeyStillValid, snap.SharedKeyProblem)
	}

	t.Setenv("JM_OBJ_BUCKET_URL", "")
	t.Setenv("JM_MINIO_URL", "http://minio:9000/")
	t.Setenv("JM_VOL_NAME", "zpool")
	if got := objectStoreBucketURL(); got != "http://minio:9000/zpool" {
		t.Fatalf("bucket URL = %q", got)
	}
	t.Setenv("JM_OBJ_BUCKET_URL", "http://s3.example/bucket")
	if got := objectStoreBucketURL(); got != "http://s3.example/bucket" {
		t.Fatalf("override = %q", got)
	}
}
