package enrollclient

// H-8 per-Mac object keys, Mac side (design §5 "Mac side", §7 M3, test C1):
// the client adopts this Mac's own key mac-<12 hex>-<gen> (MinIO-generated
// secrets carry '+' and '/'), refuses another Mac's, keeps the shared
// jm-client path unchanged, waits fast while the Manager is still creating
// the key, and reports the access-key ID in use without breaking a Manager
// that does not know that field. The fingerprint / manager_id gates apply to
// these answers exactly as to the shared key.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/credstore"
)

const (
	perMacAK1 = "mac-4f1c2a9e7b3d-1"
	perMacAK2 = "mac-4f1c2a9e7b3d-2"
	// 40 characters, MinIO-generated style: base64 alphabet with '+' and '/'.
	perMacSK = "fixture+not/a+key/fixture+not/a+key/fixture"
)

// perMacManager answers like a per-Mac Manager for an approved, bound Mac:
// the bound Manager ID, the Mac's real fingerprint, and whatever key fields
// the test sets. It records every request body.
type perMacManager struct {
	mu       sync.Mutex
	id, fp   string
	status   string
	ak, sk   string
	pending  bool
	extra    map[string]any // unknown fields a newer Manager may add
	requests []map[string]any
}

func (m *perMacManager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	m.mu.Lock()
	defer m.mu.Unlock()
	var req map[string]any
	_ = json.Unmarshal(b, &req)
	m.requests = append(m.requests, req)
	w.Header().Set(ManagerIDHeader, m.id)
	body := map[string]any{
		"manager_id": m.id, "status": m.status, "principal": principalA, "fingerprint": m.fp,
		"object_access_key": m.ak, "object_secret_key": m.sk,
	}
	if m.pending {
		body["object_key_pending"] = true
	}
	for k, v := range m.extra {
		body[k] = v
	}
	_ = json.NewEncoder(w).Encode(body)
}

func (m *perMacManager) last() map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.requests) == 0 {
		return nil
	}
	return m.requests[len(m.requests)-1]
}

// perMacMac is an approved, bound Mac (real Manager) now answered by a
// scripted per-Mac Manager with the same ID.
func perMacMac(t *testing.T) (*perMacManager, string, Identity, State) {
	t.Helper()
	rec, srv, id, st := boundApprovedMac(t)
	m := &perMacManager{id: st.ManagerID, fp: st.Fingerprint, status: "approved"}
	rec.set(m)
	return m, srv.URL, id, st
}

// C1: approved but the key is not created yet. Poll again after
// PendingPoll (15 s), not ApprovedPoll (30 min), and adopt no key, not even
// one sent alongside the flag.
func TestPerMacKeyPendingPollsFastAndAdoptsNothing(t *testing.T) {
	m, url, id, st := perMacMac(t)
	m.pending, m.ak, m.sk = true, perMacAK1, perMacSK
	r := Step(context.Background(), http.DefaultClient, url, id, st)
	if r.Phase != Approved || !r.ObjectKeyPending {
		t.Fatalf("pending key: phase=%s pending=%v %q", r.Phase, r.ObjectKeyPending, r.Message)
	}
	if r.RetryAfterMS != PendingPoll.Milliseconds() {
		t.Fatalf("pending key: next poll in %d ms, want PendingPoll %d ms", r.RetryAfterMS, PendingPoll.Milliseconds())
	}
	if r.ObjectAccessKey != "" || r.ObjectSecretKey != "" {
		t.Fatalf("pending key: adopted %q", r.ObjectAccessKey)
	}
	// Once the key exists the Mac adopts it and returns to the slow poll.
	m.pending = false
	r = Step(context.Background(), http.DefaultClient, url, id, r.State)
	if r.Phase != Approved || r.ObjectKeyPending || r.ObjectAccessKey != perMacAK1 || r.RetryAfterMS != ApprovedPoll.Milliseconds() {
		t.Fatalf("key ready: phase=%s pending=%v ak=%q retry=%d", r.Phase, r.ObjectKeyPending, r.ObjectAccessKey, r.RetryAfterMS)
	}
}

// This Mac's per-Mac key is adopted with its '+' and '/' secret intact, a
// newer generation replaces it, and the stored form round-trips through the
// service's credstore decoding (what the service hands juicefs).
func TestPerMacKeyAdoptedAndReplaced(t *testing.T) {
	m, url, id, st := perMacMac(t)
	m.ak, m.sk = perMacAK1, perMacSK
	r := Step(context.Background(), http.DefaultClient, url, id, st)
	if r.Phase != Approved || r.ObjectAccessKey != perMacAK1 || r.ObjectSecretKey != perMacSK {
		t.Fatalf("gen 1: phase=%s ak=%q sk-ok=%v %q", r.Phase, r.ObjectAccessKey, r.ObjectSecretKey == perMacSK, r.Message)
	}
	stored, ok := credstore.EncodeObjectKey(r.ObjectAccessKey, r.ObjectSecretKey)
	ak, sk, ok2 := credstore.DecodeObjectKey(stored)
	if !ok || !ok2 || ak != perMacAK1 || sk != perMacSK {
		t.Fatal("the '+' secret does not round-trip through the stored form")
	}
	m.ak = perMacAK2
	r = Step(context.Background(), http.DefaultClient, url, id, r.State)
	if r.Phase != Approved || r.ObjectAccessKey != perMacAK2 {
		t.Fatalf("gen 2: phase=%s ak=%q", r.Phase, r.ObjectAccessKey)
	}
	// A newer Manager may add fields this client does not know.
	m.extra = map[string]any{"object_key_state": "active", "object_key_gen": 2}
	if r = Step(context.Background(), http.DefaultClient, url, id, r.State); r.Phase != Approved || r.ObjectAccessKey != perMacAK2 {
		t.Fatalf("unknown fields: phase=%s %q", r.Phase, r.Message)
	}
}

// Another Mac's per-Mac key, or a malformed per-Mac name, is never adopted:
// a Mac writing with another Mac's key would be cut off by that Mac's revoke
// and named wrongly in the Manager's records.
func TestPerMacKeyOfAnotherMacRefused(t *testing.T) {
	m, url, id, st := perMacMac(t)
	for _, ak := range []string{
		"mac-000000000000-1",    // another Mac
		"mac-4f1c2a9e7b3d",      // no generation
		"mac-4f1c2a9e7b3d-0",    // generation 0
		"mac-4f1c2a9e7b3d-1000", // generation > 999
		"mac-4F1C2A9E7B3D-1",    // upper case
		"mac-4f1c2a9e7b3d-1-x",
	} {
		m.ak, m.sk = ak, perMacSK
		r := Step(context.Background(), http.DefaultClient, url, id, st)
		if r.Phase != Failed || r.ObjectAccessKey != "" || r.Persist || r.State != st.withFailures(1) {
			t.Fatalf("%s: phase=%s ak=%q persist=%v", ak, r.Phase, r.ObjectAccessKey, r.Persist)
		}
	}
}

// The shared-key path is unchanged: the real Manager delivers jm-client, and
// a Mac running jm-client sends no object_key_in_use (that Manager decodes
// strictly and would answer 400).
func TestSharedKeyPathUnchanged(t *testing.T) {
	t.Cleanup(inUseRefused.Clear)
	rec, srv, id, st := boundApprovedMac(t)
	_ = rec
	id.ObjectKeyInUse = clientAK
	r := Step(context.Background(), http.DefaultClient, srv.URL, id, st)
	if r.Phase != Approved || r.ObjectAccessKey != clientAK || r.ObjectSecretKey != clientSK {
		t.Fatalf("shared key: phase=%s ak=%q %q", r.Phase, r.ObjectAccessKey, r.Message)
	}
}

// object_key_in_use carries only this Mac's own per-Mac access-key ID:
// never a secret, never the shared key, never another Mac's name.
func TestInUseReportedOnlyForOwnPerMacKey(t *testing.T) {
	t.Cleanup(inUseRefused.Clear)
	m, url, id, st := perMacMac(t)
	m.ak, m.sk = perMacAK2, perMacSK
	cases := map[string]string{
		perMacAK1:            perMacAK1,
		clientAK:             "",
		"mac-000000000000-1": "",
		"":                   "",
	}
	for inUse, want := range cases {
		id.ObjectKeyInUse = inUse
		if r := Step(context.Background(), http.DefaultClient, url, id, st); r.Phase != Approved {
			t.Fatalf("in use %q: phase=%s %q", inUse, r.Phase, r.Message)
		}
		req := m.last()
		got, present := req["object_key_in_use"]
		switch {
		case want == "" && present:
			t.Fatalf("in use %q: sent object_key_in_use=%v", inUse, got)
		case want != "" && got != want:
			t.Fatalf("in use %q: sent object_key_in_use=%v, want %q", inUse, got, want)
		}
		raw, _ := json.Marshal(req)
		if strings.Contains(string(raw), perMacSK) {
			t.Fatal("the request carries the object secret")
		}
	}
}

// A Manager from before the field (the real one in this tree decodes
// requests with DisallowUnknownFields) answers 400 to object_key_in_use.
// The Mac must keep polling: the next step omits the field and succeeds.
func TestManagerWithoutInUseFieldKeepsPolling(t *testing.T) {
	t.Cleanup(inUseRefused.Clear)
	_, srv, id, st := boundApprovedMac(t)
	id.ObjectKeyInUse = perMacAK1 // e.g. a Manager image rolled back after M3
	first := Step(context.Background(), http.DefaultClient, srv.URL, id, st)
	if first.Phase != Failed || first.Persist || first.State.Secret != st.Secret || first.State.ManagerID != st.ManagerID {
		t.Fatalf("first step: phase=%s persist=%v %q", first.Phase, first.Persist, first.Message)
	}
	second := Step(context.Background(), http.DefaultClient, srv.URL, id, first.State)
	if second.Phase != Approved || second.ObjectAccessKey != clientAK {
		t.Fatalf("second step still refused: phase=%s %q", second.Phase, second.Message)
	}
}

// The fingerprint / manager_id gate applies to per-Mac answers: a server
// with another ID that does not know this Mac's secret cannot hand it a
// per-Mac key (even one with this Mac's name), and cannot mark it pending.
func TestPerMacKeyFromImpostorNotAdopted(t *testing.T) {
	rec, srv, id, st := boundApprovedMac(t)
	imp := &perMacManager{id: strings.Repeat("e", 32), fp: "AAAA-BBBB", status: "approved", ak: perMacAK1, sk: perMacSK}
	rec.set(imp)
	r := Step(context.Background(), http.DefaultClient, srv.URL, id, st)
	if r.Phase != Failed || r.ObjectAccessKey != "" || r.State != st.withFailures(1) {
		t.Fatalf("impostor's per-Mac key adopted: phase=%s ak=%q", r.Phase, r.ObjectAccessKey)
	}
	imp.pending, imp.ak, imp.sk = true, "", ""
	r = Step(context.Background(), http.DefaultClient, srv.URL, id, st)
	if r.Phase != Failed || r.ObjectKeyPending {
		t.Fatalf("impostor's pending answer adopted: phase=%s pending=%v", r.Phase, r.ObjectKeyPending)
	}
	// The same answer from the bound Manager with the real fingerprint is
	// adopted (the gate is the reason for the refusal above).
	rec.set(&perMacManager{id: st.ManagerID, fp: st.Fingerprint, status: "approved", ak: perMacAK1, sk: perMacSK})
	if r = Step(context.Background(), http.DefaultClient, srv.URL, id, st); r.Phase != Approved || r.ObjectAccessKey != perMacAK1 {
		t.Fatalf("bound Manager's per-Mac key refused: phase=%s %q", r.Phase, r.Message)
	}
}

func TestPerMacKeyGen(t *testing.T) {
	if p, g, ok := PerMacKeyGen("mac-4f1c2a9e7b3d-999"); !ok || p != principalA || g != 999 {
		t.Fatalf("PerMacKeyGen = %q %d %v", p, g, ok)
	}
	for _, bad := range []string{"jm-client", "mac-4f1c2a9e7b3d", "mac-4f1c2a9e7b3d-01", "mac-4f1c2a9e7b3-1", " mac-4f1c2a9e7b3d-1"} {
		if _, _, ok := PerMacKeyGen(bad); ok {
			t.Fatalf("PerMacKeyGen accepted %q", bad)
		}
	}
}
