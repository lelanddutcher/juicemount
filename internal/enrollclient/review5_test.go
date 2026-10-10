package enrollclient

// Review 5: a server at the Manager's address that does not hold this Mac's
// request must not take over an approved, bound Mac (X1 secret theft, X2
// object-key swap, X4 revoke).

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// impostor is a different server at the Manager's address. It has no record
// of this Mac, but it can echo the principal, which is a function of the
// instance_id every poll carries. Mode "200" answers approved with its own
// ID and object key; mode "403" refuses with its own ID (asking for the
// secret).
type impostor struct {
	mu      sync.Mutex
	mode    string
	id      string
	secrets []string
	ak, sk  string
}

func (i *impostor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	var req enrollRequest
	_ = json.Unmarshal(b, &req)
	i.mu.Lock()
	defer i.mu.Unlock()
	if req.Secret != "" {
		i.secrets = append(i.secrets, req.Secret)
	}
	if i.id != "" {
		w.Header().Set(ManagerIDHeader, i.id)
	}
	if r.Method == http.MethodGet {
		_ = json.NewEncoder(w).Encode(map[string]string{"manager_id": i.id})
		return
	}
	switch i.mode {
	case "403":
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":"unknown"}`))
	default:
		_ = json.NewEncoder(w).Encode(map[string]string{
			"manager_id": i.id, "status": "approved", "principal": PrincipalFor(req.InstanceID),
			"fingerprint": "AAAA-BBBB", "object_access_key": i.ak, "object_secret_key": i.sk,
		})
	}
}

func (i *impostor) saw(s string) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	for _, x := range i.secrets {
		if x == s {
			return true
		}
	}
	return false
}

// boundApprovedMac enrolls a Mac with the real Manager behind rec and
// returns its approved, bound state.
func boundApprovedMac(t *testing.T) (*recordingHandler, *httptest.Server, Identity, State) {
	t.Helper()
	mux, _ := realManagerMux(t)
	rec := &recordingHandler{}
	rec.set(mux)
	srv := httptest.NewServer(rec)
	t.Cleanup(srv.Close)
	id := Identity{InstanceID: instanceA, Host: "mac", DeviceDigest: digestMac}
	r := enrollApproved(t, srv.URL, id)
	admin(t, srv, "approve", principalA)
	r = Step(context.Background(), http.DefaultClient, srv.URL, id, r.State)
	if r.Phase != Approved || r.State.ManagerID == "" {
		t.Fatalf("setup: %s %+v", r.Phase, r.State)
	}
	return rec, srv, id, r.State
}

func short(s string) string {
	if len(s) < 4 {
		return "<" + s + ">"
	}
	return s[:4]
}

// The Mac's fingerprint is the one the real Manager records and returns.
func TestEnrollFingerprintMatchesTheManagers(t *testing.T) {
	_, _, _, st := boundApprovedMac(t)
	if st.Fingerprint == "" || Fingerprint(st.Secret) != st.Fingerprint {
		t.Fatalf("Fingerprint(secret) = %q, the Manager answered %q", Fingerprint(st.Secret), st.Fingerprint)
	}
}

// X1: an impostor that never knew the Manager ID must not harvest the
// approved Mac's secret. Before the fix: 200 with its ID (rebind), 403 with
// its ID (drop the token), then the Mac re-sent its secret to it.
func TestReview5ImpostorCannotHarvestTheSecret(t *testing.T) {
	rec, srv, id, st := boundApprovedMac(t)
	orig, bound := st.Secret, st.ManagerID
	imp := &impostor{mode: "200", id: strings.Repeat("e", 32), ak: "AKIMPOSTOR", sk: "impostor-object-secret"}
	rec.set(imp)
	first := Step(context.Background(), http.DefaultClient, srv.URL, id, st)
	if first.Phase != Failed || first.State.ManagerID != bound || first.Persist {
		t.Fatalf("X1: the impostor's 200 was adopted: phase=%s id=%s persist=%v (bound %s)", first.Phase, short(first.State.ManagerID), first.Persist, short(bound))
	}
	imp.mode = "403"
	var steps []string
	cur := first.State
	for i := 0; i < 3; i++ {
		n := Step(context.Background(), http.DefaultClient, srv.URL, id, cur)
		steps = append(steps, string(n.Phase)+":"+n.Message)
		cur = n.State
	}
	if imp.saw(orig) {
		t.Fatalf("X1: an impostor that never knew the Manager ID received the approved Mac's secret; steps %q", steps)
	}
}

// X2: the impostor's 200 must not hand an approved Mac its object-store key.
func TestReview5ImpostorObjectKeyNotAdopted(t *testing.T) {
	rec, srv, id, st := boundApprovedMac(t)
	rec.set(&impostor{mode: "200", id: strings.Repeat("e", 32), ak: "AKIMPOSTOR", sk: "impostor-object-secret"})
	n := Step(context.Background(), http.DefaultClient, srv.URL, id, st)
	if n.ObjectAccessKey != "" || n.Phase != Failed || n.State != st.withFailures(1) {
		t.Fatalf("X2: approved Mac adopted the impostor's answer: phase=%s ak=%q id=%s", n.Phase, n.ObjectAccessKey, short(n.State.ManagerID))
	}
}

// X4: a 200 "revoked" with no ID and no matching fingerprint must not
// revoke an approved, bound Mac (the app would delete its object key and its
// saved previous login).
func TestReview5ImpostorCannotRevoke(t *testing.T) {
	rec, srv, id, st := boundApprovedMac(t)
	rec.set(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "revoked", "principal": principalA, "fingerprint": "AAAA-BBBB"})
	}))
	n := Step(context.Background(), http.DefaultClient, srv.URL, id, st)
	if n.Phase != Failed || n.State.Status != "approved" || n.State.ManagerID != st.ManagerID {
		t.Fatalf("X4: an ID-less impostor's 200 'revoked' was adopted: phase=%s status=%s", n.Phase, n.State.Status)
	}
}

func (s State) withFailures(n int) State { s.Failures = n; return s }
