package enrollclient

// Review 3 finding 6: the enrollment is bound to the Manager's random ID,
// not to the spelling of its address.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// recordingHandler serves the current handler and records every secret
// and poll token it is sent, and every probe (GET).
type recordingHandler struct {
	cur     atomic.Pointer[http.Handler]
	mu      sync.Mutex
	secrets []string
	tokens  []string
	probes  int
}

func (h *recordingHandler) set(next http.Handler) { h.cur.Store(&next) }

func (h *recordingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	var body enrollRequest
	_ = json.Unmarshal(b, &body)
	h.mu.Lock()
	if r.Method == http.MethodGet {
		h.probes++
	}
	if body.Secret != "" {
		h.secrets = append(h.secrets, body.Secret)
	}
	if body.PollToken != "" {
		h.tokens = append(h.tokens, body.PollToken)
	}
	h.mu.Unlock()
	r.Body = io.NopCloser(strings.NewReader(string(b)))
	(*h.cur.Load()).ServeHTTP(w, r)
}

func (h *recordingHandler) sawSecret(s string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, x := range h.secrets {
		if x == s {
			return true
		}
	}
	return false
}

func enrollApproved(t *testing.T, url string, id Identity) Result {
	t.Helper()
	ctx := context.Background()
	r := Step(ctx, http.DefaultClient, url, id, State{})
	r = Step(ctx, http.DefaultClient, url, id, r.State)
	if r.Phase != Waiting {
		t.Fatalf("enroll: phase %s %q", r.Phase, r.Message)
	}
	return r
}

// One Manager reached under a second address (another port, another name):
// the same ID keeps the secret and the poll token, and nothing secret is
// sent to ask.
func TestEnrollSameManagerIDUnderNewAddressKeepsTheSecret(t *testing.T) {
	mux, _ := realManagerMux(t)
	first := httptest.NewServer(mux)
	defer first.Close()
	rec := &recordingHandler{}
	rec.set(mux)
	second := httptest.NewServer(rec)
	defer second.Close()
	id := Identity{InstanceID: instanceA, Host: "mac", DeviceDigest: digestMac}
	r := enrollApproved(t, first.URL, id)
	if !managerIDRe.MatchString(r.State.ManagerID) {
		t.Fatalf("not bound to a Manager ID: %+v", r.State)
	}
	admin(t, first, "approve", principalA)

	moved := Step(context.Background(), http.DefaultClient, second.URL, id, r.State)
	if !moved.Persist || moved.State.Secret != r.State.Secret || moved.State.PollToken != r.State.PollToken ||
		moved.State.ManagerID != r.State.ManagerID || moved.State.ManagerURL != second.URL {
		t.Fatalf("same Manager under a new address: %+v", moved)
	}
	if len(rec.secrets) != 0 || len(rec.tokens) != 0 || rec.probes != 1 {
		t.Fatalf("asking for the ID sent secrets=%d tokens=%d probes=%d", len(rec.secrets), len(rec.tokens), rec.probes)
	}
	polled := Step(context.Background(), http.DefaultClient, second.URL, id, moved.State)
	if polled.Phase != Approved || polled.State.Secret != r.State.Secret {
		t.Fatalf("poll under the new address: %s %q", polled.Phase, polled.Message)
	}
}

// A different Manager now answers at the same address (the NAS was
// replaced, or a tunnel now leads elsewhere). Its 403 to the poll token does
// NOT make the Mac re-send its secret: a fresh enrollment starts, and the new
// Manager never sees the old secret.
func TestEnrollDifferentManagerIDAtSameAddressStartsFresh(t *testing.T) {
	m1, _ := realManagerMux(t)
	rec := &recordingHandler{}
	rec.set(m1)
	srv := httptest.NewServer(rec)
	defer srv.Close()
	id := Identity{InstanceID: instanceA, Host: "mac", DeviceDigest: digestMac}
	r := enrollApproved(t, srv.URL, id)
	orig, bound := r.State.Secret, r.State.ManagerID

	m2, _ := realManagerMux(t)
	rec.set(m2)
	rec.mu.Lock()
	rec.secrets = nil
	rec.mu.Unlock()
	next := Step(context.Background(), http.DefaultClient, srv.URL, id, r.State)
	if next.State.Secret == orig || !next.Persist || next.Phase != NotEnrolled || next.State.ManagerID != "" {
		t.Fatalf("a different Manager's 403: %+v", next)
	}
	for i := 0; i < 3; i++ {
		next = Step(context.Background(), http.DefaultClient, srv.URL, id, next.State)
	}
	if rec.sawSecret(orig) {
		t.Fatal("the old secret was sent to a different Manager")
	}
	if next.Phase != Waiting || next.State.ManagerID == "" || next.State.ManagerID == bound {
		t.Fatalf("fresh enrollment with the new Manager: %s %+v", next.Phase, next.State)
	}
}

// A 403 from the bound Manager (it lost the poll token: store restored, or
// the principal forgotten) re-sends the secret to it, as before.
func TestEnroll403FromBoundManagerResendsTheSecret(t *testing.T) {
	mux, _ := realManagerMux(t)
	rec := &recordingHandler{}
	rec.set(mux)
	srv := httptest.NewServer(rec)
	defer srv.Close()
	id := Identity{InstanceID: instanceA, Host: "mac", DeviceDigest: digestMac}
	r := enrollApproved(t, srv.URL, id)
	st := r.State
	st.PollToken = strings.Repeat("0", 64) // a token the Manager does not know
	next := Step(context.Background(), http.DefaultClient, srv.URL, id, st)
	if next.State.PollToken != "" || next.State.Secret != st.Secret || next.State.ManagerID != st.ManagerID {
		t.Fatalf("403 from the bound Manager: %+v", next)
	}
	next = Step(context.Background(), http.DefaultClient, srv.URL, id, next.State)
	if next.Phase != Waiting || next.State.PollToken == "" || !rec.sawSecret(st.Secret) {
		t.Fatalf("re-sent request: %s %+v", next.Phase, next.State)
	}
}

// An answer that cannot be trusted either way (429 or 503 without an ID,
// from the Manager or a gateway in front of it) changes nothing.
func TestEnrollUndecidedAnswerKeepsTheBinding(t *testing.T) {
	for _, code := range []int{http.StatusTooManyRequests, http.StatusServiceUnavailable, http.StatusBadGateway} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(code) }))
		st := State{ManagerURL: srv.URL, ManagerID: strings.Repeat("a", 32), InstanceID: instanceA, Secret: strings.Repeat("s", 40), PollToken: strings.Repeat("b", 64)}
		r := Step(context.Background(), srv.Client(), srv.URL, Identity{InstanceID: instanceA, Host: "h"}, st)
		srv.Close()
		if r.Persist || r.Phase != Failed || r.State.Secret != st.Secret || r.State.ManagerID != st.ManagerID {
			t.Fatalf("HTTP %d without an ID: %+v", code, r)
		}
	}
}
