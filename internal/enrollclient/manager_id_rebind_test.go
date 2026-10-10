package enrollclient

// Review 4 finding 1: an approved Mac must keep its enrollment when the
// Manager that holds it answers without its ID (an image from before IDs) or
// with a new one (its ID was lost but its enrollments were not).

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/manager"
)

// idlessManager wraps a real Manager and strips the Manager ID from every
// answer, as a Manager image from before 5fb0fab sends.
type idlessManager struct{ h http.Handler }

func (o idlessManager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rec := httptest.NewRecorder()
	o.h.ServeHTTP(rec, r)
	body := rec.Body.Bytes()
	var m map[string]any
	if json.Unmarshal(body, &m) == nil {
		delete(m, "manager_id")
		body, _ = json.Marshal(m)
	}
	for k, v := range rec.Header() {
		if k == http.CanonicalHeaderKey(ManagerIDHeader) || k == "Content-Length" {
			continue
		}
		w.Header()[k] = v
	}
	w.WriteHeader(rec.Code)
	_, _ = w.Write(body)
}

// restartableManager runs a real Manager over one state directory and can
// be restarted on it (a new process over the same files).
type restartableManager struct {
	t   *testing.T
	dir string
}

func newRestartableManager(t *testing.T) *restartableManager {
	t.Helper()
	dir := t.TempDir()
	proxyDir := filepath.Join(dir, "respproxy")
	if err := os.MkdirAll(proxyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("JM_RESPPROXY_DIR", proxyDir)
	t.Setenv("JM_META_ADMIN_PASSWORD", "")
	t.Setenv("JM_OBJ_CLIENT_ACCESS_KEY", clientAK)
	t.Setenv("JM_OBJ_CLIENT_SECRET_KEY", clientSK)
	startFakeProxyAck(t, proxyDir)
	return &restartableManager{t: t, dir: dir}
}

func (m *restartableManager) start() *http.ServeMux {
	mux := http.NewServeMux()
	mgr := manager.Register(mux, "", manager.Config{
		JuiceFSBin: "/dev/null", FUSEMount: filepath.Join(m.dir, "jfs"), SourceRoots: []string{"/sources"},
		DestMount: "/jfs", AdminKey: adminKey, StateFile: filepath.Join(m.dir, "state.json"),
	})
	m.t.Cleanup(mgr.StopAll)
	return mux
}

// stripStoreManagerID rewrites the principal store without its manager_id,
// as a backup taken before the upgrade has it.
func (m *restartableManager) stripStoreManagerID() {
	m.t.Helper()
	path := filepath.Join(m.dir, "principals-store.json")
	b, err := os.ReadFile(path)
	if err != nil {
		m.t.Fatal(err)
	}
	var f map[string]any
	if err := json.Unmarshal(b, &f); err != nil {
		m.t.Fatal(err)
	}
	if _, ok := f["manager_id"]; !ok {
		m.t.Fatal("the store holds no manager_id to strip")
	}
	delete(f, "manager_id")
	b, _ = json.Marshal(f)
	if err := os.WriteFile(path, b, 0o600); err != nil {
		m.t.Fatal(err)
	}
}

// approvedMac enrolls instanceA at url, approves it, and polls once.
func approvedMac(t *testing.T, srv *httptest.Server) (Identity, Result) {
	t.Helper()
	id := Identity{InstanceID: instanceA, Host: "mac", DeviceDigest: digestMac}
	r := enrollApproved(t, srv.URL, id)
	admin(t, srv, "approve", principalA)
	r = Step(context.Background(), http.DefaultClient, srv.URL, id, r.State)
	if r.Phase != Approved || !managerIDRe.MatchString(r.State.ManagerID) {
		t.Fatalf("setup: %s %+v", r.Phase, r.State)
	}
	return id, r
}

// pollApproved polls n times and requires every answer to be Approved with
// the secret kept.
func pollApproved(t *testing.T, what string, srv *httptest.Server, id Identity, st State, n int) State {
	t.Helper()
	orig := st.Secret
	var phases []string
	for i := 0; i < n; i++ {
		r := Step(context.Background(), http.DefaultClient, srv.URL, id, st)
		phases = append(phases, string(r.Phase)+":"+r.Message)
		st = r.State
		if r.Phase != Approved || st.Secret != orig {
			t.Fatalf("%s: the approved Mac lost its enrollment (secret kept=%v); phases %q", what, st.Secret == orig, phases)
		}
	}
	return st
}

// E1: the Manager holding this Mac's approval is rolled back to an image
// that sends no ID. Its 200 to the poll token proves it still holds the
// enrollment: the Mac stays approved, keeps its secret and its binding.
func TestEnrollManagerWithoutIDKeepsApprovedMac(t *testing.T) {
	mux, _ := realManagerMux(t)
	rec := &recordingHandler{}
	rec.set(mux)
	srv := httptest.NewServer(rec)
	defer srv.Close()
	id, r := approvedMac(t, srv)
	rec.set(idlessManager{mux})
	st := pollApproved(t, "E1 (Manager without ID)", srv, id, r.State, 3)
	if st.ManagerID != r.State.ManagerID {
		t.Fatalf("an answer without an ID changed the binding: %q -> %q", r.State.ManagerID, st.ManagerID)
	}
}

// E2: the principal store is restored from a backup taken before the
// upgrade (no manager_id in it). The Manager keeps its ID (it is kept apart
// from the store too), so the Mac stays approved and bound to the same ID.
func TestEnrollPreIDStoreRestoreKeepsApprovedMac(t *testing.T) {
	m := newRestartableManager(t)
	rec := &recordingHandler{}
	rec.set(m.start())
	srv := httptest.NewServer(rec)
	defer srv.Close()
	id, r := approvedMac(t, srv)
	m.stripStoreManagerID()
	rec.set(m.start())
	st := pollApproved(t, "E2 (pre-ID store restored)", srv, id, r.State, 3)
	if st.ManagerID != r.State.ManagerID {
		t.Fatalf("restoring a pre-ID store changed the Manager ID: %q -> %q", r.State.ManagerID, st.ManagerID)
	}
}

// E2, worst case: the ID is lost everywhere (store and ID file restored from
// before IDs), so the Manager mints a new one, but it still holds this Mac's
// approval and poll token. Its 200 rebinds the Mac to the new ID; no
// Conflict, no new secret.
func TestEnrollManagerIDLostEverywhereRebindsOnApproval(t *testing.T) {
	m := newRestartableManager(t)
	rec := &recordingHandler{}
	rec.set(m.start())
	srv := httptest.NewServer(rec)
	defer srv.Close()
	id, r := approvedMac(t, srv)
	m.stripStoreManagerID()
	if err := os.Remove(filepath.Join(m.dir, "principals-store.manager-id")); err != nil {
		t.Fatal(err)
	}
	rec.set(m.start())
	st := pollApproved(t, "Manager ID lost everywhere", srv, id, r.State, 3)
	if st.ManagerID == r.State.ManagerID || !managerIDRe.MatchString(st.ManagerID) {
		t.Fatalf("not rebound to the Manager's new ID: %q (was %q)", st.ManagerID, r.State.ManagerID)
	}
}

// A 403 or 409 without an ID (an old Manager image, a gateway in front of
// the Manager) says nothing about which Manager answered: Failed, nothing
// persisted, the secret, token and binding kept. A 409 with a different
// well-formed ID is another Manager: start afresh.
func TestEnrollRefusalWithoutIDIsUndecided(t *testing.T) {
	bound := strings.Repeat("a", 32)
	base := State{ManagerID: bound, InstanceID: instanceA, Secret: strings.Repeat("s", 40), PollToken: strings.Repeat("b", 64), Status: "approved", Principal: principalA}
	for _, tc := range []struct {
		code      int
		answerID  string
		wantFresh bool
	}{
		{http.StatusForbidden, "", false},
		{http.StatusConflict, "", false},
		{http.StatusConflict, strings.Repeat("c", 32), true},
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if tc.answerID != "" {
				w.Header().Set(ManagerIDHeader, tc.answerID)
			}
			w.WriteHeader(tc.code)
		}))
		st := base
		st.ManagerURL = srv.URL
		r := Step(context.Background(), srv.Client(), srv.URL, Identity{InstanceID: instanceA, Host: "h"}, st)
		srv.Close()
		if tc.wantFresh {
			if r.Phase != NotEnrolled || !r.Persist || r.State.Secret == st.Secret || r.State.ManagerID != "" {
				t.Fatalf("HTTP %d with a different ID: %+v", tc.code, r)
			}
			continue
		}
		if r.Persist || r.Phase != Failed || r.State.Secret != st.Secret || r.State.PollToken != st.PollToken ||
			r.State.ManagerID != bound || r.State.Status != "approved" {
			t.Fatalf("HTTP %d without an ID: %+v", tc.code, r)
		}
	}
}
