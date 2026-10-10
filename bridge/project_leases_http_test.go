//go:build !serviceclient

package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/metadata"
	jmnfs "github.com/lelanddutcher/juicemount/nfs"
)

func TestProjectLeaseRoutesRegisteredAndGuarded(t *testing.T) {
	routes := controlPlaneRoutes()
	for _, p := range []string{"/project-leases", "/project-leases/release"} {
		if routes[p] == nil {
			t.Fatalf("route %s not registered", p)
		}
	}
	globalProjectLeases.Store(nil)
	rec := httptest.NewRecorder()
	handleProjectLeasesHTTP(rec, httptest.NewRequest(http.MethodGet, "/project-leases", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET without a client: %d", rec.Code)
	}
	// The release route's OUTER guard runs before the client is consulted:
	// even with no client, a GET is 405 and a headerless POST is 403 with the
	// control-plane message (not 503, not the client's own refusal).
	rec = httptest.NewRecorder()
	handleProjectLeasesHTTP(rec, httptest.NewRequest(http.MethodGet, "/project-leases/release?project=A/B", nil))
	if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != "POST" {
		t.Fatalf("GET release: %d allow=%q, want 405 from the outer guard", rec.Code, rec.Header().Get("Allow"))
	}
	rec = httptest.NewRecorder()
	handleProjectLeasesHTTP(rec, httptest.NewRequest(http.MethodPost, "/project-leases/release?project=A/B", nil))
	if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), "missing local control header") {
		t.Fatalf("headerless POST release: %d %q, want the outer guard's 403", rec.Code, rec.Body.String())
	}

	c, err := jmnfs.NewProjectLeaseClient(jmnfs.ProjectLeaseConfig{RedisURL: "redis://127.0.0.1:1/0"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop()
	globalProjectLeases.Store(c)
	defer globalProjectLeases.Store(nil)
	rec = httptest.NewRecorder()
	handleProjectLeasesHTTP(rec, httptest.NewRequest(http.MethodGet, "/project-leases", nil))
	var st jmnfs.ProjectLeaseStatus
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &st) != nil || !st.Enabled {
		t.Fatalf("GET with a client: %d %s", rec.Code, rec.Body.String())
	}
}

type failingLeaseCreds struct{}

func (failingLeaseCreds) MetaPassword() (string, error) { return "", errors.New("keychain locked") }

func testSpoolStore(t *testing.T) *jmnfs.SpoolStore {
	t.Helper()
	store, err := metadata.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := metadata.InitSpoolSchema(store.DB()); err != nil {
		t.Fatal(err)
	}
	sp, err := jmnfs.NewSpoolStore(t.TempDir(), 0, metadata.NewSpoolStore(store.DB()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sp.Stop)
	return sp
}

// startBridgeProjectLeases builds a client only from a real URL, a spool and
// readable credentials. Every case uses a working spool, so a broken gate
// fails at the assertion rather than panicking in a started client.
func TestStartBridgeProjectLeasesGates(t *testing.T) {
	sp := testSpoolStore(t)
	if c := startBridgeProjectLeases("", sp); c != nil {
		c.Stop()
		t.Fatal("empty Redis URL started a client")
	}
	if c := startBridgeProjectLeases("redis://127.0.0.1:1/0", nil); c != nil {
		c.Stop()
		t.Fatal("nil spool started a client")
	}
	old := bridgeLeaseCredentials
	bridgeLeaseCredentials = failingLeaseCreds{}
	if c := startBridgeProjectLeases("redis://127.0.0.1:1/0", sp); c != nil {
		c.Stop()
		bridgeLeaseCredentials = old
		t.Fatal("credential failure still started a client")
	}
	bridgeLeaseCredentials = old
	c := startBridgeProjectLeases("redis://127.0.0.1:1/0", sp)
	if c == nil {
		t.Fatal("valid URL, spool and credentials built no client")
	}
	c.Stop()
}
