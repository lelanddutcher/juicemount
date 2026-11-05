package manager

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

// TestSavedDestinationRejectsUnknownDirection checks that a saved-destination
// reference goes through the same direction + source checks as a path
// destination: an unknown direction must be refused and must not queue a job
// for a source outside the permitted roots.
func TestSavedDestinationRejectsUnknownDirection(t *testing.T) {
	mux := http.NewServeMux()
	mgr := Register(mux, "", Config{
		JuiceFSBin:  "/dev/null",
		FUSEMount:   "/mnt/juicefs",
		SourceRoots: []string{"/sources"},
		DestMount:   "/jfs",
		AdminKey:    "the-admin-key-for-tests",
		StateFile:   filepath.Join(t.TempDir(), "state.json"),
	})
	defer mgr.StopAll()

	do := func(path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		req.Header.Set("X-JuiceMount-Admin-Key", "the-admin-key-for-tests")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		return w
	}

	w := do("/api/destinations", `{"name":"dst","kind":"file","config":{"path":"/tmp/jm-dst"}}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("create destination: got %d body=%s", w.Code, w.Body.String())
	}

	for _, ep := range []string{"/api/migrate", "/api/resolve-destination"} {
		for _, dir := range []string{"x", "IN", "sideways"} {
			body := `{"source":"/etc","destination":"file://dst","direction":"` + dir + `"}`
			w := do(ep, body)
			if w.Code != http.StatusBadRequest && w.Code != http.StatusForbidden {
				t.Errorf("%s direction=%q source=/etc: got %d, want 400/403 — body=%s", ep, dir, w.Code, w.Body.String())
			}
		}
	}
	if n := len(mgr.List()); n != 0 {
		t.Errorf("unexpected jobs queued: %d", n)
	}
}
