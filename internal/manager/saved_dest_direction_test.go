package manager

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestSavedDestinationRejectsUnknownDirection checks that a saved-destination
// reference goes through the same direction + source checks as a path
// destination: an unknown direction must be refused and must not queue a job
// for a source outside the permitted roots.
func TestSavedDestinationRejectsUnknownDirection(t *testing.T) {
	root := t.TempDir()
	srcDir := filepath.Join(root, "src")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mgr := Register(mux, "", Config{
		JuiceFSBin:  "/dev/null",
		FUSEMount:   "/mnt/juicefs",
		SourceRoots: []string{root},
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
	body := func(source, dir string) string {
		b, _ := json.Marshal(map[string]string{"source": source, "destination": "file://dst", "direction": dir})
		return string(b)
	}

	// File destinations must sit under a source root.
	cfg, _ := json.Marshal(map[string]any{"name": "dst", "kind": "file", "config": map[string]string{"path": filepath.Join(root, "dst")}})
	if w := do("/api/destinations", string(cfg)); w.Code != http.StatusCreated {
		t.Fatalf("create destination: got %d body=%s", w.Code, w.Body.String())
	}

	for _, ep := range []string{"/api/migrate", "/api/resolve-destination"} {
		for _, dir := range []string{"x", "IN", "sideways"} {
			if w := do(ep, body("/etc", dir)); w.Code != http.StatusBadRequest {
				t.Errorf("%s direction=%q source=/etc: got %d, want 400 — body=%s", ep, dir, w.Code, w.Body.String())
			}
		}
		// A known direction still gates the source.
		if w := do(ep, body("/etc", "in")); w.Code != http.StatusForbidden {
			t.Errorf("%s direction=in source=/etc: got %d, want 403 — body=%s", ep, w.Code, w.Body.String())
		}
	}
	if n := len(mgr.List()); n != 0 {
		t.Fatalf("unexpected jobs queued: %d", n)
	}

	// Positive control: a permitted source with a known direction is
	// accepted, so the refusals above come from the direction and source
	// checks, not from the destination setup. (Preview only: a submit
	// would start a job runner this test does not need.)
	for _, dir := range []string{"", "in"} {
		if w := do("/api/resolve-destination", body(srcDir, dir)); w.Code != http.StatusOK {
			t.Errorf("resolve-destination direction=%q permitted source: got %d — body=%s", dir, w.Code, w.Body.String())
		}
	}
}

// TestCheckSyncSource pins the shared default-deny source gate used by
// /api/migrate, the saved-destination branches and the schedule gate.
func TestCheckSyncSource(t *testing.T) {
	root := t.TempDir()
	// The source must exist: on macOS t.TempDir sits under /var, a symlink
	// to /private/var, and pathAllowed resolves symlinks only on paths
	// that exist, so a missing path under the root would not match it.
	if err := os.MkdirAll(filepath.Join(root, "a"), 0o755); err != nil {
		t.Fatal(err)
	}
	a := &API{sourceRoots: []string{root}, destMount: "/jfs"}
	cases := []struct {
		path string
		dir  Direction
		want int
	}{
		{filepath.Join(root, "a"), "", 0},
		{filepath.Join(root, "a"), DirectionIn, 0},
		{"/etc", DirectionIn, http.StatusForbidden},
		{"/jfs/project", DirectionOut, 0},
		{filepath.Join(root, "a"), DirectionOut, http.StatusForbidden},
		// Between reads from the volume, like Out; it used to fall into
		// the schedule gate's In branch.
		{"/jfs/project", DirectionBetween, 0},
		{filepath.Join(root, "a"), DirectionBetween, http.StatusForbidden},
		{"/jfs/project", "IN", http.StatusBadRequest},
		{filepath.Join(root, "a"), "sideways", http.StatusBadRequest},
	}
	for _, c := range cases {
		code, err := a.checkSyncSource(c.path, c.dir)
		if code != c.want || (err == nil) != (c.want == 0) {
			t.Errorf("checkSyncSource(%q, %q) = %d, %v; want %d", c.path, c.dir, code, err, c.want)
		}
	}
}
