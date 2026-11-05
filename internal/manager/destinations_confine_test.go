package manager

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const destTestAdminKey = "dest-test-admin-key-not-for-production"

// registerDestTest wires a full API with one source root and returns the
// mux plus the destinations store behind it.
func registerDestTest(t *testing.T, root, statePath string) (*http.ServeMux, *destinationStoreImpl) {
	t.Helper()
	mux := http.NewServeMux()
	mgr := Register(mux, "", Config{
		JuiceFSBin:  "/dev/null",
		FUSEMount:   "/mnt/juicefs",
		SourceRoots: []string{root},
		DestMount:   "/jfs",
		AdminKey:    destTestAdminKey,
		StateFile:   statePath,
	})
	t.Cleanup(mgr.StopAll)
	store, ok := mgr.destinations.(*destinationStoreImpl)
	if !ok || store == nil {
		t.Fatalf("destinations store not wired")
	}
	return mux, store
}

func destRequest(t *testing.T, mux *http.ServeMux, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("X-JuiceMount-Admin-Key", destTestAdminKey)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

func fileDestBody(name, path string) string {
	b, _ := json.Marshal(map[string]any{"name": name, "kind": "file", "config": map[string]string{"path": path}})
	return string(b)
}

func TestFileDestinationSaveRefusesPathOutsideRoots(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "escape")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	if err := os.Symlink(filepath.Join(outside, "missing"), filepath.Join(root, "dangling")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	mux, _ := registerDestTest(t, root, "")

	for i, tc := range []struct {
		path string
		want int
	}{
		{"/etc", http.StatusForbidden},
		{root + "/../" + filepath.Base(outside), http.StatusForbidden},
		{filepath.Join(root, "escape"), http.StatusForbidden},
		{filepath.Join(root, "escape", "not-yet-created"), http.StatusForbidden},
		{filepath.Join(root, "dangling", "sub"), http.StatusForbidden},
		{"relative/path", http.StatusBadRequest},
	} {
		w := destRequest(t, mux, http.MethodPost, "/api/destinations", fileDestBody(fmt.Sprintf("bad%d", i), tc.path))
		if w.Code != tc.want {
			t.Errorf("create with path %q: got %d, want %d (%s)", tc.path, w.Code, tc.want, w.Body.String())
		}
	}

	inside := filepath.Join(root, "backups")
	if w := destRequest(t, mux, http.MethodPost, "/api/destinations", fileDestBody("good", inside)); w.Code != http.StatusCreated {
		t.Fatalf("create inside root: %d %s", w.Code, w.Body.String())
	}
	if w := destRequest(t, mux, http.MethodPut, "/api/destinations/good", fileDestBody("good", "/etc")); w.Code != http.StatusForbidden {
		t.Errorf("update to /etc: got %d, want 403 (%s)", w.Code, w.Body.String())
	}
}

// writeLegacyFileDestState writes a state file holding a file destination
// saved by an older build that did not confine the path.
func writeLegacyFileDestState(t *testing.T, statePath, name, path string) {
	t.Helper()
	key, err := deriveCredKey([]byte(destTestAdminKey))
	if err != nil {
		t.Fatalf("derive key: %v", err)
	}
	plain, _ := json.Marshal(map[string]string{"path": path})
	blob, err := encryptSecret(plain, key)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	st := persistedState{
		SchemaVersion: schemaVersion,
		Jobs:          map[string]*Job{},
		Destinations: []destinationsState{{
			Name: name, Kind: "file",
			EncryptedConfig: base64.StdEncoding.EncodeToString(blob),
			CreatedAt:       1, UpdatedAt: 1,
		}},
	}
	raw, _ := json.Marshal(st)
	if err := os.WriteFile(statePath, raw, 0o600); err != nil {
		t.Fatalf("write state: %v", err)
	}
}

func TestStoredUnconfinedFileDestinationRefusedAtUseAndTest(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	statePath := filepath.Join(t.TempDir(), "state.json")
	writeLegacyFileDestState(t, statePath, "legacy", outside)
	mux, store := registerDestTest(t, root, statePath)

	w := destRequest(t, mux, http.MethodPost, "/api/destinations/legacy/test", "")
	if w.Code != http.StatusForbidden {
		t.Errorf("test of unconfined destination: got %d, want 403 (%s)", w.Code, w.Body.String())
	}
	if ents, _ := os.ReadDir(outside); len(ents) != 0 {
		t.Errorf("test wrote into the unconfined directory: %v", ents)
	}

	d, err := store.getPlaintext("legacy")
	if err != nil {
		t.Fatalf("getPlaintext: %v", err)
	}
	if uri, _, err := d.ToSyncURI(false); err == nil {
		t.Errorf("ToSyncURI accepted unconfined path, uri %q", uri)
	}
	sched := scheduleState{Destination: DestinationRef{Name: "legacy"}}
	if _, uri, _, msg := resolveScheduleSpec(sched, store, ""); msg == "" {
		t.Errorf("schedule resolved unconfined destination to %q", uri)
	}
}

func TestLegacyConfinedFileDestinationUsableAfterTest(t *testing.T) {
	root := t.TempDir()
	inside := filepath.Join(root, "backups")
	if err := os.Mkdir(inside, 0o755); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(t.TempDir(), "state.json")
	writeLegacyFileDestState(t, statePath, "legacy", inside)
	mux, store := registerDestTest(t, root, statePath)

	d, err := store.getPlaintext("legacy")
	if err != nil {
		t.Fatalf("getPlaintext: %v", err)
	}
	if _, _, err := d.ToSyncURI(false); err == nil || !strings.Contains(err.Error(), "Test Connection") {
		t.Fatalf("unchecked legacy destination: err = %v, want a Test Connection instruction", err)
	}
	if w := destRequest(t, mux, http.MethodPost, "/api/destinations/legacy/test", ""); w.Code != http.StatusOK {
		t.Fatalf("test: %d %s", w.Code, w.Body.String())
	}
	d, err = store.getPlaintext("legacy")
	if err != nil {
		t.Fatalf("getPlaintext: %v", err)
	}
	uri, _, err := d.ToSyncURI(false)
	if err != nil {
		t.Fatalf("ToSyncURI after test: %v", err)
	}
	resolved, _ := filepath.EvalSymlinks(inside)
	if want := "file://" + resolved; strings.TrimSuffix(uri, "/") != want {
		t.Errorf("uri = %q, want %q", uri, want)
	}
}

func TestFileDestinationRefusedWhenPathMovesOutsideAfterSave(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	mux, store := registerDestTest(t, root, "")
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatal(err)
	}
	if w := destRequest(t, mux, http.MethodPost, "/api/destinations", fileDestBody("moved", target)); w.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	d, err := store.getPlaintext("moved")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := d.ToSyncURI(false); err != nil {
		t.Fatalf("ToSyncURI before swap: %v", err)
	}
	// Replace the saved directory with a symlink that leaves the root.
	if err := os.Remove(target); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, target); err != nil {
		t.Fatal(err)
	}
	if uri, _, err := d.ToSyncURI(false); err == nil {
		t.Errorf("ToSyncURI followed the swapped symlink to %q", uri)
	}
}
