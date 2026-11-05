package manager

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity/binder"
	"github.com/lelanddutcher/juicemount/internal/smartfolders"
	"github.com/lelanddutcher/juicemount/internal/smartfolders/jfsmeta"
)

func continuityTestConfig(t *testing.T) Config {
	t.Helper()
	state := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(state, 0o755); err != nil {
		t.Fatal(err)
	}
	// Nothing listens there: the subscriber retries until Stop.
	return Config{StateFile: filepath.Join(state, "jobs.json"), MetaURL: "redis://127.0.0.1:1/0"}
}

func envOf(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestContinuityOffOpensNothing(t *testing.T) {
	for _, v := range []string{"", "off", "maybe"} {
		cfg := continuityTestConfig(t)
		if svc := buildContinuity(cfg, envOf(map[string]string{"JM_CONTINUITY": v})); svc != nil {
			svc.Stop()
			t.Fatalf("JM_CONTINUITY=%q started the binder", v)
		}
		if _, err := os.Lstat(filepath.Join(filepath.Dir(cfg.StateFile), continuityDirName)); !os.IsNotExist(err) {
			t.Fatalf("JM_CONTINUITY=%q created the catalog directory: %v", v, err)
		}
	}
	a := &API{}
	rec := httptest.NewRecorder()
	a.handleContinuityAssets(rec, httptest.NewRequest(http.MethodGet, "/api/continuity/assets?path=A/x.mov", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("assets while off: %d", rec.Code)
	}
}

func TestContinuityOpensPrivateCatalogAndReopensIt(t *testing.T) {
	cfg := continuityTestConfig(t)
	dir := filepath.Join(filepath.Dir(cfg.StateFile), continuityDirName)
	// A directory left with a wider mode is narrowed, not refused.
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	on := envOf(map[string]string{"JM_CONTINUITY": "on"})
	svc := buildContinuity(cfg, on)
	if svc == nil {
		t.Fatal("not started")
	}
	svc.stopWait = 5 * time.Second
	svc.start()
	first, err := svc.store.LibraryID()
	if err != nil {
		t.Fatal(err)
	}
	stopped := make(chan struct{})
	go func() { svc.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("Stop hung on an unreachable Redis")
	}
	info, err := os.Stat(dir)
	if err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("catalog directory mode %v, %v", info.Mode(), err)
	}
	if info, err := os.Stat(filepath.Join(dir, continuityCatalogName)); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("catalog file: %v, %v", info, err)
	}
	again := buildContinuity(cfg, on)
	if again == nil {
		t.Fatal("existing catalog not reopened")
	}
	defer again.Stop()
	if second, err := again.store.LibraryID(); err != nil || second != first {
		t.Fatalf("reopened library %s, %v; want %s", second, err, first)
	}
}

func TestContinuityRefusesCatalogInsideTheVolume(t *testing.T) {
	cfg := continuityTestConfig(t)
	cfg.FUSEMount = filepath.Dir(cfg.StateFile)
	if svc := buildContinuity(cfg, envOf(map[string]string{"JM_CONTINUITY": "on"})); svc != nil {
		svc.Stop()
		t.Fatal("catalog opened inside the mount")
	}
	if _, err := os.Lstat(filepath.Join(cfg.FUSEMount, continuityDirName)); !os.IsNotExist(err) {
		t.Fatalf("directory created inside the mount: %v", err)
	}
}

func TestContinuityRefusesSymlinkedDirectory(t *testing.T) {
	cfg := continuityTestConfig(t)
	elsewhere := t.TempDir()
	if err := os.Symlink(elsewhere, filepath.Join(filepath.Dir(cfg.StateFile), continuityDirName)); err != nil {
		t.Fatal(err)
	}
	if svc := buildContinuity(cfg, envOf(map[string]string{"JM_CONTINUITY": "on"})); svc != nil {
		svc.Stop()
		t.Fatal("catalog opened through a symlink")
	}
}

// fakeVolume is a fixed volume: path -> inode.
type fakeVolume struct{ files map[string]uint64 }

func (v fakeVolume) VolumeUUID() string { return "00000000-0000-4000-8000-0000000000d2" }
func (v fakeVolume) LookupIno(p string) (jfsmeta.Node, error) {
	if ino, ok := v.files[p]; ok {
		return jfsmeta.Node{Ino: ino, Type: smartfolders.TypeRegular, Nlink: 1, Parent: 1}, nil
	}
	return jfsmeta.Node{}, syscall.ENOENT
}
func (v fakeVolume) StatIno(ino uint64) (jfsmeta.Node, error) {
	for _, i := range v.files {
		if i == ino {
			return jfsmeta.Node{Ino: ino, Type: smartfolders.TypeRegular, Nlink: 1, Parent: 1}, nil
		}
	}
	return jfsmeta.Node{}, syscall.ENOENT
}
func (v fakeVolume) GetPaths(ino uint64) ([]string, error) {
	var out []string
	for p, i := range v.files {
		if i == ino {
			out = append(out, p)
		}
	}
	return out, nil
}
func (v fakeVolume) Close() error { return nil }

func TestContinuityAssetsAPI(t *testing.T) {
	cfg := continuityTestConfig(t)
	store, err := openContinuityCatalog(filepath.Dir(cfg.StateFile))
	if err != nil {
		t.Fatal(err)
	}
	vol := fakeVolume{files: map[string]uint64{"Projects/clip.mov": 42, "Projects/new.mov": 43}}
	b, err := binder.New(binder.Config{Store: store, Connect: func() (binder.Volume, error) { return vol, nil }})
	if err != nil {
		t.Fatal(err)
	}
	svc := &continuityService{binder: b, store: store}
	svc.start()
	defer svc.Stop()
	b.Offer(binder.Event{Op: "create", Path: "Projects/clip.mov", Inode: 42})
	deadline := time.Now().Add(10 * time.Second)
	for b.Stats().Handled < 1 {
		if time.Now().After(deadline) {
			t.Fatalf("event not handled: %+v", b.Stats())
		}
		time.Sleep(5 * time.Millisecond)
	}
	a := &API{adminKey: "test-admin-key", continuity: svc}
	get := func(url string, key bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, url, nil)
		if key {
			req.Header.Set("X-JuiceMount-Admin-Key", "test-admin-key")
		}
		rec := httptest.NewRecorder()
		a.auth(a.handleContinuityAssets)(rec, req.WithContext(context.Background()))
		return rec
	}
	if rec := get("/api/continuity/assets?path=Projects/clip.mov", false); rec.Code != http.StatusUnauthorized {
		t.Fatalf("no key: %d", rec.Code)
	}
	rec := get("/api/continuity/assets?path=Projects/clip.mov", true)
	var view binder.AssetView
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &view) != nil {
		t.Fatalf("by path: %d %s", rec.Code, rec.Body)
	}
	want := binder.AssetID(vol.VolumeUUID(), 42)
	if view.AssetID != want || view.Path != "Projects/clip.mov" || len(view.Locations) != 1 || view.Locations[0].Locator != "ino:42" {
		t.Fatalf("by path: %+v", view)
	}
	if rec := get("/api/continuity/assets?id="+string(want), true); rec.Code != http.StatusOK {
		t.Fatalf("by id: %d %s", rec.Code, rec.Body)
	}
	for url, code := range map[string]int{
		"/api/continuity/assets?path=Projects/new.mov":                     http.StatusNotFound, // not catalogued
		"/api/continuity/assets?path=Projects/none.mov":                    http.StatusNotFound,
		"/api/continuity/assets?id=asset_00000000000000000000000000000001": http.StatusNotFound,
		"/api/continuity/assets?id=nope":                                   http.StatusBadRequest,
		"/api/continuity/assets?path=../x":                                 http.StatusBadRequest,
		"/api/continuity/assets":                                           http.StatusBadRequest,
		"/api/continuity/assets?path=Projects/clip.mov&id=" + string(want): http.StatusBadRequest,
	} {
		if rec := get(url, true); rec.Code != code {
			t.Errorf("%s: %d %s, want %d", url, rec.Code, rec.Body, code)
		}
	}
	status := httptest.NewRecorder()
	a.handleContinuityStatus(status, httptest.NewRequest(http.MethodGet, "/api/continuity/status", nil))
	var st struct {
		Enabled bool         `json:"enabled"`
		Binder  binder.Stats `json:"binder"`
	}
	if json.Unmarshal(status.Body.Bytes(), &st) != nil || !st.Enabled || st.Binder.Applied != 1 || st.Binder.Complete {
		t.Fatalf("status: %s", status.Body)
	}
}
