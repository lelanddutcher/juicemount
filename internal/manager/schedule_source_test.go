package manager

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// scheduleSourceFixture lays out a permitted root with a symlink that
// points outside it:
//
//	<base>/root/                 permitted source root
//	<base>/root/link   ->        <base>/outside/inner
//	<base>/outside/inner/
//	<base>/outside/secret/data   outside every permitted root
//
// The path text "<root>/link/../secret" cleans to "<root>/secret"
// (inside the root), but the kernel resolves ".." after the symlink, so
// opening the raw text reaches "<base>/outside/secret".
type scheduleSourceFixture struct {
	root, outside string
	mux           *http.ServeMux
	mgr           *JobManager
	store         *scheduleStoreImpl
	sources       chan string
}

func newScheduleSourceFixture(t *testing.T) *scheduleSourceFixture {
	t.Helper()
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("resolve temp dir: %v", err)
	}
	f := &scheduleSourceFixture{
		root:    filepath.Join(base, "root"),
		outside: filepath.Join(base, "outside"),
		sources: make(chan string, 4),
	}
	for _, d := range []string{f.root, filepath.Join(f.outside, "inner"), filepath.Join(f.outside, "secret")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
	}
	if err := os.WriteFile(filepath.Join(f.outside, "secret", "data"), []byte("outside"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.Symlink(filepath.Join(f.outside, "inner"), filepath.Join(f.root, "link")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	f.mux = http.NewServeMux()
	f.mgr = Register(f.mux, "", Config{
		JuiceFSBin:  "/dev/null",
		FUSEMount:   "/mnt/juicefs",
		SourceRoots: []string{f.root},
		DestMount:   "/jfs",
		AdminKey:    destTestAdminKey,
	})
	t.Cleanup(f.mgr.StopAll)
	f.mgr.SetRunner(func(_ context.Context, _ string, _ RunSyncSpec, src, _ string, _ SyncOptions, _ []string, _ chan<- ProgressEvent) error {
		select {
		case f.sources <- src:
		default:
		}
		return nil
	})
	store, ok := f.mgr.schedules.(*scheduleStoreImpl)
	if !ok || store == nil {
		t.Fatal("schedule store not wired")
	}
	f.store = store

	w := destRequest(t, f.mux, http.MethodPost, "/api/destinations", `{
		"name": "remote",
		"kind": "s3",
		"config": {"endpoint": "https://s3.example.com", "bucket": "b", "access_key": "AK", "secret_key": "SK"}
	}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("destination POST: %d %s", w.Code, w.Body.String())
	}
	return f
}

func (f *scheduleSourceFixture) createSchedule(t *testing.T, name, source string) int {
	t.Helper()
	body := fmt.Sprintf(`{
		"name": %q,
		"source": {"path": %q, "direction": "in"},
		"destination": {"name": "remote"},
		"cron": "0 2 1 1 *"
	}`, name, source)
	return destRequest(t, f.mux, http.MethodPost, "/api/schedules", body).Code
}

func (f *scheduleSourceFixture) nextSource(t *testing.T) string {
	t.Helper()
	select {
	case src := <-f.sources:
		return src
	case <-time.After(10 * time.Second):
		t.Fatal("no job reached the runner")
		return ""
	}
}

// assertConfined fails if the submitted job source is not the cleaned
// path text, or if opening it on disk reaches outside the root.
func (f *scheduleSourceFixture) assertConfined(t *testing.T, src string) {
	t.Helper()
	if src != filepath.Clean(src) {
		t.Errorf("job source %q is not the cleaned path the gate approved (%q)", src, filepath.Clean(src))
	}
	if b, err := os.ReadFile(src + "/data"); err == nil {
		t.Errorf("job source %q reads a file outside the permitted root (content %q)", src, b)
	}
	if real, err := filepath.EvalSymlinks(src); err == nil && real != f.root && !strings.HasPrefix(real, f.root+"/") {
		t.Errorf("job source %q resolves to %q, outside root %q", src, real, f.root)
	}
}

// TestScheduleSubmitsCleanedSource: a schedule whose source has ".."
// after a symlink must queue the cleaned path the gate checked, not the
// raw text the kernel would resolve through the symlink.
func TestScheduleSubmitsCleanedSource(t *testing.T) {
	f := newScheduleSourceFixture(t)
	raw := f.root + "/link/../secret"

	if code := f.createSchedule(t, "dotdot", raw); code != http.StatusCreated {
		t.Fatalf("create schedule: %d, want 201", code)
	}
	w := destRequest(t, f.mux, http.MethodPost, "/api/schedules/dotdot/run", "")
	if w.Code != http.StatusAccepted {
		t.Fatalf("run-now: %d %s", w.Code, w.Body.String())
	}
	src := f.nextSource(t)
	if src != filepath.Join(f.root, "secret") {
		t.Errorf("run-now job source = %q, want %q", src, filepath.Join(f.root, "secret"))
	}
	f.assertConfined(t, src)
}

// TestStoredScheduleRawSourceConfinedAtFire: a schedule row saved before
// this fix keeps its raw path text in the state file. Both the cron fire
// path and run-now must still queue only the cleaned path.
func TestStoredScheduleRawSourceConfinedAtFire(t *testing.T) {
	f := newScheduleSourceFixture(t)
	if code := f.createSchedule(t, "legacy", filepath.Join(f.root, "placeholder")); code != http.StatusCreated {
		t.Fatalf("create schedule: %d, want 201", code)
	}
	// Simulate the persisted pre-upgrade row: raw, uncleaned path text.
	raw := f.root + "/link/../secret"
	f.store.mu.Lock()
	f.store.rows[f.store.findRowLocked("legacy")].Source.Path = raw
	f.store.mu.Unlock()

	f.store.fire("legacy")
	f.assertConfined(t, f.nextSource(t))

	if _, err := f.store.runNow("legacy"); err != nil {
		t.Fatalf("runNow: %v", err)
	}
	f.assertConfined(t, f.nextSource(t))
}

// TestScheduleRefusesSymlinkOutOfRoot pins the symlink policy shared
// with /api/migrate: pathAllowed follows symlinks with EvalSymlinks, so
// a source that is (or sits under) a symlink pointing outside the root
// is refused at create, and a stored row like that is blocked at fire.
func TestScheduleRefusesSymlinkOutOfRoot(t *testing.T) {
	f := newScheduleSourceFixture(t)
	for _, p := range []string{filepath.Join(f.root, "link"), f.root + "/link/./"} {
		if code := f.createSchedule(t, "viaLink", p); code == http.StatusCreated {
			t.Errorf("create schedule with source %q: 201, want refusal", p)
		}
	}
	w := destRequest(t, f.mux, http.MethodPost, "/api/migrate",
		fmt.Sprintf(`{"source": %q, "destination": "/jfs/x", "direction": "in"}`, filepath.Join(f.root, "link")))
	if w.Code != http.StatusForbidden {
		t.Errorf("migrate with symlinked source: %d, want 403", w.Code)
	}

	if code := f.createSchedule(t, "stored", filepath.Join(f.root, "placeholder")); code != http.StatusCreated {
		t.Fatalf("create schedule: %d, want 201", code)
	}
	f.store.mu.Lock()
	f.store.rows[f.store.findRowLocked("stored")].Source.Path = filepath.Join(f.root, "link")
	f.store.mu.Unlock()
	if _, err := f.store.runNow("stored"); err == nil {
		t.Error("runNow with a stored symlink-out source: nil error, want refusal")
	}
	select {
	case src := <-f.sources:
		t.Errorf("a job was queued for refused source %q", src)
	default:
	}
}
