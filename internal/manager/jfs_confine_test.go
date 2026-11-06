package manager

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The tests in this file pin the volume boundary. A user-facing volume
// path (/jfs/...) is mapped to <FUSEMount>/... and opened by the Manager.
// The volume can hold symlinks written by any client, so the Manager must
// judge where the kernel lands, not what the text says.
//
// Fixture layout (all under one temp folder):
//
//	fuse/                 the FUSE mount (FUSEMount)
//	fuse/escape  -> outside/        absolute symlink that leaves the volume
//	fuse/real/a.txt
//	fuse/inlink  -> fuse/real       absolute symlink inside the volume
//	fuse/rel     -> real            relative symlink inside the volume
//	fuse/dangling -> missing        dangling symlink (target unknown)
//	fuse/.trash/2026-01-01-00/1-entry.txt          trash entries
//	fuse/.trash/2026-01-01-00/3-entry.txt
//	fuse/.trash/2026-01-01-00/2-link -> outside/root  a trashed symlink
//	outside/secret.txt
//	outside/root/inner.txt          mode 0700
//	outside/victim.txt
//	src/                  the permitted source root (Out destinations)

const confineAdminKey = "confine-test-admin-key-not-for-production"

type confineFixture struct {
	base, fuse, outside, src string
	mux                      *http.ServeMux
	mgr                      *JobManager
}

func newConfineFixture(t *testing.T) *confineFixture {
	t.Helper()
	base := t.TempDir()
	f := &confineFixture{
		base:    base,
		fuse:    filepath.Join(base, "fuse"),
		outside: filepath.Join(base, "outside"),
		src:     filepath.Join(base, "src"),
	}
	for _, d := range []string{
		filepath.Join(f.fuse, "real"),
		filepath.Join(f.fuse, ".trash", "2026-01-01-00"),
		filepath.Join(f.outside, "root"),
		f.src,
	} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for p, body := range map[string]string{
		filepath.Join(f.fuse, "real", "a.txt"):                          "in volume",
		filepath.Join(f.fuse, ".trash", "2026-01-01-00", "1-entry.txt"): "trashed",
		filepath.Join(f.fuse, ".trash", "2026-01-01-00", "3-entry.txt"): "trashed",
		filepath.Join(f.outside, "secret.txt"):                          "outside",
		filepath.Join(f.outside, "root", "inner.txt"):                   "outside",
		filepath.Join(f.outside, "victim.txt"):                          "outside",
	} {
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chmod(filepath.Join(f.outside, "root"), 0o700); err != nil {
		t.Fatal(err)
	}
	for link, target := range map[string]string{
		filepath.Join(f.fuse, "escape"):                            f.outside,
		filepath.Join(f.fuse, "inlink"):                            filepath.Join(f.fuse, "real"),
		filepath.Join(f.fuse, "rel"):                               "real",
		filepath.Join(f.fuse, "dangling"):                          filepath.Join(base, "missing"),
		filepath.Join(f.fuse, ".trash", "2026-01-01-00", "2-link"): filepath.Join(f.outside, "root"),
	} {
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
	}
	f.mux = http.NewServeMux()
	f.mgr = Register(f.mux, "", Config{
		JuiceFSBin:  "/dev/null",
		FUSEMount:   f.fuse,
		SourceRoots: []string{f.src},
		DestMount:   "/jfs",
		AdminKey:    confineAdminKey,
	})
	// Queued jobs must not run juicefs; the runner records nothing.
	f.mgr.SetRunner(func(context.Context, string, RunSyncSpec, string, string, SyncOptions, []string, chan<- ProgressEvent) error {
		return nil
	})
	t.Cleanup(func() {
		waitJobsSettled(t, f.mgr)
		f.mgr.StopAll()
	})
	return f
}

func (f *confineFixture) do(t *testing.T, method, path, body string, hdr ...string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("X-JuiceMount-Admin-Key", confineAdminKey)
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	w := httptest.NewRecorder()
	f.mux.ServeHTTP(w, req)
	return w
}

func jsonBody(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func q(p string) string { return url.QueryEscape(p) }

// TestVolumeReadsRefuseEscapingSymlink: browse-jfs, preview,
// resolve-destination and warmup refuse a volume path whose symlink leads
// outside the volume.
func TestVolumeReadsRefuseEscapingSymlink(t *testing.T) {
	f := newConfineFixture(t)
	for _, p := range []string{"/jfs/escape", "/jfs/escape/root", "/jfs/real/../escape/root", "/jfs/dangling", "/jfs/dangling/x"} {
		t.Run(p, func(t *testing.T) {
			if w := f.do(t, http.MethodGet, "/api/browse-jfs?path="+q(p), ""); w.Code != http.StatusForbidden {
				t.Errorf("browse-jfs %s: got %d, want 403; body=%s", p, w.Code, w.Body.String())
			}
			if w := f.do(t, http.MethodGet, "/api/preview?path="+q(p), ""); w.Code != http.StatusForbidden {
				t.Errorf("preview %s: got %d, want 403; body=%s", p, w.Code, w.Body.String())
			}
			body := jsonBody(t, map[string]any{"source": p, "destination": f.src, "direction": "out"})
			if w := f.do(t, http.MethodPost, "/api/resolve-destination", body); w.Code != http.StatusForbidden {
				t.Errorf("resolve-destination out %s: got %d, want 403; body=%s", p, w.Code, w.Body.String())
			}
			if w := f.do(t, http.MethodPost, "/api/maintenance/warmup?path="+q(p), ""); w.Code != http.StatusForbidden {
				t.Errorf("warmup %s: got %d, want 403; body=%s", p, w.Code, w.Body.String())
			}
		})
	}
}

// TestMigrateRefusesEscapingSymlink: no job is queued for an Out source,
// or an In destination, that leaves the volume through a symlink. Saved
// destinations (Out and Between sources) are refused the same way.
func TestMigrateRefusesEscapingSymlink(t *testing.T) {
	f := newConfineFixture(t)
	if w := f.do(t, http.MethodPost, "/api/destinations", fileDestBody("bk", f.src)); w.Code != http.StatusCreated {
		t.Fatalf("destination create: %d %s", w.Code, w.Body.String())
	}
	cases := []map[string]any{
		{"source": "/jfs/escape", "destination": f.src, "direction": "out"},
		{"source": "/jfs/escape/root", "destination": f.src, "direction": "out"},
		{"source": f.src, "destination": "/jfs/escape/landed", "direction": "in"},
		{"source": "/jfs/escape", "destination": "file://bk", "direction": "out"},
		{"source": "/jfs/escape", "destination": "file://bk", "direction": "between"},
	}
	for _, c := range cases {
		body := jsonBody(t, c)
		if w := f.do(t, http.MethodPost, "/api/migrate", body); w.Code != http.StatusForbidden {
			t.Errorf("migrate %s: got %d, want 403; body=%s", body, w.Code, w.Body.String())
		}
		if w := f.do(t, http.MethodPost, "/api/resolve-destination", body); w.Code != http.StatusForbidden {
			t.Errorf("resolve-destination %s: got %d, want 403; body=%s", body, w.Code, w.Body.String())
		}
	}
	if n := len(f.mgr.List()); n != 0 {
		t.Errorf("%d jobs queued, want 0", n)
	}
}

// TestScheduleRefusesEscapingOutSource: a backup schedule cannot name an
// Out source that leaves the volume through a symlink.
func TestScheduleRefusesEscapingOutSource(t *testing.T) {
	f := newConfineFixture(t)
	if w := f.do(t, http.MethodPost, "/api/destinations", fileDestBody("bk", f.src)); w.Code != http.StatusCreated {
		t.Fatalf("destination create: %d %s", w.Code, w.Body.String())
	}
	body := `{"name":"nightly","source":{"path":"/jfs/escape","direction":"out"},"destination":{"name":"bk"},"cron":"0 2 1 1 *"}`
	if w := f.do(t, http.MethodPost, "/api/schedules", body); w.Code < 400 || w.Code >= 500 {
		t.Errorf("schedule with escaping Out source: got %d, want a 4xx refusal; body=%s", w.Code, w.Body.String())
	}
}

// TestRunSyncRefusesEscapingVolumePath: the runner itself re-checks a
// volume source or destination at the point of use, so a job that
// reached the queue some other way (a stored schedule, a resumed job)
// still cannot leave the volume.
func TestRunSyncRefusesEscapingVolumePath(t *testing.T) {
	f := newConfineFixture(t)
	spec := RunSyncSpec{Mode: ModeEmbedded, FUSEMount: f.fuse}
	cases := []struct{ src, dst string }{
		{"/jfs/escape", f.src},
		{"/jfs/escape/root", f.src},
		{"/jfs/real/../escape", f.src},
		{f.src, "/jfs/escape/landed"},
	}
	for _, c := range cases {
		err := RunSync(context.Background(), "/nonexistent/juicefs", spec, c.src, c.dst, SyncOptions{}, nil, nil)
		if err == nil || !strings.Contains(err.Error(), "outside the JuiceFS volume") {
			t.Errorf("RunSync(%s -> %s): err = %v, want a volume-boundary refusal", c.src, c.dst, err)
		}
	}
}

// TestPermissionsRefuseEscapingPath: inspect and fix judge the same
// cleaned path they act on, and refuse one that leaves the volume.
func TestPermissionsRefuseEscapingPath(t *testing.T) {
	f := newConfineFixture(t)
	f.mgr.SetMountOwner(os.Getuid(), os.Getgid())
	outRoot := filepath.Join(f.outside, "root")
	for _, p := range []string{"/jfs/escape/root", "/jfs/escape", "/jfs/rel/../escape/root"} {
		t.Run(p, func(t *testing.T) {
			if w := f.do(t, http.MethodGet, "/api/permissions/inspect?path="+q(p), ""); w.Code != http.StatusForbidden {
				t.Errorf("inspect %s: got %d, want 403; body=%s", p, w.Code, w.Body.String())
			}
			body := jsonBody(t, map[string]any{"path": p, "recursive": true})
			if w := f.do(t, http.MethodPost, "/api/permissions/fix", body, "X-Confirm-Fix", "yes"); w.Code != http.StatusForbidden {
				t.Errorf("fix %s: got %d, want 403; body=%s", p, w.Code, w.Body.String())
			}
			if fi, err := os.Stat(outRoot); err != nil || fi.Mode().Perm() != 0o700 {
				t.Fatalf("folder outside the volume changed: mode %v err %v, want 0700", fi.Mode().Perm(), err)
			}
		})
	}
	// ".." after an escaping symlink: the cleaned path is /jfs/outside/root,
	// which is inside the volume and does not exist. inspect must judge
	// and stat that cleaned path, and fix must not touch the folder the
	// raw text would reach through the symlink.
	p := "/jfs/escape/../outside/root"
	w := f.do(t, http.MethodGet, "/api/permissions/inspect?path="+q(p), "")
	var got permInspectResponse
	if err := json.Unmarshal(w.Body.Bytes(), &got); w.Code != http.StatusOK || err != nil || got.Exists || got.Path != "/jfs/outside/root" {
		t.Errorf("inspect %s: got %d %+v, want 200 exists=false path=/jfs/outside/root; body=%s", p, w.Code, got, w.Body.String())
	}
	body := jsonBody(t, map[string]any{"path": p, "recursive": true})
	if w := f.do(t, http.MethodPost, "/api/permissions/fix", body, "X-Confirm-Fix", "yes"); w.Code != http.StatusNotFound {
		t.Errorf("fix %s: got %d, want 404 (the cleaned path does not exist); body=%s", p, w.Code, w.Body.String())
	}
	if fi, err := os.Stat(outRoot); err != nil || fi.Mode().Perm() != 0o700 {
		t.Fatalf("folder outside the volume changed: mode %v err %v, want 0700", fi.Mode().Perm(), err)
	}
}

// TestPermissionsFixDoesNotFollowSymlinksMidWalk: a recursive fix on a
// folder inside the volume leaves the target of a symlink found during
// the walk alone.
func TestPermissionsFixDoesNotFollowSymlinksMidWalk(t *testing.T) {
	f := newConfineFixture(t)
	f.mgr.SetMountOwner(os.Getuid(), os.Getgid())
	outRoot := filepath.Join(f.outside, "root")
	if err := os.Symlink(outRoot, filepath.Join(f.fuse, "real", "planted")); err != nil {
		t.Fatal(err)
	}
	body := jsonBody(t, map[string]any{"path": "/jfs/real", "recursive": true})
	if w := f.do(t, http.MethodPost, "/api/permissions/fix", body, "X-Confirm-Fix", "yes"); w.Code != http.StatusOK {
		t.Fatalf("fix /jfs/real: got %d, want 200; body=%s", w.Code, w.Body.String())
	}
	if fi, err := os.Stat(outRoot); err != nil || fi.Mode().Perm() != 0o700 {
		t.Fatalf("symlink target outside the volume changed: mode %v err %v, want 0700", fi.Mode().Perm(), err)
	}
}

// TestTrashRefusesEscapingPaths: restore and delete use the cleaned
// entry and target paths, and refuse one that resolves outside the volume.
func TestTrashRefusesEscapingPaths(t *testing.T) {
	f := newConfineFixture(t)
	for _, target := range []string{"/jfs/escape/planted.txt", "/jfs/escape/sub/planted.txt", "/jfs/dangling/planted.txt"} {
		body := jsonBody(t, map[string]string{"path": "/jfs/.trash/2026-01-01-00/1-entry.txt", "target_path": target})
		if w := f.do(t, http.MethodPost, "/api/trash/restore", body); w.Code != http.StatusForbidden {
			t.Errorf("restore to %s: got %d, want 403; body=%s", target, w.Code, w.Body.String())
		}
	}
	// ".." after an escaping symlink: the restore must use the cleaned
	// target, /jfs/planted.txt, inside the volume.
	body := jsonBody(t, map[string]string{"path": "/jfs/.trash/2026-01-01-00/3-entry.txt", "target_path": "/jfs/escape/../planted.txt"})
	if w := f.do(t, http.MethodPost, "/api/trash/restore", body); w.Code != http.StatusOK {
		t.Errorf("restore to /jfs/escape/../planted.txt: got %d, want 200 at the cleaned target; body=%s", w.Code, w.Body.String())
	}
	if _, err := os.Lstat(filepath.Join(f.fuse, "planted.txt")); err != nil {
		t.Errorf("restore did not land at the cleaned target inside the volume: %v", err)
	}
	for _, p := range []string{filepath.Join(f.base, "planted.txt"), filepath.Join(f.outside, "planted.txt"), filepath.Join(f.outside, "sub")} {
		if _, err := os.Lstat(p); err == nil {
			t.Errorf("restore wrote outside the volume: %s", p)
		}
	}
	// An entry path that climbs out of a trashed symlink.
	sneaky := "/jfs/.trash/2026-01-01-00/2-link/../victim.txt"
	body = jsonBody(t, map[string]string{"path": sneaky, "target_path": "/jfs/real/victim.txt"})
	if w := f.do(t, http.MethodPost, "/api/trash/restore", body); w.Code < 400 || w.Code >= 500 {
		t.Errorf("restore of entry %s: got %d, want a 4xx refusal; body=%s", sneaky, w.Code, w.Body.String())
	}
	body = jsonBody(t, map[string]string{"path": sneaky})
	if w := f.do(t, http.MethodPost, "/api/trash/delete", body); w.Code < 400 || w.Code >= 500 {
		t.Errorf("delete of entry %s: got %d, want a 4xx refusal; body=%s", sneaky, w.Code, w.Body.String())
	}
	if _, err := os.Stat(filepath.Join(f.outside, "victim.txt")); err != nil {
		t.Errorf("file outside the volume was moved or deleted: %v", err)
	}
}

// TestSymlinksInsideVolumeKeepWorking: a symlink that stays inside the
// volume is followed as before by every volume-side caller.
func TestSymlinksInsideVolumeKeepWorking(t *testing.T) {
	f := newConfineFixture(t)
	f.mgr.SetMountOwner(os.Getuid(), os.Getgid())
	for _, p := range []string{"/jfs/inlink", "/jfs/rel", "/jfs/rel/../real", "/jfs"} {
		t.Run(p, func(t *testing.T) {
			w := f.do(t, http.MethodGet, "/api/browse-jfs?path="+q(p), "")
			if w.Code != http.StatusOK {
				t.Fatalf("browse-jfs %s: got %d; body=%s", p, w.Code, w.Body.String())
			}
			if p != "/jfs" && !strings.Contains(w.Body.String(), `"a.txt"`) {
				t.Errorf("browse-jfs %s: a.txt not listed; body=%s", p, w.Body.String())
			}
			if w := f.do(t, http.MethodGet, "/api/preview?path="+q(p), ""); w.Code != http.StatusOK {
				t.Errorf("preview %s: got %d; body=%s", p, w.Code, w.Body.String())
			}
			body := jsonBody(t, map[string]any{"source": p, "destination": f.src, "direction": "out"})
			if w := f.do(t, http.MethodPost, "/api/resolve-destination", body); w.Code != http.StatusOK {
				t.Errorf("resolve-destination %s: got %d; body=%s", p, w.Code, w.Body.String())
			}
			if w := f.do(t, http.MethodPost, "/api/migrate", body); w.Code != http.StatusAccepted {
				t.Errorf("migrate out %s: got %d; body=%s", p, w.Code, w.Body.String())
			}
			w = f.do(t, http.MethodGet, "/api/permissions/inspect?path="+q(p), "")
			var got permInspectResponse
			if err := json.Unmarshal(w.Body.Bytes(), &got); w.Code != http.StatusOK || err != nil || !got.Exists {
				t.Errorf("inspect %s: got %d; body=%s", p, w.Code, w.Body.String())
			}
		})
	}
	in := jsonBody(t, map[string]any{"source": f.src, "destination": "/jfs/inlink/imported", "direction": "in"})
	if w := f.do(t, http.MethodPost, "/api/migrate", in); w.Code != http.StatusAccepted {
		t.Errorf("migrate in to /jfs/inlink/imported: got %d; body=%s", w.Code, w.Body.String())
	}
	fix := jsonBody(t, map[string]any{"path": "/jfs/inlink", "recursive": true})
	if w := f.do(t, http.MethodPost, "/api/permissions/fix", fix, "X-Confirm-Fix", "yes"); w.Code != http.StatusOK {
		t.Errorf("fix /jfs/inlink: got %d; body=%s", w.Code, w.Body.String())
	}
	spec := RunSyncSpec{Mode: ModeEmbedded, FUSEMount: f.fuse}
	err := RunSync(context.Background(), "/nonexistent/juicefs", spec, "/jfs/inlink", f.src, SyncOptions{}, nil, nil)
	if err == nil || strings.Contains(err.Error(), "outside the JuiceFS volume") {
		t.Errorf("RunSync(/jfs/inlink): err = %v, want it to pass the boundary and fail on the missing binary", err)
	}
	// Restore through an in-volume symlink lands inside the volume.
	body := jsonBody(t, map[string]string{"path": "/jfs/.trash/2026-01-01-00/1-entry.txt", "target_path": "/jfs/inlink/restored.txt"})
	if w := f.do(t, http.MethodPost, "/api/trash/restore", body); w.Code != http.StatusOK {
		t.Fatalf("restore through in-volume symlink: got %d; body=%s", w.Code, w.Body.String())
	}
	if _, err := os.Stat(filepath.Join(f.fuse, "real", "restored.txt")); err != nil {
		t.Errorf("restored file not found under the volume: %v", err)
	}
	// Deleting a trashed symlink removes the link, not its target.
	body = jsonBody(t, map[string]string{"path": "/jfs/.trash/2026-01-01-00/2-link"})
	if w := f.do(t, http.MethodPost, "/api/trash/delete", body); w.Code != http.StatusNoContent {
		t.Fatalf("delete trashed symlink: got %d; body=%s", w.Code, w.Body.String())
	}
	if _, err := os.Stat(filepath.Join(f.outside, "root", "inner.txt")); err != nil {
		t.Errorf("deleting a trashed symlink touched its target: %v", err)
	}
}
