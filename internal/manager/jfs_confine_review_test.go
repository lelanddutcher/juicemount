package manager

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Tests for the second round of volume-boundary hardening: the post-sync
// permissions step, the trash root, a restore onto the volume root, the
// shared /jfs prefix check, and the linear-time resolver. Fixture layout:
// see newConfineFixture. Symlinks inside a sync tree are handled in the
// juicefs file backend (follow-up PR), not here.

// recordingJuicefs writes an executable stand-in for juicefs. It records its
// arguments, one per line, in the returned file, runs body, and exits 0.
func recordingJuicefs(t *testing.T, body string) (bin, argsFile string) {
	t.Helper()
	return recordingJuicefsAs(t, stockVersionAnswer, body)
}

// recordingJuicefsAs is recordingJuicefs with a chosen --version answer.
func recordingJuicefsAs(t *testing.T, versionAnswer, body string) (bin, argsFile string) {
	t.Helper()
	dir := t.TempDir()
	argsFile = filepath.Join(dir, "args")
	bin = filepath.Join(dir, "juicefs")
	// Answers --version like a stock juicefs, so RunSync keeps roots by
	// path (sync_roots.go) and the recorded arguments show them.
	script := "#!/bin/sh\n" + versionAnswer +
		"printf '%s\\n' \"$@\" > '" + argsFile + "'\n" + body + "\nexit 0\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin, argsFile
}

func readArgs(t *testing.T, argsFile string) []string {
	t.Helper()
	b, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatalf("juicefs was not run: %v", err)
	}
	return strings.Split(strings.TrimSpace(string(b)), "\n")
}

// TestPostSyncPermissionsSurviveDestinationSwap: the post-sync mode and
// owner fixup runs after a sync that may take hours. A destination folder
// swapped for a symlink during the sync must not lead it outside the
// volume, and a normal destination must still get the client mode.
func TestPostSyncPermissionsSurviveDestinationSwap(t *testing.T) {
	skipUnlessVolumeSyncPlatform(t)
	f := newConfineFixture(t)
	outRoot := filepath.Join(f.outside, "root")
	outFile := filepath.Join(outRoot, "inner.txt")
	if err := os.Chmod(outFile, 0o600); err != nil {
		t.Fatal(err)
	}
	spec := RunSyncSpec{Mode: ModeEmbedded, FUSEMount: f.fuse, OwnerUID: os.Getuid(), OwnerGID: os.Getgid(), SourceRoots: []string{f.src}}
	unchanged := func(when string) {
		t.Helper()
		for p, want := range map[string]os.FileMode{outRoot: 0o700, outFile: 0o600} {
			if fi, err := os.Stat(p); err != nil || fi.Mode().Perm() != want {
				t.Errorf("%s: %s outside the volume changed: mode %v err %v, want %v", when, p, fi.Mode().Perm(), err, want)
			}
		}
	}
	for _, dest := range []string{"/jfs/swapped", "file://" + filepath.Join(f.fuse, "swapped2") + "/"} {
		folder := filepath.Join(f.fuse, strings.TrimSuffix(filepath.Base(strings.TrimSuffix(dest, "/")), "/"))
		if err := os.MkdirAll(folder, 0o755); err != nil {
			t.Fatal(err)
		}
		answer := fencedVersionAnswer // into the volume
		if strings.HasPrefix(dest, "file://") {
			answer = stockVersionAnswer
		}
		bin, _ := recordingJuicefsAs(t, answer, "rm -rf '"+folder+"' && ln -s '"+outRoot+"' '"+folder+"'")
		if err := RunSync(context.Background(), bin, spec, f.src, dest, DefaultSyncOptions(), nil, nil); err != nil {
			t.Fatalf("RunSync to %s: %v", dest, err)
		}
		unchanged("swap of " + dest)
	}
	// No swap: the destination gets u+rwX,g+rwX,o+rX.
	good := filepath.Join(f.fuse, "good")
	if err := os.MkdirAll(good, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(good, "f.txt"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	bin, _ := recordingJuicefsAs(t, fencedVersionAnswer, "")
	if err := RunSync(context.Background(), bin, spec, f.src, "/jfs/good", DefaultSyncOptions(), nil, nil); err != nil {
		t.Fatalf("RunSync to /jfs/good: %v", err)
	}
	for p, want := range map[string]os.FileMode{good: 0o775, filepath.Join(good, "f.txt"): 0o664} {
		if fi, err := os.Stat(p); err != nil || fi.Mode().Perm() != want {
			t.Errorf("post-sync fixup: %s mode %v err %v, want %v", p, fi.Mode().Perm(), err, want)
		}
	}
}

// TestApplyPermsBeneathNeverFollowsSymlinks pins the walker that replaces
// chmod -R / chown -R.
func TestApplyPermsBeneathNeverFollowsSymlinks(t *testing.T) {
	f := newConfineFixture(t)
	uid, gid := os.Getuid(), os.Getgid()
	outRoot := filepath.Join(f.outside, "root")
	mountReal, err := walkPhysicalPath(f.fuse, false)
	if err != nil {
		t.Fatal(err)
	}
	tree := filepath.Join(f.fuse, "tree")
	if err := os.MkdirAll(filepath.Join(tree, "sub"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tree, "sub", "f"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outRoot, filepath.Join(tree, "sub", "link")); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(tree, "fifo"), 0o600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- applyPermsBeneath(context.Background(), mountReal, "tree", uid, gid, true) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("walk: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("walk blocked (did it open the FIFO?)")
	}
	for p, want := range map[string]os.FileMode{
		tree: 0o775, filepath.Join(tree, "sub"): 0o775, filepath.Join(tree, "sub", "f"): 0o664,
		filepath.Join(tree, "fifo"): 0o664, outRoot: 0o700,
	} {
		if fi, err := os.Stat(p); err != nil || fi.Mode().Perm() != want {
			t.Errorf("%s: mode %v err %v, want %v", p, fi.Mode().Perm(), err, want)
		}
	}
	// The named entry, or a folder on the way to it, is a symlink: the
	// caller's check is stale, so the walk refuses.
	for _, rel := range []string{"escape", "escape/root", "inlink"} {
		if err := applyPermsBeneath(context.Background(), mountReal, rel, uid, gid, true); err == nil {
			t.Errorf("walk of %s through a symlink: no error", rel)
		}
	}
	if fi, _ := os.Stat(outRoot); fi.Mode().Perm() != 0o700 {
		t.Errorf("folder outside the volume changed: %v", fi.Mode().Perm())
	}
}

// TestTrashDeleteRefusesTrashRootAndBuckets: per-entry delete must not
// purge the trash root or a whole bucket without the Empty confirmation.
func TestTrashDeleteRefusesTrashRootAndBuckets(t *testing.T) {
	f := newConfineFixture(t)
	for _, p := range []string{"/jfs/.trash", "/jfs/.trash/", "/jfs/.trash/2026-01-01-00", "/jfs/.trash/2026-01-01-00/"} {
		body := jsonBody(t, map[string]string{"path": p})
		if w := f.do(t, http.MethodPost, "/api/trash/delete", body); w.Code != http.StatusBadRequest {
			t.Errorf("delete %s: got %d, want 400; body=%s", p, w.Code, w.Body.String())
		}
		body = jsonBody(t, map[string]string{"path": p, "target_path": "/jfs/restored"})
		if w := f.do(t, http.MethodPost, "/api/trash/restore", body); w.Code != http.StatusBadRequest {
			t.Errorf("restore %s: got %d, want 400; body=%s", p, w.Code, w.Body.String())
		}
	}
	if _, err := os.Stat(filepath.Join(f.fuse, ".trash", "2026-01-01-00", "1-entry.txt")); err != nil {
		t.Errorf("trash entries were purged: %v", err)
	}
}

// TestTrashRestoreRefusesVolumeRootTarget: a restore onto the volume root
// is refused; a collision rename there would name a sibling of the mount.
func TestTrashRestoreRefusesVolumeRootTarget(t *testing.T) {
	f := newConfineFixture(t)
	for _, target := range []string{"/jfs", "/jfs/", "/jfs/real/.."} {
		body := jsonBody(t, map[string]string{"path": "/jfs/.trash/2026-01-01-00/1-entry.txt", "target_path": target})
		w := f.do(t, http.MethodPost, "/api/trash/restore", body)
		if w.Code != http.StatusBadRequest {
			t.Errorf("restore to %s: got %d, want 400; body=%s", target, w.Code, w.Body.String())
		}
		if strings.Contains(w.Body.String(), f.base) {
			t.Errorf("restore to %s: reply names a FUSE-side path: %s", target, w.Body.String())
		}
	}
	entries, _ := os.ReadDir(f.base)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "fuse") && e.Name() != "fuse" {
			t.Errorf("restore created %s next to the mount", e.Name())
		}
	}
}

// TestVolumePrefixIsOneCheck: "/jfsx/..." is not a volume path, for both
// RunSync's confinement and its destination rewrite.
func TestVolumePrefixIsOneCheck(t *testing.T) {
	for p, want := range map[string]bool{"/jfs": true, "/jfs/a": true, "/jfsx": false, "/jfsx/a": false, "/jf": false} {
		if got := isVolumeUserPath(p); got != want {
			t.Errorf("isVolumeUserPath(%q) = %v, want %v", p, got, want)
		}
	}
	f := newConfineFixture(t)
	bin, argsFile := recordingJuicefs(t, "")
	spec := RunSyncSpec{Mode: ModeEmbedded, FUSEMount: f.fuse}
	if err := RunSync(context.Background(), bin, spec, f.src, "/jfsx/y", DefaultSyncOptions(), nil, nil); err != nil {
		t.Fatalf("RunSync: %v", err)
	}
	args := readArgs(t, argsFile)
	if dst := args[len(args)-1]; strings.HasPrefix(dst, "file://"+f.fuse) {
		t.Errorf("/jfsx/y was rewritten onto the FUSE mount: %s", dst)
	}
}

// TestResolverIsLinearInDepth: 950 nested real folders, through every
// path check that shares walkPhysicalPath: the volume side
// (confineVolumePath, browse-jfs), the host side (pathAllowed) and file
// destinations (resolveFileDestPath, and saving one). The resolvers they
// replaced called EvalSymlinks once per missing component and took
// seconds (3.0 s for 475 existing plus 475 missing); each check must stay
// well under a second. Two shapes: all 950 levels exist, and 475 exist
// with 475 missing below them.
func TestResolverIsLinearInDepth(t *testing.T) {
	f := newConfineFixture(t)
	const limit = 500 * time.Millisecond
	timed := func(label string, fn func()) {
		t.Helper()
		start := time.Now()
		fn()
		if d := time.Since(start); d > limit {
			t.Errorf("%s took %v, want under %v", label, d, limit)
		}
	}
	shapes := []struct{ name, existing, missing string }{
		{"950 existing", strings.Repeat("/abc", 950), ""},
		{"475 existing + 475 missing", strings.Repeat("/abd", 475), strings.Repeat("/xyz", 475)},
	}
	for _, sh := range shapes {
		t.Run(sh.name, func(t *testing.T) {
			// The OS must allow the path: Linux PATH_MAX is 4096, macOS 1024.
			// Linear time does not depend on the OS, so a shorter limit skips.
			pathMax := maxResolvePathBytes
			if runtime.GOOS != "linux" {
				pathMax = 1023
			}
			if len(f.fuse+sh.existing+sh.missing) > pathMax || len(f.src+sh.existing+sh.missing) > pathMax {
				t.Skipf("a %d-component path is longer than this OS allows (%d bytes)", 950, pathMax)
			}
			// Volume side.
			if err := os.MkdirAll(f.fuse+sh.existing, 0o755); err != nil {
				t.Fatal(err)
			}
			jfs := "/jfs" + sh.existing + sh.missing
			timed("confineVolumePath", func() {
				vp, err := confineVolumePath(jfs, "/jfs", f.fuse, true)
				if err != nil || !strings.HasSuffix(vp.Real, sh.existing+sh.missing) {
					t.Errorf("confineVolumePath: %+v, %v", vp, err)
				}
			})
			timed("browse-jfs", func() {
				if w := f.do(t, http.MethodGet, "/api/browse-jfs?path="+q(jfs), ""); w.Code == http.StatusForbidden {
					t.Errorf("browse-jfs of a deep in-volume path: 403")
				}
			})
			// Host side and file destinations, under the source root.
			if err := os.MkdirAll(f.src+sh.existing, 0o755); err != nil {
				t.Fatal(err)
			}
			host := f.src + sh.existing + sh.missing
			a := &API{sourceRoots: []string{f.src}}
			timed("pathAllowed", func() {
				if !a.pathAllowed(host) {
					t.Errorf("pathAllowed refused a deep path under the root")
				}
			})
			timed("resolveFileDestPath", func() {
				got, err := resolveFileDestPath(host)
				if err != nil || !strings.HasSuffix(got, sh.existing+sh.missing) {
					t.Errorf("resolveFileDestPath: %q, %v", got, err)
				}
			})
			name := "deep" + strings.ReplaceAll(strings.ToLower(sh.name[:3]), " ", "")
			timed("save file destination", func() {
				if w := f.do(t, http.MethodPost, "/api/destinations", fileDestBody(name, host)); w.Code != http.StatusCreated {
					t.Errorf("save deep file destination: %d %s", w.Code, w.Body.String())
				}
			})
		})
	}
}

// TestWalkPhysicalPath pins the shared resolver's kernel-like behaviour.
func TestWalkPhysicalPath(t *testing.T) {
	base, err := walkPhysicalPath(t.TempDir(), false)
	if err != nil {
		t.Fatal(err)
	}
	mk := func(p string) {
		if err := os.MkdirAll(filepath.Join(base, p), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	ln := func(target, link string) {
		if err := os.Symlink(target, filepath.Join(base, link)); err != nil {
			t.Fatal(err)
		}
	}
	mk("a/b")
	mk("out/x")
	ln(filepath.Join(base, "out"), "a/abs")
	ln("../out", "a/rel")
	ln(filepath.Join(base, "nowhere/y"), "a/dangling")
	ln("loop2", "loop1")
	ln("loop1", "loop2")
	if err := os.WriteFile(filepath.Join(base, "a", "file"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		in, want string
		lenient  bool
		wantErr  error
	}{
		{in: "a/b", want: "a/b"},
		{in: "a/abs/x", want: "out/x"},
		{in: "a/rel/x", want: "out/x"},
		{in: "a/abs/..", want: ""}, // ".." after the symlink: parent of out
		{in: "a/abs/new/file", want: "out/new/file"},
		{in: "a/b/missing/../x", wantErr: errors.New("climbs")},
		{in: "a/dangling", wantErr: errDanglingSymlink},
		{in: "a/dangling", want: "nowhere/y", lenient: true},
		{in: "loop1", wantErr: syscall.ELOOP},
		{in: "a/file/x", wantErr: syscall.ENOTDIR},
	}
	for _, c := range cases {
		// Concatenate: filepath.Join would collapse ".." before the resolver.
		got, err := walkPhysicalPath(base+"/"+c.in, c.lenient)
		switch {
		case c.wantErr != nil:
			if err == nil || (!errors.Is(err, c.wantErr) && !strings.Contains(err.Error(), c.wantErr.Error())) {
				t.Errorf("%s: got %q, %v; want error %v", c.in, got, err, c.wantErr)
			}
		case err != nil || got != filepath.Join(base, c.want):
			t.Errorf("%s: got %q, %v; want %q", c.in, got, err, filepath.Join(base, c.want))
		}
	}
}

// stockVersionAnswer makes a stand-in juicefs answer --version like the
// stock juicefs (no -jmN suffix), as RunSync asks before every sync.
const stockVersionAnswer = "if [ \"$1\" = --version ]; then echo 'juicefs version 1.3.1+2025-12-02.e0032b2a'; exit 0; fi\n"

// fencedVersionAnswer answers like the juicefs built from third_party/juicefs
// (-jm3): RunSync then hands it open roots and the project fence. A sync
// into the volume needs it; an older juicefs is refused there.
const fencedVersionAnswer = "if [ \"$1\" = --version ]; then echo 'juicefs version 1.3.1+2025-12-02.e0032b2ae5e9-jm3'; exit 0; fi\n"
