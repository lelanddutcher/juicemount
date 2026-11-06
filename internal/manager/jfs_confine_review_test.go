package manager

import (
	"context"
	"errors"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Tests for the second round of volume-boundary hardening: symlinks deep
// inside a sync tree, the post-sync permissions step, the trash root, a
// restore onto the volume root, the shared /jfs prefix check, and the
// linear-time resolver. Fixture layout: see newConfineFixture.

// recordingJuicefs writes an executable stand-in for juicefs. It records its
// arguments, one per line, in the returned file, runs body, and exits 0.
func recordingJuicefs(t *testing.T, body string) (bin, argsFile string) {
	t.Helper()
	dir := t.TempDir()
	argsFile = filepath.Join(dir, "args")
	bin = filepath.Join(dir, "juicefs")
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > '" + argsFile + "'\n" + body + "\nexit 0\n"
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

func hasArg(args []string, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}

// addDeepEscape stores fuse/proj/deep/a/b/evil -> outside, next to a real
// file, so a sync of /jfs/proj meets the symlink four levels down.
func (f *confineFixture) addDeepEscape(t *testing.T) {
	t.Helper()
	d := filepath.Join(f.fuse, "proj", "deep", "a", "b")
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, "real.txt"), []byte("in volume"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(f.outside, filepath.Join(d, "evil")); err != nil {
		t.Fatal(err)
	}
}

// TestSyncCopiesVolumeSymlinksAsLinks: a sync that reads or writes the
// FUSE mount passes --links when both ends can store symlinks, so juicefs
// copies a symlink as a link instead of following it.
func TestSyncCopiesVolumeSymlinksAsLinks(t *testing.T) {
	f := newConfineFixture(t)
	f.addDeepEscape(t)
	if err := os.MkdirAll(filepath.Join(f.fuse, "dest"), 0o755); err != nil {
		t.Fatal(err)
	}
	spec := RunSyncSpec{Mode: ModeEmbedded, FUSEMount: f.fuse}
	cases := []struct {
		name, src, dst string
		links          bool
	}{
		{"out to a host root", "/jfs/proj", filepath.Join(f.src, "out"), true},
		{"out to a saved file destination", "/jfs/proj", "file://" + filepath.Join(f.src, "bk") + "/", true},
		{"out to a saved jfs destination", "/jfs/proj", "jfs://other/", true},
		{"in into a volume destination", f.src, "/jfs/dest", true},
		{"host to host does not touch the volume", f.src, filepath.Join(f.src, "copy"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bin, argsFile := recordingJuicefs(t, "")
			if err := RunSync(context.Background(), bin, spec, c.src, c.dst, DefaultSyncOptions(), nil, nil); err != nil {
				t.Fatalf("RunSync: %v", err)
			}
			if got := hasArg(readArgs(t, argsFile), "--links"); got != c.links {
				t.Errorf("--links passed = %v, want %v; args %v", got, c.links, readArgs(t, argsFile))
			}
		})
	}
}

// TestSyncRefusesEscapingSourceForLinklessDestination: an object store
// cannot hold a symlink, and juicefs then drops --links and follows
// symlinks. A volume source holding a symlink that leads outside the
// volume is refused before juicefs runs. In-volume symlinks are fine.
func TestSyncRefusesEscapingSourceForLinklessDestination(t *testing.T) {
	f := newConfineFixture(t)
	f.addDeepEscape(t)
	spec := RunSyncSpec{Mode: ModeEmbedded, FUSEMount: f.fuse}
	for _, dst := range []string{"s3://bucket/backups/", "b2://bucket/", "webdav://dav.example/x/"} {
		bin, argsFile := recordingJuicefs(t, "")
		err := RunSync(context.Background(), bin, spec, "/jfs/proj", dst, DefaultSyncOptions(), nil, nil)
		if err == nil || !strings.Contains(err.Error(), "/jfs/proj/deep/a/b/evil") || !strings.Contains(err.Error(), "cannot store symlinks") {
			t.Errorf("Out to %s: err = %v, want a refusal naming /jfs/proj/deep/a/b/evil", dst, err)
		}
		if _, statErr := os.Stat(argsFile); statErr == nil {
			t.Errorf("Out to %s: juicefs ran despite the refusal", dst)
		}
	}
	// A tree whose symlinks stay inside the volume still syncs (followed,
	// as before; juicefs would ignore --links here anyway).
	if err := os.Symlink(filepath.Join(f.fuse, "real", "a.txt"), filepath.Join(f.fuse, "real", "alias.txt")); err != nil {
		t.Fatal(err)
	}
	bin, argsFile := recordingJuicefs(t, "")
	if err := RunSync(context.Background(), bin, spec, "/jfs/real", "s3://bucket/x/", DefaultSyncOptions(), nil, nil); err != nil {
		t.Fatalf("Out of an in-volume-only tree to s3: %v", err)
	}
	if hasArg(readArgs(t, argsFile), "--links") {
		t.Errorf("--links passed for an s3 destination")
	}
}

// TestSyncRefusesVolumeDestinationWithEscapingSymlink: juicefs writes
// through a symlink already in the destination, even with --links. A
// volume destination holding one that leads outside is refused.
func TestSyncRefusesVolumeDestinationWithEscapingSymlink(t *testing.T) {
	f := newConfineFixture(t)
	victim := filepath.Join(f.base, "victim")
	if err := os.MkdirAll(victim, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(f.fuse, "dest", "x"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(victim, filepath.Join(f.fuse, "dest", "x", "sub")); err != nil {
		t.Fatal(err)
	}
	bin, argsFile := recordingJuicefs(t, "")
	spec := RunSyncSpec{Mode: ModeEmbedded, FUSEMount: f.fuse}
	err := RunSync(context.Background(), bin, spec, f.src, "/jfs/dest", DefaultSyncOptions(), nil, nil)
	if err == nil || !strings.Contains(err.Error(), "/jfs/dest/x/sub") {
		t.Fatalf("In into a destination holding an escaping symlink: err = %v, want a refusal naming /jfs/dest/x/sub", err)
	}
	if _, statErr := os.Stat(argsFile); statErr == nil {
		t.Errorf("juicefs ran despite the refusal")
	}
}

// realJuicefs returns the juicefs binary named by JM_TEST_JUICEFS_BIN
// (build it from third_party/juicefs), or skips.
func realJuicefs(t *testing.T) string {
	t.Helper()
	bin := os.Getenv("JM_TEST_JUICEFS_BIN")
	if bin == "" {
		t.Skip("JM_TEST_JUICEFS_BIN not set; build juicefs from third_party/juicefs to run this test")
	}
	return bin
}

// useRealSync makes the fixture's jobs run the real RunSync with bin.
func (f *confineFixture) useRealSync(bin string) {
	f.mgr.SetRunner(func(ctx context.Context, _ string, spec RunSyncSpec, src, dst string, opts SyncOptions, env []string, progress chan<- ProgressEvent) error {
		return RunSync(ctx, bin, spec, src, dst, opts, env, progress)
	})
}

// regularFilesNamed lists regular files named name under root (no
// symlinks followed).
func regularFilesNamed(t *testing.T, root, name string) []string {
	t.Helper()
	var out []string
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err == nil && d.Type().IsRegular() && d.Name() == name {
			out = append(out, p)
		}
		return nil
	})
	return out
}

// TestRealJuicefsOutCopiesDeepSymlinkAsLink runs the vendored juicefs
// through /api/migrate: an Out job of a tree holding a deep symlink to a
// folder outside the volume must copy the link, not the folder's files,
// both to a host root and to a saved file destination.
func TestRealJuicefsOutCopiesDeepSymlinkAsLink(t *testing.T) {
	bin := realJuicefs(t)
	f := newConfineFixture(t)
	f.addDeepEscape(t)
	f.useRealSync(bin)
	bk := filepath.Join(f.src, "bk")
	if err := os.MkdirAll(bk, 0o755); err != nil {
		t.Fatal(err)
	}
	if w := f.do(t, http.MethodPost, "/api/destinations", fileDestBody("bk", bk)); w.Code != http.StatusCreated {
		t.Fatalf("destination create: %d %s", w.Code, w.Body.String())
	}
	hostOut := filepath.Join(f.src, "hostout")
	for _, c := range []struct{ dest, root string }{{hostOut, hostOut}, {"file://bk", bk}} {
		body := jsonBody(t, map[string]any{"source": "/jfs/proj", "destination": c.dest, "direction": "out"})
		if w := f.do(t, http.MethodPost, "/api/migrate", body); w.Code != http.StatusAccepted {
			t.Fatalf("migrate out to %s: %d %s", c.dest, w.Code, w.Body.String())
		}
		waitJobsSettled(t, f.mgr)
		if got := regularFilesNamed(t, c.root, "real.txt"); len(got) != 1 {
			t.Errorf("out to %s: the sync did not copy the in-volume file (found %v)", c.dest, got)
		}
		if got := regularFilesNamed(t, c.root, "secret.txt"); len(got) != 0 {
			t.Errorf("out to %s: content from outside the volume was copied: %v", c.dest, got)
		}
		fi, err := os.Lstat(filepath.Join(c.root, "deep", "a", "b", "evil"))
		if err != nil || fi.Mode()&fs.ModeSymlink == 0 {
			t.Errorf("out to %s: deep symlink not copied as a link (err %v)", c.dest, err)
		}
	}
	for _, j := range f.mgr.List() {
		if s := j.GetSnapshot(); s.State != JobDone {
			t.Errorf("job %s -> %s: state %s, error %q", s.Source, s.Destination, s.State, s.Error)
		}
	}
}

// TestRealJuicefsInRefusesDestinationSymlink runs the vendored juicefs
// through /api/migrate: an In job into a volume folder that holds a
// symlink to a folder outside the volume must write nothing there.
func TestRealJuicefsInRefusesDestinationSymlink(t *testing.T) {
	bin := realJuicefs(t)
	f := newConfineFixture(t)
	f.useRealSync(bin)
	victim := filepath.Join(f.base, "victim")
	for _, d := range []string{victim, filepath.Join(f.fuse, "dest", "x"), filepath.Join(f.src, "in", "x", "sub")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(victim, filepath.Join(f.fuse, "dest", "x", "sub")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.src, "in", "x", "sub", "payload.txt"), []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	body := jsonBody(t, map[string]any{"source": filepath.Join(f.src, "in"), "destination": "/jfs/dest", "direction": "in"})
	if w := f.do(t, http.MethodPost, "/api/migrate", body); w.Code != http.StatusAccepted {
		t.Fatalf("migrate in: %d %s", w.Code, w.Body.String())
	}
	waitJobsSettled(t, f.mgr)
	if _, err := os.Lstat(filepath.Join(victim, "payload.txt")); err == nil {
		t.Errorf("the sync wrote through the destination symlink into a folder outside the volume")
	}
	for _, j := range f.mgr.List() {
		if s := j.GetSnapshot(); s.State != JobError || !strings.Contains(s.Error, "/jfs/dest/x/sub") {
			t.Errorf("job state %s, error %q; want failed naming /jfs/dest/x/sub", s.State, s.Error)
		}
	}
}

// TestPostSyncPermissionsSurviveDestinationSwap: the post-sync mode and
// owner fixup runs after a sync that may take hours. A destination folder
// swapped for a symlink during the sync must not lead it outside the
// volume, and a normal destination must still get the client mode.
func TestPostSyncPermissionsSurviveDestinationSwap(t *testing.T) {
	f := newConfineFixture(t)
	outRoot := filepath.Join(f.outside, "root")
	outFile := filepath.Join(outRoot, "inner.txt")
	if err := os.Chmod(outFile, 0o600); err != nil {
		t.Fatal(err)
	}
	spec := RunSyncSpec{Mode: ModeEmbedded, FUSEMount: f.fuse, OwnerUID: os.Getuid(), OwnerGID: os.Getgid()}
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
		bin, _ := recordingJuicefs(t, "rm -rf '"+folder+"' && ln -s '"+outRoot+"' '"+folder+"'")
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
	bin, _ := recordingJuicefs(t, "")
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

// TestResolverIsLinearInDepth: 950 nested real folders. The old
// resolvers called EvalSymlinks once per missing component and took
// seconds; the forward walk must stay well under a second.
func TestResolverIsLinearInDepth(t *testing.T) {
	f := newConfineFixture(t)
	existing := strings.Repeat("/abc", 475)
	if err := os.MkdirAll(f.fuse+existing, 0o755); err != nil {
		t.Fatal(err)
	}
	deep := "/jfs" + existing + strings.Repeat("/xyz", 475)
	if len(f.fuse+existing+strings.Repeat("/xyz", 475)) > maxResolvePathInput {
		t.Fatalf("fixture path too long")
	}
	start := time.Now()
	vp, err := confineVolumePath(deep, "/jfs", f.fuse, true)
	if err != nil {
		t.Fatalf("confineVolumePath: %v", err)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Errorf("confineVolumePath took %v for 950 components, want well under 1s", d)
	}
	if !strings.HasSuffix(vp.Real, existing+strings.Repeat("/xyz", 475)) {
		t.Errorf("resolved %q", vp.Real)
	}
	start = time.Now()
	if w := f.do(t, "GET", "/api/browse-jfs?path="+q(deep), ""); w.Code == http.StatusForbidden {
		t.Errorf("browse-jfs of a deep in-volume path: 403")
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Errorf("browse-jfs took %v", d)
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
