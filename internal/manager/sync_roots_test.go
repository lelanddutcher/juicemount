//go:build linux

package manager

import (
	"bytes"
	"context"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// versionedJuicefs writes a stand-in juicefs that answers --version with
// version and otherwise records its arguments, one per line, plus where
// fds 3 and 4 point (the sync roots the Manager opened).
func versionedJuicefs(t *testing.T, version string) (bin, argsFile, fdsFile string) {
	t.Helper()
	dir := t.TempDir()
	argsFile, fdsFile = filepath.Join(dir, "args"), filepath.Join(dir, "fds")
	bin = filepath.Join(dir, "juicefs")
	script := "#!/bin/sh\nif [ \"$1\" = --version ]; then echo '" + version + "'; exit 0; fi\n" +
		"printf '%s\\n' \"$@\" > '" + argsFile + "'\n" +
		"for n in 3 4; do readlink /proc/$$/fd/$n; done > '" + fdsFile + "' 2>/dev/null\n" +
		"env > '" + fdsFile + ".env'\nexit 0\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin, argsFile, fdsFile
}

const (
	jm3Version = "juicefs version 1.3.1+2025-12-02.e0032b2ae5e9-jm3"
	jm1Version = "juicefs version 1.3.1+2025-12-02.e0032b2ae5e9-jm1"
)

func lines(t *testing.T, p string) []string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read %s: %v", p, err)
	}
	return strings.Split(strings.TrimSpace(string(b)), "\n")
}

func contains(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

// With a confining juicefs, local roots are passed as open folders, with
// JM_FILE_ROOT_FD_ONLY=1, and --links is used only when the destination is
// the volume (In, Between). Out to a host root, sftp, nfs and s3 get no
// --links. In standalone mode a /jfs source is read as jfs://.
func TestRunSyncPassesOpenRootsAndLinksOnlyBetweenConfinedEnds(t *testing.T) {
	l := newSyncLayout(t)
	mustWrite(t, filepath.Join(l.fuse, "proj/a.txt"), "a")
	mustWrite(t, filepath.Join(l.host, "in/b.txt"), "b")
	embedded := l.spec()
	standalone := RunSyncSpec{Mode: ModeStandalone, MetaURL: "redis://127.0.0.1:1/1", VolName: "vol", SourceRoots: []string{l.host}}
	cases := []struct {
		name           string
		spec           RunSyncSpec
		src, dst       string
		links          bool
		srcFd, dstFd   string // expected folder behind the fd, "" = not an fd
		srcArg, dstArg string
	}{
		{"out to a host root", embedded, "/jfs/proj", filepath.Join(l.host, "out"), false, filepath.Join(l.fuse, "proj"), filepath.Join(l.host, "out"), "file:///proc/self/fd/3/", "file:///proc/self/fd/4/"},
		{"in to the volume", embedded, filepath.Join(l.host, "in"), "/jfs/dest", true, filepath.Join(l.host, "in"), filepath.Join(l.fuse, "dest"), "file:///proc/self/fd/3/", "file:///proc/self/fd/4/"},
		{"out to sftp", embedded, "/jfs/proj", "sftp://u@h:22/backup/", false, filepath.Join(l.fuse, "proj"), "", "file:///proc/self/fd/3/", "sftp://u@h:22/backup/"},
		{"out to nfs", embedded, "/jfs/proj", "nfs://h/export/", false, filepath.Join(l.fuse, "proj"), "", "file:///proc/self/fd/3/", "nfs://h/export/"},
		{"out to s3", embedded, "/jfs/proj", "s3://bucket/x/", false, filepath.Join(l.fuse, "proj"), "", "file:///proc/self/fd/3/", "s3://bucket/x/"},
		{"standalone in to jfs://", standalone, filepath.Join(l.host, "in"), "/jfs/dest", true, filepath.Join(l.host, "in"), "", "file:///proc/self/fd/3/", "jfs://vol/dest/"},
		{"standalone out from jfs://", standalone, "/jfs/proj", filepath.Join(l.host, "out2"), false, "", filepath.Join(l.host, "out2"), "jfs://vol/proj/", "file:///proc/self/fd/3/"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bin, argsFile, fdsFile := versionedJuicefs(t, jm3Version)
			if err := RunSync(context.Background(), bin, c.spec, c.src, c.dst, DefaultSyncOptions(), nil, nil); err != nil {
				t.Fatalf("RunSync: %v", err)
			}
			args := lines(t, argsFile)
			if got := contains(args, "--links"); got != c.links {
				t.Errorf("--links = %v, want %v; args %v", got, c.links, args)
			}
			if got := args[len(args)-2]; got != c.srcArg {
				t.Errorf("source arg %q, want %q", got, c.srcArg)
			}
			if got := args[len(args)-1]; got != c.dstArg {
				t.Errorf("destination arg %q, want %q", got, c.dstArg)
			}
			fds := lines(t, fdsFile)
			want := []string{}
			for _, f := range []string{c.srcFd, c.dstFd} {
				if f != "" {
					want = append(want, f)
				}
			}
			for i, w := range want {
				if len(fds) <= i || fds[i] != w {
					t.Errorf("fd %d -> %v, want %s", 3+i, fds, w)
				}
			}
			if env := strings.Join(lines(t, fdsFile+".env"), "\n"); !strings.Contains(env, "JM_FILE_ROOT_FD_ONLY=1") {
				t.Errorf("JM_FILE_ROOT_FD_ONLY=1 not set for juicefs")
			}
		})
	}
	// A host root outside every configured source root is refused.
	bin, _, _ := versionedJuicefs(t, jm3Version)
	if err := RunSync(context.Background(), bin, embedded, "/jfs/proj", l.outside, DefaultSyncOptions(), nil, nil); err == nil || !strings.Contains(err.Error(), "not under a configured source root") {
		t.Errorf("Out to a folder outside the source roots: %v", err)
	}
}

// An older juicefs (-jm1 or -jm2, or no suffix) keeps the previous
// behaviour: roots by path, no --links, and one warning. -jm2 confines
// file:// roots but has no project fence (patch 0017), so it is not used
// as a confining build.
func TestRunSyncOlderJuicefsKeepsPathRoots(t *testing.T) {
	l := newSyncLayout(t)
	mustWrite(t, filepath.Join(l.fuse, "proj/a.txt"), "a")
	var buf bytes.Buffer
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	for _, v := range []string{jm1Version, "juicefs version 1.3.1+2025-12-02.e0032b2ae5e9-jm2", "juicefs version 1.3.1+2025-12-02.e0032b2ae5e9"} {
		bin, argsFile, _ := versionedJuicefs(t, v)
		for i := 0; i < 2; i++ {
			if err := RunSync(context.Background(), bin, l.spec(), "/jfs/proj", filepath.Join(l.host, "out"), DefaultSyncOptions(), nil, nil); err != nil {
				t.Fatalf("RunSync: %v", err)
			}
		}
		args := lines(t, argsFile)
		if contains(args, "--links") {
			t.Errorf("%s: --links passed", v)
		}
		if got := args[len(args)-2]; got != "file://"+filepath.Join(l.fuse, "proj")+"/" {
			t.Errorf("%s: source arg %q", v, got)
		}
		if n := strings.Count(buf.String(), bin); n != 1 {
			t.Errorf("%s: warning logged %d times, want once", v, n)
		}
	}
}

// preserve_structure=false keeps its old meaning: the source's contents
// go into the destination itself. Both roots keep a trailing slash, so no
// same-prefix sibling of the source is included.
func TestPreserveStructureOffCopiesContentsIntoDestination(t *testing.T) {
	l := newSyncLayout(t)
	mustWrite(t, filepath.Join(l.fuse, "proj/a.txt"), "a")
	mustWrite(t, filepath.Join(l.host, "photos/b.txt"), "b")
	opts := DefaultSyncOptions()
	opts.PreserveStructure = false
	for _, c := range []struct{ src, dst, wantSrc, wantDst string }{
		{"/jfs/proj", l.host, "file://" + filepath.Join(l.fuse, "proj") + "/", "file://" + l.host + "/"},
		{"/jfs/proj", filepath.Join(l.host, "sub"), "file://" + filepath.Join(l.fuse, "proj") + "/", "file://" + filepath.Join(l.host, "sub") + "/"},
		{filepath.Join(l.host, "photos"), "/jfs", "file://" + filepath.Join(l.host, "photos") + "/", "file://" + l.fuse + "/"},
		{filepath.Join(l.host, "photos"), "/jfs/sub", "file://" + filepath.Join(l.host, "photos") + "/", "file://" + filepath.Join(l.fuse, "sub") + "/"},
	} {
		// Out: an older juicefs gets paths by name, so the args show them.
		// Into the volume only -jm3 runs; its roots are open folders, so
		// the args are where those folders are (fd 3 and 4).
		version := jm1Version
		if strings.HasPrefix(c.dst, "/jfs") {
			version = jm3Version
		}
		bin, argsFile, fdsFile := versionedJuicefs(t, version)
		if err := RunSync(context.Background(), bin, l.spec(), c.src, c.dst, opts, nil, nil); err != nil {
			t.Fatalf("RunSync(%s -> %s): %v", c.src, c.dst, err)
		}
		args := lines(t, argsFile)
		if version == jm3Version {
			args = nil
			for _, fd := range lines(t, fdsFile) {
				args = append(args, "file://"+fd+"/")
			}
		}
		if got := args[len(args)-2]; got != c.wantSrc {
			t.Errorf("%s -> %s: source arg %q, want %q", c.src, c.dst, got, c.wantSrc)
		}
		if got := args[len(args)-1]; got != c.wantDst {
			t.Errorf("%s -> %s: destination arg %q, want %q", c.src, c.dst, got, c.wantDst)
		}
	}
}

// Real juicefs, preserve_structure=false, both directions, at a root and
// below one: the source's contents land in the destination itself (the
// old meaning, so existing schedules keep writing where they did), and a
// same-prefix sibling of the source is not copied.
func TestJuicefsPreserveStructureOff(t *testing.T) {
	bin := realJuicefsBin(t)
	opts := DefaultSyncOptions()
	opts.PreserveStructure = false
	for _, c := range []struct {
		name, src, dst string
		want           string // the source's file, in the destination itself
		basename       string // where a basename folder would be
		sibling        string // where the old slash-less form put projX/photosX
	}{
		{"out to a host root", "/jfs/proj", "", "host/a.txt", "host/proj", "hostX"},
		{"out below a host root", "/jfs/proj", "sub", "host/sub/a.txt", "host/sub/proj", "host/subX"},
		{"in to the volume root", "photos", "/jfs", "fuse/b.txt", "fuse/photos", "fuseX"},
		{"in below the volume root", "photos", "/jfs/sub", "fuse/sub/b.txt", "fuse/sub/photos", "fuse/subX"},
	} {
		t.Run(c.name, func(t *testing.T) {
			l := newSyncLayout(t)
			mustWrite(t, filepath.Join(l.fuse, "proj/a.txt"), "a")
			mustWrite(t, filepath.Join(l.fuse, "projX/x.txt"), "x")
			mustWrite(t, filepath.Join(l.host, "photos/b.txt"), "b")
			mustWrite(t, filepath.Join(l.host, "photosX/x.txt"), "x")
			src, dst := c.src, c.dst
			if !strings.HasPrefix(src, "/jfs") {
				src = filepath.Join(l.host, src)
			}
			if !strings.HasPrefix(dst, "/jfs") {
				dst = filepath.Join(l.host, dst)
			}
			if err := l.run(t, bin, src, dst, opts); err != nil {
				t.Fatalf("RunSync: %v", err)
			}
			if _, err := os.Stat(filepath.Join(l.base, c.want)); err != nil {
				t.Errorf("source contents not copied into the destination (%s): %v", c.want, err)
			}
			if _, err := os.Stat(filepath.Join(l.base, c.basename)); err == nil {
				t.Errorf("a basename folder %s was created", c.basename)
			}
			if _, err := os.Stat(filepath.Join(l.base, c.sibling)); err == nil {
				t.Errorf("a same-prefix sibling was copied to %s", c.sibling)
			}
			if _, err := os.Stat(filepath.Join(l.base, filepath.Dir(c.want), "x.txt")); err == nil {
				t.Errorf("a same-prefix sibling's contents landed in the destination")
			}
		})
	}
}

// Review probe (item 1): with preserve_structure=false, juicefs got roots
// without a trailing slash and confined to their parent folder (at a
// source root, the folder above it). A relative link in the host root,
// esc -> ../victim, then led an Out job's writes into victim/.
func TestJuicefsPreserveStructureOffStaysInRoot(t *testing.T) {
	bin := realJuicefsBin(t)
	l := newSyncLayout(t)
	mustLink(t, "../victim", filepath.Join(l.host, "esc"))
	mustLink(t, "../../victim", filepath.Join(l.host, "proj/esc"))
	mustWrite(t, filepath.Join(l.fuse, "proj/esc/payload.txt"), "payload")
	opts := DefaultSyncOptions()
	opts.PreserveStructure = false
	_ = l.run(t, bin, "/jfs/proj", l.host, opts)
	emptyDir(t, "preserve_structure=false", l.victim)
}

// Review probe (item 2): a Mac can swap any folder below the FUSE mount.
// A destination flipping between an in-volume folder and a host folder
// while syncs run must never lead a write to the host folder, and a
// flipping source must never be read from the host folder.
func TestJuicefsRootSwapRace(t *testing.T) {
	bin := realJuicefsBin(t)
	l := newSyncLayout(t)
	mustMkdir(t, filepath.Join(l.fuse, "real"))
	mustWrite(t, filepath.Join(l.host, "in/payload.txt"), "payload")
	mustWrite(t, filepath.Join(l.fuse, "realsrc/ok.txt"), "ok")
	flip := func(link, inside string, stop *atomic.Bool) {
		tmp := link + ".tmp"
		for i := 0; !stop.Load(); i++ {
			target := inside
			if i%2 == 1 {
				target = l.victim
				if strings.HasSuffix(link, "src") {
					target = l.outside
				}
			}
			_ = os.Remove(tmp)
			if os.Symlink(target, tmp) == nil {
				_ = os.Rename(tmp, link)
			}
		}
	}
	var stop atomic.Bool
	done := make(chan struct{}, 2)
	go func() { flip(filepath.Join(l.fuse, "dest"), "real", &stop); done <- struct{}{} }()
	go func() { flip(filepath.Join(l.fuse, "src"), "realsrc", &stop); done <- struct{}{} }()
	deadline := time.Now().Add(40 * time.Second)
	for i := 0; i < 25 && time.Now().Before(deadline); i++ {
		_ = l.run(t, bin, filepath.Join(l.host, "in"), "/jfs/dest", DefaultSyncOptions())
		_ = l.run(t, bin, "/jfs/src", filepath.Join(l.host, "out"), DefaultSyncOptions())
	}
	stop.Store(true)
	<-done
	<-done
	emptyDir(t, "destination swap", l.victim)
	noRegularFile(t, "source swap", filepath.Join(l.host, "out"), "secret.txt")
}

// probeFake writes a stand-in juicefs whose first --version answer runs
// first (a shell snippet); later answers print the -jm3 line. Every
// --version call is recorded in calls; a sync records its arguments.
func probeFake(t *testing.T, first string) (bin, argsFile, calls string) {
	t.Helper()
	dir := t.TempDir()
	bin, argsFile, calls = filepath.Join(dir, "juicefs"), filepath.Join(dir, "args"), filepath.Join(dir, "calls")
	count := filepath.Join(dir, "count")
	script := "#!/bin/sh\nif [ \"$1\" = --version ]; then\n  echo x >> '" + calls + "'\n" +
		"  if [ ! -e '" + count + "' ]; then touch '" + count + "'; " + first + "; fi\n" +
		"  echo '" + jm3Version + "'; exit 0\nfi\n" +
		"printf '%s\\n' \"$@\" > '" + argsFile + "'\nexit 0\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin, argsFile, calls
}

func shortProbeTimeout(t *testing.T) {
	prev := juicefsVersionTimeout
	juicefsVersionTimeout = 500 * time.Millisecond
	t.Cleanup(func() { juicefsVersionTimeout = prev })
}

// A failed, slow or noisy `juicefs --version` is not cached as "not
// confined": the sync fails with a message that says what was found, and
// the next sync asks again. A -jm3 line followed by any other output is
// unknown too (it used to parse as stock and run unconfined).
func TestJuicefsVersionProbeRetriesAfterFailure(t *testing.T) {
	shortProbeTimeout(t)
	l := newSyncLayout(t)
	mustWrite(t, filepath.Join(l.fuse, "proj/a.txt"), "a")
	for _, c := range []struct{ first, want string }{
		{"exit 3", "failed (exit status 3)"},
		{"sleep 3", "did not answer within"},
		{"echo garbage; exit 0", `printed "garbage"`},
		{"echo '" + jm3Version + "'; echo 'note: wrapper'; exit 0", "not a single"},
	} {
		t.Run(c.first, func(t *testing.T) {
			bin, argsFile, _ := probeFake(t, c.first)
			err := RunSync(context.Background(), bin, l.spec(), "/jfs/proj", filepath.Join(l.host, "out"), DefaultSyncOptions(), nil, nil)
			if err == nil || !strings.Contains(err.Error(), "sync not started") || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("first sync: %v, want a refusal containing %q", err, c.want)
			}
			if _, err := os.Stat(argsFile); err == nil {
				t.Fatalf("juicefs sync ran while its version was unknown")
			}
			if err := RunSync(context.Background(), bin, l.spec(), "/jfs/proj", filepath.Join(l.host, "out"), DefaultSyncOptions(), nil, nil); err != nil {
				t.Fatalf("second sync: %v", err)
			}
			if args := lines(t, argsFile); args[len(args)-2] != "file:///proc/self/fd/3/" {
				t.Errorf("second sync did not use the confined mode: %v", args)
			}
		})
	}
	// A binary that cannot run at all names the setting to fix.
	err := RunSync(context.Background(), filepath.Join(t.TempDir(), "missing-juicefs"), l.spec(), "/jfs/proj", filepath.Join(l.host, "out"), DefaultSyncOptions(), nil, nil)
	if err == nil || !strings.Contains(err.Error(), "cannot run") || !strings.Contains(err.Error(), "--juicefs") {
		t.Errorf("missing binary: %v", err)
	}
}

// A probe whose grandchild keeps stdout open still returns within the
// timeout plus the 1 s WaitDelay (bounded here at timeout + 2 s).
func TestJuicefsVersionProbeBoundedWhenStdoutHeld(t *testing.T) {
	shortProbeTimeout(t)
	l := newSyncLayout(t)
	mustWrite(t, filepath.Join(l.fuse, "proj/a.txt"), "a")
	bin, _, _ := probeFake(t, "sh -c 'sleep 30' & sleep 30")
	start := time.Now()
	err := RunSync(context.Background(), bin, l.spec(), "/jfs/proj", filepath.Join(l.host, "out"), DefaultSyncOptions(), nil, nil)
	if d := time.Since(start); d > juicefsVersionTimeout+2*time.Second {
		t.Errorf("probe with a grandchild holding stdout took %v, want under %v", d, juicefsVersionTimeout+2*time.Second)
	}
	if err == nil || !strings.Contains(err.Error(), "did not answer within") {
		t.Errorf("held stdout: %v", err)
	}
}

// Concurrent syncs share one running probe instead of each waiting their
// turn behind a lock.
func TestJuicefsVersionProbeSingleFlight(t *testing.T) {
	l := newSyncLayout(t)
	mustWrite(t, filepath.Join(l.fuse, "proj/a.txt"), "a")
	bin, _, calls := probeFake(t, "sleep 1")
	start := time.Now()
	errs := make(chan error, 6)
	for i := 0; i < 6; i++ {
		go func(i int) {
			errs <- RunSync(context.Background(), bin, l.spec(), "/jfs/proj", filepath.Join(l.host, "out", strconv.Itoa(i)), DefaultSyncOptions(), nil, nil)
		}(i)
	}
	for i := 0; i < 6; i++ {
		if err := <-errs; err != nil {
			t.Errorf("sync: %v", err)
		}
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Errorf("6 concurrent syncs took %v; the probe should run once", d)
	}
	if n := len(lines(t, calls)); n != 1 {
		t.Errorf("--version ran %d times, want 1", n)
	}
}

// With JM_FILE_ROOT_FD_ONLY=1 the patched juicefs sync refuses a file://
// root named by path.
func TestJuicefsRefusesPathRootWhenFdOnly(t *testing.T) {
	bin := realJuicefsBin(t)
	l := newSyncLayout(t)
	mustWrite(t, filepath.Join(l.host, "in/a.txt"), "a")
	cmd := exec.Command(bin, "sync", "file://"+filepath.Join(l.host, "in")+"/", "file://"+filepath.Join(l.host, "out")+"/")
	cmd.Env = append(os.Environ(), "JM_FILE_ROOT_FD_ONLY=1")
	out, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "must be an open folder") {
		t.Errorf("path root with JM_FILE_ROOT_FD_ONLY=1: err %v, output %s", err, out)
	}
	if _, err := os.Stat(filepath.Join(l.host, "out")); err == nil {
		t.Errorf("the refused sync created its destination")
	}
}

// A dry run changes nothing on disk: a missing destination is not
// created, and an existing one is not touched (no post-sync fixup).
func TestJuicefsDryRunCreatesNothing(t *testing.T) {
	bin := realJuicefsBin(t)
	l := newSyncLayout(t)
	mustWrite(t, filepath.Join(l.host, "in/sub/a.txt"), "a")
	opts := DefaultSyncOptions()
	opts.DryRun = true
	spec := l.spec()
	spec.OwnerUID, spec.OwnerGID = os.Getuid(), os.Getgid()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := RunSync(ctx, bin, spec, filepath.Join(l.host, "in"), "/jfs/new/deeper", opts, nil, nil); err != nil {
		t.Fatalf("dry run into a missing destination: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(l.fuse, "new")); err == nil {
		t.Errorf("a dry run created the destination")
	}
	existing := filepath.Join(l.fuse, "exists")
	if err := os.MkdirAll(existing, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := RunSync(ctx, bin, spec, filepath.Join(l.host, "in"), "/jfs/exists", opts, nil, nil); err != nil {
		t.Fatalf("dry run into an existing destination: %v", err)
	}
	entries, _ := os.ReadDir(existing)
	fi, _ := os.Stat(existing)
	if len(entries) != 0 || fi.Mode().Perm() != 0o700 {
		t.Errorf("a dry run changed the destination: %d entries, mode %v", len(entries), fi.Mode().Perm())
	}
}

// The Manager's minimum stays -jm3: later builds (-jm4 added ranged first
// reads, patch 0018; the shipped -jm5 batches Redis WATCH round trips, patch
// 0019) are accepted, older ones are not.
func TestVersionConfinesLaterBuilds(t *testing.T) {
	base := "juicefs version 1.3.1+2025-12-02.e0032b2ae5e9"
	for v, want := range map[string]bool{
		base + "-jm3": true, base + "-jm4": true, base + "-jm5": true, base + "-jm12": true,
		base + "-jm2": false, base + "-jm1": false, base: false,
	} {
		if got := versionConfines(v); got != want {
			t.Errorf("versionConfines(%q) = %v, want %v", v, got, want)
		}
	}
}
