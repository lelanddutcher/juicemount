//go:build linux

package manager

import (
	"bytes"
	"context"
	"log"
	"os"
	"path/filepath"
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
		"for n in 3 4; do readlink /proc/$$/fd/$n; done > '" + fdsFile + "' 2>/dev/null\nexit 0\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin, argsFile, fdsFile
}

const (
	jm2Version = "juicefs version 1.3.1+2025-12-02.e0032b2ae5e9-jm2"
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

// With a confining juicefs, local roots are passed as open folders and
// --links is used only when both ends are confined file:// or jfs://.
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
		{"out to a host root", embedded, "/jfs/proj", filepath.Join(l.host, "out"), true, filepath.Join(l.fuse, "proj"), filepath.Join(l.host, "out"), "file:///proc/self/fd/3/", "file:///proc/self/fd/4/"},
		{"in to the volume", embedded, filepath.Join(l.host, "in"), "/jfs/dest", true, filepath.Join(l.host, "in"), filepath.Join(l.fuse, "dest"), "file:///proc/self/fd/3/", "file:///proc/self/fd/4/"},
		{"out to sftp", embedded, "/jfs/proj", "sftp://u@h:22/backup/", false, filepath.Join(l.fuse, "proj"), "", "file:///proc/self/fd/3/", "sftp://u@h:22/backup/"},
		{"out to nfs", embedded, "/jfs/proj", "nfs://h/export/", false, filepath.Join(l.fuse, "proj"), "", "file:///proc/self/fd/3/", "nfs://h/export/"},
		{"out to s3", embedded, "/jfs/proj", "s3://bucket/x/", false, filepath.Join(l.fuse, "proj"), "", "file:///proc/self/fd/3/", "s3://bucket/x/"},
		{"standalone in to jfs://", standalone, filepath.Join(l.host, "in"), "/jfs/dest", true, filepath.Join(l.host, "in"), "", "file:///proc/self/fd/3/", "jfs://vol/dest/"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bin, argsFile, fdsFile := versionedJuicefs(t, jm2Version)
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
			if c.srcFd != "" && (len(fds) < 1 || fds[0] != c.srcFd) {
				t.Errorf("fd 3 -> %v, want %s", fds, c.srcFd)
			}
			if c.dstFd != "" && (len(fds) < 2 || fds[1] != c.dstFd) {
				t.Errorf("fd 4 -> %v, want %s", fds, c.dstFd)
			}
		})
	}
	// A host root outside every configured source root is refused.
	bin, _, _ := versionedJuicefs(t, jm2Version)
	if err := RunSync(context.Background(), bin, embedded, "/jfs/proj", l.outside, DefaultSyncOptions(), nil, nil); err == nil || !strings.Contains(err.Error(), "not under a configured source root") {
		t.Errorf("Out to a folder outside the source roots: %v", err)
	}
}

// An older juicefs (-jm1, or no suffix) keeps the previous behaviour:
// roots by path, no --links, and one warning.
func TestRunSyncOlderJuicefsKeepsPathRoots(t *testing.T) {
	l := newSyncLayout(t)
	mustWrite(t, filepath.Join(l.fuse, "proj/a.txt"), "a")
	var buf bytes.Buffer
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	for _, v := range []string{jm1Version, "juicefs version 1.3.1+2025-12-02.e0032b2ae5e9"} {
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

// preserve_structure=false copies the source folder itself, as
// <destination>/<basename>/; juicefs gets roots with a trailing slash.
func TestPreserveStructureOffNamesTheBasenameFolder(t *testing.T) {
	l := newSyncLayout(t)
	mustWrite(t, filepath.Join(l.fuse, "proj/a.txt"), "a")
	mustWrite(t, filepath.Join(l.host, "photos/b.txt"), "b")
	opts := DefaultSyncOptions()
	opts.PreserveStructure = false
	for _, c := range []struct{ src, dst, wantDst string }{
		{"/jfs/proj", l.host, "file://" + filepath.Join(l.host, "proj") + "/"},
		{"/jfs/proj", filepath.Join(l.host, "sub"), "file://" + filepath.Join(l.host, "sub", "proj") + "/"},
		{filepath.Join(l.host, "photos"), "/jfs", "file://" + filepath.Join(l.fuse, "photos") + "/"},
		{filepath.Join(l.host, "photos"), "/jfs/sub", "file://" + filepath.Join(l.fuse, "sub", "photos") + "/"},
	} {
		bin, argsFile, _ := versionedJuicefs(t, jm1Version) // paths by name, so the args show them
		if err := RunSync(context.Background(), bin, l.spec(), c.src, c.dst, opts, nil, nil); err != nil {
			t.Fatalf("RunSync(%s -> %s): %v", c.src, c.dst, err)
		}
		args := lines(t, argsFile)
		if got := args[len(args)-1]; got != c.wantDst {
			t.Errorf("%s -> %s: destination arg %q, want %q", c.src, c.dst, got, c.wantDst)
		}
		if got := args[len(args)-2]; !strings.HasSuffix(got, "/") {
			t.Errorf("%s -> %s: source arg %q has no trailing slash", c.src, c.dst, got)
		}
	}
}

// Real juicefs, preserve_structure=false, both directions, at a root and
// below one: the source folder lands as <destination>/<basename>/, and a
// same-prefix sibling of the source is not copied.
func TestJuicefsPreserveStructureOff(t *testing.T) {
	bin := realJuicefsBin(t)
	opts := DefaultSyncOptions()
	opts.PreserveStructure = false
	for _, c := range []struct {
		name, src, dst, want, sibling string
	}{
		{"out to a host root", "/jfs/proj", "", "host/proj/a.txt", "host/projX"},
		{"out below a host root", "/jfs/proj", "sub", "host/sub/proj/a.txt", "host/sub/projX"},
		{"in to the volume root", "photos", "/jfs", "fuse/photos/b.txt", "fuse/photosX"},
		{"in below the volume root", "photos", "/jfs/sub", "fuse/sub/photos/b.txt", "fuse/sub/photosX"},
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
				t.Errorf("source folder not copied as %s: %v", c.want, err)
			}
			if _, err := os.Stat(filepath.Join(l.base, c.sibling)); err == nil {
				t.Errorf("same-prefix sibling copied to %s", c.sibling)
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
