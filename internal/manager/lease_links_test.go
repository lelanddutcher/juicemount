//go:build linux

package manager

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Leases follow projects (/data/<P>, projectDepth). A Manager sync checks
// out the projects it writes (checkoutForSync) and then must not write, or
// leave a link that resolves, into any other project. These tests run the
// real juicefs (JM_TEST_JUICEFS_BIN) like juicefs_beneath_test.go.

// leaseLayout is a syncLayout whose volume holds /data/projA and
// /data/projB (two projects).
func leaseLayout(t *testing.T) (syncLayout, string, string) {
	t.Helper()
	l := newSyncLayout(t)
	a := filepath.Join(l.fuse, "data", "projA")
	b := filepath.Join(l.fuse, "data", "projB")
	mustWrite(t, filepath.Join(a, "keep.txt"), "a")
	mustWrite(t, filepath.Join(b, "file.txt"), "b")
	return l, a, b
}

func (l syncLayout) runLeased(t *testing.T, bin string, spec RunSyncSpec, src, dst string) (*fakeLeaser, ProgressEvent, error) {
	t.Helper()
	fl := &fakeLeaser{}
	spec.Leases = fl
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ch := make(chan ProgressEvent, 1024)
	err := RunSync(ctx, bin, spec, src, dst, DefaultSyncOptions(), nil, ch)
	close(ch)
	var last ProgressEvent
	for ev := range ch {
		last = mergeProgress(last, ev)
	}
	return fl, last, err
}

// onlyLeased fails the test unless the sync checked out exactly projA.
func onlyLeased(t *testing.T, fl *fakeLeaser) {
	t.Helper()
	if strings.Join(fl.asked, ",") != "path:/data/projA" {
		t.Fatalf("checked out %v, want [path:/data/projA]", fl.asked)
	}
}

// projBUntouched fails the test if anything but file.txt is in projB.
func projBUntouched(t *testing.T, label, b string) {
	t.Helper()
	if got := tree(t, b); !sameList(got, []string{"file.txt"}) {
		t.Errorf("%s: projB (not leased) was written: %v", label, got)
	}
}

// Embedded, In above project depth: the destination is /jfs/data, the
// source holds projA/, so only projA is checked out. A relative link
// projA/link -> ../projB already in the volume must not carry the write
// into projB.
func TestLeaseInDoesNotWriteThroughLinkIntoOtherProject(t *testing.T) {
	bin := realJuicefsBin(t)
	for _, target := range []string{"../projB", "../projB/sub/.."} {
		t.Run(target, func(t *testing.T) {
			l, a, b := leaseLayout(t)
			mustLink(t, target, filepath.Join(a, "link"))
			src := filepath.Join(l.host, "in")
			mustWrite(t, filepath.Join(src, "projA/link/payload.txt"), "payload")
			mustWrite(t, filepath.Join(src, "projA/plain.txt"), "plain")
			fl, _, err := l.runLeased(t, bin, l.spec(), src, "/jfs/data")
			onlyLeased(t, fl)
			projBUntouched(t, "embedded in", b)
			if err == nil {
				t.Errorf("the sync reported success after a write through a link into another project")
			}
		})
	}
}

// Embedded, In into a project (and above project depth): a source link
// whose target leaves the destination project is not stored in the volume
// (it would resolve into projB later); it is skipped and reported. Links
// that stay in the project are still copied as links.
func TestLeaseInSkipsSourceLinksLeavingTheProject(t *testing.T) {
	bin := realJuicefsBin(t)
	for _, c := range []struct{ dst, prefix string }{{"/jfs/data/projA", ""}, {"/jfs/data", "projA/"}} {
		t.Run(c.dst, func(t *testing.T) {
			l, a, b := leaseLayout(t)
			src := filepath.Join(l.host, "in")
			mustWrite(t, filepath.Join(src, c.prefix+"sub/real.txt"), "r")
			mustLink(t, "../projB/file.txt", filepath.Join(src, c.prefix+"out-file"))
			mustLink(t, "../../projB", filepath.Join(src, c.prefix+"sub/out-dir"))
			mustLink(t, "sub/real.txt", filepath.Join(src, c.prefix+"in-file"))
			mustLink(t, "../sub", filepath.Join(src, c.prefix+"sub/self"))
			fl, report, err := l.runLeased(t, bin, l.spec(), src, c.dst)
			if err != nil {
				t.Fatalf("RunSync: %v", err)
			}
			onlyLeased(t, fl)
			for _, n := range []string{"out-file", "sub/out-dir"} {
				if _, err := os.Lstat(filepath.Join(a, n)); err == nil {
					got, _ := os.Readlink(filepath.Join(a, n))
					t.Errorf("link %s -> %s into projB was stored in projA", n, got)
				}
			}
			if got, err := os.Readlink(filepath.Join(a, "in-file")); err != nil || got != "sub/real.txt" {
				t.Errorf("same-project link: %q %v", got, err)
			}
			if got, err := os.Readlink(filepath.Join(a, "sub/self")); err != nil || got != "../sub" {
				t.Errorf("same-project link: %q %v", got, err)
			}
			if report.SkippedLinks != 2 {
				t.Errorf("skipped %d links %v, want 2", report.SkippedLinks, report.SkippedLinkSample)
			}
			projBUntouched(t, "embedded in", b)
		})
	}
}

// Embedded: a destination named inside projA but reached through a symlink
// into projB is refused before juicefs runs (the leases are projA's).
func TestLeaseDestinationThroughLinkIntoOtherProjectIsRefused(t *testing.T) {
	bin := realJuicefsBin(t)
	l, a, b := leaseLayout(t)
	mustLink(t, "../projB", filepath.Join(a, "escape"))
	src := filepath.Join(l.host, "in")
	mustWrite(t, filepath.Join(src, "payload.txt"), "payload")
	fl, _, err := l.runLeased(t, bin, l.spec(), src, "/jfs/data/projA/escape")
	onlyLeased(t, fl)
	if err == nil || !strings.Contains(err.Error(), "different project") {
		t.Errorf("RunSync: %v, want a refusal", err)
	}
	projBUntouched(t, "destination through a link", b)
	// A destination reached through a link inside its own project works.
	mustMkdir(t, filepath.Join(a, "sub"))
	mustLink(t, "sub", filepath.Join(a, "alias"))
	if _, _, err := l.runLeased(t, bin, l.spec(), src, "/jfs/data/projA/alias"); err != nil {
		t.Fatalf("RunSync into a same-project link: %v", err)
	}
	if _, err := os.Stat(filepath.Join(a, "sub/payload.txt")); err != nil {
		t.Errorf("same-project destination link: %v", err)
	}
}

// Embedded, Between (volume to volume): a link in another project's tree
// that resolves into projB is not copied into projA.
func TestLeaseBetweenSkipsLinksLeavingTheProject(t *testing.T) {
	bin := realJuicefsBin(t)
	l, a, b := leaseLayout(t)
	c := filepath.Join(l.fuse, "data", "projC")
	mustWrite(t, filepath.Join(c, "real.txt"), "c")
	mustLink(t, "../projB/file.txt", filepath.Join(c, "out"))
	mustLink(t, "real.txt", filepath.Join(c, "in"))
	fl, report, err := l.runLeased(t, bin, l.spec(), "/jfs/data/projC", "/jfs/data/projA")
	if err != nil {
		t.Fatalf("RunSync: %v", err)
	}
	onlyLeased(t, fl)
	if _, err := os.Lstat(filepath.Join(a, "out")); err == nil {
		t.Errorf("link into projB stored in projA")
	}
	if got, err := os.Readlink(filepath.Join(a, "in")); err != nil || got != "real.txt" {
		t.Errorf("same-project link: %q %v", got, err)
	}
	if report.SkippedLinks != 1 {
		t.Errorf("skipped %d links %v, want 1", report.SkippedLinks, report.SkippedLinkSample)
	}
	projBUntouched(t, "between", b)
}

// Standalone (jfs://): the same two cases through the volume's own
// namespace. Needs JM_TEST_JFS_META.
func TestLeaseStandaloneLinksStayInTheProject(t *testing.T) {
	bin := realJuicefsBin(t)
	meta := os.Getenv("JM_TEST_JFS_META")
	if meta == "" {
		t.Skip("JM_TEST_JFS_META not set; point it at a throwaway Redis database to run this test")
	}
	l := newSyncLayout(t)
	const vol = "jmlease"
	env := append(os.Environ(), vol+"="+meta)
	jfs := func(args ...string) (string, error) {
		cmd := exec.Command(bin, args...)
		cmd.Env = env
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	flushRedisDB(t, meta)
	if out, err := jfs("format", "--storage", "file", "--bucket", filepath.Join(l.base, "bucket")+"/", meta, vol); err != nil {
		t.Fatalf("format: %v\n%s", err, out)
	}
	seed := filepath.Join(l.base, "seed")
	mustWrite(t, filepath.Join(seed, "data/projA/keep.txt"), "a")
	mustWrite(t, filepath.Join(seed, "data/projB/file.txt"), "b")
	mustLink(t, "../projB", filepath.Join(seed, "data/projA/link"))
	mustWrite(t, filepath.Join(seed, "data/projA/sub/s.txt"), "s") // sync copies no empty folder
	mustLink(t, "sub", filepath.Join(seed, "data/projA/inner"))
	if out, err := jfs("sync", "--links", "file://"+seed+"/", "jfs://"+vol+"/"); err != nil {
		t.Fatalf("seed: %v\n%s", err, out)
	}
	spec := RunSyncSpec{Mode: ModeStandalone, MetaURL: meta, VolName: vol, SourceRoots: []string{l.host}}

	// Write through projA/link into projB.
	src := filepath.Join(l.host, "in")
	mustWrite(t, filepath.Join(src, "projA/link/payload.txt"), "payload")
	fl, _, err := l.runLeased(t, bin, spec, src, "/jfs/data")
	onlyLeased(t, fl)
	if err == nil {
		t.Errorf("standalone in: the sync reported success after a write through a link into another project")
	}

	// A source link leaving the project, copied into projA.
	src2 := filepath.Join(l.host, "in2")
	mustLink(t, "../projB/file.txt", filepath.Join(src2, "out-file"))
	mustLink(t, "keep.txt", filepath.Join(src2, "in-file"))
	fl, report, err := l.runLeased(t, bin, spec, src2, "/jfs/data/projA")
	if err != nil {
		t.Fatalf("standalone in2: %v", err)
	}
	onlyLeased(t, fl)
	if report.SkippedLinks != 1 {
		t.Errorf("standalone in2: skipped %d links %v, want 1", report.SkippedLinks, report.SkippedLinkSample)
	}

	// The destination named through projA/link (into projB).
	src3 := filepath.Join(l.host, "in3")
	mustWrite(t, filepath.Join(src3, "via-dest.txt"), "payload")
	if _, _, err := l.runLeased(t, bin, spec, src3, "/jfs/data/projA/link"); err == nil {
		t.Errorf("standalone: a destination through a link into projB was written")
	}

	// A write through a same-project link works.
	src4 := filepath.Join(l.host, "in4")
	mustWrite(t, filepath.Join(src4, "inner/same.txt"), "same")
	if _, _, err := l.runLeased(t, bin, spec, src4, "/jfs/data/projA"); err != nil {
		t.Fatalf("standalone: write through a same-project link: %v", err)
	}

	// Read the volume back through the backend and check projB and projA.
	back := filepath.Join(l.base, "back")
	if out, err := jfs("sync", "--links", "jfs://"+vol+"/data/", "file://"+back+"/"); err != nil {
		t.Fatalf("read back: %v\n%s", err, out)
	}
	projBUntouched(t, "standalone", filepath.Join(back, "projB"))
	if _, err := os.Lstat(filepath.Join(back, "projA/out-file")); err == nil {
		t.Errorf("standalone: link into projB stored in projA")
	}
	if got, err := os.Readlink(filepath.Join(back, "projA/in-file")); err != nil || got != "keep.txt" {
		t.Errorf("standalone: same-project link: %q %v", got, err)
	}
	if b, err := os.ReadFile(filepath.Join(back, "projA/sub/same.txt")); err != nil || string(b) != "same" {
		t.Errorf("standalone: write through a same-project link: %q %v", b, err)
	}
}
