//go:build linux

package manager

import (
	"bufio"
	"context"
	"fmt"
	"io/fs"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// These tests run the real juicefs built from third_party/juicefs (set
// JM_TEST_JUICEFS_BIN; CI builds it). They pin that a sync cannot read or
// write outside its roots through a symlink in the tree: juicefs's file://
// backend resolves every path beneath its root (patch 0016), the Manager
// hands it the roots as open folders, and RunSync passes --links only
// into the volume.
//
// The volume is a plain folder standing in for the FUSE mount (embedded
// mode, file:///<FUSEMount>/...). The jfs:// variants (standalone mode)
// also need JM_TEST_JFS_META, a Redis URL for a throwaway volume.

func realJuicefsBin(t *testing.T) string {
	t.Helper()
	bin := os.Getenv("JM_TEST_JUICEFS_BIN")
	if bin == "" {
		t.Skip("JM_TEST_JUICEFS_BIN not set; build juicefs from third_party/juicefs to run this test")
	}
	return bin
}

// syncLayout: base/fuse (the volume), base/host (a permitted host root),
// base/outside/secret.txt and base/victim/ (both outside every root).
type syncLayout struct{ base, fuse, host, outside, victim string }

func newSyncLayout(t *testing.T) syncLayout {
	t.Helper()
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	l := syncLayout{base, filepath.Join(base, "fuse"), filepath.Join(base, "host"), filepath.Join(base, "outside"), filepath.Join(base, "victim")}
	for _, d := range []string{l.fuse, l.host, l.outside, l.victim} {
		mustMkdir(t, d)
	}
	mustWrite(t, filepath.Join(l.outside, "secret.txt"), "secret")
	return l
}

func mustMkdir(t *testing.T, p string) {
	t.Helper()
	if err := os.MkdirAll(p, 0o755); err != nil {
		t.Fatal(err)
	}
}

func mustWrite(t *testing.T, p, body string) {
	t.Helper()
	mustMkdir(t, filepath.Dir(p))
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func mustLink(t *testing.T, target, link string) {
	t.Helper()
	mustMkdir(t, filepath.Dir(link))
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
}

// emptyDir fails the test if dir holds anything.
func emptyDir(t *testing.T, label, dir string) {
	t.Helper()
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("%s: %s outside every root was written: %v", label, dir, names)
	}
}

// noRegularFile fails the test if a regular file named name exists under
// root (symlinks are not followed).
func noRegularFile(t *testing.T, label, root, name string) {
	t.Helper()
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err == nil && d.Type().IsRegular() && d.Name() == name {
			t.Errorf("%s: %s was copied from outside the source root", label, p)
		}
		return nil
	})
}

func (l syncLayout) spec() RunSyncSpec {
	return RunSyncSpec{Mode: ModeEmbedded, FUSEMount: l.fuse, SourceRoots: []string{l.host}}
}

func (l syncLayout) run(t *testing.T, bin, src, dst string, opts SyncOptions) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	return RunSync(ctx, bin, l.spec(), src, dst, opts, nil, nil)
}

// runReport runs RunSync like run and also returns the merged progress
// (the job's view: copied counts and skipped symlinks).
func (l syncLayout) runReport(t *testing.T, bin, src, dst string, opts SyncOptions) (ProgressEvent, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ch := make(chan ProgressEvent, 1024)
	err := RunSync(ctx, bin, l.spec(), src, dst, opts, nil, ch)
	close(ch)
	var last ProgressEvent
	for ev := range ch {
		last = mergeProgress(last, ev)
	}
	return last, err
}

// tree lists everything under root, relative, sorted: folders end in
// "/", symlinks in "@" (not followed).
func tree(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || p == root {
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		switch {
		case d.Type()&fs.ModeSymlink != 0:
			rel += "@"
		case d.IsDir():
			rel += "/"
		}
		out = append(out, rel)
		return nil
	})
	sort.Strings(out)
	return out
}

func sameList(got, want []string) bool {
	sort.Strings(want)
	return strings.Join(got, "|") == strings.Join(want, "|")
}

// Single hop, Out: a symlink deep in a volume tree that points outside the
// volume is neither followed nor copied (Out runs without --links, and the
// confined source root skips a link it will not follow, with a report). A
// relative link that stays inside the source is copied as a file. The job
// succeeds.
func TestJuicefsOutSingleHopNeitherFollowsNorCopiesLink(t *testing.T) {
	bin := realJuicefsBin(t)
	l := newSyncLayout(t)
	mustWrite(t, filepath.Join(l.fuse, "proj/deep/a/b/real.txt"), "in volume")
	mustLink(t, l.outside, filepath.Join(l.fuse, "proj/deep/a/b/evil"))
	mustLink(t, "real.txt", filepath.Join(l.fuse, "proj/deep/a/b/alias.txt"))
	out := filepath.Join(l.host, "out")
	report, err := l.runReport(t, bin, "/jfs/proj", out, DefaultSyncOptions())
	if err != nil {
		t.Fatalf("RunSync: %v", err)
	}
	want := []string{"deep/", "deep/a/", "deep/a/b/", "deep/a/b/alias.txt", "deep/a/b/real.txt"}
	if got := tree(t, out); !sameList(got, want) {
		t.Errorf("copied %v, want %v", got, want)
	}
	if report.SkippedLinks != 1 || len(report.SkippedLinkSample) != 1 || report.SkippedLinkSample[0] != "/jfs/proj/deep/a/b/evil" {
		t.Errorf("skipped report: %d %v, want 1 [/jfs/proj/deep/a/b/evil]", report.SkippedLinks, report.SkippedLinkSample)
	}
}

// Single hop and chains, In: a volume destination holding a symlink, or a
// chain of symlinks, that ends outside the volume must not be written
// through.
func TestJuicefsInDoesNotWriteThroughDestinationLinks(t *testing.T) {
	bin := realJuicefsBin(t)
	cases := []struct {
		name  string
		setup func(l syncLayout, dest string)
		key   string
	}{
		{"single hop", func(l syncLayout, dest string) {
			mustLink(t, l.victim, filepath.Join(dest, "sub"))
		}, "sub/payload.txt"},
		{"chain, absolute second hop", func(l syncLayout, dest string) {
			mustLink(t, "other", filepath.Join(dest, "x"))
			mustLink(t, l.victim, filepath.Join(dest, "other/y"))
		}, "x/y/payload.txt"},
		{"chain, relative second hop", func(l syncLayout, dest string) {
			mustLink(t, "other", filepath.Join(dest, "x"))
			mustLink(t, "../../../victim", filepath.Join(dest, "other/z"))
		}, "x/z/payload.txt"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			l := newSyncLayout(t)
			dest := filepath.Join(l.fuse, "dest")
			mustMkdir(t, dest)
			c.setup(l, dest)
			src := filepath.Join(l.host, "in")
			mustWrite(t, filepath.Join(src, c.key), "payload")
			err := l.run(t, bin, src, "/jfs/dest", DefaultSyncOptions())
			emptyDir(t, c.name, l.victim)
			if err == nil {
				t.Errorf("%s: the sync reported success", c.name)
			}
		})
	}
}

// Probe A, the host side: an Out job into a host root that holds a chain
// of symlinks ending outside it must not write through the chain.
func TestJuicefsOutDoesNotWriteThroughHostRootChain(t *testing.T) {
	bin := realJuicefsBin(t)
	l := newSyncLayout(t)
	dest := filepath.Join(l.host, "outdest")
	mustLink(t, "other", filepath.Join(dest, "x"))
	mustLink(t, l.victim, filepath.Join(dest, "other/y"))
	mustWrite(t, filepath.Join(l.fuse, "proj/x/y/payload.txt"), "payload")
	err := l.run(t, bin, "/jfs/proj", dest, DefaultSyncOptions())
	emptyDir(t, "out into a host root chain", l.victim)
	if err == nil {
		t.Errorf("the sync reported success")
	}
}

// Probe B: a destination that cannot store links (mem://, like s3, b2 and
// webdav) makes juicefs drop --links and follow source links. A chain in
// the volume source that ends outside the volume must not be read.
func TestJuicefsLinklessDestinationDoesNotReadThroughSourceChain(t *testing.T) {
	bin := realJuicefsBin(t)
	l := newSyncLayout(t)
	src := filepath.Join(l.fuse, "srcb")
	mustWrite(t, filepath.Join(src, "plain.txt"), "plain")
	mustLink(t, "other", filepath.Join(src, "x"))
	mustLink(t, l.outside, filepath.Join(src, "other/y"))
	// The chain's second hop is reached twice (as x/y and other/y); both
	// are skipped and reported, and the job succeeds.
	report, err := l.runReport(t, bin, "/jfs/srcb", "mem://bucket/b/", DefaultSyncOptions())
	if err != nil {
		t.Fatalf("sync to a link-less destination: %v", err)
	}
	if report.SkippedLinks != 2 {
		t.Errorf("skipped %d links, want 2: %v", report.SkippedLinks, report.SkippedLinkSample)
	}
	// The same listing path into a folder: juicefs with no --links, run
	// directly; it exits 0 and only the plain file arrives.
	out := filepath.Join(l.host, "nolinks")
	cmd := exec.Command(bin, "sync", "file://"+src+"/", "file://"+out+"/")
	if outb, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("juicefs sync without --links: %v\n%s", err, outb)
	}
	if got := tree(t, out); !sameList(got, []string{"plain.txt"}) {
		t.Errorf("copied %v, want [plain.txt]", got)
	}
}

// Probe C: job 1 would copy a link into a host root, job 2 then writes a
// tree whose folder has the link's name. Out runs without --links, so job
// 1 stores no link (it skips it, and succeeds), and job 2 writes a real
// folder.
func TestJuicefsCopiedLinkIsNotWrittenThrough(t *testing.T) {
	bin := realJuicefsBin(t)
	l := newSyncLayout(t)
	root := filepath.Join(l.host, "croot")
	mustLink(t, l.victim, filepath.Join(l.fuse, "c1/d"))
	mustWrite(t, filepath.Join(l.fuse, "c1/keep.txt"), "keep")
	mustWrite(t, filepath.Join(l.fuse, "c2/d/payload.txt"), "payload")
	report, err := l.runReport(t, bin, "/jfs/c1", root, DefaultSyncOptions())
	if err != nil {
		t.Fatalf("job 1: %v", err)
	}
	if got := tree(t, root); !sameList(got, []string{"keep.txt"}) || report.SkippedLinks != 1 {
		t.Errorf("job 1 copied %v, skipped %d; want [keep.txt], 1", got, report.SkippedLinks)
	}
	if err := l.run(t, bin, "/jfs/c2", root, DefaultSyncOptions()); err != nil {
		t.Fatalf("job 2: %v", err)
	}
	if got := tree(t, root); !sameList(got, []string{"d/", "d/payload.txt", "keep.txt"}) {
		t.Errorf("after job 2: %v", got)
	}
	emptyDir(t, "job 2", l.victim)
}

// Probe D: a destination folder swapped for a symlink while juicefs is
// copying. Every later write must still stay beneath the root.
func TestJuicefsSwapDuringCopyIsRefused(t *testing.T) {
	bin := realJuicefsBin(t)
	l := newSyncLayout(t)
	src := filepath.Join(l.host, "dsrc")
	mustWrite(t, filepath.Join(src, "a/1.bin"), strings.Repeat("x", 512*1024))
	mustWrite(t, filepath.Join(src, "a/2.bin"), "second")
	mustWrite(t, filepath.Join(src, "a/3.bin"), "third")
	dest := filepath.Join(l.fuse, "ddest")
	opts := DefaultSyncOptions()
	opts.Threads = 1
	opts.BWLimit = 1 // Mbps: 1.bin takes about 4 s
	done := make(chan error, 1)
	go func() { done <- l.run(t, bin, src, "/jfs/ddest", opts) }()
	deadline := time.Now().Add(60 * time.Second)
	for {
		if fi, err := os.Lstat(filepath.Join(dest, "a")); err == nil && fi.IsDir() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("juicefs never created the destination folder")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := os.Rename(filepath.Join(dest, "a"), filepath.Join(dest, "a.old")); err != nil {
		t.Fatal(err)
	}
	mustLink(t, l.victim, filepath.Join(dest, "a"))
	err := <-done
	emptyDir(t, "swap", l.victim)
	if err == nil {
		t.Errorf("the sync reported success after the swap")
	}
}

// In-volume links that stay beneath the root keep working: a relative
// link in the destination is written through, and one in an In source is
// copied as a link (--links into the volume).
func TestJuicefsLinksInsideTheRootKeepWorking(t *testing.T) {
	bin := realJuicefsBin(t)
	l := newSyncLayout(t)
	dest := filepath.Join(l.fuse, "dest")
	mustMkdir(t, filepath.Join(dest, "real"))
	mustLink(t, "real", filepath.Join(dest, "alias"))
	src := filepath.Join(l.host, "in")
	mustWrite(t, filepath.Join(src, "alias/payload.txt"), "payload")
	mustLink(t, "alias/payload.txt", filepath.Join(src, "rel-link"))
	if err := l.run(t, bin, src, "/jfs/dest", DefaultSyncOptions()); err != nil {
		t.Fatalf("RunSync: %v", err)
	}
	if b, err := os.ReadFile(filepath.Join(dest, "real/payload.txt")); err != nil || string(b) != "payload" {
		t.Errorf("write through an in-root link: %q %v", b, err)
	}
	if got, err := os.Readlink(filepath.Join(dest, "rel-link")); err != nil || got != "alias/payload.txt" {
		t.Errorf("source link not copied as a link: %q %v", got, err)
	}
}

// Standalone mode: volume paths reach juicefs as jfs://<vol>/..., and the
// jfs backend resolves symlinks inside the volume's own namespace. An
// absolute symlink stored in the volume names a path inside the volume,
// not on the host. Needs JM_TEST_JFS_META: a Redis URL for a throwaway
// database (the test empties it, then formats a volume there).
func TestJuicefsStandaloneAbsoluteLinksStayInVolume(t *testing.T) {
	bin := realJuicefsBin(t)
	meta := os.Getenv("JM_TEST_JFS_META")
	if meta == "" {
		t.Skip("JM_TEST_JFS_META not set; point it at a throwaway Redis database to run this test")
	}
	l := newSyncLayout(t)
	const vol = "jmbeneath"
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
	// Seed the volume with absolute links to host folders outside it.
	seed := filepath.Join(l.base, "seed")
	mustWrite(t, filepath.Join(seed, "proj/real.txt"), "in volume")
	mustLink(t, l.outside, filepath.Join(seed, "proj/evil"))     // absolute: never followed (ENOTSUP)
	mustLink(t, "missing.txt", filepath.Join(seed, "proj/gone")) // relative, no target (ENOENT)
	mustLink(t, l.victim, filepath.Join(seed, "dest/sub"))
	// A loop (ELOOP when followed): not a missing target, so it is not
	// skipped; it must still fail the sync.
	mustWrite(t, filepath.Join(seed, "proj2/real.txt"), "in volume")
	mustLink(t, "lb", filepath.Join(seed, "proj2/la"))
	mustLink(t, "la", filepath.Join(seed, "proj2/lb"))
	if out, err := jfs("sync", "--links", "file://"+seed+"/", "jfs://"+vol+"/"); err != nil {
		t.Fatalf("seed: %v\n%s", err, out)
	}
	// Out without --links: juicefs follows evil inside the volume, where
	// the host path does not exist; nothing from the host is copied.
	out := filepath.Join(l.host, "out")
	_, _ = jfs("sync", "jfs://"+vol+"/proj/", "file://"+out+"/")
	noRegularFile(t, "standalone out", out, "secret.txt")
	if _, err := os.Stat(filepath.Join(out, "real.txt")); err != nil {
		t.Errorf("standalone out: the in-volume file was not copied: %v", err)
	}
	// In through RunSync (standalone): writes under dest/sub stay in the
	// volume.
	src := filepath.Join(l.host, "in")
	mustWrite(t, filepath.Join(src, "sub/payload.txt"), "payload")
	spec := RunSyncSpec{Mode: ModeStandalone, MetaURL: meta, VolName: vol, SourceRoots: []string{l.host}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	_ = RunSync(ctx, bin, spec, src, "/jfs/dest", DefaultSyncOptions(), nil, nil)
	emptyDir(t, "standalone in", l.victim)
	// Out through RunSync (standalone): the /jfs source is read as jfs://,
	// not as a host path.
	out2 := filepath.Join(l.host, "out2")
	// evil is absolute (the jfs:// backend never follows it) and gone has
	// no target: both are skipped and reported, and the job succeeds.
	ch := make(chan ProgressEvent, 1024)
	err := RunSync(ctx, bin, spec, "/jfs/proj", out2, DefaultSyncOptions(), nil, ch)
	close(ch)
	var report ProgressEvent
	for ev := range ch {
		report = mergeProgress(report, ev)
	}
	if err != nil {
		t.Errorf("standalone out: %v", err)
	}
	if got := tree(t, out2); !sameList(got, []string{"real.txt"}) {
		t.Errorf("standalone out copied %v, want [real.txt]", got)
	}
	sort.Strings(report.SkippedLinkSample)
	if report.SkippedLinks != 2 || strings.Join(report.SkippedLinkSample, " ") != "/jfs/proj/evil /jfs/proj/gone" {
		t.Errorf("standalone out: skipped %d links %v, want 2 [/jfs/proj/evil /jfs/proj/gone]", report.SkippedLinks, report.SkippedLinkSample)
	}
	// Only a missing target is skipped. A link that fails any other way
	// (here ELOOP) is not dropped silently: the sync fails.
	ch = make(chan ProgressEvent, 1024)
	err = RunSync(ctx, bin, spec, "/jfs/proj2", filepath.Join(l.host, "out3"), DefaultSyncOptions(), nil, ch)
	close(ch)
	report = ProgressEvent{}
	for ev := range ch {
		report = mergeProgress(report, ev)
	}
	if err == nil {
		t.Errorf("standalone out of a tree with a link loop: the sync succeeded; an ELOOP link must fail it")
	}
	if report.SkippedLinks != 0 {
		t.Errorf("standalone out of a link loop: %d links skipped %v, want none", report.SkippedLinks, report.SkippedLinkSample)
	}
}

// flushRedisDB empties the Redis database named by a redis://host:port/db
// URL (no password), so each run formats a fresh volume.
func flushRedisDB(t *testing.T, meta string) {
	t.Helper()
	u, err := url.Parse(meta)
	if err != nil || u.Scheme != "redis" {
		t.Fatalf("JM_TEST_JFS_META must be redis://host:port/db: %q", meta)
	}
	db := strings.TrimPrefix(u.Path, "/")
	if db == "" {
		db = "0"
	}
	conn, err := net.DialTimeout("tcp", u.Host, 5*time.Second)
	if err != nil {
		t.Fatalf("redis: %v", err)
	}
	defer conn.Close()
	cmd := fmt.Sprintf("*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n*1\r\n$7\r\nFLUSHDB\r\n", len(db), db)
	if _, err := conn.Write([]byte(cmd)); err != nil {
		t.Fatalf("redis: %v", err)
	}
	r := bufio.NewReader(conn)
	for i := 0; i < 2; i++ {
		line, err := r.ReadString('\n')
		if err != nil || !strings.HasPrefix(line, "+OK") {
			t.Fatalf("redis FLUSHDB: %q %v", line, err)
		}
	}
}

// Out of a volume tree holding the kinds of links a Mac leaves behind: an
// absolute Mac path, an absolute link back into the volume, links to an
// outside folder and file, and a relative link inside. The job succeeds;
// the four links the confined backend will not follow are skipped and
// reported; in-root content is copied; nothing outside is read.
func TestJuicefsOutSkipsLinksItWillNotFollow(t *testing.T) {
	bin := realJuicefsBin(t)
	l := newSyncLayout(t)
	proj := filepath.Join(l.fuse, "proj")
	mustWrite(t, filepath.Join(proj, "real.txt"), "r")
	mustWrite(t, filepath.Join(proj, "sub/in.txt"), "i")
	mustLink(t, "/Volumes/Media/clip.mov", filepath.Join(proj, "clip.mov"))
	mustLink(t, filepath.Join(proj, "sub"), filepath.Join(proj, "absin"))
	mustLink(t, l.outside, filepath.Join(proj, "outdir"))
	mustLink(t, filepath.Join(l.outside, "secret.txt"), filepath.Join(proj, "outfile"))
	mustLink(t, "sub/in.txt", filepath.Join(proj, "rel.txt"))
	out := filepath.Join(l.host, "out")
	start := time.Now()
	report, err := l.runReport(t, bin, "/jfs/proj", out, DefaultSyncOptions())
	if err != nil {
		t.Fatalf("RunSync: %v", err)
	}
	if d := time.Since(start); d > 30*time.Second {
		t.Errorf("sync took %v (retries per skipped link?)", d)
	}
	if got := tree(t, out); !sameList(got, []string{"real.txt", "rel.txt", "sub/", "sub/in.txt"}) {
		t.Errorf("copied %v", got)
	}
	if b, _ := os.ReadFile(filepath.Join(out, "rel.txt")); string(b) != "i" {
		t.Errorf("relative link copied as %q, want the file it names", b)
	}
	if report.SkippedLinks != 4 {
		t.Errorf("skipped %d links, want 4: %v", report.SkippedLinks, report.SkippedLinkSample)
	}
	for _, n := range []string{"/jfs/proj/clip.mov", "/jfs/proj/absin", "/jfs/proj/outdir", "/jfs/proj/outfile"} {
		found := false
		for _, s := range report.SkippedLinkSample {
			found = found || s == n
		}
		if !found {
			t.Errorf("skipped sample %v does not name %s", report.SkippedLinkSample, n)
		}
	}
	noRegularFile(t, "out", out, "secret.txt")
}

// A link to "." inside the tree loops until the kernel's ELOOP limit, as
// before this change. The sync must still finish.
func TestJuicefsLoopLinkDoesNotHang(t *testing.T) {
	bin := realJuicefsBin(t)
	l := newSyncLayout(t)
	mustWrite(t, filepath.Join(l.fuse, "proj/real.txt"), "r")
	mustLink(t, ".", filepath.Join(l.fuse, "proj/loop"))
	done := make(chan error, 1)
	go func() {
		_, err := l.runReport(t, bin, "/jfs/proj", filepath.Join(l.host, "out"), DefaultSyncOptions())
		done <- err
	}()
	select {
	case err := <-done:
		t.Logf("loop link: sync finished: %v", err)
	case <-time.After(3 * time.Minute):
		t.Fatal("a sync of a tree with a loop link did not finish")
	}
	if _, err := os.Stat(filepath.Join(l.host, "out", "real.txt")); err != nil {
		t.Errorf("real.txt not copied: %v", err)
	}
}
