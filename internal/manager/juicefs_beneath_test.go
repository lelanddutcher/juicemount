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
	"strings"
	"testing"
	"time"
)

// These tests run the real juicefs built from third_party/juicefs (set
// JM_TEST_JUICEFS_BIN; CI builds it). They pin that a sync cannot read or
// write outside its roots through a symlink in the tree: juicefs's file://
// backend resolves every path beneath its root (patch 0016), and RunSync
// passes --links when both ends can store links.
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

func (l syncLayout) spec() RunSyncSpec { return RunSyncSpec{Mode: ModeEmbedded, FUSEMount: l.fuse} }

func (l syncLayout) run(t *testing.T, bin, src, dst string, opts SyncOptions) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	return RunSync(ctx, bin, l.spec(), src, dst, opts, nil, nil)
}

// Single hop, Out: a symlink deep in a volume tree that points outside the
// volume is copied as a link; the folder it points at is not copied.
func TestJuicefsOutSingleHopCopiesLinkNotTarget(t *testing.T) {
	bin := realJuicefsBin(t)
	l := newSyncLayout(t)
	mustWrite(t, filepath.Join(l.fuse, "proj/deep/a/b/real.txt"), "in volume")
	mustLink(t, l.outside, filepath.Join(l.fuse, "proj/deep/a/b/evil"))
	out := filepath.Join(l.host, "out")
	if err := l.run(t, bin, "/jfs/proj", out, DefaultSyncOptions()); err != nil {
		t.Fatalf("RunSync: %v", err)
	}
	if _, err := os.Stat(filepath.Join(out, "deep/a/b/real.txt")); err != nil {
		t.Errorf("the in-volume file was not copied: %v", err)
	}
	noRegularFile(t, "out", out, "secret.txt")
	if fi, err := os.Lstat(filepath.Join(out, "deep/a/b/evil")); err != nil || fi.Mode()&fs.ModeSymlink == 0 {
		t.Errorf("deep symlink not copied as a link: %v", err)
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
	err := l.run(t, bin, "/jfs/srcb", "mem://bucket/b/", DefaultSyncOptions())
	if err == nil || !strings.Contains(err.Error(), "outside the file storage root") {
		t.Errorf("sync to a link-less destination: err = %v; want the chain refused (outside the file storage root)", err)
	}
	// The same listing path without mem://: run juicefs with no --links
	// between two folders, and check nothing from outside arrives.
	out := filepath.Join(l.host, "nolinks")
	cmd := exec.Command(bin, "sync", "file://"+src+"/", "file://"+out+"/")
	_ = cmd.Run()
	noRegularFile(t, "no --links", out, "secret.txt")
	if _, err := os.Stat(filepath.Join(out, "plain.txt")); err != nil {
		t.Errorf("the plain file was not copied: %v", err)
	}
}

// Probe C: Out job 1 copies a link into a host root; Out job 2 then
// writes a tree whose folder has the link's name. The second job must not
// write through the copied link.
func TestJuicefsCopiedLinkIsNotWrittenThrough(t *testing.T) {
	bin := realJuicefsBin(t)
	l := newSyncLayout(t)
	root := filepath.Join(l.host, "croot")
	mustLink(t, l.victim, filepath.Join(l.fuse, "c1/d"))
	mustWrite(t, filepath.Join(l.fuse, "c2/d/payload.txt"), "payload")
	if err := l.run(t, bin, "/jfs/c1", root, DefaultSyncOptions()); err != nil {
		t.Fatalf("job 1: %v", err)
	}
	err := l.run(t, bin, "/jfs/c2", root, DefaultSyncOptions())
	emptyDir(t, "job 2", l.victim)
	if err == nil {
		t.Errorf("job 2 reported success")
	}
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
// link in the destination is written through, and one in the source is
// copied as a link.
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
	mustLink(t, l.outside, filepath.Join(seed, "proj/evil"))
	mustLink(t, l.victim, filepath.Join(seed, "dest/sub"))
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
	spec := RunSyncSpec{Mode: ModeStandalone, MetaURL: meta, VolName: vol}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	_ = RunSync(ctx, bin, spec, src, "/jfs/dest", DefaultSyncOptions(), nil, nil)
	emptyDir(t, "standalone in", l.victim)
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
