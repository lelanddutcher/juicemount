//go:build linux || darwin || windows

package jfsmeta_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/redisurl"
	"github.com/lelanddutcher/juicemount/internal/smartfolders"
	"github.com/lelanddutcher/juicemount/internal/smartfolders/jfsmeta"
	"github.com/lelanddutcher/juicemount/internal/smartfolders/sftest"
	"github.com/redis/go-redis/v9"
)

// metaTestURL is JM_TEST_SMARTFOLDERS_JFS_REDIS, a caller-owned, disposable
// Redis database of its own (not shared with the Manager's tests, which run
// in parallel with this package): every test
// FLUSHes it and formats a throwaway JuiceFS volume in it (file:// bucket in
// a temp dir; nothing is ever written to the bucket).
func metaTestURL(t *testing.T) string {
	t.Helper()
	u := os.Getenv("JM_TEST_SMARTFOLDERS_JFS_REDIS")
	if u == "" {
		t.Skip("set JM_TEST_SMARTFOLDERS_JFS_REDIS to a disposable Redis database of its own (it is flushed) to run the JuiceFS metadata backend tests")
	}
	return u
}

var volumeSeq atomic.Int64

// FormatThrowawayVolume flushes the database and formats a new volume in it.
func formatThrowawayVolume(t *testing.T, url string) {
	t.Helper()
	opts, err := redisurl.Parse(url)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opts)
	defer rdb.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	t.Cleanup(func() {
		c := redis.NewClient(opts)
		defer c.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.FlushDB(ctx).Err()
	})
	conf := meta.DefaultConf()
	conf.NoBGJob, conf.MaxDeletes = true, 0
	m := meta.NewClient(url, conf)
	n := volumeSeq.Add(1)
	format := &meta.Format{
		Name:      fmt.Sprintf("sftest%d", n),
		UUID:      fmt.Sprintf("00000000-0000-4000-8000-%012d", n),
		Storage:   "file",
		Bucket:    t.TempDir() + "/",
		BlockSize: 4096,
		TrashDays: 1, // so a stray trash move would be visible
	}
	if err := m.Init(format, false); err != nil {
		t.Fatalf("format: %v", err)
	}
	_ = m.Shutdown()
}

// metaFixture sets up and inspects the volume with its own metadata client;
// the reconciler uses the production jfsmeta.FS (with its session).
type metaFixture struct {
	fs    *jfsmeta.FS
	admin meta.Meta
	ctx   meta.Context
}

func newMetaFixture(t *testing.T) sftest.Fixture {
	t.Helper()
	url := metaTestURL(t)
	formatThrowawayVolume(t, url)
	fsys, err := jfsmeta.Open(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fsys.Close() })
	conf := meta.DefaultConf()
	conf.NoBGJob, conf.MaxDeletes = true, 0
	admin := meta.NewClient(url, conf)
	if _, err := admin.Load(true); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Shutdown() })
	return &metaFixture{fs: fsys, admin: admin, ctx: meta.Background()}
}

func (f *metaFixture) FS() smartfolders.FS { return f.fs }

func (f *metaFixture) resolve(t *testing.T, p string) meta.Ino {
	t.Helper()
	ino := meta.RootInode
	if p == "" || p == "." {
		return ino
	}
	for _, c := range strings.Split(p, "/") {
		var next meta.Ino
		var attr meta.Attr
		if st := f.admin.Lookup(f.ctx, ino, c, &next, &attr, false); st != 0 {
			t.Fatalf("lookup %s in %s: %v", c, p, st)
		}
		ino = next
	}
	return ino
}

func (f *metaFixture) MkdirAll(t *testing.T, p string) {
	t.Helper()
	ino := meta.RootInode
	for _, c := range strings.Split(p, "/") {
		var next meta.Ino
		var attr meta.Attr
		st := f.admin.Lookup(f.ctx, ino, c, &next, &attr, false)
		if st == syscall.ENOENT {
			st = f.admin.Mkdir(f.ctx, ino, c, 0o755, 0o022, 0, &next, &attr)
		}
		if st != 0 {
			t.Fatalf("mkdir %s: %v", p, st)
		}
		ino = next
	}
}

func (f *metaFixture) WriteFile(t *testing.T, p string) {
	t.Helper()
	if d := path.Dir(p); d != "." {
		f.MkdirAll(t, d)
	}
	var ino meta.Ino
	var attr meta.Attr
	if st := f.admin.Create(f.ctx, f.resolve(t, path.Dir(p)), path.Base(p), 0o644, 0o022, syscall.O_EXCL, &ino, &attr); st != 0 {
		t.Fatalf("create %s: %v", p, st)
	}
	_ = f.admin.Close(f.ctx, ino)
}

func (f *metaFixture) Symlink(t *testing.T, text, p string) {
	t.Helper()
	var ino meta.Ino
	var attr meta.Attr
	if st := f.admin.Symlink(f.ctx, f.resolve(t, path.Dir(p)), path.Base(p), text, &ino, &attr); st != 0 {
		t.Fatalf("symlink %s: %v", p, st)
	}
}

func (f *metaFixture) Remove(t *testing.T, p string) {
	t.Helper()
	if st := f.admin.Unlink(f.ctx, f.resolve(t, path.Dir(p)), path.Base(p), true); st != 0 {
		t.Fatalf("unlink %s: %v", p, st)
	}
}

func (f *metaFixture) Rmdir(p string) error {
	parent := meta.RootInode
	if d := path.Dir(p); d != "." {
		for _, c := range strings.Split(d, "/") {
			var next meta.Ino
			var attr meta.Attr
			if st := f.admin.Lookup(f.ctx, parent, c, &next, &attr, false); st != 0 {
				return st
			}
			parent = next
		}
	}
	if st := f.admin.Rmdir(f.ctx, parent, path.Base(p), true); st != 0 {
		return st
	}
	return nil
}

func (f *metaFixture) names(t *testing.T, ino meta.Ino) []string {
	t.Helper()
	var entries []*meta.Entry
	if st := f.admin.Readdir(f.ctx, ino, 0, &entries); st != 0 {
		t.Fatalf("readdir: %v", st)
	}
	var out []string
	for _, e := range entries {
		if n := string(e.Name); n != "." && n != ".." {
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out
}

func (f *metaFixture) ReadDir(t *testing.T, p string) []string {
	t.Helper()
	return f.names(t, f.resolve(t, p))
}

func (f *metaFixture) TrashCount(t *testing.T) int {
	t.Helper()
	n := 0
	var entries []*meta.Entry
	if st := f.admin.Readdir(f.ctx, meta.TrashInode, 0, &entries); st != 0 {
		return 0
	}
	for _, e := range entries {
		if name := string(e.Name); name != "." && name != ".." {
			n += len(f.names(t, e.Inode))
		}
	}
	return n
}

func TestReconcilerOnJuiceFSMetadata(t *testing.T) {
	metaTestURL(t)
	sftest.Run(t, newMetaFixture)
}

// The backend opens no JuiceFS session: a session's background refresh
// exits the process when the volume format changes (see TestMetaFSSurvives
// FormatRemoval), and none of its operations needs one.
func TestMetaFSOpensNoSession(t *testing.T) {
	url := metaTestURL(t)
	formatThrowawayVolume(t, url)
	fsys, err := jfsmeta.Open(url)
	if err != nil {
		t.Fatal(err)
	}
	conf := meta.DefaultConf()
	conf.NoBGJob = true
	probe := meta.NewClient(url, conf)
	if _, err := probe.Load(true); err != nil {
		t.Fatal(err)
	}
	defer probe.Shutdown()
	if _, err := fsys.Mkdir("D", 0o705); err != nil {
		t.Fatal(err)
	}
	if _, err := fsys.Symlink("../x", "D/l"); err != nil {
		t.Fatal(err)
	}
	sessions, err := probe.ListSessions()
	if err != nil || len(sessions) != 0 {
		t.Fatalf("sessions while open = %d, %v", len(sessions), err)
	}
	// Without a session the usage counters are flushed explicitly.
	fsys.Flush()
	rdb := redis.NewClient(mustOpts(t, url))
	defer rdb.Close()
	if n, err := rdb.Get(context.Background(), "totalInodes").Int64(); err != nil || n < 2 {
		t.Fatalf("totalInodes after Flush = %d, %v", n, err)
	}
	if err := fsys.Close(); err != nil {
		t.Fatal(err)
	}
}

func mustOpts(t *testing.T, url string) *redis.Options {
	t.Helper()
	opts, err := redisurl.Parse(url)
	if err != nil {
		t.Fatal(err)
	}
	return opts
}

// A JuiceFS session's refresh calls os.Exit(11) within one heartbeat (12 s
// +-5%) of the volume's format disappearing. The backend must not: run it
// in a child process, delete the format, wait past a heartbeat, and require
// a clean exit.
func TestMetaFSSurvivesFormatRemoval(t *testing.T) {
	url := metaTestURL(t)
	if os.Getenv("JM_SF_FORMAT_REMOVAL_CHILD") == "1" {
		fsys, err := jfsmeta.Open(url)
		if err != nil {
			fmt.Println("open:", err)
			os.Exit(3)
		}
		rdb := redis.NewClient(mustOpts(t, url))
		if err := rdb.Del(context.Background(), "setting").Err(); err != nil {
			os.Exit(4)
		}
		time.Sleep(14 * time.Second)
		_ = fsys.Close()
		os.Exit(0)
	}
	formatThrowawayVolume(t, url)
	cmd := exec.Command(os.Args[0], "-test.run=^TestMetaFSSurvivesFormatRemoval$", "-test.count=1")
	cmd.Env = append(os.Environ(), "JM_SF_FORMAT_REMOVAL_CHILD=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("the backend's process did not survive the format removal: %v\n%s", err, out)
	}
}

// Open is bounded even when the endpoint accepts connections and never
// answers (the JuiceFS client's own retries took 78 s).
func TestMetaFSOpenIsBounded(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			defer c.Close() // hold it open, never reply
		}
	}()
	start := time.Now()
	if _, err := jfsmeta.OpenWithin("redis://"+ln.Addr().String()+"/1", time.Second); err == nil {
		t.Fatal("opened a silent endpoint")
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("Open took %s", d)
	}
	if jfsmeta.OpenTimeout > 20*time.Second {
		t.Fatalf("default bound %s is too long", jfsmeta.OpenTimeout)
	}
}

func TestMetaFSRefusesBadURL(t *testing.T) {
	for _, u := range []string{"", "mysql://x", "redis://", "redis://host:notaport/1", "file:///tmp/x"} {
		if _, err := jfsmeta.Open(u); err == nil {
			t.Errorf("Open(%q) succeeded", u)
		}
	}
}

// Guards the suite's trash assertion: the fixture really sees trash entries.
func TestTrashCountSeesTrash(t *testing.T) {
	metaTestURL(t)
	fx := newMetaFixture(t).(*metaFixture)
	fx.WriteFile(t, "P/x.mov")
	if n := fx.TrashCount(t); n != 0 {
		t.Fatalf("fresh trash = %d", n)
	}
	if st := fx.admin.Unlink(fx.ctx, fx.resolve(t, "P"), "x.mov"); st != 0 {
		t.Fatal(st)
	}
	if n := fx.TrashCount(t); n != 1 {
		t.Fatalf("trash after an unlink without skip = %d, want 1", n)
	}
}
