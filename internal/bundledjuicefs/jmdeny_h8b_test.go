//go:build h8b_acceptance

package bundledjuicefs

// H-8 option B, lane L3: a write the per-project Redis proxy refuses must
// reach the user as "Read-only file system", decided on the first attempt,
// and must not leave the refused slice's uploaded blocks behind. These run the
// real third_party/juicefs meta and VFS code (through the root module's
// replace) against a disposable Redis behind denyStub.

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/prometheus/client_golang/prometheus"
)

func newDeniableMeta(t *testing.T, name string) (meta.Meta, *denyStub, *meta.Format, func(string) (int64, error)) {
	return newDeniableMetaConf(t, name, meta.DefaultConf())
}

func newDeniableMetaConf(t *testing.T, name string, conf *meta.Config) (meta.Meta, *denyStub, *meta.Format, func(string) (int64, error)) {
	t.Helper()
	addr, db, rdb := disposableRedis(t)
	stub := startDenyStub(t, addr)
	m := meta.NewClient(fmt.Sprintf("redis://%s/%d", stub.addr(), db), conf)
	t.Cleanup(func() { _ = m.Shutdown() })
	format := &meta.Format{Name: name, UUID: name + "-uuid", Storage: "mem", BlockSize: 4096, Compression: "none", DirStats: true}
	if err := m.Init(format, true); err != nil {
		t.Fatal(err)
	}
	llen := func(key string) (int64, error) { return rdb.LLen(t.Context(), key).Result() }
	return m, stub, format, llen
}

func TestH8BJMDENYIsEROFSSingleAttempt(t *testing.T) {
	m, stub, _, llen := newDeniableMeta(t, "h8b-jmdeny")
	ctx := meta.Background()
	var ino meta.Ino
	var attr meta.Attr
	if st := m.Create(ctx, meta.RootInode, "reel.mov", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create before deny: %v", st)
	}
	var id uint64
	if st := m.NewSlice(ctx, &id); st != 0 {
		t.Fatalf("new slice: %v", st)
	}

	stub.armed.Store(true)
	before := stub.denied.Load()
	st := m.Write(ctx, ino, 0, 0, meta.Slice{Id: id, Size: 4096, Len: 4096}, time.Now())
	if st != syscall.EROFS {
		t.Fatalf("refused slice commit returned %q (errno %d), want EROFS", st, st)
	}
	if got := stub.denied.Load() - before; got != 1 {
		t.Fatalf("refused slice commit took %d EXEC attempts, want exactly 1", got)
	}
	if n, err := llen(fmt.Sprintf("c%d_0", ino)); err != nil || n != 0 {
		t.Fatalf("refused commit left %d slice records (err %v)", n, err)
	}

	before = stub.denied.Load()
	var dir meta.Ino
	st = m.Mkdir(ctx, meta.RootInode, "Scott Adams", 0755, 0, 0, &dir, &attr)
	if st != syscall.EROFS {
		t.Fatalf("refused mkdir returned %q (errno %d), want EROFS", st, st)
	}
	if got := stub.denied.Load() - before; got != 1 {
		t.Fatalf("refused mkdir took %d EXEC attempts, want exactly 1", got)
	}

	// Disarmed, the same client works again: the refusal did not poison the
	// pooled connection (DISCARD left it outside MULTI).
	stub.armed.Store(false)
	if st := m.Mkdir(ctx, meta.RootInode, "Scott Adams", 0755, 0, 0, &dir, &attr); st != 0 {
		t.Fatalf("mkdir after deny lifted: %v", st)
	}
}

// liveObjects tracks object keys so the test can prove the refused slice's
// uploaded blocks were removed, not left for a server gc.
type liveObjects struct {
	object.ObjectStorage
	mu   sync.Mutex
	puts int
	live map[string]bool
}

func (o *liveObjects) Put(key string, in io.Reader, getters ...object.AttrGetter) error {
	err := o.ObjectStorage.Put(key, in, getters...)
	if err == nil {
		o.mu.Lock()
		o.puts++
		o.live[key] = true
		o.mu.Unlock()
	}
	return err
}

func (o *liveObjects) Delete(key string, getters ...object.AttrGetter) error {
	err := o.ObjectStorage.Delete(key, getters...)
	o.mu.Lock()
	delete(o.live, key)
	o.mu.Unlock()
	return err
}

func (o *liveObjects) snapshot() (int, []string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	var keys []string
	for k := range o.live {
		if strings.HasPrefix(k, "chunks/") {
			keys = append(keys, k)
		}
	}
	return o.puts, keys
}

func TestH8BSliceCommitDenyCloseIsEROFS(t *testing.T) {
	m, stub, format, llen := newDeniableMeta(t, "h8b-slice")
	mem, err := object.CreateStorage("mem", "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	blob := &liveObjects{ObjectStorage: mem, live: map[string]bool{}}
	chunkConf := chunk.Config{BlockSize: format.BlockSize * 1024, Compress: format.Compression,
		MaxUpload: 2, BufferSize: 30 << 20, CacheDir: "memory", CacheSize: 10 << 20, MaxRetries: 1}
	conf := &vfs.Config{Meta: meta.DefaultConf(), Format: *format, Version: "h8b", Chunk: &chunkConf, FuseOpts: &vfs.FuseOptions{}}
	registry := prometheus.NewRegistry()
	store := chunk.NewCachedStore(blob, chunkConf, registry)
	v := vfs.NewVFS(conf, m, store, prometheus.WrapRegistererWithPrefix("juicefs_", registry), registry)

	ctx := vfs.NewLogContext(meta.NewContext(1, 0, []uint32{0}))
	entry, fh, st := v.Create(ctx, meta.RootInode, "A001_C002.mov", 0644, 0, uint32(syscall.O_RDWR))
	if st != 0 {
		t.Fatalf("create before deny: %v", st)
	}
	data := []byte(strings.Repeat("frame", 2000))
	if st := v.Write(ctx, entry.Inode, data, 0, fh); st != 0 {
		t.Fatalf("buffered write: %v", st)
	}

	stub.armed.Store(true)
	st = v.Flush(ctx, entry.Inode, fh, 0)
	v.Release(ctx, entry.Inode, fh)
	if st != syscall.EROFS {
		t.Fatalf("close after a refused slice commit returned %q (errno %d), want EROFS (Read-only file system)", st, st)
	}
	if got := stub.denied.Load(); got != 1 {
		t.Fatalf("refused slice commit took %d EXEC attempts, want exactly 1", got)
	}
	if n, err := llen(fmt.Sprintf("c%d_0", entry.Inode)); err != nil || n != 0 {
		t.Fatalf("refused commit left %d slice records (err %v)", n, err)
	}
	// The blocks were uploaded before the commit was refused. Nothing
	// references that slice ID, so the writer must delete them.
	deadline := time.Now().Add(10 * time.Second)
	for {
		puts, live := blob.snapshot()
		if puts == 0 {
			t.Fatal("no block was uploaded; the test did not reach the slice commit")
		}
		if len(live) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("refused slice left %d uploaded blocks: %v", len(live), live)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Compaction runs on reads and writes even with --no-bgjob. A Mac that
// compacts a fragmented file in a project another Mac holds uploads the merged
// slice (up to 64 MiB) and then has the commit refused. With --max-deletes 0
// upstream's "wasted compaction" path deletes nothing, so every refused attempt
// would leak that object. The refused (uncommitted) slice must be deleted, and
// the file must still read back exactly as committed.
//
// The Redis engine reports the refusal as EINVAL when its double-check read
// (HGET sliceRef) shows no reference, and as EROFS when that read is refused
// too; both clauses in compactChunk are exercised.
func TestH8BCompactionDenyLeavesNoUploadedSlice(t *testing.T) {
	for _, tc := range []struct {
		name           string
		denyDoubleRead bool
	}{{"EINVAL", false}, {"EROFS", true}} {
		t.Run(tc.name, func(t *testing.T) { compactionDenyCase(t, tc.denyDoubleRead) })
	}
}

func compactionDenyCase(t *testing.T, denyDoubleRead bool) {
	metaConf := meta.DefaultConf()
	metaConf.MaxDeletes = 0 // the Mac mount's --max-deletes 0
	m, stub, format, _ := newDeniableMetaConf(t, "h8b-compact", metaConf)
	mem, err := object.CreateStorage("mem", "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	blob := &liveObjects{ObjectStorage: mem, live: map[string]bool{}}
	chunkConf := chunk.Config{BlockSize: format.BlockSize * 1024, Compress: format.Compression,
		MaxUpload: 2, BufferSize: 30 << 20, CacheDir: "memory", CacheSize: 10 << 20, MaxRetries: 1}
	registry := prometheus.NewRegistry()
	store := chunk.NewCachedStore(blob, chunkConf, registry)
	// cmd/mount.go registerMetaMsg wires these for a real mount.
	m.OnMsg(meta.DeleteSlice, func(args ...interface{}) error {
		return store.Remove(args[0].(uint64), int(args[1].(uint32)))
	})
	m.OnMsg(meta.CompactChunk, func(args ...interface{}) error {
		return vfs.Compact(chunkConf, store, args[0].([]meta.Slice), args[1].(uint64))
	})
	conf := &vfs.Config{Meta: metaConf, Format: *format, Version: "h8b", Chunk: &chunkConf, FuseOpts: &vfs.FuseOptions{}}
	v := vfs.NewVFS(conf, m, store, prometheus.WrapRegistererWithPrefix("juicefs_", registry), registry)

	ctx := vfs.NewLogContext(meta.NewContext(1, 0, []uint32{0}))
	entry, fh, st := v.Create(ctx, meta.RootInode, "fragmented.prproj", 0644, 0, uint32(syscall.O_RDWR))
	if st != 0 {
		t.Fatalf("create: %v", st)
	}
	// Six separately committed slices in chunk 0 (an autosaving editor).
	var want []byte
	for i := 0; i < 6; i++ {
		part := []byte(strings.Repeat(string(rune('a'+i)), 5000))
		want = append(want, part...)
		if st := v.Write(ctx, entry.Inode, part, uint64(i*5000), fh); st != 0 {
			t.Fatalf("write %d: %v", i, st)
		}
		if st := v.Flush(ctx, entry.Inode, fh, 0); st != 0 {
			t.Fatalf("flush %d: %v", i, st)
		}
	}
	v.Release(ctx, entry.Inode, fh)
	committedPuts, committed := blob.snapshot()

	stub.denySliceRefRead.Store(denyDoubleRead)
	stub.armed.Store(true)
	var slices []meta.Slice
	if st := m.Read(meta.Background(), entry.Inode, 0, &slices); st != 0 || len(slices) < 5 {
		t.Fatalf("read before compaction: %v, %d slices", st, len(slices))
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		puts, live := blob.snapshot()
		if puts > committedPuts && stub.denied.Load() > 0 && len(live) == len(committed) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("refused compaction: uploads %d->%d, denied EXECs %d, live blocks %d (want %d, the committed slices only): %v",
				committedPuts, puts, stub.denied.Load(), len(live), len(committed), live)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// The committed data is untouched and still reads back exactly.
	_, rfh, st := v.Open(ctx, entry.Inode, uint32(syscall.O_RDONLY))
	if st != 0 {
		t.Fatalf("open after refused compaction: %v", st)
	}
	defer v.Release(ctx, entry.Inode, rfh)
	got := make([]byte, len(want))
	n, st := v.Read(ctx, entry.Inode, got, 0, rfh)
	if st != 0 || n != len(want) || string(got) != string(want) {
		t.Fatalf("read after refused compaction: %d bytes, %v, equal=%v", n, st, string(got[:n]) == string(want))
	}
}

// An unauthenticated client (or a wrong password) is refused by the proxy on
// every command. The bundled juicefs must fail the mount at once, with the
// refusal in its output, without mounting and without a retry storm.
func TestH8BAuthRequiredMountFailsCleanly(t *testing.T) {
	addr, db, rdb := disposableRedis(t)
	if err := rdb.Set(t.Context(), "h8b:sentinel", "before", 0).Err(); err != nil {
		t.Fatal(err)
	}
	stub := startDenyStub(t, addr)
	dir := t.TempDir()
	bin := filepath.Join(dir, "juicefs")
	build := exec.Command("../../scripts/build-bundled-juicefs.sh", bin)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build bundled juicefs: %v\n%s", err, out)
	}
	mnt := filepath.Join(dir, "mnt")
	if err := os.Mkdir(mnt, 0700); err != nil {
		t.Fatal(err)
	}
	stub.denyAll.Store(true)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "mount", fmt.Sprintf("redis://mac-edit2@%s/%d", stub.addr(), db), mnt,
		"--no-usage-report", "--no-bgjob", "--max-deletes", "0", "-o", "nobrowse",
		"--cache-dir", filepath.Join(dir, "cache"), "--metrics", "127.0.0.1:0")
	cmd.Env = []string{"HOME=" + dir, "PATH=/usr/bin:/bin", "TMPDIR=" + os.TempDir(),
		"JFS_SUPERVISOR=" + fmt.Sprint(os.Getpid()), "META_PASSWORD=wrong"}
	start := time.Now()
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("mount did not fail within 30s:\n%s", out)
	}
	if err == nil {
		t.Fatalf("mount succeeded without authentication:\n%s", out)
	}
	if !strings.Contains(string(out), "JMDENY AUTH_REQUIRED") {
		t.Fatalf("mount failure does not name the refusal:\n%s", out)
	}
	if mounts, _ := exec.Command("/sbin/mount").Output(); strings.Contains(string(mounts), mnt) {
		t.Fatalf("%s is mounted after an AUTH_REQUIRED refusal", mnt)
	}
	if n := stub.refused.Load(); n > 20 {
		t.Fatalf("juicefs sent %d refused commands; want a prompt failure, not retries", n)
	}
	// Nothing reached Redis: the DB holds exactly what it held before.
	if keys, err := rdb.Keys(t.Context(), "*").Result(); err != nil || len(keys) != 1 || keys[0] != "h8b:sentinel" {
		t.Fatalf("Redis keys after the refused mount = %v (%v), want only the sentinel", keys, err)
	}
	if v, err := rdb.Get(t.Context(), "h8b:sentinel").Result(); err != nil || v != "before" {
		t.Fatalf("sentinel = %q (%v)", v, err)
	}
	t.Logf("failed in %v after %d refused commands", time.Since(start).Round(time.Millisecond), stub.refused.Load())
}
