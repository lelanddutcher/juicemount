package thumbcache

import (
	"bytes"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func mustOpen(t *testing.T, dir string, maxBytes int64) *Cache {
	t.Helper()
	c, err := Open(dir, maxBytes)
	if err != nil {
		t.Fatalf("Open(%q, %d): %v", dir, maxBytes, err)
	}
	return c
}

func mustPut(t *testing.T, c *Cache, inode uint64, kind string, payload []byte) {
	t.Helper()
	n, err := c.Put(inode, kind, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Put(%d, %q): %v", inode, kind, err)
	}
	if n != int64(len(payload)) {
		t.Fatalf("Put(%d, %q) wrote %d bytes, want %d", inode, kind, n, len(payload))
	}
}

// diskUsage walks dir and returns total bytes and file count of regular
// files, failing the test if any temp file is found.
func diskUsage(t *testing.T, dir string) (int64, int) {
	t.Helper()
	var total int64
	var files int
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasPrefix(d.Name(), ".tmp-") {
			t.Errorf("stray temp file on disk: %s", p)
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		files++
		return nil
	})
	if err != nil {
		t.Fatalf("diskUsage(%q): %v", dir, err)
	}
	return total, files
}

func payload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + i%26)
	}
	return b
}

func TestPutPathRoundtrip(t *testing.T) {
	dir := t.TempDir()
	c := mustOpen(t, dir, 1<<20)
	defer c.Close()

	body := []byte("poster-bytes")
	mustPut(t, c, 42, "poster", body)

	if !c.Has(42, "poster") {
		t.Fatal("Has(42, poster) = false after Put")
	}
	p, ok := c.Path(42, "poster")
	if !ok {
		t.Fatal("Path(42, poster) = miss after Put")
	}
	want := filepath.Join(dir, "2a", "42_poster") // 42&0xff = 0x2a
	if p != want {
		t.Fatalf("Path = %q, want layout path %q", p, want)
	}
	got, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read cached blob: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("cached blob = %q, want %q", got, body)
	}

	if _, ok := c.Path(42, "strip"); ok {
		t.Fatal("Path(42, strip) = hit, want miss")
	}
	if c.Has(7, "poster") {
		t.Fatal("Has(7, poster) = true, want false")
	}

	s := c.Stats()
	wantStats := Stats{
		Bytes:    int64(len(body)),
		MaxBytes: 1 << 20,
		Files:    1,
		Hits:     1,
		Misses:   1, // the Path(42, "strip") miss; Has never counts
		Puts:     1,
	}
	if s != wantStats {
		t.Fatalf("Stats = %+v, want %+v", s, wantStats)
	}
}

func TestPathTouchesMtime(t *testing.T) {
	dir := t.TempDir()
	c := mustOpen(t, dir, 1<<20)
	defer c.Close()

	mustPut(t, c, 1, "poster", payload(10))
	p, _ := c.Path(1, "poster")

	before, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if _, ok := c.Path(1, "poster"); !ok {
		t.Fatal("Path miss on resident blob")
	}
	after, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if !after.ModTime().After(before.ModTime()) {
		t.Fatalf("Path hit did not bump mtime: before=%v after=%v", before.ModTime(), after.ModTime())
	}
}

func TestRestartPersistence(t *testing.T) {
	dir := t.TempDir()
	c1 := mustOpen(t, dir, 1<<20)
	body := payload(100)
	mustPut(t, c1, 1, "poster", body)
	mustPut(t, c1, 2, "strip", payload(50))
	if err := c1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	c2 := mustOpen(t, dir, 1<<20)
	defer c2.Close()
	if !c2.Has(1, "poster") || !c2.Has(2, "strip") {
		t.Fatal("blobs missing after reopen")
	}
	s := c2.Stats()
	if s.Bytes != 150 || s.Files != 2 {
		t.Fatalf("after reopen Bytes=%d Files=%d, want 150/2", s.Bytes, s.Files)
	}
	p, ok := c2.Path(1, "poster")
	if !ok {
		t.Fatal("Path miss after reopen")
	}
	got, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatal("blob content changed across reopen")
	}
}

func TestLRUEvictionTouchedSurvives(t *testing.T) {
	dir := t.TempDir()
	c := mustOpen(t, dir, 350)
	defer c.Close()

	mustPut(t, c, 1, "k", payload(100)) // A
	mustPut(t, c, 2, "k", payload(100)) // B
	mustPut(t, c, 3, "k", payload(100)) // C
	if _, ok := c.Path(1, "k"); !ok {   // touch A
		t.Fatal("touch of A missed")
	}
	mustPut(t, c, 4, "k", payload(100)) // D forces eviction

	if c.Has(2, "k") {
		t.Fatal("B should be evicted (oldest untouched)")
	}
	for _, ino := range []uint64{1, 3, 4} {
		if !c.Has(ino, "k") {
			t.Fatalf("inode %d should have survived eviction", ino)
		}
	}
	s := c.Stats()
	if s.Evictions != 1 || s.Bytes != 300 || s.Files != 3 {
		t.Fatalf("Stats = %+v, want Evictions=1 Bytes=300 Files=3", s)
	}

	// Evicted blob is gone from disk too.
	if _, err := os.Lstat(filepath.Join(dir, "02", "2_k")); !os.IsNotExist(err) {
		t.Fatalf("evicted blob still on disk (err=%v)", err)
	}
}

func TestRestartRecencySeededFromMtime(t *testing.T) {
	dir := t.TempDir()
	c1 := mustOpen(t, dir, 250)
	mustPut(t, c1, 1, "k", payload(100))
	mustPut(t, c1, 2, "k", payload(100))
	c1.Close()

	// Make inode 1 look fresh and inode 2 look stale on disk.
	now := time.Now()
	old := now.Add(-time.Hour)
	if err := os.Chtimes(filepath.Join(dir, "01", "1_k"), now, now); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(filepath.Join(dir, "02", "2_k"), old, old); err != nil {
		t.Fatal(err)
	}

	c2 := mustOpen(t, dir, 250)
	defer c2.Close()
	mustPut(t, c2, 3, "k", payload(100)) // forces one eviction

	if c2.Has(2, "k") {
		t.Fatal("stale-mtime blob should have been evicted first after restart")
	}
	if !c2.Has(1, "k") || !c2.Has(3, "k") {
		t.Fatal("fresh blobs should have survived")
	}
}

func TestReplacementByteAccounting(t *testing.T) {
	dir := t.TempDir()
	c := mustOpen(t, dir, 1<<20)
	defer c.Close()

	mustPut(t, c, 9, "poster", payload(100))
	if s := c.Stats(); s.Bytes != 100 || s.Files != 1 {
		t.Fatalf("after first Put: Bytes=%d Files=%d, want 100/1", s.Bytes, s.Files)
	}

	newBody := payload(40)
	mustPut(t, c, 9, "poster", newBody)
	s := c.Stats()
	if s.Bytes != 40 || s.Files != 1 || s.Puts != 2 || s.Evictions != 0 {
		t.Fatalf("after replace: %+v, want Bytes=40 Files=1 Puts=2 Evictions=0", s)
	}

	p, ok := c.Path(9, "poster")
	if !ok {
		t.Fatal("Path miss after replace")
	}
	got, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, newBody) {
		t.Fatal("replaced blob does not serve new content")
	}

	diskBytes, diskFiles := diskUsage(t, dir)
	if diskBytes != 40 || diskFiles != 1 {
		t.Fatalf("disk has %d bytes in %d files, want 40/1", diskBytes, diskFiles)
	}
}

func TestOversizedRejected(t *testing.T) {
	dir := t.TempDir()
	c := mustOpen(t, dir, 100)
	defer c.Close()

	mustPut(t, c, 6, "k", payload(60)) // pre-existing state that must survive

	if _, err := c.Put(5, "poster", bytes.NewReader(payload(101))); err == nil {
		t.Fatal("oversized Put succeeded, want error")
	}
	s := c.Stats()
	if s.Bytes != 60 || s.Files != 1 || s.Puts != 1 || s.Evictions != 0 {
		t.Fatalf("cache state disturbed by rejected oversized Put: %+v", s)
	}
	if c.Has(5, "poster") {
		t.Fatal("oversized blob is resident")
	}
	if diskBytes, diskFiles := diskUsage(t, dir); diskBytes != 60 || diskFiles != 1 {
		t.Fatalf("disk state disturbed: %d bytes in %d files, want 60/1", diskBytes, diskFiles)
	}

	// A blob exactly at the limit is allowed (only strictly-larger rejects).
	mustPut(t, c, 5, "poster", payload(100))
	if s := c.Stats(); s.Bytes != 100 || s.Files != 1 || s.Evictions != 1 {
		t.Fatalf("limit-sized Put: %+v, want Bytes=100 Files=1 Evictions=1", s)
	}
}

func TestBadKindRejected(t *testing.T) {
	dir := t.TempDir()
	c := mustOpen(t, dir, 1<<20)
	defer c.Close()

	for _, kind := range []string{"../x", "a/b", `a\b`, "..", ""} {
		if _, err := c.Put(1, kind, strings.NewReader("x")); err == nil {
			t.Errorf("Put with kind %q succeeded, want error", kind)
		}
		if c.Has(1, kind) {
			t.Errorf("Has(1, %q) = true", kind)
		}
		if _, ok := c.Path(1, kind); ok {
			t.Errorf("Path(1, %q) = hit", kind)
		}
	}
	if s := c.Stats(); s.Bytes != 0 || s.Files != 0 || s.Puts != 0 {
		t.Fatalf("bad kinds mutated cache: %+v", s)
	}
	// Nothing may have escaped or landed inside the cache dir.
	if diskBytes, diskFiles := diskUsage(t, dir); diskBytes != 0 || diskFiles != 0 {
		t.Fatalf("bad kinds left files: %d bytes in %d files", diskBytes, diskFiles)
	}
	if _, err := os.Lstat(filepath.Join(filepath.Dir(dir), "x")); !os.IsNotExist(err) {
		t.Fatalf("path traversal escaped the cache dir (err=%v)", err)
	}
}

func TestOpenCleansTmpFiles(t *testing.T) {
	dir := t.TempDir()
	shard := filepath.Join(dir, "07")
	if err := os.MkdirAll(shard, 0o755); err != nil {
		t.Fatal(err)
	}
	tmp := filepath.Join(shard, ".tmp-x")
	if err := os.WriteFile(tmp, payload(1024), 0o600); err != nil {
		t.Fatal(err)
	}
	blob := filepath.Join(shard, "7_poster") // 7&0xff = 0x07
	if err := os.WriteFile(blob, payload(10), 0o600); err != nil {
		t.Fatal(err)
	}

	c := mustOpen(t, dir, 1<<20)
	defer c.Close()

	if _, err := os.Lstat(tmp); !os.IsNotExist(err) {
		t.Fatalf("Open did not delete stray temp file (err=%v)", err)
	}
	s := c.Stats()
	if s.Bytes != 10 || s.Files != 1 {
		t.Fatalf("temp file counted in stats: %+v, want Bytes=10 Files=1", s)
	}
	if !c.Has(7, "poster") {
		t.Fatal("legit blob lost during tmp cleanup")
	}
}

func TestOpenDefaultsMaxBytes(t *testing.T) {
	for _, mb := range []int64{0, -1} {
		c := mustOpen(t, t.TempDir(), mb)
		if got := c.Stats().MaxBytes; got != DefaultMaxBytes {
			t.Fatalf("Open(dir, %d): MaxBytes = %d, want DefaultMaxBytes (%d)", mb, got, int64(DefaultMaxBytes))
		}
		c.Close()
	}
}

func TestReopenSmallerLimitTrims(t *testing.T) {
	dir := t.TempDir()
	c1 := mustOpen(t, dir, 1<<20)
	mustPut(t, c1, 1, "k", payload(100))
	mustPut(t, c1, 2, "k", payload(100))
	mustPut(t, c1, 3, "k", payload(100))
	c1.Close()

	c2 := mustOpen(t, dir, 150)
	defer c2.Close()
	s := c2.Stats()
	if s.Bytes > 150 {
		t.Fatalf("reopen with smaller limit left Bytes=%d > MaxBytes=150", s.Bytes)
	}
	if s.Files != 1 || s.Bytes != 100 || s.Evictions != 2 {
		t.Fatalf("trim on reopen: %+v, want Files=1 Bytes=100 Evictions=2", s)
	}
	diskBytes, diskFiles := diskUsage(t, dir)
	if diskBytes != s.Bytes || diskFiles != s.Files {
		t.Fatalf("disk (%d bytes/%d files) diverges from stats %+v", diskBytes, diskFiles, s)
	}
}

func TestClosedCache(t *testing.T) {
	dir := t.TempDir()
	c := mustOpen(t, dir, 1<<20)
	mustPut(t, c, 1, "k", payload(10))
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := c.Put(2, "k", strings.NewReader("x")); err == nil {
		t.Fatal("Put on closed cache succeeded")
	}
	if c.Has(1, "k") {
		t.Fatal("Has on closed cache returned true")
	}
	if _, ok := c.Path(1, "k"); ok {
		t.Fatal("Path on closed cache returned hit")
	}
	// Blob survives on disk for the next Open.
	c2 := mustOpen(t, dir, 1<<20)
	defer c2.Close()
	if !c2.Has(1, "k") {
		t.Fatal("blob lost across Close/Open")
	}
}

func TestConcurrencySmoke(t *testing.T) {
	dir := t.TempDir()
	const maxBytes = 8 << 10
	c := mustOpen(t, dir, maxBytes)

	const goroutines = 16
	const ops = 1000
	kinds := []string{"poster", "strip"}

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, 0xC0FFEE))
			for i := 0; i < ops; i++ {
				inode := uint64(rng.IntN(32))
				kind := kinds[rng.IntN(len(kinds))]
				switch rng.IntN(3) {
				case 0:
					body := payload(1 + rng.IntN(512))
					if _, err := c.Put(inode, kind, bytes.NewReader(body)); err != nil {
						t.Errorf("concurrent Put(%d, %q): %v", inode, kind, err)
						return
					}
				case 1:
					c.Path(inode, kind)
				case 2:
					c.Has(inode, kind)
				}
			}
		}(uint64(g))
	}
	wg.Wait()

	s := c.Stats()
	if s.Bytes < 0 || s.Bytes > maxBytes {
		t.Fatalf("Bytes=%d out of bounds [0, %d]", s.Bytes, int64(maxBytes))
	}
	diskBytes, diskFiles := diskUsage(t, dir)
	if diskBytes != s.Bytes || diskFiles != s.Files {
		t.Fatalf("disk (%d bytes/%d files) diverges from stats (%d bytes/%d files)",
			diskBytes, diskFiles, s.Bytes, s.Files)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// --- C5: invalidation (stale-derivative correctness) -----------------------
//
// The cache is keyed {inode, kind} and persists across restarts, so an inode
// whose source bytes changed (in-place overwrite, recycled inode) would keep
// serving the OLD blob forever. Invalidate is the escape hatch; these tests
// pin the two properties the serve-path gate depends on: the in-RAM index
// entry is dropped AND the on-disk file is gone (so the next Open, which
// rebuilds the index by walking the directory, cannot resurrect it).

func TestInvalidateDropsIndexAndFile(t *testing.T) {
	dir := t.TempDir()
	c := mustOpen(t, dir, 1<<20)
	defer c.Close()

	body := payload(64)
	mustPut(t, c, 42, "thumbnail", body)
	p, ok := c.Path(42, "thumbnail")
	if !ok {
		t.Fatal("Path(42, thumbnail) = miss right after Put")
	}
	if _, err := os.Stat(p); err != nil {
		t.Fatalf("blob not on disk after Put: %v", err)
	}

	if !c.Invalidate(42, "thumbnail") {
		t.Fatal("Invalidate(42, thumbnail) = false, want true (entry was resident)")
	}
	if c.Has(42, "thumbnail") {
		t.Error("Has(42, thumbnail) = true after Invalidate")
	}
	if _, ok := c.Path(42, "thumbnail"); ok {
		t.Error("Path(42, thumbnail) = hit after Invalidate")
	}
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Errorf("on-disk blob still present after Invalidate: stat err = %v", err)
	}

	s := c.Stats()
	if s.Bytes != 0 || s.Files != 0 {
		t.Errorf("Stats after Invalidate: Bytes=%d Files=%d, want 0/0", s.Bytes, s.Files)
	}
	if s.Invalidations != 1 {
		t.Errorf("Stats.Invalidations = %d, want 1", s.Invalidations)
	}
	if s.Evictions != 0 {
		t.Errorf("Stats.Evictions = %d, want 0 (invalidation is not a capacity eviction)", s.Evictions)
	}
}

// TestInvalidateSurvivesReopen is the restart half of the bug: a stale poster
// must not come back when thumbcache.Open re-indexes the directory.
func TestInvalidateSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	c := mustOpen(t, dir, 1<<20)
	mustPut(t, c, 7, "thumbnail", payload(32))
	mustPut(t, c, 7, "waveform", payload(16))
	c.Invalidate(7, "thumbnail")
	c.Close()

	c2 := mustOpen(t, dir, 1<<20)
	defer c2.Close()
	if c2.Has(7, "thumbnail") {
		t.Error("invalidated blob came back after reopen — the on-disk file was not removed")
	}
	if !c2.Has(7, "waveform") {
		t.Error("Invalidate(7, thumbnail) also dropped the waveform blob")
	}
}

func TestInvalidateMissIsNoop(t *testing.T) {
	dir := t.TempDir()
	c := mustOpen(t, dir, 1<<20)
	defer c.Close()

	mustPut(t, c, 1, "thumbnail", payload(20))
	before := c.Stats()
	if c.Invalidate(99, "thumbnail") {
		t.Error("Invalidate on an absent key = true, want false")
	}
	if c.Invalidate(1, "nosuchkind") {
		t.Error("Invalidate on an absent kind = true, want false")
	}
	if c.Invalidate(1, "bad/kind") {
		t.Error("Invalidate with an unsafe kind = true, want false")
	}
	after := c.Stats()
	if after.Bytes != before.Bytes || after.Files != before.Files || after.Invalidations != 0 {
		t.Errorf("no-op Invalidate mutated state: before=%+v after=%+v", before, after)
	}
	if !c.Has(1, "thumbnail") {
		t.Error("unrelated entry lost")
	}
}

// TestInvalidateRemovesUnindexedBlob covers the restart-adjacent case where a
// blob is on disk under the canonical name but not in this process's index
// (written by an earlier run whose index we haven't rebuilt). It must still be
// deleted, or the next Open picks it up and serves it.
func TestInvalidateRemovesUnindexedBlob(t *testing.T) {
	dir := t.TempDir()
	c := mustOpen(t, dir, 1<<20)
	defer c.Close()

	shard, canonical := c.blobPath(1234, "thumbnail")
	if err := os.MkdirAll(shard, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(canonical, payload(24), 0o644); err != nil {
		t.Fatal(err)
	}
	if c.Invalidate(1234, "thumbnail") {
		t.Error("Invalidate = true for an unindexed blob, want false (nothing was indexed)")
	}
	if _, err := os.Stat(canonical); !os.IsNotExist(err) {
		t.Errorf("unindexed on-disk blob survived Invalidate: stat err = %v", err)
	}
}

func TestInvalidateInodeDropsEveryKind(t *testing.T) {
	dir := t.TempDir()
	c := mustOpen(t, dir, 1<<20)
	defer c.Close()

	mustPut(t, c, 5, "thumbnail", payload(10))
	mustPut(t, c, 5, "waveform", payload(12))
	mustPut(t, c, 5, "filmstrip", payload(14))
	mustPut(t, c, 6, "thumbnail", payload(16))

	if n := c.InvalidateInode(5); n != 3 {
		t.Errorf("InvalidateInode(5) = %d, want 3", n)
	}
	for _, kind := range []string{"thumbnail", "waveform", "filmstrip"} {
		if c.Has(5, kind) {
			t.Errorf("inode 5 %s survived InvalidateInode", kind)
		}
	}
	if !c.Has(6, "thumbnail") {
		t.Error("InvalidateInode(5) dropped inode 6")
	}
	s := c.Stats()
	if s.Files != 1 || s.Invalidations != 3 {
		t.Errorf("Stats = %+v, want Files=1 Invalidations=3", s)
	}
	// Byte accounting stays exact.
	total, files := diskUsage(t, dir)
	if files != 1 || total != s.Bytes {
		t.Errorf("disk = %d bytes/%d files, index = %d bytes/%d files", total, files, s.Bytes, s.Files)
	}
}
