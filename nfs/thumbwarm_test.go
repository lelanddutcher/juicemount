package nfs

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// fakeThumbCache is an in-RAM ThumbBlobCache.
type fakeThumbCache struct {
	mu   sync.Mutex
	blob map[string][]byte
}

func newFakeThumbCache() *fakeThumbCache {
	return &fakeThumbCache{blob: make(map[string][]byte)}
}
func (f *fakeThumbCache) key(inode uint64, kind string) string {
	return fmt.Sprintf("%d/%s", inode, kind)
}
func (f *fakeThumbCache) Has(inode uint64, kind string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.blob[f.key(inode, kind)]
	return ok
}
func (f *fakeThumbCache) Put(inode uint64, kind string, r io.Reader) (int64, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}
	f.mu.Lock()
	f.blob[f.key(inode, kind)] = b
	f.mu.Unlock()
	return int64(len(b)), nil
}
func (f *fakeThumbCache) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.blob)
}

// waitForCond polls until cond or timeout.
func waitForCond(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

// TestThumbWarmerHydratesDir: a listed dir's children with ready thumbnails
// land in the cache; children without stay negative and are not re-resolved
// on a second visit.
func TestThumbWarmerHydratesDir(t *testing.T) {
	dir := t.TempDir()
	// Two real blobs on "FUSE" (plain files stand in for the mount).
	blobA := filepath.Join(dir, "a.jpg")
	blobB := filepath.Join(dir, "b.jpg")
	os.WriteFile(blobA, []byte("poster-a"), 0644)
	os.WriteFile(blobB, []byte("poster-b"), 0644)

	var callsMu sync.Mutex
	resolveCalls := make(map[uint64]int)
	resolve := func(inode uint64) (string, bool) {
		callsMu.Lock()
		resolveCalls[inode]++
		callsMu.Unlock()
		switch inode {
		case 11:
			return blobA, true
		case 12:
			return blobB, true
		default:
			return "", false // inode 13: no derivative
		}
	}
	resolveCount := func(inode uint64) int {
		callsMu.Lock()
		defer callsMu.Unlock()
		return resolveCalls[inode]
	}
	children := func(d string) []ThumbChildRef {
		if d != "proj/reel1" {
			return nil
		}
		return []ThumbChildRef{
			{Inode: 11, Name: "a.mov"},
			{Inode: 12, Name: "b.mov"},
			{Inode: 13, Name: "c.mov"},
			{Inode: 14, Name: "sub", IsDir: true},
			{Inode: 15, Name: "._a.mov"},
		}
	}
	cache := newFakeThumbCache()
	w := NewThumbWarmer(cache, resolve, children)
	defer w.Stop()

	w.WarmDirAsync("proj/reel1")
	waitForCond(t, "2 thumbs hydrated", func() bool { return cache.count() == 2 })
	if !cache.Has(11, thumbWarmKind) || !cache.Has(12, thumbWarmKind) {
		t.Fatal("hydrated blobs missing from cache")
	}
	if cache.Has(13, thumbWarmKind) || cache.Has(14, thumbWarmKind) || cache.Has(15, thumbWarmKind) {
		t.Fatal("negative/dir/AppleDouble child was hydrated")
	}

	// Second visit inside the dedupe TTL: dropped at enqueue — no new
	// resolve calls for ANY inode (incl. the negative one).
	before := resolveCount(13)
	w.WarmDirAsync("proj/reel1")
	time.Sleep(150 * time.Millisecond)
	if got := resolveCount(13); got != before {
		t.Fatalf("deduped visit re-resolved negative inode (calls %d -> %d)", before, got)
	}
}

// TestThumbWarmerNegativeTTL: with the dedupe bypassed (fresh dirs listing
// the same child), a negative inode is resolved once and then served from
// the negative cache.
func TestThumbWarmerNegativeTTL(t *testing.T) {
	var calls int
	var mu sync.Mutex
	resolve := func(inode uint64) (string, bool) {
		mu.Lock()
		calls++
		mu.Unlock()
		return "", false
	}
	children := func(d string) []ThumbChildRef {
		return []ThumbChildRef{{Inode: 42, Name: "x.mov"}}
	}
	w := NewThumbWarmer(newFakeThumbCache(), resolve, children)
	defer w.Stop()

	w.WarmDirAsync("d1")
	waitForCond(t, "first resolve", func() bool { mu.Lock(); defer mu.Unlock(); return calls == 1 })
	w.WarmDirAsync("d2") // different dir, same child inode
	time.Sleep(150 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("negative inode resolved %d times, want 1 (negative cache miss)", calls)
	}
}

// TestThumbWarmerOversizedBlobRejected: a blob past thumbWarmBlobCap is not
// cached and the inode goes negative.
func TestThumbWarmerOversizedBlobRejected(t *testing.T) {
	dir := t.TempDir()
	big := filepath.Join(dir, "big.jpg")
	f, _ := os.Create(big)
	f.Truncate(thumbWarmBlobCap + 1)
	f.Close()

	resolve := func(inode uint64) (string, bool) { return big, true }
	children := func(d string) []ThumbChildRef {
		return []ThumbChildRef{{Inode: 7, Name: "huge.mov"}}
	}
	cache := newFakeThumbCache()
	w := NewThumbWarmer(cache, resolve, children)
	defer w.Stop()

	w.WarmDirAsync("d")
	time.Sleep(300 * time.Millisecond)
	if cache.count() != 0 {
		t.Fatal("oversized blob was cached")
	}
}

// TestThumbWarmerNilSafe: the readdir hook path with no warmer wired.
func TestThumbWarmerNilSafe(t *testing.T) {
	var w *ThumbWarmer
	w.WarmDirAsync("anything") // must not panic
	w.Stop()
}
