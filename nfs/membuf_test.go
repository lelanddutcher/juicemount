package nfs

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestMemBufBasic(t *testing.T) {
	mb := NewMemoryBuffer(10*1024*1024, 100*1024*1024) // 10MB threshold, 100MB budget
	defer mb.Stop()

	// Create a small test file in a hermetic temp dir. membuf.loadFile opens
	// fusePath directly, so a real temp file exercises the true load path. The
	// old hardcoded testFUSEPath ("/Users/USER/.juicemount/fuse-internal") never
	// exists on any machine, so os.WriteFile failed (error ignored), os.Open in
	// loadFile then failed, and every Get/ReadAt missed → these 4 tests failed.
	testPath := filepath.Join(t.TempDir(), "__membuf_test.txt")
	testData := []byte("hello memory buffer test data - this is a small file for testing")
	if err := os.WriteFile(testPath, testData, 0644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	// First Get triggers async load, returns nil
	data := mb.Get("__membuf_test.txt", int64(len(testData)), testPath)
	if data != nil {
		t.Log("First Get returned data immediately (fast load)")
	} else {
		t.Log("First Get returned nil (async loading)")
	}

	// Wait for async load
	time.Sleep(500 * time.Millisecond)

	// Second Get should return the buffered data
	data = mb.Get("__membuf_test.txt", int64(len(testData)), testPath)
	if data == nil {
		t.Fatal("Second Get returned nil — buffer should be loaded")
	}
	if string(data) != string(testData) {
		t.Fatalf("data mismatch: got %q", string(data))
	}
	t.Logf("Memory buffer hit: %d bytes", len(data))

	// Stats
	buffered, totalMB, hits, misses, _ := mb.Stats()
	t.Logf("Stats: %d buffered, %.2f MB, hits=%d, misses=%d", buffered, totalMB, hits, misses)
}

func TestMemBufReadAt(t *testing.T) {
	mb := NewMemoryBuffer(10*1024*1024, 100*1024*1024)
	defer mb.Stop()

	testPath := filepath.Join(t.TempDir(), "__membuf_readat.txt")
	testData := make([]byte, 1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	if err := os.WriteFile(testPath, testData, 0644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	// Trigger load
	mb.Get("__membuf_readat.txt", int64(len(testData)), testPath)
	time.Sleep(500 * time.Millisecond)

	// ReadAt at various offsets
	buf := make([]byte, 100)
	n, hit := mb.ReadAt("__membuf_readat.txt", buf, 500, int64(len(testData)), testPath)
	if !hit {
		t.Fatal("ReadAt should hit")
	}
	if n != 100 {
		t.Fatalf("ReadAt: n=%d, want 100", n)
	}
	// Verify data matches
	for i := 0; i < n; i++ {
		if buf[i] != testData[500+i] {
			t.Fatalf("data mismatch at offset %d", 500+i)
		}
	}
	t.Log("ReadAt verified at offset 500")
}

func TestMemBufThreshold(t *testing.T) {
	mb := NewMemoryBuffer(1024, 10*1024*1024) // 1KB threshold
	defer mb.Stop()

	// A file larger than threshold should not be buffered
	data := mb.Get("large_file.mov", 10*1024*1024, "/fake/path")
	if data != nil {
		t.Fatal("file larger than threshold should not be buffered")
	}

	_, _, _, misses, _ := mb.Stats()
	if misses != 0 {
		t.Fatalf("misses=%d, large file should be silently skipped (not counted as miss)", misses)
	}
}

func TestMemBufInvalidate(t *testing.T) {
	mb := NewMemoryBuffer(10*1024*1024, 100*1024*1024)
	defer mb.Stop()

	testPath := filepath.Join(t.TempDir(), "__membuf_invalidate.txt")
	if err := os.WriteFile(testPath, []byte("original"), 0644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	mb.Get("__membuf_invalidate.txt", 8, testPath)
	time.Sleep(500 * time.Millisecond)

	// Should be buffered
	data := mb.Get("__membuf_invalidate.txt", 8, testPath)
	if data == nil {
		t.Fatal("should be buffered")
	}

	// Invalidate
	mb.Invalidate("__membuf_invalidate.txt")

	// Should no longer be buffered
	data = mb.Get("__membuf_invalidate.txt", 8, testPath)
	if data != nil {
		t.Fatal("should not be buffered after invalidate")
	}
}

func TestMemBufBudget(t *testing.T) {
	mb := NewMemoryBuffer(1024*1024, 2*1024*1024) // 1MB threshold, 2MB budget
	defer mb.Stop()

	dir := t.TempDir()
	testPath1 := filepath.Join(dir, "__membuf_budget1.bin")
	testPath2 := filepath.Join(dir, "__membuf_budget2.bin")
	testPath3 := filepath.Join(dir, "__membuf_budget3.bin")

	data := make([]byte, 900*1024) // 900KB each
	for _, p := range []string{testPath1, testPath2, testPath3} {
		if err := os.WriteFile(p, data, 0644); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
	}

	// Load first two (1.8MB total, under 2MB budget)
	mb.Get("__membuf_budget1.bin", 900*1024, testPath1)
	mb.Get("__membuf_budget2.bin", 900*1024, testPath2)
	time.Sleep(500 * time.Millisecond)

	// Third should fail (would exceed 2MB budget)
	result := mb.Get("__membuf_budget3.bin", 900*1024, testPath3)
	if result != nil {
		t.Fatal("third file should not be buffered (exceeds budget)")
	}

	buffered, totalMB, _, _, _ := mb.Stats()
	t.Logf("Budget test: %d files buffered, %.2f MB used (budget=2MB)", buffered, totalMB)

	if buffered != 2 {
		t.Fatalf("expected 2 buffered files, got %d", buffered)
	}
}

// TestMemBufStaleLoadDoesNotClobberReplacement guards the identity bug in
// loadFile's cleanup paths: a load stuck in os.Open on a wedged FUSE mount can
// be Invalidate'd (every write/rename/delete invalidates) and REPLACED by a
// fresh Get for the same path; the stale loader's later cleanup must NOT delete
// the good replacement entry or subtract its bytes from totalSize (a monotonic
// budget leak that eventually disables all buffering until restart).
func TestMemBufStaleLoadDoesNotClobberReplacement(t *testing.T) {
	mb := NewMemoryBuffer(10*1024*1024, 100*1024*1024)
	defer mb.Stop()
	dir := t.TempDir()

	// A FIFO makes loadFile's os.Open BLOCK until we open the write end —
	// deterministically simulating a load wedged on a slow/hung FUSE mount.
	fifo := filepath.Join(dir, "slow.bin")
	if err := syscall.Mkfifo(fifo, 0o644); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}

	// 1. First Get launches loadFile(entry1); it blocks in os.Open(fifo).
	mb.Get("x", 1024, fifo)
	time.Sleep(150 * time.Millisecond) // let the goroutine reach os.Open

	// 2. Invalidate drops entry1; a fresh Get inserts entry2 from a REAL file
	//    of a DIFFERENT size, which loads successfully.
	mb.Invalidate("x")
	real := filepath.Join(dir, "real.bin")
	realData := make([]byte, 4096)
	for i := range realData {
		realData[i] = byte(i)
	}
	if err := os.WriteFile(real, realData, 0o644); err != nil {
		t.Fatal(err)
	}
	mb.Get("x", 4096, real)
	time.Sleep(300 * time.Millisecond) // let entry2 load
	if got := mb.Get("x", 4096, real); got == nil {
		t.Fatal("precondition failed: replacement entry should be buffered")
	}

	// 3. Unblock entry1's os.Open: open+close the FIFO write end with no data
	//    → entry1's read sees EOF → totalRead(0) < 1024 → short-load cleanup.
	w, err := os.OpenFile(fifo, os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open fifo write end: %v", err)
	}
	w.Close()
	time.Sleep(300 * time.Millisecond) // let entry1's stale cleanup run

	// 4. The replacement must survive — a hit, not a re-triggered async miss.
	if got := mb.Get("x", 4096, real); got == nil {
		t.Fatal("stale loadFile clobbered the good replacement entry (identity bug)")
	}
	for i := 0; i < 4096; i++ {
		if got := mb.Get("x", 4096, real); got != nil && got[i] != realData[i] {
			t.Fatalf("replacement data corrupted at %d", i)
		}
	}
}

func TestMemBufThroughNFS(t *testing.T) {
	srv, store := setupReadTestServer(t)
	mountPoint := mountNFS(t, srv.Addr())

	// Find a small file to test memory buffering (skip ._* files)
	var testFile string
	var testSize int64
	children, _ := store.ListChildren(".")
	for _, e := range children {
		if !e.IsDir && e.Size > 0 && e.Size < 1*1024*1024 &&
			!strings.HasPrefix(e.Name, "._") && e.Name != ".DS_Store" {
			testFile = e.Path
			testSize = e.Size
			break
		}
	}
	if testFile == "" {
		t.Skip("no small files found")
	}

	nfsPath := filepath.Join(mountPoint, testFile)

	// First read — may trigger membuf async load
	t.Logf("Testing membuf with %s (%d bytes)", testFile, testSize)
	buf := make([]byte, 4096)
	f, err := os.Open(nfsPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	f.Read(buf)
	f.Close()

	time.Sleep(1 * time.Second) // wait for async load

	// Second read — should hit membuf
	start := time.Now()
	f2, _ := os.Open(nfsPath)
	f2.Read(buf)
	f2.Close()
	dur := time.Since(start)

	t.Logf("Second read (should hit membuf): %v", dur)

	// Check membuf stats
	buffered, totalMB, hits, misses, _ := srv.handler.memBuf.Stats()
	t.Logf("MemBuf stats: %d buffered, %.2f MB, hits=%d, misses=%d",
		buffered, totalMB, hits, misses)
}
