package nfs

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMemBufBasic(t *testing.T) {
	mb := NewMemoryBuffer(10*1024*1024, 100*1024*1024) // 10MB threshold, 100MB budget
	defer mb.Stop()

	// Create a small test file on FUSE
	testPath := filepath.Join(testFUSEPath, "__membuf_test.txt")
	testData := []byte("hello memory buffer test data - this is a small file for testing")
	os.WriteFile(testPath, testData, 0644)
	defer os.Remove(testPath)

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

	testPath := filepath.Join(testFUSEPath, "__membuf_readat.txt")
	testData := make([]byte, 1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	os.WriteFile(testPath, testData, 0644)
	defer os.Remove(testPath)

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

	testPath := filepath.Join(testFUSEPath, "__membuf_invalidate.txt")
	os.WriteFile(testPath, []byte("original"), 0644)
	defer os.Remove(testPath)

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

	testPath1 := filepath.Join(testFUSEPath, "__membuf_budget1.bin")
	testPath2 := filepath.Join(testFUSEPath, "__membuf_budget2.bin")
	testPath3 := filepath.Join(testFUSEPath, "__membuf_budget3.bin")

	data := make([]byte, 900*1024) // 900KB each
	os.WriteFile(testPath1, data, 0644)
	os.WriteFile(testPath2, data, 0644)
	os.WriteFile(testPath3, data, 0644)
	defer os.Remove(testPath1)
	defer os.Remove(testPath2)
	defer os.Remove(testPath3)

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
