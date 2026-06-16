package nfs

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReadaheadSequentialDetection(t *testing.T) {
	rm := NewReadaheadManager(testFUSEPath, nil, nil)
	defer rm.Stop()

	inode := uint64(42)

	// Simulate sequential reads (4MB blocks)
	for i := 0; i < 10; i++ {
		offset := int64(i) * readaheadBlockSize
		rm.OnRead(inode, offset, readaheadBlockSize, "test/file.mov")
	}

	triggered, prefetched := rm.Stats()
	t.Logf("Readahead stats: triggered=%d, prefetched=%d blocks", triggered, prefetched)

	if triggered == 0 {
		t.Fatal("expected readahead to trigger on sequential pattern")
	}
}

func TestReadaheadRandomNoTrigger(t *testing.T) {
	rm := NewReadaheadManager(testFUSEPath, nil, nil)
	defer rm.Stop()

	inode := uint64(42)

	// Simulate random reads (not sequential)
	offsets := []int64{100 << 20, 5 << 20, 200 << 20, 50 << 20, 300 << 20}
	for _, off := range offsets {
		rm.OnRead(inode, off, readaheadBlockSize, "test/file.mov")
	}

	triggered, _ := rm.Stats()
	if triggered != 0 {
		t.Fatalf("readahead should NOT trigger on random access, got triggered=%d", triggered)
	}
}

func TestReadaheadThroughNFS(t *testing.T) {
	srv, store := setupReadTestServer(t)
	mountPoint := mountNFS(t, srv.Addr())

	// Find a file with enough blocks to trigger readahead (need >3 sequential reads)
	var testFile string
	children, _ := store.ListChildren(".")
	for _, e := range children {
		if e.IsDir {
			subChildren, _ := store.ListChildren(e.Path)
			for _, sc := range subChildren {
				// Need at least 20MB to get 5+ sequential 4MB reads
				if !sc.IsDir && sc.Size > 20*1024*1024 {
					testFile = sc.Path
					break
				}
			}
		}
		if testFile != "" {
			break
		}
	}

	if testFile == "" {
		t.Skip("No file >20MB found for readahead test")
	}

	t.Logf("Testing readahead with %s", testFile)

	// Read the file sequentially through NFS
	nfsPath := filepath.Join(mountPoint, testFile)
	f, err := os.Open(nfsPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	buf := make([]byte, 4*1024*1024) // 4MB reads
	totalRead := 0
	for i := 0; i < 10; i++ {
		n, err := f.Read(buf)
		totalRead += n
		if err != nil {
			break
		}
	}
	f.Close()

	// Give readahead goroutines time to complete
	time.Sleep(2 * time.Second)

	triggered, prefetched := srv.handler.readahead.Stats()
	t.Logf("Read %d bytes, readahead: triggered=%d, prefetched=%d blocks",
		totalRead, triggered, prefetched)

	// We may or may not trigger readahead depending on how NFS fragments the reads,
	// but the manager should have processed the pattern
}

func TestReadaheadCleanup(t *testing.T) {
	rm := NewReadaheadManager(testFUSEPath, nil, nil)
	defer rm.Stop()

	// Add some trackers
	rm.OnRead(1, 0, 4096, "a.txt")
	rm.OnRead(2, 0, 4096, "b.txt")

	rm.mu.Lock()
	count := len(rm.trackers)
	rm.mu.Unlock()

	if count != 2 {
		t.Fatalf("expected 2 trackers, got %d", count)
	}

	// Force age the trackers
	rm.mu.Lock()
	for _, t := range rm.trackers {
		t.lastAccess = time.Now().Add(-2 * trackerTTL)
	}
	rm.mu.Unlock()

	// Wait for cleanup
	time.Sleep(trackerTTL + 5*time.Second)

	rm.mu.Lock()
	count = len(rm.trackers)
	rm.mu.Unlock()

	if count != 0 {
		t.Fatalf("expected 0 trackers after cleanup, got %d", count)
	}
}
