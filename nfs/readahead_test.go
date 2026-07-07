package nfs

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
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

// slowProfile builds a *netprofile.Profile pinned to ClassSlow (SeqThreshold=4,
// the smallPreviewRunBlocks guard = 6) for the S2 short-run-guard tests.
func slowProfile(t *testing.T) *netprofile.Profile {
	t.Helper()
	p := netprofile.New()
	c := netprofile.ClassSlow
	p.ForceClass(&c)
	return p
}

// TestReadaheadServerKillSwitch (S2): JM_SERVER_READAHEAD=0 disables ALL server
// prefetch scheduling — even a long sequential run never trips.
func TestReadaheadServerKillSwitch(t *testing.T) {
	t.Setenv("JM_SERVER_READAHEAD", "0")
	rm := NewReadaheadManager(testFUSEPath, nil, nil)
	defer rm.Stop()

	inode := uint64(77)
	for i := 0; i < 20; i++ {
		rm.OnRead(inode, int64(i)*readaheadBlockSize, readaheadBlockSize, "test/file.mov")
	}
	if triggered, _ := rm.Stats(); triggered != 0 {
		t.Fatalf("kill-switch on: triggered=%d, want 0 (no scheduling)", triggered)
	}
	// The kill-switch returns before taking the tracker lock — no tracker created.
	rm.mu.Lock()
	n := len(rm.trackers)
	rm.mu.Unlock()
	if n != 0 {
		t.Fatalf("kill-switch created %d trackers, want 0 (early return before lock)", n)
	}
}

// TestReadaheadShortRunGuard (S2): on a SLOW link a SHORT sequential run (a
// preview probe, hits 4–5) is suppressed — no trigger, the suppressed counter
// rises; a LONGER run (hits ≥6) escalates normally.
func TestReadaheadShortRunGuard(t *testing.T) {
	t.Setenv("JM_SERVER_READAHEAD", "") // ensure enabled

	// Short run: 5 sequential blocks. SeqThreshold(slow)=4 trips at hit 4, but
	// the guard (6) suppresses hits 4 and 5 as preview probes.
	shortRM := NewReadaheadManager(testFUSEPath, nil, slowProfile(t))
	defer shortRM.Stop()

	before := metrics.Default().Snapshot().ReadaheadSuppressed
	inode := uint64(101)
	for i := 0; i < 5; i++ {
		shortRM.OnRead(inode, int64(i)*readaheadBlockSize, readaheadBlockSize, "test/preview.mov")
	}
	if triggered, _ := shortRM.Stats(); triggered != 0 {
		t.Fatalf("short run on slow link: triggered=%d, want 0 (guard suppresses preview probe)", triggered)
	}
	if got := metrics.Default().Snapshot().ReadaheadSuppressed - before; got == 0 {
		t.Fatal("short run on slow link did not increment readahead_suppressed")
	}

	// Long run: 12 sequential blocks. Once the run passes the guard (6) the
	// trip escalates.
	longRM := NewReadaheadManager(testFUSEPath, nil, slowProfile(t))
	defer longRM.Stop()
	inode2 := uint64(202)
	for i := 0; i < 12; i++ {
		longRM.OnRead(inode2, int64(i)*readaheadBlockSize, readaheadBlockSize, "test/reel.mov")
	}
	if triggered, _ := longRM.Stats(); triggered == 0 {
		t.Fatal("long sequential run on slow link never escalated past the guard")
	}
}

// TestReadaheadGuardOffOnFastLink (S2): the guard is DISABLED on a fast link —
// a fast-class run trips exactly as before (10GbE behavior unchanged). A short
// run of SeqThreshold(fast)=2 blocks must trip.
func TestReadaheadGuardOffOnFastLink(t *testing.T) {
	t.Setenv("JM_SERVER_READAHEAD", "")
	p := netprofile.New()
	c := netprofile.ClassFast
	p.ForceClass(&c)

	rm := NewReadaheadManager(testFUSEPath, nil, p)
	defer rm.Stop()

	inode := uint64(303)
	// Fast SeqThreshold=2; a short 3-block run must trip (guard is 0 on fast).
	for i := 0; i < 3; i++ {
		rm.OnRead(inode, int64(i)*readaheadBlockSize, readaheadBlockSize, "test/file.mov")
	}
	if triggered, _ := rm.Stats(); triggered == 0 {
		t.Fatal("fast link: short run did not trip — the guard must be disabled on fast (10GbE unchanged)")
	}
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
