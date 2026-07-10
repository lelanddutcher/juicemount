package metadata

import (
	"sync"
	"testing"
	"time"
)

// TestKeyspaceRequeueWiring pins the B4' burst-ordering fix's plumbing: a
// requeueFunc stored on rc feeds inodes back into the live coalescer, which
// reconciles them on the next flush. This is the path reconcileDir uses when
// it discovers a never-mirrored child dir (whose own create-events were
// dropped as unknown-ancestor before the parent existed in the mirror).
func TestKeyspaceRequeueWiring(t *testing.T) {
	t.Setenv("JM_WAN_MODE", "")
	SetClassSignals(func() string { return "en21" }, func() bool { return true }) // LAN: debounce 200ms

	var (
		mu         sync.Mutex
		reconciled []uint64
	)
	rc := &RedisClient{}
	rc.testReconcileDir = func(inode uint64) error {
		mu.Lock()
		reconciled = append(reconciled, inode)
		mu.Unlock()
		return nil
	}

	co := newInodeCoalescer(rc)
	requeue := requeueFunc(co.add)
	rc.keyspaceRequeue.Store(&requeue)
	defer rc.keyspaceRequeue.Store(nil)
	defer co.stop()

	// Simulate reconcileDir's post-upsert requeue of a newly-discovered dir.
	if fnp := rc.keyspaceRequeue.Load(); fnp == nil {
		t.Fatal("requeue not wired")
	} else {
		(*fnp)(777)
	}

	// The coalescer's debounce fires within ~200ms (LAN tuning) — poll briefly.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(reconciled)
		mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(reconciled) != 1 || reconciled[0] != 777 {
		t.Fatalf("requeued dir not reconciled: got %v, want [777]", reconciled)
	}
}

// TestKeyspaceRequeueNilSafe: with no live coalescer (SCAN/pinwarm-driven
// reconciles), the requeue pointer is nil and reconcileDir's guard must treat
// that as a no-op — this test just pins the Load()==nil contract.
func TestKeyspaceRequeueNilSafe(t *testing.T) {
	rc := &RedisClient{}
	if fnp := rc.keyspaceRequeue.Load(); fnp != nil {
		t.Fatalf("zero-value requeue pointer should be nil, got %v", fnp)
	}
}
