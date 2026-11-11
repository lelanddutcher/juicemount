package nfs

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

// Demand first on a slow link (demand_first.go). The Linux remote lab has no
// NFS layer, so these tests pin the NFS-side ordering: read-ahead waits for
// demand reads, a fresh stream keeps one speculative block in flight, a
// deferred block whose reader left is given back, and a JUKEBOX retry joins
// the pread it repeats instead of starting another.

func TestDemandFirstOnlyOnSlowAndMetered(t *testing.T) {
	t.Setenv("JM_DEMAND_FIRST", "")
	for c, want := range map[netprofile.LinkClass]bool{
		netprofile.ClassSlow: true, netprofile.ClassMetered: true,
		netprofile.ClassMedium: false, netprofile.ClassFast: false,
	} {
		if got := demandFirstForClass(c); got != want {
			t.Fatalf("demand first on %s = %v, want %v", c, got, want)
		}
	}
	t.Setenv("JM_DEMAND_FIRST", "0")
	if demandFirstForClass(netprofile.ClassSlow) {
		t.Fatal("JM_DEMAND_FIRST=0 must turn the rules off")
	}
}

func TestDemandGateWaitIdle(t *testing.T) {
	g := newDemandGate()
	if w, idle := g.waitIdle(time.Second, nil); w != 0 || !idle {
		t.Fatalf("idle gate waited %s (idle %v)", w, idle)
	}
	end1, end2 := g.begin(), g.begin()
	end1()
	end1() // ignored
	if g.inflight() != 1 {
		t.Fatalf("in flight %d, want 1", g.inflight())
	}
	if w, idle := g.waitIdle(40*time.Millisecond, nil); idle || w < 30*time.Millisecond {
		t.Fatalf("busy gate: waited %s, idle %v; want the full bound and not idle", w, idle)
	}
	stop := make(chan struct{})
	close(stop)
	if _, idle := g.waitIdle(time.Minute, stop); idle {
		t.Fatal("a closed stop channel must end the wait")
	}
	go func() { time.Sleep(20 * time.Millisecond); end2() }()
	if _, idle := g.waitIdle(5*time.Second, nil); !idle {
		t.Fatal("the wait did not end when the last demand read ended")
	}
}

func TestDemandStreamCapRamp(t *testing.T) {
	const mib = 1 << 20
	for _, tc := range []struct {
		seq  int64
		want int
	}{{0, 1}, {mib, 1}, {4*mib - 1, 1}, {4 * mib, 2}, {8 * mib, 3}, {64 * mib, 3}} {
		if got := demandStreamCap(tc.seq, 3); got != tc.want {
			t.Fatalf("cap after %d sequential bytes = %d, want %d", tc.seq, got, tc.want)
		}
	}
}

// demandWindowManager is a slow-link window manager with its own demand gate.
func demandWindowManager(t *testing.T, blocks int) (*ReadaheadManager, *blockingReads, string, *demandGate) {
	t.Helper()
	t.Setenv("JM_SERVER_READAHEAD", "")
	t.Setenv("JM_DEMAND_FIRST", "")
	resetPrefetchOfflineState(t)
	forceDefaultClass(t, netprofile.ClassSlow)
	fuseDataInFlight.Store(0)
	t.Cleanup(func() { fuseDataInFlight.Store(0) })
	dir, name := windowTestFile(t, blocks)
	rm := NewReadaheadManager(dir, nil, slowProfile(t))
	g := newDemandGate()
	rm.demand = g
	br := newBlockingReads()
	rm.prefetchReadAt = br.readAt
	t.Cleanup(func() {
		br.unblock()
		rm.Stop()
	})
	return rm, br, name, g
}

func noPrefetchStart(t *testing.T, br *blockingReads, d time.Duration, why string) {
	t.Helper()
	select {
	case off := <-br.started:
		t.Fatalf("a read-ahead block at %d started %s", off, why)
	case <-time.After(d):
	}
}

// Rule 1: a read-ahead block does not start while a demand read is in
// flight; it starts when the demand read ends.
func TestSlowWindowWaitsForDemandRead(t *testing.T) {
	rm, br, name, g := demandWindowManager(t, 32)
	endDemand := g.begin()
	readSequential(rm, 601, name, 0, readaheadBlockSize)
	noPrefetchStart(t, br, 150*time.Millisecond, "while a demand read was in flight")
	endDemand()
	// Blocks start in their own goroutines, so block 1 or 2 may report first.
	if off := awaitPrefetchSignal(t, br.started); off != readaheadBlockSize && off != 2*readaheadBlockSize {
		t.Fatalf("first read-ahead block at %d, want block 1 or 2", off)
	}
}

// Rule 1 is bounded: a demand read that never ends holds read-ahead off for
// demandDefer only.
func TestSlowWindowDemandWaitIsBounded(t *testing.T) {
	rm, br, name, g := demandWindowManager(t, 32)
	rm.demandDefer = 100 * time.Millisecond
	defer g.begin()() // held for the whole test
	start := time.Now()
	readSequential(rm, 602, name, 0, readaheadBlockSize)
	awaitPrefetchSignal(t, br.started)
	if waited := time.Since(start); waited < 80*time.Millisecond {
		t.Fatalf("read-ahead started after %s, before the bound", waited)
	}
}

// Rule 2: a scrub-sized run (1 MiB, the strong WAN signal) gets one block in
// flight. Once the reader has read whole blocks in sequence the window opens
// to the manager cap. The round re-checks the cap as each block ends, so the fake
// blocks here take a moment and then finish.
func TestSlowWindowRampsFromOneBlock(t *testing.T) {
	rm, _, name, _ := demandWindowManager(t, 64)
	var inflight, peak, done atomic.Int32
	rm.prefetchReadAt = func(_ *os.File, buf []byte, _ int64) (int, error) {
		n := inflight.Add(1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		time.Sleep(30 * time.Millisecond)
		inflight.Add(-1)
		done.Add(1)
		return len(buf), nil
	}
	const inode = 603
	readSequential(rm, inode, name, 0, 1<<20)
	waitUntil(t, "the scrub window", func() bool { return done.Load() >= 3 })
	if p := peak.Load(); p != 1 {
		t.Fatalf("peak blocks in flight after a 1 MiB run = %d, want 1", p)
	}
	waitWindowIdle(t, rm, inode)
	peak.Store(0)
	// Past the blocks the scrub window already fetched, so a full window
	// remains to fetch.
	readSequential(rm, inode, name, 1<<20, 10*readaheadBlockSize)
	waitWindowIdle(t, rm, inode)
	if p := peak.Load(); p != slowWindowCap {
		t.Fatalf("peak blocks in flight after ten blocks = %d, want %d", p, slowWindowCap)
	}
}

// Rule 3: a block that waited for a demand read is given back unread when the
// reader seeks away meanwhile.
func TestSlowWindowGivesBackDeferredBlockAfterSeek(t *testing.T) {
	rm, br, name, g := demandWindowManager(t, 400)
	const inode = 604
	endDemand := g.begin()
	readSequential(rm, inode, name, 0, readaheadBlockSize) // claims block 1, deferred
	time.Sleep(30 * time.Millisecond)
	rm.OnRead(inode, 300*readaheadBlockSize, windowTestSubread, name) // a scrub jump
	endDemand()
	noPrefetchStart(t, br, 150*time.Millisecond, "for the old position after the seek")
	if n := rm.windowInflight.Load(); n != 0 {
		t.Fatalf("windowed slots still reserved: %d", n)
	}
}

// Medium and fast links keep read-ahead as it was: no wait for demand reads.
func TestMediumReadaheadDoesNotWaitForDemand(t *testing.T) {
	t.Setenv("JM_SERVER_READAHEAD", "")
	resetPrefetchOfflineState(t)
	dir, name := windowTestFile(t, 32)
	p := netprofile.New()
	c := netprofile.ClassMedium
	p.ForceClass(&c)
	rm := NewReadaheadManager(dir, nil, p)
	g := newDemandGate()
	rm.demand = g
	defer g.begin()() // a demand read in flight throughout
	started := make(chan int64, 64)
	rm.prefetchReadAt = func(_ *os.File, buf []byte, off int64) (int, error) {
		started <- off
		return len(buf), nil
	}
	defer rm.Stop()
	readSequential(rm, 605, name, 0, 4*readaheadBlockSize)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("medium-link read-ahead waited for a demand read")
	}
}

// fakePread is a pread that blocks until released, then returns its bytes.
type fakePread struct {
	calls   atomic.Int32
	release chan struct{}
	once    sync.Once
	data    byte
}

func newFakePread(data byte) *fakePread {
	return &fakePread{release: make(chan struct{}), data: data}
}

func (f *fakePread) unblock() { f.once.Do(func() { close(f.release) }) }

func (f *fakePread) read(p []byte, _ int64) (int, error) {
	f.calls.Add(1)
	<-f.release
	for i := range p {
		p[i] = f.data
	}
	return len(p), nil
}

func resetPendingReads(t *testing.T) {
	t.Helper()
	t.Setenv("JM_FUSE_READ_JOIN", "")
	prior := pendingFUSEReadResults
	pendingFUSEReadResults = newPendingFUSEReads(time.Minute, 64)
	fuseDataInFlight.Store(0)
	t.Cleanup(func() {
		pendingFUSEReadResults = prior
		fuseDataInFlight.Store(0)
	})
}

// The lab's retry loop: a pread passes its bound (JUKEBOX); the client
// retries the same read. The retry joins the running pread, takes no new data
// slot and starts no new pread; when the pread ends, the next retry gets its
// bytes; later reads read normally again.
func TestJUKEBOXRetryJoinsRunningPread(t *testing.T) {
	resetPendingReads(t)
	fp := newFakePread(0xAB)
	defer fp.unblock()
	const path, off, bound = "/fuse/reel.mov", int64(1 << 20), 40 * time.Millisecond
	p := make([]byte, 256<<10)

	if _, err := joinOrReadFUSE(fp.read, path, "reel.mov", p, off, bound); !errors.Is(err, errFUSETimeout) {
		t.Fatalf("first read err %v, want the JUKEBOX timeout", err)
	}
	if foregroundDemand.inflight() != 1 {
		t.Fatalf("the timed-out pread must still count as a demand read (in flight %d)", foregroundDemand.inflight())
	}
	inUse, _ := fuseDataGateDepth()
	for i := 0; i < 3; i++ {
		if _, err := joinOrReadFUSE(fp.read, path, "reel.mov", p, off, bound); !errors.Is(err, errFUSETimeout) {
			t.Fatalf("retry %d err %v, want JUKEBOX while the pread runs", i, err)
		}
	}
	if n := fp.calls.Load(); n != 1 {
		t.Fatalf("preads started = %d, want 1 (retries must join)", n)
	}
	if now, _ := fuseDataGateDepth(); now != inUse {
		t.Fatalf("retries took data slots: %d -> %d", inUse, now)
	}

	fp.unblock()
	waitUntil(t, "the pread to end", func() bool { return foregroundDemand.inflight() == 0 })
	for i := range p {
		p[i] = 0
	}
	n, err := joinOrReadFUSE(fp.read, path, "reel.mov", p, off, bound)
	if err != nil || n != len(p) || p[0] != 0xAB || p[len(p)-1] != 0xAB {
		t.Fatalf("retry after the pread ended: n=%d err=%v first=%x", n, err, p[0])
	}
	if c := fp.calls.Load(); c != 1 || pendingFUSEReadResults.len() != 0 {
		t.Fatalf("served retry: preads %d (want 1), registry %d (want 0)", c, pendingFUSEReadResults.len())
	}
	if inUse, _ := fuseDataGateDepth(); inUse != 0 {
		t.Fatalf("data gate leaked %d slots", inUse)
	}
	// The result answers one retry only; the next read is a new pread.
	if _, err := joinOrReadFUSE(fp.read, path, "reel.mov", p, off, bound); err != nil {
		t.Fatalf("fresh read: %v", err)
	}
	if c := fp.calls.Load(); c != 2 {
		t.Fatalf("preads %d, want 2 after the served result was consumed", c)
	}
}

// A retry that waits on the joined pread returns its bytes as soon as the
// pread ends, not at the next bound.
func TestJUKEBOXRetryReturnsWhenJoinedPreadEnds(t *testing.T) {
	resetPendingReads(t)
	fp := newFakePread(0x11)
	defer fp.unblock()
	p := make([]byte, 4096)
	if _, err := joinOrReadFUSE(fp.read, "/fuse/a", "a", p, 0, 20*time.Millisecond); !errors.Is(err, errFUSETimeout) {
		t.Fatalf("err %v, want timeout", err)
	}
	go func() { time.Sleep(30 * time.Millisecond); fp.unblock() }()
	start := time.Now()
	n, err := joinOrReadFUSE(fp.read, "/fuse/a", "a", p, 0, 5*time.Second)
	if err != nil || n != len(p) || p[0] != 0x11 {
		t.Fatalf("joined retry: n=%d err=%v", n, err)
	}
	if waited := time.Since(start); waited > 2*time.Second {
		t.Fatalf("joined retry waited %s", waited)
	}
}

// Only the same file, offset and length join. Another read starts its own
// pread.
func TestJUKEBOXJoinNeedsSameRead(t *testing.T) {
	resetPendingReads(t)
	fp := newFakePread(0x22)
	defer fp.unblock()
	p := make([]byte, 4096)
	joinOrReadFUSE(fp.read, "/fuse/a", "a", p, 0, 10*time.Millisecond)
	joinOrReadFUSE(fp.read, "/fuse/a", "a", p, 4096, 10*time.Millisecond)
	joinOrReadFUSE(fp.read, "/fuse/b", "b", p, 0, 10*time.Millisecond)
	joinOrReadFUSE(fp.read, "/fuse/a", "a", p[:1024], 0, 10*time.Millisecond)
	if c := fp.calls.Load(); c != 4 {
		t.Fatalf("preads %d, want 4 distinct reads", c)
	}
	if pendingFUSEReadResults.len() != 4 {
		t.Fatalf("registry holds %d, want 4", pendingFUSEReadResults.len())
	}
	fp.unblock()
	waitUntil(t, "preads to end", func() bool { return foregroundDemand.inflight() == 0 })
}

// JM_FUSE_READ_JOIN=0 restores the old behavior: every retry is a new pread.
// A full registry falls back the same way.
func TestJUKEBOXJoinRollbackAndBound(t *testing.T) {
	for _, mode := range []string{"rollback", "full"} {
		t.Run(mode, func(t *testing.T) {
			resetPendingReads(t)
			if mode == "rollback" {
				t.Setenv("JM_FUSE_READ_JOIN", "0")
			} else {
				pendingFUSEReadResults = newPendingFUSEReads(time.Minute, 0)
			}
			fp := newFakePread(0x33)
			p := make([]byte, 4096)
			for i := 0; i < 2; i++ {
				joinOrReadFUSE(fp.read, "/fuse/a", "a", p, 0, 10*time.Millisecond)
			}
			if c := fp.calls.Load(); c != 2 {
				t.Fatalf("preads %d, want 2 (no join)", c)
			}
			if pendingFUSEReadResults.len() != 0 {
				t.Fatal("nothing may be registered")
			}
			fp.unblock()
			waitUntil(t, "preads to end", func() bool { return foregroundDemand.inflight() == 0 })
		})
	}
}

// A finished result that no retry claims is dropped after the TTL.
func TestPendingReadExpires(t *testing.T) {
	resetPendingReads(t)
	pendingFUSEReadResults = newPendingFUSEReads(30*time.Millisecond, 64)
	fp := newFakePread(0x44)
	p := make([]byte, 4096)
	joinOrReadFUSE(fp.read, "/fuse/a", "a", p, 0, 10*time.Millisecond)
	fp.unblock()
	waitUntil(t, "the result to expire", func() bool { return pendingFUSEReadResults.len() == 0 })
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}
