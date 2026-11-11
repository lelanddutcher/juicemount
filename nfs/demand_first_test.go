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

// fakePread is a pread that blocks until released, then returns the file's
// content at that moment (content can change, like a file replaced on disk).
type fakePread struct {
	calls   atomic.Int32
	release chan struct{}
	once    sync.Once
	content atomic.Value // byte
}

func newFakePread(data byte) *fakePread {
	f := &fakePread{release: make(chan struct{})}
	f.content.Store(data)
	return f
}

func (f *fakePread) unblock() { f.once.Do(func() { close(f.release) }) }

func (f *fakePread) read(p []byte, _ int64) (int, error) {
	f.calls.Add(1)
	<-f.release
	b := f.content.Load().(byte)
	for i := range p {
		p[i] = b
	}
	return len(p), nil
}

// resetPendingReads gives the test its own registry, a slow link (the join
// applies on slow and metered links only) and an empty data gate.
func resetPendingReads(t *testing.T) {
	t.Helper()
	t.Setenv("JM_FUSE_READ_JOIN", "")
	t.Setenv("JM_DEMAND_FIRST", "")
	prior := pendingFUSEReadResults
	pendingFUSEReadResults = newPendingFUSEReads(64)
	priorClass := demandFirstClass
	demandFirstClass = func() netprofile.LinkClass { return netprofile.ClassSlow }
	fuseDataInFlight.Store(0)
	t.Cleanup(func() {
		pendingFUSEReadResults = prior
		demandFirstClass = priorClass
		fuseDataInFlight.Store(0)
	})
}

var testFD = new(os.File)

func readKey(name string, inode uint64, mtime int64, off int64, n int) *pendingReadKey {
	return &pendingReadKey{name: name, inode: inode, size: 64 << 20, mtime: mtime, fd: testFD, off: off, n: n}
}

func waitPreadsDone(t *testing.T) {
	t.Helper()
	waitUntil(t, "preads to end", func() bool { return foregroundDemand.inflight() == 0 && pendingFUSEReadResults.len() == 0 })
}

// The lab's retry loop: a pread passes its bound (JUKEBOX); the client
// retries the same read. The retry joins the running pread, takes no new data
// slot and starts no new pread; a retry waiting when the pread ends gets its
// bytes.
func TestJUKEBOXRetryJoinsRunningPread(t *testing.T) {
	resetPendingReads(t)
	fp := newFakePread(0xAB)
	defer fp.unblock()
	const off, bound = int64(1 << 20), 40 * time.Millisecond
	key := readKey("reel.mov", 7, 1, off, 256<<10)
	p := make([]byte, 256<<10)

	if _, err := joinOrReadFUSE(fp.read, key, "reel.mov", p, off, bound); !errors.Is(err, errFUSETimeout) {
		t.Fatalf("first read err %v, want the JUKEBOX timeout", err)
	}
	if foregroundDemand.inflight() != 1 {
		t.Fatalf("the timed-out pread must still count as a demand read (in flight %d)", foregroundDemand.inflight())
	}
	inUse, _ := fuseDataGateDepth()
	for i := 0; i < 3; i++ {
		if _, err := joinOrReadFUSE(fp.read, key, "reel.mov", p, off, bound); !errors.Is(err, errFUSETimeout) {
			t.Fatalf("retry %d err %v, want JUKEBOX while the pread runs", i, err)
		}
	}
	if n := fp.calls.Load(); n != 1 {
		t.Fatalf("preads started = %d, want 1 (retries must join)", n)
	}
	if now, _ := fuseDataGateDepth(); now != inUse {
		t.Fatalf("retries took data slots: %d -> %d", inUse, now)
	}
	go func() { time.Sleep(30 * time.Millisecond); fp.unblock() }()
	for i := range p {
		p[i] = 0
	}
	n, err := joinOrReadFUSE(fp.read, key, "reel.mov", p, off, 5*time.Second)
	if err != nil || n != len(p) || p[0] != 0xAB || p[len(p)-1] != 0xAB {
		t.Fatalf("waiting retry: n=%d err=%v first=%x", n, err, p[0])
	}
	waitPreadsDone(t)
	if c := fp.calls.Load(); c != 1 {
		t.Fatalf("preads %d, want 1", c)
	}
	if inUse, _ := fuseDataGateDepth(); inUse != 0 {
		t.Fatalf("data gate leaked %d slots", inUse)
	}
}

// The review's probe (C1): a pread times out, ends with nobody waiting, and
// the file changes. A later read must get the new bytes, not the old result.
func TestJUKEBOXFinishedResultIsNotKept(t *testing.T) {
	resetPendingReads(t)
	fp := newFakePread('O')
	key := readKey("reel.mov", 7, 1, 0, 4)
	p := make([]byte, 4)
	if _, err := joinOrReadFUSE(fp.read, key, "reel.mov", p, 0, 20*time.Millisecond); !errors.Is(err, errFUSETimeout) {
		t.Fatalf("err %v, want timeout", err)
	}
	fp.unblock() // the old pread ends, nobody waits for it
	waitPreadsDone(t)
	fp.content.Store(byte('N')) // the range is rewritten (same open identity)
	n, err := joinOrReadFUSE(fp.read, key, "reel.mov", p, 0, time.Second)
	if err != nil || n != 4 || string(p) != "NNNN" {
		t.Fatalf("read after the old pread ended = %q, %v; want the new bytes", p[:n], err)
	}
	if c := fp.calls.Load(); c != 2 {
		t.Fatalf("preads %d, want 2 (a fresh pread)", c)
	}
}

// A replacement by rename is a new inode, an in-place write a new mtime:
// neither joins a pread of the old content, even while it still runs.
func TestJUKEBOXReplacedFileNeverJoins(t *testing.T) {
	resetPendingReads(t)
	fp := newFakePread('O')
	p := make([]byte, 4)
	for _, k := range []*pendingReadKey{
		readKey("reel.mov", 7, 1, 0, 4), // original
		readKey("reel.mov", 8, 1, 0, 4), // renamed over: new inode
		readKey("reel.mov", 7, 2, 0, 4), // written in place: new mtime
	} {
		if _, err := joinOrReadFUSE(fp.read, k, "reel.mov", p, 0, 10*time.Millisecond); !errors.Is(err, errFUSETimeout) {
			t.Fatalf("err %v, want timeout", err)
		}
	}
	if c := fp.calls.Load(); c != 3 {
		t.Fatalf("preads %d, want 3: a changed file must not join the old pread", c)
	}
	fp.unblock()
	waitPreadsDone(t)
}

// Invalidating a path (write, rename, remove, remote event) drops its
// entries; a retry already waiting gets JUKEBOX, never the old bytes, and
// the next retry reads afresh. A directory invalidation covers its subtree.
func TestJUKEBOXInvalidationDropsJoin(t *testing.T) {
	for _, tc := range []struct {
		name, forget string
		tree         bool
	}{{"file", "dir/reel.mov", false}, {"tree", "dir", true}} {
		t.Run(tc.name, func(t *testing.T) {
			resetPendingReads(t)
			fp := newFakePread('O')
			key := readKey("dir/reel.mov", 7, 1, 0, 4)
			p := make([]byte, 4)
			joinOrReadFUSE(fp.read, key, "dir/reel.mov", p, 0, 10*time.Millisecond)
			got := make(chan error, 1)
			go func() {
				q := make([]byte, 4)
				_, err := joinOrReadFUSE(fp.read, key, "dir/reel.mov", q, 0, 5*time.Second)
				got <- err
			}()
			time.Sleep(20 * time.Millisecond) // the retry is waiting on the pread
			pendingFUSEReadResults.forget(tc.forget, tc.tree)
			if pendingFUSEReadResults.len() != 0 {
				t.Fatal("forget left the entry")
			}
			fp.unblock()
			if err := <-got; !errors.Is(err, errFUSETimeout) {
				t.Fatalf("waiting retry err %v, want JUKEBOX after invalidation", err)
			}
			waitPreadsDone(t)
		})
	}
	resetPendingReads(t)
	pendingFUSEReadResults.forget("dir", true) // other names are untouched
}

// A pread older than a few bounds is presumed wedged (a dead mount): a retry
// starts its own instead of joining it for ever.
func TestJUKEBOXDoesNotJoinWedgedPread(t *testing.T) {
	resetPendingReads(t)
	fp := newFakePread('O')
	defer fp.unblock()
	key := readKey("reel.mov", 7, 1, 0, 4)
	p := make([]byte, 4)
	const bound = 10 * time.Millisecond
	joinOrReadFUSE(fp.read, key, "reel.mov", p, 0, bound)
	time.Sleep((pendingReadJoinMaxAge + 1) * bound)
	joinOrReadFUSE(fp.read, key, "reel.mov", p, 0, bound)
	if c := fp.calls.Load(); c != 2 {
		t.Fatalf("preads %d, want 2: a wedged pread must not be joined", c)
	}
}

// Only the same file, descriptor, offset and length join.
func TestJUKEBOXJoinNeedsSameRead(t *testing.T) {
	resetPendingReads(t)
	fp := newFakePread(0x22)
	p := make([]byte, 4096)
	other := *readKey("a", 1, 1, 0, 4096)
	other.fd = new(os.File) // after a remount: a new descriptor
	for _, k := range []*pendingReadKey{
		readKey("a", 1, 1, 0, 4096), readKey("a", 1, 1, 4096, 4096),
		readKey("b", 2, 1, 0, 4096), readKey("a", 1, 1, 0, 1024), &other,
	} {
		joinOrReadFUSE(fp.read, k, k.name, p[:k.n], k.off, 10*time.Millisecond)
	}
	if c := fp.calls.Load(); c != 5 {
		t.Fatalf("preads %d, want 5 distinct reads", c)
	}
	fp.unblock()
	waitPreadsDone(t)
}

// The join is off on medium and fast links, with JM_FUSE_READ_JOIN=0, for a
// read without identity, and when the registry is full: every retry is a new
// pread, as before.
func TestJUKEBOXJoinOffCases(t *testing.T) {
	for _, mode := range []string{"medium", "rollback", "full", "no-key"} {
		t.Run(mode, func(t *testing.T) {
			resetPendingReads(t)
			key := readKey("a", 1, 1, 0, 4096)
			switch mode {
			case "medium":
				demandFirstClass = func() netprofile.LinkClass { return netprofile.ClassMedium }
			case "rollback":
				t.Setenv("JM_FUSE_READ_JOIN", "0")
			case "full":
				pendingFUSEReadResults = newPendingFUSEReads(0)
			case "no-key":
				key = nil
			}
			fp := newFakePread(0x33)
			p := make([]byte, 4096)
			for i := 0; i < 2; i++ {
				joinOrReadFUSE(fp.read, key, "a", p, 0, 10*time.Millisecond)
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

// cachedFile names its reads by open-time identity; AppleDouble sidecars and
// files without identity never join (a sidecar read seeds the shared cache).
func TestCachedFilePendingReadKey(t *testing.T) {
	mt := time.Unix(100, 5)
	f := &cachedFile{name: "dir/reel.mov", inode: 9, fuseFD: testFD, cachedInfo: &snapshotFileInfo{size: 77, mtime: mt}}
	k := f.pendingReadKey(4096, 512)
	if k == nil || k.name != "dir/reel.mov" || k.inode != 9 || k.size != 77 || k.mtime != mt.UnixNano() ||
		k.fd != testFD || k.off != 4096 || k.n != 512 {
		t.Fatalf("key = %+v", k)
	}
	if (&cachedFile{name: "dir/._reel.mov", fuseFD: testFD, cachedInfo: &snapshotFileInfo{}}).pendingReadKey(0, 1) != nil {
		t.Fatal("an AppleDouble sidecar must not join")
	}
	if (&cachedFile{name: "x", fuseFD: testFD}).pendingReadKey(0, 1) != nil {
		t.Fatal("a file without open-time identity must not join")
	}
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
