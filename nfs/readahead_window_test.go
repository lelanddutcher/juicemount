package nfs

import (
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

// Slow-link windowed readahead. Before this change a slow link kept about one
// 4 MB block in flight: each round walked one buffer serially, started at an
// unaligned offset that mostly re-read the previous round, and was dropped
// when two rounds already ran. These tests pin the replacement: whole aligned
// blocks, each fetched once, up to Workers in flight, foreground slots kept.

const windowTestSubread = 256 << 10

// slowWindowCap is the windowed blocks in flight on Slow: Workers 4, but one
// below the background gate share (8/2 = 4), so 3.
const slowWindowCap = 3

// windowTestFile makes a sparse file of n blocks in a fresh directory.
func windowTestFile(t *testing.T, blocks int) (string, string) {
	t.Helper()
	dir := t.TempDir()
	const name = "reel.mov"
	f, err := os.Create(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(int64(blocks) * readaheadBlockSize); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return dir, name
}

// forceDefaultClass pins the process profile, which the FUSE data gate and the
// Read-QoS lanes consult, so the gate runs at the slow-link width.
func forceDefaultClass(t *testing.T, c netprofile.LinkClass) {
	t.Helper()
	netprofile.Default().ForceClass(&c)
	t.Cleanup(func() { netprofile.Default().ForceClass(nil) })
}

// readSequential issues foreground-shaped 256 KiB subreads over [from, to).
func readSequential(rm *ReadaheadManager, inode uint64, name string, from, to int64) {
	for off := from; off < to; off += windowTestSubread {
		rm.OnRead(inode, off, windowTestSubread, name)
	}
}

// waitWindowIdle waits until no pump serves inode and no round is active.
func waitWindowIdle(t *testing.T, rm *ReadaheadManager, inode uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		rm.mu.Lock()
		tr := rm.trackers[inode]
		busy := tr != nil && tr.pumping
		rm.mu.Unlock()
		if !busy && rm.activePrefetch.Load() == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("windowed prefetch did not go idle")
}

type readLog struct {
	mu   sync.Mutex
	offs []int64
	lens []int
}

func (l *readLog) add(off int64, n int) {
	l.mu.Lock()
	l.offs = append(l.offs, off)
	l.lens = append(l.lens, n)
	l.mu.Unlock()
}

func (l *readLog) snapshot() ([]int64, []int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]int64(nil), l.offs...), append([]int(nil), l.lens...)
}

func TestSlowPolicyIsWindowedAndMeteredIsNot(t *testing.T) {
	for _, tc := range []struct {
		cls  netprofile.LinkClass
		want netprofile.ReadaheadPolicy
	}{
		{netprofile.ClassSlow, netprofile.ReadaheadPolicy{Enabled: true, SeqThreshold: 4, Blocks: 6, Workers: 4, ParallelFetch: true}},
		{netprofile.ClassMetered, netprofile.ReadaheadPolicy{Enabled: true, SeqThreshold: 6, Blocks: 1, Workers: 1}},
		{netprofile.ClassMedium, netprofile.ReadaheadPolicy{Enabled: true, SeqThreshold: 3, Blocks: 4, Workers: 2}},
		{netprofile.ClassFast, netprofile.ReadaheadPolicy{Enabled: true, SeqThreshold: 2, Blocks: 16, Workers: 8}},
	} {
		p := netprofile.New()
		c := tc.cls
		p.ForceClass(&c)
		if got := p.Readahead(); got != tc.want {
			t.Errorf("%s policy = %+v, want %+v", tc.cls, got, tc.want)
		}
	}
}

// Every slow-link prefetch read is one whole aligned block, no block is read
// twice, and the blocks form one contiguous run just past the reader.
func TestSlowWindowReadsAlignedBlocksOnce(t *testing.T) {
	t.Setenv("JM_SERVER_READAHEAD", "")
	resetPrefetchOfflineState(t)
	forceDefaultClass(t, netprofile.ClassSlow)
	dir, name := windowTestFile(t, 32)
	rm := NewReadaheadManager(dir, nil, slowProfile(t))
	defer rm.Stop()
	var log readLog
	rm.prefetchReadAt = func(fd *os.File, buf []byte, off int64) (int, error) {
		log.add(off, len(buf))
		return fd.ReadAt(buf, off)
	}

	const inode = 501
	// Read on to the middle of block 6 in subreads. Each subread moves the
	// window; settling after each one keeps the reader from overtaking the
	// pump, so every block the window covers is fetched by it.
	for off := int64(0); off < 6*readaheadBlockSize+windowTestSubread; off += windowTestSubread {
		rm.OnRead(inode, off, windowTestSubread, name)
		waitWindowIdle(t, rm, inode)
	}

	offs, lens := log.snapshot()
	if len(offs) == 0 {
		t.Fatal("no prefetch reads on a long sequential slow-link run")
	}
	seen := map[int64]bool{}
	for i, off := range offs {
		if off%readaheadBlockSize != 0 {
			t.Errorf("prefetch read at %d is not block aligned", off)
		}
		if lens[i] != readaheadBlockSize {
			t.Errorf("prefetch read at %d has length %d, want one block", off, lens[i])
		}
		if seen[off] {
			t.Errorf("block at %d fetched twice (overlapping re-read)", off)
		}
		seen[off] = true
	}
	sort.Slice(offs, func(i, j int) bool { return offs[i] < offs[j] })
	for i := 1; i < len(offs); i++ {
		if offs[i] != offs[i-1]+readaheadBlockSize {
			t.Errorf("window has a hole between %d and %d", offs[i-1], offs[i])
		}
	}
	// The last read ended inside block 6, so the window is blocks 7..12.
	if last, want := offs[len(offs)-1], int64(12)*readaheadBlockSize; last != want {
		t.Errorf("window ends at block %d, want 12 (6 blocks past the reader)", last/readaheadBlockSize)
	}
	if first := offs[0]; first < readaheadBlockSize {
		t.Errorf("window fetched block 0, which the reader was already reading")
	}
}

// blockingReads holds every prefetch ReadAt until released and tracks the
// peak number held at once.
type blockingReads struct {
	once     sync.Once
	release  chan struct{}
	started  chan int64
	inflight atomic.Int32
	peak     atomic.Int32
}

func newBlockingReads() *blockingReads {
	return &blockingReads{release: make(chan struct{}), started: make(chan int64, 64)}
}

// unblock releases every held read; safe to call more than once. Defer it
// after rm.Stop so it runs first and Stop can join the readers.
func (b *blockingReads) unblock() { b.once.Do(func() { close(b.release) }) }

func (b *blockingReads) readAt(fd *os.File, buf []byte, off int64) (int, error) {
	n := b.inflight.Add(1)
	for {
		p := b.peak.Load()
		if n <= p || b.peak.CompareAndSwap(p, n) {
			break
		}
	}
	b.started <- off
	<-b.release
	b.inflight.Add(-1)
	return len(buf), nil
}

// The blocks of one window are fetched in parallel, never more than Workers
// at once, and the rest of the window follows as slots free.
func TestSlowWindowFetchesInParallelUpToWorkers(t *testing.T) {
	t.Setenv("JM_SERVER_READAHEAD", "")
	resetPrefetchOfflineState(t)
	forceDefaultClass(t, netprofile.ClassSlow)
	fuseDataInFlight.Store(0)
	t.Cleanup(func() { fuseDataInFlight.Store(0) })
	dir, name := windowTestFile(t, 32)
	rm := NewReadaheadManager(dir, nil, slowProfile(t))
	windowMechanicsOnly(rm)
	defer rm.Stop()
	br := newBlockingReads()
	rm.prefetchReadAt = br.readAt
	defer br.unblock()

	const inode = 502
	readSequential(rm, inode, name, 0, readaheadBlockSize)
	for i := 0; i < slowWindowCap; i++ {
		awaitPrefetchSignal(t, br.started)
	}
	// Give an over-eager pump time to start another read.
	time.Sleep(50 * time.Millisecond)
	if got := br.inflight.Load(); got != slowWindowCap {
		t.Fatalf("blocks in flight = %d, want %d (background share minus one)", got, slowWindowCap)
	}
	if inUse, _ := fuseDataGateDepth(); inUse != slowWindowCap {
		t.Fatalf("FUSE data gate holds %d slots, want one per block in flight (%d)", inUse, slowWindowCap)
	}
	br.unblock()
	waitWindowIdle(t, rm, inode)
	if p := br.peak.Load(); p != slowWindowCap {
		t.Fatalf("peak blocks in flight = %d, want %d", p, slowWindowCap)
	}
	if _, prefetched := rm.Stats(); prefetched != 6 {
		t.Fatalf("prefetched %d blocks, want the 6-block window", prefetched)
	}
	if inUse, _ := fuseDataGateDepth(); inUse != 0 {
		t.Fatalf("gate leaked %d slots", inUse)
	}
}

// While the slow-link prefetcher holds every background slot it may take,
// foreground reads still get FUSE data slots at once (no 250 ms spin into
// JUKEBOX) and the Read-QoS bulk lane keeps room for them.
func TestSlowWindowLeavesForegroundReserve(t *testing.T) {
	t.Setenv("JM_SERVER_READAHEAD", "")
	resetPrefetchOfflineState(t)
	forceDefaultClass(t, netprofile.ClassSlow)
	if !fuseDataGateEnabled {
		t.Skip("gate disabled")
	}
	fuseDataInFlight.Store(0)
	t.Cleanup(func() { fuseDataInFlight.Store(0) })
	dir, name := windowTestFile(t, 32)
	rm := NewReadaheadManager(dir, nil, slowProfile(t))
	windowMechanicsOnly(rm)
	defer rm.Stop()
	br := newBlockingReads()
	rm.prefetchReadAt = br.readAt
	defer br.unblock()

	readSequential(rm, 503, name, 0, readaheadBlockSize)
	for i := 0; i < slowWindowCap; i++ {
		awaitPrefetchSignal(t, br.started)
	}
	width := effectiveFUSEDataWidth()
	if width != 8 {
		t.Fatalf("slow-link gate width = %d, want 8", width)
	}
	// Prefetch leaves one background slot for the drainer and friends;
	// past that, background is full.
	spare, ok := tryAcquireFUSEDataBackground()
	if !ok {
		t.Fatal("prefetch filled the whole background share; the drainer has no slot")
	}
	if rel, ok := tryAcquireFUSEDataBackground(); ok {
		rel()
		t.Fatal("background admitted past its half of the gate")
	}
	spare()
	// Foreground gets the reserved half immediately.
	var rels []func()
	t0 := time.Now()
	for i := int64(0); i < width/2; i++ {
		rel, ok := acquireFUSEData()
		if !ok {
			t.Fatalf("foreground read %d refused while only prefetch held the gate", i+1)
		}
		rels = append(rels, rel)
	}
	if waited := time.Since(t0); waited > fuseDataGateWait/2 {
		t.Fatalf("foreground waited %v for reserved slots", waited)
	}
	for _, r := range rels {
		r()
	}
	// The whole window holds one bulk token; foreground bulk reads find the
	// rest of the lane free.
	if held := len(defaultReadQoS.bulk); held != 1 {
		t.Fatalf("prefetch holds %d Read-QoS bulk tokens, want 1 for the window", held)
	}
}

// Metered keeps the historical serial round: one block, one read at a time.
func TestMeteredReadaheadStaysSerialOneBlock(t *testing.T) {
	t.Setenv("JM_SERVER_READAHEAD", "")
	resetPrefetchOfflineState(t)
	dir, name := windowTestFile(t, 32)
	p := netprofile.New()
	c := netprofile.ClassMetered
	p.ForceClass(&c)
	rm := NewReadaheadManager(dir, nil, p)
	defer rm.Stop()
	var log readLog
	var inflight, peak atomic.Int32
	rm.prefetchReadAt = func(fd *os.File, buf []byte, off int64) (int, error) {
		n := inflight.Add(1)
		if n > peak.Load() {
			peak.Store(n)
		}
		log.add(off, len(buf))
		time.Sleep(5 * time.Millisecond)
		inflight.Add(-1)
		return len(buf), nil
	}
	for i := 0; i < 7; i++ {
		rm.OnRead(504, int64(i)*readaheadBlockSize, readaheadBlockSize, name)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, prefetched := rm.Stats(); prefetched > 0 && rm.activePrefetch.Load() == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	rm.Stop()
	offs, _ := log.snapshot()
	if len(offs) != 1 || offs[0] != 7*readaheadBlockSize {
		t.Fatalf("metered prefetch reads = %v, want one block at %d", offs, 7*readaheadBlockSize)
	}
	if peak.Load() != 1 {
		t.Fatalf("metered peak in flight = %d, want 1", peak.Load())
	}
}

// When the reader stops, the window stops moving: the pump fetches at most
// the window already earned, and once the reader is idle past windowIdle it
// claims nothing more even if the window is not finished.
func TestSlowWindowStopsForAbandonedReader(t *testing.T) {
	t.Setenv("JM_SERVER_READAHEAD", "")
	resetPrefetchOfflineState(t)
	forceDefaultClass(t, netprofile.ClassSlow)
	fuseDataInFlight.Store(0)
	t.Cleanup(func() { fuseDataInFlight.Store(0) })
	dir, name := windowTestFile(t, 32)
	rm := NewReadaheadManager(dir, nil, slowProfile(t))
	windowMechanicsOnly(rm)
	defer rm.Stop()
	rm.windowIdle = 20 * time.Millisecond
	br := newBlockingReads()
	rm.prefetchReadAt = br.readAt
	defer br.unblock()

	const inode = 505
	readSequential(rm, inode, name, 0, readaheadBlockSize)
	for i := 0; i < slowWindowCap; i++ {
		awaitPrefetchSignal(t, br.started)
	}
	// The reader walks away. Let the idle limit pass, then finish the reads.
	time.Sleep(3 * rm.windowIdle)
	br.unblock()
	waitWindowIdle(t, rm, inode)
	if _, prefetched := rm.Stats(); prefetched != slowWindowCap {
		t.Fatalf("abandoned stream prefetched %d blocks, want only the %d already in flight", prefetched, slowWindowCap)
	}
	if p := br.peak.Load(); p > slowWindowCap {
		t.Fatalf("peak in flight %d exceeds Workers", p)
	}
}

// A seek ends the old window; the pump does not keep fetching ahead of a
// position the reader left.
func TestSlowWindowSeekStopsOldWindow(t *testing.T) {
	t.Setenv("JM_SERVER_READAHEAD", "")
	resetPrefetchOfflineState(t)
	forceDefaultClass(t, netprofile.ClassSlow)
	fuseDataInFlight.Store(0)
	t.Cleanup(func() { fuseDataInFlight.Store(0) })
	dir, name := windowTestFile(t, 64)
	rm := NewReadaheadManager(dir, nil, slowProfile(t))
	windowMechanicsOnly(rm)
	defer rm.Stop()
	br := newBlockingReads()
	rm.prefetchReadAt = br.readAt
	defer br.unblock()

	const inode = 506
	readSequential(rm, inode, name, 0, readaheadBlockSize)
	for i := 0; i < slowWindowCap; i++ {
		awaitPrefetchSignal(t, br.started)
	}
	// Seek far back; a single read does not qualify for a new window.
	rm.OnRead(inode, 40*readaheadBlockSize, windowTestSubread, name)
	rm.OnRead(inode, 0, windowTestSubread, name)
	br.unblock()
	waitWindowIdle(t, rm, inode)
	if _, prefetched := rm.Stats(); prefetched != slowWindowCap {
		t.Fatalf("prefetched %d blocks after the seek, want only the %d in flight", prefetched, slowWindowCap)
	}
}

// Offline after admission: every gate slot comes back and Stop joins.
func TestSlowWindowOfflineReleasesGate(t *testing.T) {
	t.Setenv("JM_SERVER_READAHEAD", "")
	resetPrefetchOfflineState(t)
	forceDefaultClass(t, netprofile.ClassSlow)
	fuseDataInFlight.Store(0)
	t.Cleanup(func() { fuseDataInFlight.Store(0) })
	dir, name := windowTestFile(t, 32)
	rm := NewReadaheadManager(dir, nil, slowProfile(t))
	windowMechanicsOnly(rm)
	br := newBlockingReads()
	rm.prefetchReadAt = br.readAt
	defer br.unblock()

	readSequential(rm, 507, name, 0, readaheadBlockSize)
	for i := 0; i < slowWindowCap; i++ {
		awaitPrefetchSignal(t, br.started)
	}
	pin.SetOffline(true)
	br.unblock()
	rm.Stop()
	if inUse, _ := fuseDataGateDepth(); inUse != 0 {
		t.Fatalf("gate leaked %d slots after offline", inUse)
	}
	if _, prefetched := rm.Stats(); prefetched != slowWindowCap {
		t.Fatalf("prefetched %d blocks, want only the %d admitted before offline", prefetched, slowWindowCap)
	}
}

// windowMechanicsOnly turns off the demand-first rules (demand_first.go) for
// a test that pins the window's own mechanics: block claims, the manager-wide
// cap, the gate and stop conditions. Rule 2 would otherwise hold a fresh
// stream to one block in flight. demand_first_test.go covers the rules.
func windowMechanicsOnly(rm *ReadaheadManager) {
	rm.demandFirst = func() bool { return false }
}
