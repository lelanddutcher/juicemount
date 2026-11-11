package nfs

import (
	"errors"
	"io"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

// Tests for the read-slot rules of the 2026-11-11 laptop lab: foreground is
// admitted apart from background, a slow read is joined for its whole life,
// and read-ahead takes no speculative block for a scrub while blocks are slow.

// resetFUSEDataGateCounts empties both gate counters (tests that hold slots
// without their releases).
func resetFUSEDataGateCounts() {
	fuseDataInFlight.Store(0)
	fuseBackgroundInFlight.Store(0)
}

// resetDemandJukebox clears rule 4's link-wide signal for this test.
func resetDemandJukebox(t *testing.T) {
	t.Helper()
	prior, priorSlow := demandJukeboxAt.Load(), demandSlowAt.Load()
	demandJukeboxAt.Store(0)
	demandSlowAt.Store(0)
	t.Cleanup(func() { demandJukeboxAt.Store(prior); demandSlowAt.Store(priorSlow) })
}

// On a slow link, with background holding all it may, foreground still gets
// every slot of the class width, and never more.
func TestForegroundGetsTheWholeSlowWidthBesideBackground(t *testing.T) {
	if !fuseDataGateEnabled {
		t.Skip("gate disabled")
	}
	forceDefaultClass(t, netprofile.ClassSlow)
	resetFUSEDataGateCounts()
	t.Cleanup(resetFUSEDataGateCounts)
	var rels []func()
	t.Cleanup(func() {
		for _, r := range rels {
			r()
		}
	})
	for {
		rel, ok := tryAcquireFUSEDataBackground()
		if !ok {
			break
		}
		rels = append(rels, rel)
	}
	bg := fuseBackgroundInFlight.Load()
	if bg == 0 {
		t.Fatal("background got no slot (test is not exercising the split)")
	}
	width := effectiveFUSEDataWidth()
	for i := int64(0); i < width; i++ {
		rel, ok := acquireFUSEData()
		if !ok {
			t.Fatalf("foreground read %d of %d refused while background held %d slots", i+1, width, bg)
		}
		rels = append(rels, rel)
	}
	if rel, ok := acquireFUSEData(); ok {
		rel()
		t.Fatalf("foreground admitted past the class width %d", width)
	}
	if total := fuseDataInFlight.Load(); total > int64(fuseDataGateWidth()) {
		t.Fatalf("%d in flight, over the base ceiling %d", total, fuseDataGateWidth())
	}
	// Background still yields to foreground: with foreground at its width,
	// no new background slot.
	for _, r := range rels[:bg] {
		r()
	}
	rels = rels[bg:]
	if rel, ok := tryAcquireFUSEDataBackground(); ok {
		rel()
		t.Fatal("background admitted while foreground holds the class width")
	}
}

// On fast links admission is unchanged: the total is bounded by the base.
func TestForegroundAdmissionUnchangedOnFastLinks(t *testing.T) {
	if !fuseDataGateEnabled {
		t.Skip("gate disabled")
	}
	forceDefaultClass(t, netprofile.ClassFast)
	resetFUSEDataGateCounts()
	t.Cleanup(resetFUSEDataGateCounts)
	var rels []func()
	t.Cleanup(func() {
		for _, r := range rels {
			r()
		}
	})
	for {
		rel, ok := tryAcquireFUSEDataBackground()
		if !ok {
			break
		}
		rels = append(rels, rel)
	}
	n := 0
	for {
		rel, ok := acquireFUSEData()
		if !ok {
			break
		}
		rels = append(rels, rel)
		n++
	}
	if total := fuseDataInFlight.Load(); total != int64(fuseDataGateWidth()) {
		t.Fatalf("fast link: %d in flight (%d foreground), want the base %d", total, n, fuseDataGateWidth())
	}
}

// Rule 4: with slow blocks, a scrub stream gets no speculative block, and a
// stream that has read a whole block sequentially still does.
func TestSlowBlocksStopScrubReadahead(t *testing.T) {
	resetDemandJukebox(t)
	rm := &ReadaheadManager{}
	scrub := &readTracker{sequentialBytes: 1 << 20}
	seq := &readTracker{sequentialBytes: 2 * readaheadBlockSize}
	if got := rm.windowStreamCapLocked(scrub, 3, true); got != 1 {
		t.Fatalf("fast blocks: scrub cap %d, want 1", got)
	}
	rm.blockNS.Store(int64(2 * demandDeferMax))
	rm.blockAt.Store(time.Now().UnixNano())
	if got := rm.windowStreamCapLocked(scrub, 3, true); got != 0 {
		t.Fatalf("slow blocks: scrub cap %d, want 0", got)
	}
	if got := rm.windowStreamCapLocked(seq, 3, true); got < 1 {
		t.Fatalf("slow blocks: sequential cap %d, want at least 1", got)
	}
	if got := rm.windowStreamCapLocked(scrub, 3, false); got != 3 {
		t.Fatalf("rules off: cap %d, want the policy's 3", got)
	}
	// A slow block time older than demandSlowWindow no longer counts, so a
	// scrub gets read-ahead again after the link recovers.
	rm.blockAt.Store(time.Now().Add(-2 * demandSlowWindow).UnixNano())
	if got := rm.windowStreamCapLocked(scrub, 3, true); got != 1 {
		t.Fatalf("stale slow block time: scrub cap %d, want 1", got)
	}
	// A demand read that outlived its bound also marks blocks slow.
	rm.blockNS.Store(0)
	noteDemandJukebox()
	if got := rm.windowStreamCapLocked(scrub, 3, true); got != 0 {
		t.Fatalf("after a demand JUKEBOX: scrub cap %d, want 0", got)
	}
}

// Rule 4: while blocks are slow, the defer outlasts demandDefer and ends when
// the demand read ends.
func TestSlowBlocksDeferHasNoTimeLimit(t *testing.T) {
	resetDemandJukebox(t)
	t.Setenv("JM_DEMAND_FIRST", "")
	rm := &ReadaheadManager{stopCh: make(chan struct{}), demand: newDemandGate(),
		demandFirst: func() bool { return true }, demandDefer: 20 * time.Millisecond}
	rm.blockNS.Store(int64(2 * demandDeferMax))
	rm.blockAt.Store(time.Now().UnixNano())
	end := rm.demand.begin()
	done := make(chan time.Duration, 1)
	go func() {
		t0 := time.Now()
		rm.waitForDemand(nil)
		done <- time.Since(t0)
	}()
	select {
	case d := <-done:
		t.Fatalf("the defer ended after %v with the demand read still in flight", d)
	case <-time.After(200 * time.Millisecond):
	}
	end()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the defer did not end when the demand read ended")
	}
	// Blocks fast again: the configured limit applies.
	rm.blockNS.Store(int64(time.Second))
	end = rm.demand.begin()
	defer end()
	t0 := time.Now()
	rm.waitForDemand(nil)
	if d := time.Since(t0); d > time.Second {
		t.Fatalf("fast blocks: defer took %v, want about 20 ms", d)
	}
}

// A JUKEBOX retry of a pread that is still running after 3 FUSE bounds (a
// slow link) joins it; it does not start a second pread.
func TestJUKEBOXJoinsASlowPreadPastThreeBounds(t *testing.T) {
	resetPendingReads(t)
	fp := newFakePread('S')
	key := readKey("reel.mov", 7, 1, 0, 4)
	p := make([]byte, 4)
	const bound = 10 * time.Millisecond
	joinOrReadFUSE(fp.read, key, "reel.mov", p, 0, bound)
	time.Sleep(5 * bound)
	go func() { time.Sleep(bound / 2); fp.unblock() }()
	n, err := joinOrReadFUSE(fp.read, key, "reel.mov", p, 0, 20*bound)
	if c := fp.calls.Load(); c != 1 {
		t.Fatalf("preads %d, want 1: the retry must join the running pread", c)
	}
	if err != nil || n != 4 || p[0] != 'S' {
		t.Fatalf("joined retry got n=%d err=%v p=%q", n, err, p)
	}
	waitPreadsDone(t)
}

// Rule 5: a speculative block is read in 1 MiB pieces, and a piece after the
// first waits while a demand read is in flight, so a demand read of the same
// file (which macFUSE runs one at a time) waits for at most one piece.
func TestReadaheadBlockYieldsToDemandBetweenPieces(t *testing.T) {
	resetDemandJukebox(t)
	t.Setenv("JM_DEMAND_FIRST", "")
	rm := &ReadaheadManager{stopCh: make(chan struct{}), demand: newDemandGate(),
		demandFirst: func() bool { return true }, demandDefer: 5 * time.Second, chunkWithHook: true}
	pieces := make(chan int64, 8)
	var endDemand func()
	rm.prefetchReadAt = func(_ *os.File, buf []byte, off int64) (int, error) {
		if len(buf) != demandChunk {
			t.Errorf("piece of %d bytes, want %d", len(buf), demandChunk)
		}
		if off == 0 {
			endDemand = rm.demand.begin() // a demand read arrives during piece 1
		}
		pieces <- off
		return len(buf), nil
	}
	buf := make([]byte, readaheadBlockSize)
	done := make(chan error, 1)
	go func() {
		n, _, err := rm.readBlock(nil, buf, 0, nil, nil)
		if err == nil && n != len(buf) {
			err = io.ErrShortBuffer
		}
		done <- err
	}()
	if off := <-pieces; off != 0 {
		t.Fatalf("first piece at %d", off)
	}
	select {
	case off := <-pieces:
		t.Fatalf("piece at %d started while a demand read was in flight", off)
	case <-time.After(150 * time.Millisecond):
	}
	endDemand()
	for want := int64(demandChunk); want < readaheadBlockSize; want += demandChunk {
		select {
		case off := <-pieces:
			if off != want {
				t.Fatalf("piece at %d, want %d", off, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("piece at %d never started after the demand read ended", want)
		}
	}
	if err := <-done; err != nil {
		t.Fatalf("readBlock: %v", err)
	}
}

// Without the demand-first rules a block is one read, as before.
func TestReadaheadBlockIsOneReadWithoutDemandFirst(t *testing.T) {
	rm := &ReadaheadManager{stopCh: make(chan struct{}), demandFirst: func() bool { return false }, chunkWithHook: true}
	calls := 0
	rm.prefetchReadAt = func(_ *os.File, buf []byte, _ int64) (int, error) {
		calls++
		return len(buf), nil
	}
	if n, _, err := rm.readBlock(nil, make([]byte, readaheadBlockSize), 0, nil, nil); err != nil || n != readaheadBlockSize || calls != 1 {
		t.Fatalf("n=%d err=%v calls=%d, want one whole-block read", n, err, calls)
	}
}

// Rule 4: two demand preads of 256 KiB that take longer than demandSlowPread
// within a minute mark blocks slow, as a JUKEBOX does; a short or fast one, or
// a single slow one, does not.
func TestSlowDemandPreadMarksBlocksSlow(t *testing.T) {
	resetDemandJukebox(t)
	resetFUSEDataGateCounts()
	t.Cleanup(resetFUSEDataGateCounts)
	slow := func(d time.Duration) func([]byte, int64) (int, error) {
		return func(p []byte, _ int64) (int, error) { time.Sleep(d); return len(p), nil }
	}
	gate := newDemandGate()
	boundedFUSEReadCall(slow(0), make([]byte, 256<<10), 0, time.Second, gate, nil)
	boundedFUSEReadCall(slow(demandSlowPread+50*time.Millisecond), make([]byte, 4096), 0, 10*time.Second, gate, nil)
	if recentDemandJukebox() {
		t.Fatal("a fast pread, or a slow small one, marked blocks slow")
	}
	boundedFUSEReadCall(slow(demandSlowPread+50*time.Millisecond), make([]byte, 256<<10), 0, 10*time.Second, gate, nil)
	if recentDemandJukebox() {
		t.Fatal("one slow 256 KiB demand pread marked blocks slow; two are needed")
	}
	boundedFUSEReadCall(slow(demandSlowPread+50*time.Millisecond), make([]byte, 256<<10), 0, 10*time.Second, gate, nil)
	if !recentDemandJukebox() {
		t.Fatal("two slow 256 KiB demand preads did not mark blocks slow")
	}
}

// piecesManager is a demand-first manager whose test hook reads in pieces.
func piecesManager(t *testing.T, read func(*os.File, []byte, int64) (int, error)) *ReadaheadManager {
	t.Helper()
	resetDemandJukebox(t)
	t.Setenv("JM_DEMAND_FIRST", "")
	return &ReadaheadManager{stopCh: make(chan struct{}), demand: newDemandGate(),
		demandFirst: func() bool { return true }, demandDefer: 5 * time.Second, chunkWithHook: true,
		prefetchReadAt: read}
}

// Rule 5: while a block waits between pieces it holds no gate slot, so the
// spool drainer's background slot is free even when read-ahead held the rest
// of the background share; the block then takes a slot again.
func TestReadaheadPieceWaitFreesItsGateSlot(t *testing.T) {
	if !fuseDataGateEnabled {
		t.Skip("gate disabled")
	}
	forceDefaultClass(t, netprofile.ClassSlow)
	resetFUSEDataGateCounts()
	t.Cleanup(resetFUSEDataGateCounts)
	var held []func()
	t.Cleanup(func() {
		for _, r := range held {
			r()
		}
	})
	var endDemand func()
	piece := make(chan int64, 8)
	var rm *ReadaheadManager
	rm = piecesManager(t, func(_ *os.File, buf []byte, off int64) (int, error) {
		if off == 0 {
			endDemand = rm.demand.begin()
		}
		piece <- off
		return len(buf), nil
	})
	// Other blocks hold every background slot but one; this block takes it.
	for i := int64(1); i < backgroundFUSEDataWidth(); i++ {
		rel, ok := tryAcquireFUSEDataBackground()
		if !ok {
			t.Fatal("setup: no background slot")
		}
		held = append(held, rel)
	}
	rel, ok := tryAcquireFUSEDataBackground()
	if !ok {
		t.Fatal("setup: no slot for the block")
	}
	g := &blockGate{release: rel, acquire: tryAcquireFUSEDataBackground}
	done := make(chan error, 1)
	go func() {
		_, _, err := rm.readBlock(nil, make([]byte, readaheadBlockSize), 0, g, nil)
		g.drop()
		done <- err
	}()
	<-piece
	waitUntil(t, "the block to give its slot back", func() bool {
		return fuseBackgroundInFlight.Load() == backgroundFUSEDataWidth()-1
	})
	drain, ok := tryAcquireFUSEDataBackground()
	if !ok {
		t.Fatal("the drainer got no background slot while read-ahead waited between pieces")
	}
	drain()
	endDemand()
	if err := <-done; err != nil {
		t.Fatalf("readBlock: %v", err)
	}
	if got := fuseBackgroundInFlight.Load(); got != backgroundFUSEDataWidth()-1 {
		t.Fatalf("background in flight %d after the block, want %d (slot leaked or double-released)", got, backgroundFUSEDataWidth()-1)
	}
}

// Rule 5 under rule 4: the wait between pieces stays capped (a block must
// not hold its round and mapping job for minutes), and the time spent waiting
// is not block time.
func TestReadaheadPieceWaitIsCappedAndNotCounted(t *testing.T) {
	var rm *ReadaheadManager
	rm = piecesManager(t, func(_ *os.File, buf []byte, _ int64) (int, error) { return len(buf), nil })
	rm.demandDefer = 60 * time.Millisecond
	rm.blockNS.Store(int64(2 * demandDeferMax)) // rule 4 on
	rm.blockAt.Store(time.Now().UnixNano())
	end := rm.demand.begin()
	defer end()
	t0 := time.Now()
	n, active, err := rm.readBlock(nil, make([]byte, readaheadBlockSize), 0, nil, nil)
	if err != nil || n != readaheadBlockSize {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if took := time.Since(t0); took > 2*time.Second {
		t.Fatalf("block took %v with a demand read in flight; each wait must stop at demandDefer", took)
	}
	if active > 50*time.Millisecond {
		t.Fatalf("active time %v includes the waits between pieces", active)
	}
}

// A block whose round no longer wants it (binding stopping, class change,
// reader moved) ends at the next piece, with the bytes read so far.
func TestReadaheadPieceWaitEndsOnCancel(t *testing.T) {
	var rm *ReadaheadManager
	rm = piecesManager(t, func(_ *os.File, buf []byte, _ int64) (int, error) { return len(buf), nil })
	end := rm.demand.begin()
	defer end()
	var stopping atomic.Bool
	go func() { time.Sleep(100 * time.Millisecond); stopping.Store(true) }()
	t0 := time.Now()
	n, _, err := rm.readBlock(nil, make([]byte, readaheadBlockSize), 0, nil, stopping.Load)
	if !errors.Is(err, errPrefetchStopped) || n != demandChunk {
		t.Fatalf("n=%d err=%v, want one piece and errPrefetchStopped", n, err)
	}
	if took := time.Since(t0); took > time.Second {
		t.Fatalf("cancel took %v, want about one wait slice", took)
	}
}

// The window's cancel: a seek away, a reader past the block, a class without
// parallel fetch, or a stopping binding each end the block.
func TestWindowBlockCancel(t *testing.T) {
	pol := netprofile.ReadaheadPolicy{Enabled: true, ParallelFetch: true, Blocks: 6, Workers: 3}
	rm := &ReadaheadManager{trackers: map[uint64]*readTracker{}, stopCh: make(chan struct{}),
		policyOverride: func() netprofile.ReadaheadPolicy { return pol }}
	tr := &readTracker{lastOffset: 0, windowEnd: 3 * readaheadBlockSize}
	rm.trackers[9] = tr
	off := int64(readaheadBlockSize)
	cancel := rm.windowBlockCancel(9, tr, off, nil)
	if cancel() {
		t.Fatal("a wanted block was cancelled")
	}
	rm.mu.Lock()
	tr.windowEnd = 0 // a seek reset the window
	rm.mu.Unlock()
	if !cancel() {
		t.Fatal("a block was kept after a seek reset the window")
	}
	rm.mu.Lock()
	tr.windowEnd, tr.lastOffset = 3*readaheadBlockSize, 2*readaheadBlockSize // reader passed it
	rm.mu.Unlock()
	if !cancel() {
		t.Fatal("a block the reader already passed was kept")
	}
	rm.mu.Lock()
	tr.lastOffset = 0
	rm.mu.Unlock()
	pol.ParallelFetch = false // the link became metered
	if !cancel() {
		t.Fatal("a windowed block was kept after the class left parallel fetch")
	}
}
