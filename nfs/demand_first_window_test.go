package nfs

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

// The slow-link window's guarantees (PR #62) with the demand-first rules ON,
// as they are by default on Slow: the stream ramps first (a sequential reader
// past three blocks), then the same assertions as the mechanics tests in
// readahead_window_test.go and readahead_window_share_test.go. Then the stop
// conditions while a round waits for a demand read (rule 1).

// armedReads completes reads at once until armed, then holds them like
// blockingReads, so a test can ramp a stream before measuring.
type armedReads struct {
	armed atomic.Bool
	br    *blockingReads
}

func (a *armedReads) readAt(fd *os.File, buf []byte, off int64) (int, error) {
	if !a.armed.Load() {
		return len(buf), nil
	}
	return a.br.readAt(fd, buf, off)
}

// rulesOnWindow is a Slow manager with the demand-first rules on and its own
// idle demand gate.
func rulesOnWindow(t *testing.T, blocks int, prof *netprofile.Profile) (*ReadaheadManager, *armedReads, string) {
	t.Helper()
	t.Setenv("JM_SERVER_READAHEAD", "")
	t.Setenv("JM_DEMAND_FIRST", "")
	resetPrefetchOfflineState(t)
	forceDefaultClass(t, netprofile.ClassSlow)
	fuseDataInFlight.Store(0)
	t.Cleanup(func() { fuseDataInFlight.Store(0) })
	dir, name := windowTestFile(t, blocks)
	if prof == nil {
		prof = slowProfile(t)
	}
	rm := NewReadaheadManager(dir, nil, prof)
	rm.demand = newDemandGate()
	ar := &armedReads{br: newBlockingReads()}
	rm.prefetchReadAt = ar.readAt
	if !rm.demandFirstOn() {
		t.Fatal("the demand-first rules must be on for this test")
	}
	return rm, ar, name
}

// rampThenMeasure ramps the stream through three blocks with reads that
// complete at once, then arms the held reads and reads on to nine blocks,
// which opens a fresh six-block window. It waits until slowWindowCap blocks
// are in flight and returns the prefetch count before the measured phase.
func rampThenMeasure(t *testing.T, rm *ReadaheadManager, ar *armedReads, inode uint64, name string, beforeMeasure func()) int64 {
	t.Helper()
	readSequential(rm, inode, name, 0, 3*readaheadBlockSize)
	waitWindowIdle(t, rm, inode)
	_, base := rm.Stats()
	if beforeMeasure != nil {
		beforeMeasure()
	}
	ar.armed.Store(true)
	// A round claims again only when one of its blocks ends, and the held
	// reads never end. Hold a demand read while the reader extends the
	// window, so the round (rule 1) claims once, against the whole window.
	endDemand := rm.demand.begin()
	readSequential(rm, inode, name, 3*readaheadBlockSize, 9*readaheadBlockSize)
	endDemand()
	for i := 0; i < slowWindowCap; i++ {
		select {
		case <-ar.br.started:
		case <-time.After(5 * time.Second):
			rm.mu.Lock()
			tr := *rm.trackers[inode]
			rm.mu.Unlock()
			t.Fatalf("only %d of %d blocks started: gate=%d held=%d tracker %+v windowInflight=%d active=%d bulkHeld=%d prefetchHeld=%d",
				i, slowWindowCap, fuseDataInFlight.Load(), ar.br.inflight.Load(), tr, rm.windowInflight.Load(), rm.activePrefetch.Load(),
				len(defaultReadQoS.bulk), defaultReadQoS.prefetchHeld.Load())
		}
	}
	return base
}

func prefetchedSince(rm *ReadaheadManager, base int64) int64 {
	_, n := rm.Stats()
	return n - base
}

func TestRulesOnSlowWindowFetchesInParallelUpToWorkers(t *testing.T) {
	rm, ar, name := rulesOnWindow(t, 32, nil)
	defer rm.Stop()
	defer ar.br.unblock()
	const inode = 701
	base := rampThenMeasure(t, rm, ar, inode, name, nil)
	time.Sleep(50 * time.Millisecond)
	if got := ar.br.inflight.Load(); got != slowWindowCap {
		t.Fatalf("blocks in flight = %d, want %d", got, slowWindowCap)
	}
	if inUse, _ := fuseDataGateDepth(); inUse != slowWindowCap {
		t.Fatalf("FUSE data gate holds %d slots, want %d", inUse, slowWindowCap)
	}
	ar.br.unblock()
	waitWindowIdle(t, rm, inode)
	if p := ar.br.peak.Load(); p != slowWindowCap {
		t.Fatalf("peak blocks in flight = %d, want %d", p, slowWindowCap)
	}
	if n := prefetchedSince(rm, base); n != 6 {
		t.Fatalf("prefetched %d blocks, want the 6-block window", n)
	}
	if inUse, _ := fuseDataGateDepth(); inUse != 0 {
		t.Fatalf("gate leaked %d slots", inUse)
	}
}

func TestRulesOnSlowWindowLeavesForegroundReserve(t *testing.T) {
	if !fuseDataGateEnabled {
		t.Skip("gate disabled")
	}
	rm, ar, name := rulesOnWindow(t, 32, nil)
	defer rm.Stop()
	defer ar.br.unblock()
	rampThenMeasure(t, rm, ar, 702, name, nil)
	spare, ok := tryAcquireFUSEDataBackground()
	if !ok {
		t.Fatal("prefetch filled the whole background share; the drainer has no slot")
	}
	if rel, ok := tryAcquireFUSEDataBackground(); ok {
		rel()
		t.Fatal("background admitted past its half of the gate")
	}
	spare()
	width := effectiveFUSEDataWidth()
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
	if held := len(defaultReadQoS.bulk); held != 1 {
		t.Fatalf("prefetch holds %d Read-QoS bulk tokens, want 1", held)
	}
}

func TestRulesOnSlowWindowStopsForAbandonedReader(t *testing.T) {
	rm, ar, name := rulesOnWindow(t, 32, nil)
	defer rm.Stop()
	defer ar.br.unblock()
	const inode = 703
	base := rampThenMeasure(t, rm, ar, inode, name, nil)
	// The reader walks away now that the blocks are in flight.
	rm.mu.Lock()
	rm.windowIdle = 20 * time.Millisecond
	rm.mu.Unlock()
	time.Sleep(60 * time.Millisecond)
	ar.br.unblock()
	waitWindowIdle(t, rm, inode)
	if n := prefetchedSince(rm, base); n != slowWindowCap {
		t.Fatalf("abandoned stream prefetched %d blocks, want only the %d in flight", n, slowWindowCap)
	}
}

func TestRulesOnSlowWindowSeekStopsOldWindow(t *testing.T) {
	rm, ar, name := rulesOnWindow(t, 64, nil)
	defer rm.Stop()
	defer ar.br.unblock()
	const inode = 704
	base := rampThenMeasure(t, rm, ar, inode, name, nil)
	rm.OnRead(inode, 40*readaheadBlockSize, windowTestSubread, name)
	rm.OnRead(inode, 0, windowTestSubread, name)
	ar.br.unblock()
	waitWindowIdle(t, rm, inode)
	if n := prefetchedSince(rm, base); n != slowWindowCap {
		t.Fatalf("prefetched %d blocks after the seek, want only the %d in flight", n, slowWindowCap)
	}
}

func TestRulesOnSlowWindowOfflineReleasesGate(t *testing.T) {
	rm, ar, name := rulesOnWindow(t, 32, nil)
	defer ar.br.unblock()
	base := rampThenMeasure(t, rm, ar, 705, name, nil)
	pin.SetOffline(true)
	ar.br.unblock()
	rm.Stop()
	if inUse, _ := fuseDataGateDepth(); inUse != 0 {
		t.Fatalf("gate leaked %d slots after offline", inUse)
	}
	if n := prefetchedSince(rm, base); n != slowWindowCap {
		t.Fatalf("prefetched %d blocks, want only the %d admitted before offline", n, slowWindowCap)
	}
}

func TestRulesOnSlowWindowStopsOnMeteredHandoff(t *testing.T) {
	prof := slowProfile(t)
	rm, ar, name := rulesOnWindow(t, 64, prof)
	defer rm.Stop()
	defer ar.br.unblock()
	const inode = 706
	base := rampThenMeasure(t, rm, ar, inode, name, nil)
	c := netprofile.ClassMetered
	prof.ForceClass(&c)
	ar.br.unblock()
	waitWindowIdle(t, rm, inode)
	if n := prefetchedSince(rm, base); n != slowWindowCap {
		t.Fatalf("after the switch to Metered the round fetched %d blocks, want only the %d in flight", n, slowWindowCap)
	}
}

func TestRulesOnSlowWindowStopsOnBindingShutdown(t *testing.T) {
	rm, ar, name := rulesOnWindow(t, 64, nil)
	defer rm.Stop()
	defer ar.br.unblock()
	ctx, cancel := context.WithCancel(context.Background())
	b := &cacheBinding{workCtx: ctx}
	const inode = 707
	base := rampThenMeasure(t, rm, ar, inode, name, func() {
		rm.mappingCapture = func(uint64, string) *readaheadMappingJob {
			b.workMu.Lock()
			b.workActive++
			b.workMu.Unlock()
			return &readaheadMappingJob{b: b}
		}
	})
	cancel()
	rm.mappingCapture = nil
	ar.br.unblock()
	waitWindowIdle(t, rm, inode)
	if n := prefetchedSince(rm, base); n != slowWindowCap {
		t.Fatalf("after binding shutdown the round fetched %d blocks, want only the %d in flight", n, slowWindowCap)
	}
}

func TestRulesOnSlowWindowLeavesDrainerASlot(t *testing.T) {
	for _, tc := range []struct {
		name   string
		fgHeld int
		fgLate bool
	}{
		{"no-foreground", 0, false},
		{"one-foreground-read", 1, false},
		{"two-foreground-reads", 2, false},
		{"one-foreground-read-arrives-late", 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testSlowWindowLeavesDrainerASlotMode(t, tc.fgHeld, tc.fgLate, true)
		})
	}
}

// --- rule 1: stop conditions while a round waits for a demand read --------

// demandHeldRound starts a Slow round that waits for a held demand read.
func demandHeldRound(t *testing.T, prof *netprofile.Profile) (*ReadaheadManager, *armedReads, string, func()) {
	t.Helper()
	rm, ar, name := rulesOnWindow(t, 64, prof)
	ar.armed.Store(true)
	rm.demandDefer = time.Minute // only a stop condition can end the wait early
	endDemand := rm.demand.begin()
	t.Cleanup(func() {
		endDemand()
		ar.br.unblock()
		rm.Stop()
	})
	return rm, ar, name, endDemand
}

func waitIdleWithin(t *testing.T, rm *ReadaheadManager, inode uint64, within time.Duration, why string) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		rm.mu.Lock()
		tr := rm.trackers[inode]
		busy := tr != nil && tr.pumping
		rm.mu.Unlock()
		if !busy && rm.activePrefetch.Load() == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("the waiting round did not end within %s %s", within, why)
}

func assertNoBlockRead(t *testing.T, ar *armedReads, why string) {
	t.Helper()
	select {
	case off := <-ar.br.started:
		t.Fatalf("a read-ahead block at %d was read %s", off, why)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestDemandWaitStopsOnMeteredHandoff(t *testing.T) {
	prof := slowProfile(t)
	rm, ar, name, _ := demandHeldRound(t, prof)
	const inode = 711
	readSequential(rm, inode, name, 0, readaheadBlockSize)
	time.Sleep(60 * time.Millisecond) // the round is waiting for the demand read
	c := netprofile.ClassMetered
	prof.ForceClass(&c)
	waitIdleWithin(t, rm, inode, time.Second, "after the switch to Metered")
	assertNoBlockRead(t, ar, "after the switch to Metered")
}

func TestDemandWaitStopsOnBindingShutdown(t *testing.T) {
	rm, ar, name, _ := demandHeldRound(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	b := &cacheBinding{workCtx: ctx}
	rm.mappingCapture = func(uint64, string) *readaheadMappingJob {
		b.workMu.Lock()
		b.workActive++
		b.workMu.Unlock()
		return &readaheadMappingJob{b: b}
	}
	const inode = 712
	readSequential(rm, inode, name, 0, readaheadBlockSize)
	time.Sleep(60 * time.Millisecond)
	cancel()
	waitIdleWithin(t, rm, inode, time.Second, "after binding shutdown")
	assertNoBlockRead(t, ar, "after binding shutdown")
}

func TestDemandWaitStopsOnOffline(t *testing.T) {
	rm, ar, name, _ := demandHeldRound(t, nil)
	const inode = 713
	readSequential(rm, inode, name, 0, readaheadBlockSize)
	time.Sleep(60 * time.Millisecond)
	pin.SetOffline(true)
	waitIdleWithin(t, rm, inode, time.Second, "after going offline")
	assertNoBlockRead(t, ar, "after going offline")
}

func TestDemandWaitStopsWhenReaderSeeks(t *testing.T) {
	rm, ar, name, endDemand := demandHeldRound(t, nil)
	const inode = 714
	readSequential(rm, inode, name, 0, readaheadBlockSize)
	time.Sleep(60 * time.Millisecond)
	rm.OnRead(inode, 300*readaheadBlockSize, windowTestSubread, name) // a scrub jump
	waitIdleWithin(t, rm, inode, time.Second, "after the reader seeked away")
	endDemand()
	assertNoBlockRead(t, ar, "for the old position")
}
