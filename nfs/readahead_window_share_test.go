package nfs

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

// Shares and lifecycle of the slow-link windowed prefetch: other background
// users and foreground bulk reads keep their slots, rounds are bounded, and a
// round stops on a class change, a binding shutdown, a full gate and EOF.

// A spool drain polls the background gate every 10 ms (acquireDrainFUSESlot).
// While a slow-link stream prefetches at full depth, the drain must still get
// a slot within a few polls, also while a foreground read holds a slot
// (background admission counts foreground reads too).
func TestSlowWindowLeavesDrainerASlot(t *testing.T) {
	for _, tc := range []struct {
		name   string
		fgHeld int
		fgLate bool
	}{
		{"no-foreground", 0, false},
		{"one-foreground-read", 1, false},
		{"two-foreground-reads", 2, false},
		// The foreground read arrives while 3 blocks are already in flight.
		// An admitted block cannot be pre-empted, so the first poll may wait
		// for one block (300 ms here); prefetch must not refill after that.
		{"one-foreground-read-arrives-late", 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testSlowWindowLeavesDrainerASlot(t, tc.fgHeld, tc.fgLate)
		})
	}
}

func testSlowWindowLeavesDrainerASlot(t *testing.T, fgHeld int, fgLate bool) {
	testSlowWindowLeavesDrainerASlotMode(t, fgHeld, fgLate, false)
}

// testSlowWindowLeavesDrainerASlotMode runs the drainer-slot check with the
// demand-first rules off (window mechanics) or on (the Slow default; the
// reader ramps the stream before the drain polls start).
func testSlowWindowLeavesDrainerASlotMode(t *testing.T, fgHeld int, fgLate, rulesOn bool) {
	t.Setenv("JM_SERVER_READAHEAD", "")
	t.Setenv("JM_DEMAND_FIRST", "")
	resetPrefetchOfflineState(t)
	forceDefaultClass(t, netprofile.ClassSlow)
	if !fuseDataGateEnabled {
		t.Skip("gate disabled")
	}
	resetFUSEDataGateCounts()
	t.Cleanup(func() { resetFUSEDataGateCounts() })
	dir, name := windowTestFile(t, 400)
	rm := NewReadaheadManager(dir, nil, slowProfile(t))
	warmup := 200 * time.Millisecond
	if rulesOn {
		rm.demand = newDemandGate()
		// Rule 2 opens the window as blocks complete: let the reader ramp
		// past three blocks and the round reach the full cap.
		warmup = 1200 * time.Millisecond
	} else {
		windowMechanicsOnly(rm)
	}
	defer rm.Stop()
	var peak, inflight atomic.Int32
	rm.prefetchReadAt = func(fd *os.File, buf []byte, off int64) (int, error) {
		n := inflight.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(300 * time.Millisecond) // a cold block on a slow link
		inflight.Add(-1)
		return len(buf), nil
	}
	// Foreground reads in flight (cold reads waiting on the link).
	holdForeground := func() {
		for i := 0; i < fgHeld; i++ {
			rel, ok := acquireFUSEData()
			if !ok {
				t.Fatal("foreground read refused")
			}
			t.Cleanup(rel)
		}
	}
	if !fgLate {
		holdForeground()
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for off := int64(0); ; off += windowTestSubread {
			select {
			case <-stop:
				return
			default:
			}
			rm.OnRead(800, off, windowTestSubread, name)
			time.Sleep(5 * time.Millisecond) // the reader keeps the window full
		}
	}()
	defer func() { close(stop); wg.Wait() }()
	time.Sleep(warmup)
	if fgLate {
		holdForeground()
	}

	const bound = 100 * time.Millisecond
	for i := 0; i < 10; i++ {
		t0 := time.Now()
		// waited is the time to get the slot, not the hold after it: on
		// macOS a 20 ms sleep often oversleeps, and that slack is not the
		// drainer waiting.
		var waited time.Duration
		for {
			if rel, ok := tryAcquireFUSEDataBackground(); ok {
				waited = time.Since(t0)
				time.Sleep(20 * time.Millisecond) // hold like a drain write
				rel()
				break
			}
			if time.Since(t0) > 2*time.Second {
				waited = time.Since(t0)
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		limit := bound
		if fgLate && i == 0 {
			limit = 300*time.Millisecond + bound // one in-flight block
		}
		if waited > limit {
			t.Fatalf("drain poll %d waited %v for a background slot, want under %v", i, waited, limit)
		}
	}
	if p := peak.Load(); (fgHeld < 2 || fgLate) && p < 2 {
		t.Fatalf("prefetch never ran in parallel (peak %d); the drain test proved nothing", p)
	}
}

// Several slow-link streams prefetching at once must not take the whole
// Read-QoS bulk lane: a foreground bulk read (offset > 0) gets a token
// promptly.
func TestSlowWindowsLeaveForegroundBulkTokens(t *testing.T) {
	t.Setenv("JM_SERVER_READAHEAD", "")
	resetPrefetchOfflineState(t)
	forceDefaultClass(t, netprofile.ClassSlow)
	if !defaultReadQoS.active() {
		t.Skip("Read-QoS disabled")
	}
	dir, name := windowTestFile(t, 400)
	rm := NewReadaheadManager(dir, nil, slowProfile(t))
	defer rm.Stop()
	rm.prefetchReadAt = func(fd *os.File, buf []byte, off int64) (int, error) {
		time.Sleep(500 * time.Millisecond)
		return len(buf), nil
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for s := 0; s < 6; s++ {
		wg.Add(1)
		go func(inode uint64) {
			defer wg.Done()
			for off := int64(0); ; off += windowTestSubread {
				select {
				case <-stop:
					return
				default:
				}
				rm.OnRead(inode, off, windowTestSubread, name)
				time.Sleep(5 * time.Millisecond)
			}
		}(uint64(700 + s))
	}
	defer func() { close(stop); wg.Wait() }()

	var worst time.Duration
	for i := 0; i < 20; i++ {
		time.Sleep(50 * time.Millisecond)
		if held := defaultReadQoS.prefetchHeld.Load(); held > defaultReadQoS.prefetchBulkShare() {
			t.Fatalf("prefetch holds %d bulk tokens, share is %d", held, defaultReadQoS.prefetchBulkShare())
		}
		t0 := time.Now()
		rel := defaultReadQoS.acquire(8<<20, 1<<20)
		if d := time.Since(t0); d > worst {
			worst = d
		}
		rel()
	}
	if worst > 50*time.Millisecond {
		t.Fatalf("foreground bulk read waited %v behind prefetch rounds", worst)
	}
	if rm.activePrefetch.Load() == 0 {
		t.Log("note: no round active at the end of the probe")
	}
}

// A round claims at most policy.Blocks blocks, then ends, so a long stream
// runs many rounds. Each round captures its own mapping job (prepared after
// the round), and returns its bulk token.
func TestSlowWindowRoundsAreBounded(t *testing.T) {
	t.Setenv("JM_SERVER_READAHEAD", "")
	resetPrefetchOfflineState(t)
	forceDefaultClass(t, netprofile.ClassSlow)
	dir, name := windowTestFile(t, 40)
	rm := NewReadaheadManager(dir, nil, slowProfile(t))
	defer rm.Stop()
	var captures atomic.Int32
	rm.mappingCapture = func(uint64, string) *readaheadMappingJob {
		captures.Add(1)
		return nil
	}
	var mu sync.Mutex
	perRound := map[int32]int{}
	rm.prefetchReadAt = func(fd *os.File, buf []byte, off int64) (int, error) {
		mu.Lock()
		perRound[captures.Load()]++
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		return len(buf), nil
	}
	const inode = 510
	// A paced reader keeps moving the window, so one unbounded pump would
	// serve the whole stream in a single round.
	for off := int64(0); off < 30*readaheadBlockSize; off += windowTestSubread {
		rm.OnRead(inode, off, windowTestSubread, name)
		time.Sleep(time.Millisecond)
	}
	waitWindowIdle(t, rm, inode)
	if c := captures.Load(); c < 4 {
		t.Fatalf("a 30-block stream ran %d rounds; rounds are not bounded", c)
	}
	mu.Lock()
	defer mu.Unlock()
	for round, n := range perRound {
		if n > 6 {
			t.Fatalf("round %d fetched %d blocks, want at most Blocks (6)", round, n)
		}
	}
	if held := defaultReadQoS.prefetchHeld.Load(); held != 0 {
		t.Fatalf("%d bulk tokens still held after the stream", held)
	}
}

// A running Slow round stops claiming when the link becomes Metered.
func TestSlowWindowStopsOnMeteredHandoff(t *testing.T) {
	t.Setenv("JM_SERVER_READAHEAD", "")
	resetPrefetchOfflineState(t)
	forceDefaultClass(t, netprofile.ClassSlow)
	dir, name := windowTestFile(t, 64)
	prof := slowProfile(t)
	rm := NewReadaheadManager(dir, nil, prof)
	windowMechanicsOnly(rm)
	defer rm.Stop()
	br := newBlockingReads()
	rm.prefetchReadAt = br.readAt
	defer br.unblock()

	const inode = 511
	readSequential(rm, inode, name, 0, readaheadBlockSize)
	for i := 0; i < slowWindowCap; i++ {
		awaitPrefetchSignal(t, br.started)
	}
	c := netprofile.ClassMetered
	prof.ForceClass(&c)
	br.unblock()
	waitWindowIdle(t, rm, inode)
	if _, prefetched := rm.Stats(); prefetched != slowWindowCap {
		t.Fatalf("after the switch to Metered the Slow round fetched %d blocks, want only the %d in flight", prefetched, slowWindowCap)
	}
}

// A round stops claiming once its cache binding shuts down.
func TestSlowWindowStopsOnBindingShutdown(t *testing.T) {
	t.Setenv("JM_SERVER_READAHEAD", "")
	resetPrefetchOfflineState(t)
	forceDefaultClass(t, netprofile.ClassSlow)
	dir, name := windowTestFile(t, 64)
	rm := NewReadaheadManager(dir, nil, slowProfile(t))
	windowMechanicsOnly(rm)
	defer rm.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	b := &cacheBinding{workCtx: ctx}
	rm.mappingCapture = func(uint64, string) *readaheadMappingJob {
		b.workMu.Lock()
		b.workActive++
		b.workMu.Unlock()
		return &readaheadMappingJob{b: b}
	}
	br := newBlockingReads()
	rm.prefetchReadAt = br.readAt
	defer br.unblock()

	const inode = 512
	readSequential(rm, inode, name, 0, readaheadBlockSize)
	for i := 0; i < slowWindowCap; i++ {
		awaitPrefetchSignal(t, br.started)
	}
	cancel()
	// Do not record chunks: prepare would need a live handler. The blocks
	// already in flight finish; no new ones are claimed.
	rm.mappingCapture = nil
	br.unblock()
	waitWindowIdle(t, rm, inode)
	if _, prefetched := rm.Stats(); prefetched != slowWindowCap {
		t.Fatalf("after binding shutdown the round fetched %d blocks, want only the %d in flight", prefetched, slowWindowCap)
	}
}

// With the background gate full and nothing in flight, the round ends at
// once instead of spinning while it holds a bulk token, a round slot and a
// descriptor. The blocks are not lost: the next read retries them.
func TestSlowWindowEndsRoundWhenGateFull(t *testing.T) {
	t.Setenv("JM_SERVER_READAHEAD", "")
	resetPrefetchOfflineState(t)
	forceDefaultClass(t, netprofile.ClassSlow)
	if !fuseDataGateEnabled {
		t.Skip("gate disabled")
	}
	resetFUSEDataGateCounts()
	t.Cleanup(func() { resetFUSEDataGateCounts() })
	var held []func()
	for {
		rel, ok := tryAcquireFUSEDataBackground()
		if !ok {
			break
		}
		held = append(held, rel)
	}
	dir, name := windowTestFile(t, 32)
	rm := NewReadaheadManager(dir, nil, slowProfile(t))
	defer rm.Stop()
	var reads atomic.Int32
	rm.prefetchReadAt = func(fd *os.File, buf []byte, off int64) (int, error) {
		reads.Add(1)
		return len(buf), nil
	}
	const inode = 513
	readSequential(rm, inode, name, 0, readaheadBlockSize)
	t0 := time.Now()
	waitWindowIdle(t, rm, inode)
	if d := time.Since(t0); d > 100*time.Millisecond {
		t.Fatalf("round took %v to give up on a full gate", d)
	}
	if reads.Load() != 0 {
		t.Fatalf("read %d blocks through a full gate", reads.Load())
	}
	for _, r := range held {
		r()
	}
	// The next read retries the window from where it was.
	rm.OnRead(inode, readaheadBlockSize, windowTestSubread, name)
	waitWindowIdle(t, rm, inode)
	if reads.Load() == 0 {
		t.Fatal("blocks given back on a full gate were never fetched")
	}
}

// Near the end of the file the window stops at EOF: reads past the end are
// not repeated as the reader finishes the file.
func TestSlowWindowStopsAtEOF(t *testing.T) {
	t.Setenv("JM_SERVER_READAHEAD", "")
	resetPrefetchOfflineState(t)
	forceDefaultClass(t, netprofile.ClassSlow)
	const blocks = 5
	dir, name := windowTestFile(t, blocks)
	rm := NewReadaheadManager(dir, nil, slowProfile(t))
	defer rm.Stop()
	var pastEOF atomic.Int32
	var log readLog
	rm.prefetchReadAt = func(fd *os.File, buf []byte, off int64) (int, error) {
		if off >= blocks*readaheadBlockSize {
			pastEOF.Add(1)
		}
		log.add(off, len(buf))
		return fd.ReadAt(buf, off)
	}
	const inode = 514
	for off := int64(0); off < blocks*readaheadBlockSize; off += windowTestSubread {
		rm.OnRead(inode, off, windowTestSubread, name)
		waitWindowIdle(t, rm, inode)
	}
	if n := pastEOF.Load(); n != 0 {
		offs, _ := log.snapshot()
		t.Fatalf("%d prefetch reads past EOF (want 0): %v", n, offs)
	}
}

// The readahead share of the bulk lane applies to readahead rounds only: the
// thumbnail, preview and QuickLook warmers (tryAcquireBulk) still get the
// rest of the lane while readahead holds its full share.
func TestReadaheadBulkShareDoesNotLimitWarmers(t *testing.T) {
	forceDefaultClass(t, netprofile.ClassSlow)
	if !defaultReadQoS.active() {
		t.Skip("Read-QoS disabled")
	}
	var rels []func()
	defer func() {
		for _, r := range rels {
			r()
		}
	}()
	for i := int32(0); i < defaultReadQoS.prefetchBulkShare(); i++ {
		rel, ok := defaultReadQoS.tryAcquireBulkPrefetch()
		if !ok {
			t.Fatalf("readahead round %d refused inside its share", i+1)
		}
		rels = append(rels, rel)
	}
	if rel, ok := defaultReadQoS.tryAcquireBulkPrefetch(); ok {
		rel()
		t.Fatal("readahead admitted past its share of the bulk lane")
	}
	rel, ok := defaultReadQoS.tryAcquireBulk()
	if !ok {
		t.Fatal("a warmer was refused while only readahead held tokens; the share must not apply to it")
	}
	rel()
}

// A round gives up pumping as soon as it stops claiming, before its last
// blocks finish (and before mapping preparation), so the next read starts
// the next round while the old one drains.
func TestSlowWindowNextRoundStartsWhileLastBlocksDrain(t *testing.T) {
	t.Setenv("JM_SERVER_READAHEAD", "")
	resetPrefetchOfflineState(t)
	forceDefaultClass(t, netprofile.ClassSlow)
	resetFUSEDataGateCounts()
	t.Cleanup(func() { resetFUSEDataGateCounts() })
	dir, name := windowTestFile(t, 32)
	rm := NewReadaheadManager(dir, nil, slowProfile(t))
	defer rm.Stop()
	rm.policyOverride = func() netprofile.ReadaheadPolicy {
		return netprofile.ReadaheadPolicy{Enabled: true, SeqThreshold: 4, Blocks: 2, Workers: 4, ParallelFetch: true}
	}
	var captures atomic.Int32
	rm.mappingCapture = func(uint64, string) *readaheadMappingJob {
		captures.Add(1)
		return nil
	}
	br := newBlockingReads()
	rm.prefetchReadAt = br.readAt
	defer br.unblock()

	const inode = 515
	readSequential(rm, inode, name, 0, readaheadBlockSize)
	// Round 1 claims its 2 blocks; both stay in flight.
	awaitPrefetchSignal(t, br.started)
	awaitPrefetchSignal(t, br.started)
	// The reader moves on: the window grows by a block.
	readSequential(rm, inode, name, readaheadBlockSize, 2*readaheadBlockSize)
	select {
	case <-br.started:
	case <-time.After(2 * time.Second):
		t.Fatal("no new round started while the old round's blocks were in flight")
	}
	if c := captures.Load(); c < 2 {
		t.Fatalf("rounds started = %d, want 2", c)
	}
}

// A file that grows while it is read keeps its prefetch: the fstat size only
// bounds one round and holds back new rounds briefly.
func TestSlowWindowFollowsAGrowingFile(t *testing.T) {
	t.Setenv("JM_SERVER_READAHEAD", "")
	resetPrefetchOfflineState(t)
	forceDefaultClass(t, netprofile.ClassSlow)
	dir, name := windowTestFile(t, 5)
	rm := NewReadaheadManager(dir, nil, slowProfile(t))
	defer rm.Stop()
	rm.sizeHintTTL = 20 * time.Millisecond
	var pastOldEnd atomic.Int32
	rm.prefetchReadAt = func(fd *os.File, buf []byte, off int64) (int, error) {
		if off >= 5*readaheadBlockSize {
			pastOldEnd.Add(1)
		}
		return fd.ReadAt(buf, off)
	}
	const inode = 516
	for off := int64(0); off < 3*readaheadBlockSize; off += windowTestSubread {
		rm.OnRead(inode, off, windowTestSubread, name)
		waitWindowIdle(t, rm, inode)
	}
	if n := pastOldEnd.Load(); n != 0 {
		t.Fatalf("%d reads past the end before the file grew", n)
	}
	if err := os.Truncate(dir+"/"+name, 12*readaheadBlockSize); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * rm.sizeHintTTL)
	// The reader is still before the old end.
	for off := int64(3 * readaheadBlockSize); off < 4*readaheadBlockSize; off += windowTestSubread {
		rm.OnRead(inode, off, windowTestSubread, name)
		waitWindowIdle(t, rm, inode)
	}
	if pastOldEnd.Load() == 0 {
		t.Fatal("prefetch stopped at the old file size after the file grew")
	}
}
