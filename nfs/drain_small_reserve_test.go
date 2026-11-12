package nfs

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
	"github.com/lelanddutcher/juicemount/metadata"
)

// The 2026-11-11 laptop lab (abroad, main 47e140a): on a link with one
// background slot, a 500 MB render's upload held that slot, and 2 of 201
// saved files reached the server in 10 minutes. Small saves must keep
// draining while a large file holds the shared background share.
func TestDrainSmallFilesDrainWhileLargeDrainHoldsTheOnlySlot(t *testing.T) {
	restore := forceNetClassForTest(t, netprofile.ClassMetered)
	defer restore()
	if bg := backgroundFUSEDataWidth(); bg != 1 {
		t.Fatalf("precondition: metered background width %d, want 1", bg)
	}
	spool, d := newTestDrainer(t, DrainerConfig{})
	if d.smallSem == nil {
		t.Fatal("precondition: the small lane is disabled")
	}

	// The large file's upload: a media-lane drain that holds the only shared
	// background slot for the whole test.
	releaseLarge, ok := d.acquireDrainFUSESlotFor(false)
	if !ok {
		t.Fatal("large drain not admitted")
	}
	defer releaseLarge()
	if release, ok := tryAcquireFUSEDataBackground(); ok {
		release()
		t.Fatal("precondition: the shared background share is not full")
	}

	const n = 200
	ids := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		e := writeSpoolEntry(t, spool, fmt.Sprintf("/Project/save-%03d.prproj", i), []byte(fmt.Sprintf("save %d", i)))
		ids = append(ids, e.ID())
	}

	deadline := time.Now().Add(2 * time.Minute)
	for done := 0; done < n; {
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d small files drained while the large drain held the slot", done, n)
		}
		ctx, cancel := context.WithTimeout(context.Background(), drainPassDeadline)
		d.DrainOnceForTest(ctx)
		cancel()
		done = 0
		for _, id := range ids {
			if row, err := spool.Meta().Get(id); err == nil && row.DrainState == metadata.DrainDone {
				done++
			}
		}
	}
	if got := d.mediaGateHeld.Load(); got != 1 {
		t.Fatalf("media slots held = %d, want the large drain's 1 throughout", got)
	}
	if got := d.smallReserveHeld.Load(); got != 0 {
		t.Fatalf("small reserve still held after the drains: %d", got)
	}
}

// With JM_DRAIN_SMALL_RESERVE=1 the reserve is one slot: a second small
// drain waits while the first holds it and the shared share is full, then
// takes it once released.
func TestDrainSmallReserveIsOneSlot(t *testing.T) {
	t.Setenv("JM_DRAIN_SMALL_RESERVE", "1")
	restore := forceNetClassForTest(t, netprofile.ClassMetered)
	defer restore()
	d := &Drainer{stop: make(chan struct{})}
	defer close(d.stop)

	releaseLarge, ok := d.acquireDrainFUSESlotFor(false)
	if !ok {
		t.Fatal("large drain not admitted")
	}
	defer releaseLarge()
	first, ok := d.acquireDrainFUSESlotFor(true)
	if !ok {
		t.Fatal("first small drain not admitted on the reserve")
	}
	if d.smallReserveHeld.Load() != 1 {
		t.Fatal("first small drain is not on the reserve")
	}
	second := make(chan func(), 1)
	go func() {
		r, _ := d.acquireDrainFUSESlotFor(true)
		second <- r
	}()
	select {
	case r := <-second:
		r()
		t.Fatal("a second small drain was admitted past the one reserved slot")
	case <-time.After(60 * time.Millisecond):
	}
	first()
	first() // release is idempotent
	select {
	case r := <-second:
		r()
	case <-time.After(5 * time.Second):
		t.Fatal("second small drain not admitted after the reserve freed")
	}
	if inUse, _ := fuseDataGateDepth(); inUse != 1 {
		t.Fatalf("gate in use %d after small releases, want the large drain's 1", inUse)
	}
}

// The reserve is background: it never refuses a foreground read, and media
// drains cannot take it.
func TestDrainSmallReserveLeavesForegroundAndIsNotMedia(t *testing.T) {
	restore := forceNetClassForTest(t, netprofile.ClassMetered)
	defer restore()
	d := &Drainer{stop: make(chan struct{})}

	releaseLarge, ok := d.acquireDrainFUSESlotFor(false)
	if !ok {
		t.Fatal("large drain not admitted")
	}
	defer releaseLarge()
	releaseSmall, ok := d.acquireDrainFUSESlotFor(true)
	if !ok {
		t.Fatal("small drain not admitted")
	}
	defer releaseSmall()

	var fg []func()
	for i := 0; i < int(effectiveFUSEDataWidth()); i++ {
		r, ok := acquireFUSEData()
		if !ok {
			t.Fatalf("foreground read %d refused beside the background drains", i)
		}
		fg = append(fg, r)
	}
	for _, r := range fg {
		r()
	}

	// A second media drain must wait: the reserve belongs to the small lane.
	media := make(chan func(), 1)
	go func() {
		r, _ := d.acquireDrainFUSESlotFor(false)
		media <- r
	}()
	select {
	case r := <-media:
		r()
		t.Fatal("a media drain took the small lane's reserved slot")
	case <-time.After(60 * time.Millisecond):
	}
	close(d.stop)
	if r := <-media; r != nil {
		r()
	}
}

// Many small drains on a one-slot link with a media drain holding the slot:
// they all finish, one at a time on the reserve, and none is lost.
func TestDrainSmallReserveServesManySmallDrainsRace(t *testing.T) {
	restore := forceNetClassForTest(t, netprofile.ClassMetered)
	defer restore()
	d := &Drainer{stop: make(chan struct{})}
	defer close(d.stop)
	releaseLarge, ok := d.acquireDrainFUSESlotFor(false)
	if !ok {
		t.Fatal("large drain not admitted")
	}
	defer releaseLarge()

	var wg sync.WaitGroup
	var mu sync.Mutex
	held, peak, done := 0, 0, 0
	for i := 0; i < 4; i++ { // the small lane's four workers
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				r, ok := d.acquireDrainFUSESlotFor(true)
				if !ok {
					return
				}
				mu.Lock()
				held++
				if held > peak {
					peak = held
				}
				mu.Unlock()
				time.Sleep(100 * time.Microsecond)
				mu.Lock()
				held--
				done++
				mu.Unlock()
				r()
			}
		}()
	}
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(time.Minute):
		t.Fatalf("small drains stalled: %d of 200 done", done)
	}
	if want := int(smallReserveSlots()); done != 200 || peak != want {
		t.Fatalf("done %d (want 200), peak %d (want %d: only the reserve is free)", done, peak, want)
	}
}

// The reserve is 2 on a metered link (its shared share is 1) and 1 on a slow
// link; JM_DRAIN_SMALL_RESERVE overrides, and 0 turns it off.
func TestDrainSmallReserveSlotsByClass(t *testing.T) {
	t.Setenv("JM_DRAIN_SMALL_RESERVE", "")
	for _, tc := range []struct {
		class netprofile.LinkClass
		want  int32
	}{{netprofile.ClassMetered, 2}, {netprofile.ClassSlow, 1}, {netprofile.ClassFast, 1}} {
		restore := forceNetClassForTest(t, tc.class)
		if got := smallReserveSlots(); got != tc.want {
			restore()
			t.Fatalf("%s: reserve %d, want %d", tc.class, got, tc.want)
		}
		restore()
	}
	t.Setenv("JM_DRAIN_SMALL_RESERVE", "0")
	restore := forceNetClassForTest(t, netprofile.ClassMetered)
	defer restore()
	d := &Drainer{stop: make(chan struct{})}
	if r, ok := d.trySmallReserveFUSESlot(); ok {
		r()
		t.Fatal("reserve admitted with JM_DRAIN_SMALL_RESERVE=0")
	}
}

// Review M1: on a medium or fast link the foreground width is the ceiling
// (16). With 15 foreground reads in flight a reserve slot would take the 16th
// and refuse the next read the editor waits for. The reserve must not admit
// there, and the 16th foreground read must be admitted.
func TestDrainSmallReserveNeverTakesForegroundSlotOnFastLink(t *testing.T) {
	restore := forceNetClassForTest(t, netprofile.ClassFast)
	defer restore()
	width := int(effectiveFUSEDataWidth())
	if width != fuseDataGateWidth() {
		t.Fatalf("precondition: fast width %d, want the base %d", width, fuseDataGateWidth())
	}
	var fg []func()
	defer func() {
		for _, r := range fg {
			r()
		}
	}()
	for i := 0; i < width-1; i++ {
		r, ok := acquireFUSEData()
		if !ok {
			t.Fatalf("foreground read %d refused", i)
		}
		fg = append(fg, r)
	}
	d := &Drainer{stop: make(chan struct{})}
	if r, ok := d.trySmallReserveFUSESlot(); ok {
		r()
		t.Fatal("small reserve took a slot foreground needs to reach its width")
	}
	r, ok := acquireFUSEData()
	if !ok {
		t.Fatal("the last foreground read was refused")
	}
	fg = append(fg, r)
	// Idle fast link: still no reserve (foreground could need all 16).
	for _, r := range fg {
		r()
	}
	fg = nil
	if r, ok := d.trySmallReserveFUSESlot(); ok {
		r()
		t.Fatal("reserve admitted on an idle fast link")
	}
}

// On a slow link the reserve admits beside a full foreground width (8 of 16).
func TestDrainSmallReserveAdmitsBesideFullForegroundOnSlowLink(t *testing.T) {
	restore := forceNetClassForTest(t, netprofile.ClassSlow)
	defer restore()
	var fg []func()
	for i := 0; i < int(effectiveFUSEDataWidth()); i++ {
		r, ok := acquireFUSEData()
		if !ok {
			t.Fatalf("foreground read %d refused", i)
		}
		fg = append(fg, r)
	}
	defer func() {
		for _, r := range fg {
			r()
		}
	}()
	d := &Drainer{stop: make(chan struct{})}
	r, ok := d.trySmallReserveFUSESlot()
	if !ok {
		t.Fatal("reserve refused on a slow link with headroom under the ceiling")
	}
	r()
}
