package nfs

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Lane selection. The point of the small lane is that a 4 KiB ._ AppleDouble
// sidecar must stop consuming a media slot: sidecars are 50% of all write
// operations and 0.0054% of the bytes beside a 72 MiB master, so half the media
// lane was serving a rounding error.
//
// The media lane's width and byte envelope are NOT changed. Widening it was
// tried and measured harmful (workers 4 -> 16: 46.0 -> 40.4 files/s, p99
// 328-364 -> 372-715 ms), and it re-exposes the 2026-06-14 wedge in which
// sixteen concurrent ~66 MB copies saturated juicefs's buffer and the watchdog
// SIGKILLed the mount.

func newLaneTestDrainer(smallWorkers int, smallBytes int64) *Drainer {
	d := &Drainer{
		sem:        make(chan struct{}, 4),
		smallBytes: smallBytes,
	}
	if smallWorkers > 0 {
		d.smallSem = make(chan struct{}, smallWorkers)
	}
	return d
}

func TestSidecarSizedRowDoesNotTakeAMediaSlot(t *testing.T) {
	d := newLaneTestDrainer(8, 1<<20)
	if got := d.laneFor(4096); got != d.smallSem {
		t.Error("a 4 KiB row took the MEDIA lane — that is the whole defect: it " +
			"occupies a slot sized for a 66 MB camera master and blocks on a real " +
			"MinIO PUT just the same")
	}
}

func TestMediaSizedRowKeepsTheMediaLane(t *testing.T) {
	d := newLaneTestDrainer(8, 1<<20)
	if got := d.laneFor(72 << 20); got != d.sem {
		t.Error("a 72 MiB row was routed to the SMALL lane. The small lane is safe " +
			"only because its members contribute negligible bytes; admitting a " +
			"media file re-opens the 2026-06-14 byte envelope with a WIDER bound " +
			"than the media lane it bypassed")
	}
}

// The boundary is inclusive on the small side and exclusive above it. Stated as
// a test because an off-by-one here silently moves media into the small lane.
func TestLaneBoundaryIsInclusiveThenExclusive(t *testing.T) {
	d := newLaneTestDrainer(8, 1<<20)
	if d.laneFor(1<<20) != d.smallSem {
		t.Error("a row exactly at smallBytes was excluded from the small lane")
	}
	if d.laneFor((1<<20)+1) != d.sem {
		t.Error("a row one byte over smallBytes entered the small lane")
	}
}

// An unknown/zero size must NOT get the small lane. Erring toward media costs a
// slot; erring the other way admits an unbounded file into a lane sized on the
// assumption that nothing in it is large.
func TestUnknownSizeTakesTheMediaLane(t *testing.T) {
	d := newLaneTestDrainer(8, 1<<20)
	for _, sz := range []int64{0, -1} {
		if got := d.laneFor(sz); got != d.sem {
			t.Errorf("size=%d took the small lane; an untrusted size cannot make "+
				"the small lane's negligible-bytes promise", sz)
		}
	}
}

// The kill switch must restore byte-for-byte the previous behaviour: one lane,
// everything through it.
func TestDisablingTheLaneRoutesEverythingThroughMedia(t *testing.T) {
	d := newLaneTestDrainer(0, 1<<20) // JM_DRAIN_SMALL_WORKERS=0
	if d.smallSem != nil {
		t.Fatal("small lane was constructed despite being disabled")
	}
	for _, sz := range []int64{4096, 1 << 20, 72 << 20} {
		if got := d.laneFor(sz); got != d.sem {
			t.Errorf("size=%d did not use the media lane with the small lane "+
				"disabled — the kill switch does not restore prior behaviour", sz)
		}
	}
}

func TestSlowLinkGateSerializesMediaButNotTinyRows(t *testing.T) {
	d := newLaneTestDrainer(4, 1<<20)
	if d.slowGateAppliesTo(d.laneFor(4096)) {
		t.Fatal("a 4 KiB AppleDouble row was serialized by the slow-link media gate; the small lane is defeated")
	}
	if !d.slowGateAppliesTo(d.laneFor(72 << 20)) {
		t.Fatal("a media-sized row bypassed the slow-link gate and can saturate a cellular uplink")
	}
	if !d.slowGateAppliesTo(d.laneFor(0)) {
		t.Fatal("an unknown-size row bypassed the conservative slow-link media gate")
	}
}

// The lane changes the drainer's concurrency contract, so the new bound is
// stated explicitly rather than left implied. Total in-flight is
// Workers + SmallWorkers; the byte bound is (Workers x media sizes) +
// (SmallWorkers x smallBytes), and the second term is at most 4 MiB by default
// — which is why this does not re-open the 2026-06-14 envelope in which
// sixteen concurrent ~66 MB copies saturated juicefs's buffer.
func TestDrainerTotalConcurrencyIsMediaPlusSmall(t *testing.T) {
	d := newLaneTestDrainer(4, 1<<20)
	if got, want := cap(d.sem)+cap(d.smallSem), 8; got != want {
		t.Errorf("total drain concurrency = %d, want %d", got, want)
	}
	// The small lane is NOT wider than the media lane. Widening drain
	// concurrency was measured harmful (4 -> 16: 46.0 -> 40.4 files/s, p99
	// 328-364 -> 372-715 ms), so a small lane sized above 4 would be re-running
	// a failed experiment.
	if cap(d.smallSem) > cap(d.sem) {
		t.Errorf("small lane (%d) is wider than the media lane (%d); widening "+
			"drain concurrency is measured harmful", cap(d.smallSem), cap(d.sem))
	}
	// Worst-case added byte pressure must stay trivial.
	if added := int64(cap(d.smallSem)) * d.smallBytes; added > 8<<20 {
		t.Errorf("small lane can hold %d bytes in flight — no longer negligible "+
			"against the media envelope", added)
	}
}

// A large file written after a burst of small files drains while the small
// lane is full. Before, the dispatcher listed only the oldest rows and waited
// on the full small lane, so the idle media lane got nothing: in the remote
// lab a 500 MB file queued behind 200 small files sent no byte for 390 s.
func TestLargeRowDrainsWhileSmallLaneIsFull(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{Workers: 1, SmallWorkers: 1})
	for i := 0; i < 6; i++ {
		writeSpoolEntry(t, spool, fmt.Sprintf("/w/small_%d.xmp", i), []byte("small"))
	}
	big := bytes.Repeat([]byte{7}, int(d.smallBytes)+1)
	writeSpoolEntry(t, spool, "/w/render.mov", big)

	d.smallSem <- struct{}{} // a small drain that never finishes
	d.Start()
	defer func() {
		<-d.smallSem
		d.Stop(5 * time.Second)
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if fi, err := os.Stat(filepath.Join(d.fuseRoot, "w/render.mov")); err == nil && fi.Size() == int64(len(big)) {
			if row := mustReadyRows(t, spool); row == 0 {
				t.Fatal("small rows drained although their lane was full")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the large file did not drain while the small lane was full")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func mustReadyRows(t *testing.T, spool *SpoolStore) int {
	t.Helper()
	rows, err := spool.Meta().ListReadyIncludingWaiting(100)
	if err != nil {
		t.Fatal(err)
	}
	return len(rows)
}

// A mount check that keeps the drainer parked becomes visible after
// drainPauseVisibleAfter, and clears when the check answers again.
func TestDrainPauseBecomesVisible(t *testing.T) {
	d := &Drainer{}
	d.noteIdentityPause("statfs timed out — mount wedged")
	if got := d.DrainPausedReason(); got != "" {
		t.Fatalf("reported at once: %q", got)
	}
	d.idPause.mu.Lock()
	d.idPause.since = time.Now().Add(-drainPauseVisibleAfter - time.Second)
	d.idPause.mu.Unlock()
	d.noteIdentityPause("statfs timed out — mount wedged")
	if got := d.DrainPausedReason(); got == "" {
		t.Fatal("pause not reported after the threshold")
	}
	d.clearIdentityPause()
	if got := d.DrainPausedReason(); got != "" {
		t.Fatalf("still reported after the check answered: %q", got)
	}
}
