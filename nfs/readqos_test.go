package nfs

import (
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

func slowClassFn() netprofile.LinkClass   { return netprofile.ClassSlow }
func fastClassFn() netprofile.LinkClass   { return netprofile.ClassFast }
func mediumClassFn() netprofile.LinkClass { return netprofile.ClassMedium }

// fill occupies every token of a lane and returns a drain func.
func fillLane(lane chan struct{}) func() {
	for i := 0; i < cap(lane); i++ {
		lane <- struct{}{}
	}
	return func() {
		for i := 0; i < cap(lane); i++ {
			<-lane
		}
	}
}

// TestReadQoSInactiveOnFastAndMedium pins the cellular-revert-safety
// contract: on medium/fast links the gate admits instantly with no token —
// even with every lane full — so the validated 10GbE profile is untouched.
func TestReadQoSInactiveOnFastAndMedium(t *testing.T) {
	for _, classFn := range []func() netprofile.LinkClass{fastClassFn, mediumClassFn} {
		q := newReadQoS(2, 2, classFn)
		drain := fillLane(q.bulk)
		done := make(chan struct{})
		go func() {
			release := q.acquire(4096, 1<<20) // bulk-shaped read
			release()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("acquire blocked on an inactive (fast/medium) link")
		}
		if rel, ok := q.tryAcquireBulk(); !ok {
			t.Fatal("tryAcquireBulk shed on an inactive link")
		} else {
			rel()
		}
		drain()
	}
}

// TestReadQoSKillSwitch: JM_READ_QOS=0 disables shaping even on a slow link.
func TestReadQoSKillSwitch(t *testing.T) {
	t.Setenv("JM_READ_QOS", "0")
	q := newReadQoS(1, 1, slowClassFn)
	drain := fillLane(q.bulk)
	defer drain()
	done := make(chan struct{})
	go func() {
		q.acquire(4096, 1<<20)()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("kill switch did not disable the gate")
	}
}

// TestReadQoSInteractiveLaneIsReserved pins the point of the whole design: a
// first-block probe (off==0, small) admits immediately while the bulk lane is
// saturated.
func TestReadQoSInteractiveLaneIsReserved(t *testing.T) {
	q := newReadQoS(2, 2, slowClassFn)
	drain := fillLane(q.bulk)
	defer drain()

	done := make(chan struct{})
	go func() {
		release := q.acquire(0, 64<<10) // Finder/QL probe signature
		release()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("interactive probe queued behind a saturated bulk lane")
	}
}

// TestReadQoSBulkQueuesThenAdmits: a bulk read waits for a token and admits
// as soon as one frees — shaping, not breaking.
func TestReadQoSBulkQueuesThenAdmits(t *testing.T) {
	q := newReadQoS(1, 1, slowClassFn)
	q.bulk <- struct{}{} // hold the only token

	admitted := make(chan struct{})
	go func() {
		release := q.acquire(4<<20, 1<<20)
		release()
		close(admitted)
	}()
	select {
	case <-admitted:
		t.Fatal("bulk read admitted while the lane was full")
	case <-time.After(100 * time.Millisecond):
	}
	<-q.bulk // free the token
	select {
	case <-admitted:
	case <-time.After(2 * time.Second):
		t.Fatal("bulk read never admitted after the token freed")
	}
}

// TestReadQoSFailOpen: a bulk acquire that outwaits its bound proceeds
// gateless, and its no-op release does NOT corrupt the lane.
func TestReadQoSFailOpen(t *testing.T) {
	q := newReadQoS(1, 1, slowClassFn)
	q.bulkWait = 50 * time.Millisecond
	q.bulk <- struct{}{} // never released

	start := time.Now()
	release := q.acquire(4<<20, 1<<20)
	if waited := time.Since(start); waited < 40*time.Millisecond {
		t.Fatalf("fail-open fired too early (%v) — should wait the bound", waited)
	}
	release() // must be a no-op
	select {
	case q.bulk <- struct{}{}:
		t.Fatal("no-op release freed a token it never held")
	default: // lane still full — correct
	}
}

// TestReadQoSPrefetchShed: prefetch never queues — contended lane sheds.
func TestReadQoSPrefetchShed(t *testing.T) {
	q := newReadQoS(1, 1, slowClassFn)
	if rel, ok := q.tryAcquireBulk(); !ok {
		t.Fatal("uncontended tryAcquireBulk shed")
	} else {
		if _, ok2 := q.tryAcquireBulk(); ok2 {
			t.Fatal("second tryAcquireBulk admitted past lane width")
		}
		rel()
	}
	if rel, ok := q.tryAcquireBulk(); !ok {
		t.Fatal("tryAcquireBulk shed after release recycled the token")
	} else {
		rel()
	}
}

// TestReadQoSTokenRecycles: acquire/release cycles never leak tokens.
func TestReadQoSTokenRecycles(t *testing.T) {
	q := newReadQoS(2, 2, slowClassFn)
	for i := 0; i < 100; i++ {
		release := q.acquire(4<<20, 1<<20)
		release()
	}
	if len(q.bulk) != 0 {
		t.Fatalf("%d tokens leaked after balanced acquire/release", len(q.bulk))
	}
}
