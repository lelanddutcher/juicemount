package nfs

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

// The ceiling must hold on EVERY link class.
//
// On 2026-08-05 the machine kernel-panicked twice during a 7,000-file run. Every
// FUSE-concurrency protection was gated to slow/metered links — slowGate (cap 1),
// readQoS ("Inert on medium/fast") — so on a LAN the bulk data path had no
// ceiling, 62 concurrent writes reached macFUSE, and the kext died. A gate that
// switches itself off for the link class the user actually has is not a gate.
func TestFUSEDataGateBoundsConcurrency(t *testing.T) {
	if !fuseDataGateEnabled {
		t.Fatal("the FUSE data ceiling is disabled by default — it must be ON")
	}
	width := int(effectiveFUSEDataWidth())
	if width <= 0 || width > 32 {
		t.Fatalf("width %d is outside a sane range: 62 concurrent demonstrably killed "+
			"the macFUSE session, and 0 means no ceiling at all", width)
	}
	assertPeakAtMost(t, width, width*4)
	if inUse, _ := fuseDataGateDepth(); inUse != 0 {
		t.Errorf("%d slots still held after every caller returned — a leaked slot "+
			"permanently shrinks the ceiling", inUse)
	}
}

// THE NO-OP THIS TEST EXISTS TO CATCH. The first implementation narrowed the
// ceiling on slow links by testing `len(g) >= w` before a send on a channel of
// cap 16 — which admits instantly while 14 slots are free. Metered links got 16
// instead of 2, and the commit message advertised protection that did not
// exist. It shipped because nothing tested the narrowed path.
func TestFUSEDataGateNarrowsOnSlowLinks(t *testing.T) {
	for _, tc := range []struct {
		name  string
		class netprofile.LinkClass
		want  int
	}{
		{"metered", netprofile.ClassMetered, 2},
		{"slow", netprofile.ClassSlow, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			restore := forceNetClassForTest(t, tc.class)
			defer restore()

			if got := int(effectiveFUSEDataWidth()); got != tc.want {
				t.Fatalf("effective width on %s = %d, want %d — the class gate must "+
					"LOWER the ceiling, never leave it at the base", tc.name, got, tc.want)
			}
			assertPeakAtMost(t, tc.want, tc.want*8)
		})
	}
}

// forceNetClassForTest pins the link class through the production API so the
// narrowed path is exercised exactly as it runs in the field.
func forceNetClassForTest(t *testing.T, c netprofile.LinkClass) func() {
	t.Helper()
	netprofile.Default().ForceClass(&c)
	return func() { netprofile.Default().ForceClass(nil) }
}

// assertPeakAtMost drives `callers` concurrent acquisitions and fails if more
// than `limit` are ever admitted at once.
func assertPeakAtMost(t *testing.T, limit, callers int) {
	t.Helper()
	var concurrent, peak, admitted int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			release, ok := acquireFUSEData()
			if !ok {
				return
			}
			defer release()
			atomic.AddInt64(&admitted, 1)
			cur := atomic.AddInt64(&concurrent, 1)
			for {
				p := atomic.LoadInt64(&peak)
				if cur <= p || atomic.CompareAndSwapInt64(&peak, p, cur) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond) // stands in for the syscall
			atomic.AddInt64(&concurrent, -1)
		}()
	}
	close(start)
	wg.Wait()
	if peak > int64(limit) {
		t.Errorf("observed %d concurrent FUSE data ops against a ceiling of %d — the "+
			"ceiling did not hold, which is the condition that panicked the kernel",
			peak, limit)
	}
	if admitted == 0 {
		t.Error("nothing was admitted — a gate that admits nothing is not a pass")
	}
}

// Admission must be FAST. A patient wait re-creates the stall this codebase is
// designed against: non-write RPCs hold an rpcSem slot across dispatch, and
// ErrFUSETimeout exists so a handler frees that slot instead of parking on a
// wedged mount. The first version waited 10s — long enough to park 112 readers
// on rpcSem and stop the server reading its socket — and it also blew the 2s
// per-subread deadline by 5x.
func TestFUSEDataGateShedsFastWhenFull(t *testing.T) {
	if fuseDataGateWait > time.Second {
		t.Fatalf("admission waits up to %v; that parks an rpcSem slot and starves "+
			"LOOKUP/GETATTR, and exceeds the 2s subread deadline", fuseDataGateWait)
	}
	width := int(effectiveFUSEDataWidth())
	var held []func()
	for i := 0; i < width; i++ {
		r, ok := acquireFUSEData()
		if !ok {
			t.Fatalf("could not fill the ceiling at slot %d/%d", i, width)
		}
		held = append(held, r)
	}
	defer func() {
		for _, r := range held {
			r()
		}
	}()

	start := time.Now()
	r, ok := acquireFUSEData()
	elapsed := time.Since(start)
	if r == nil {
		t.Fatal("nil release on the refusal path — callers defer it unconditionally")
	}
	if ok {
		r()
		t.Fatal("admitted past a full ceiling")
	}
	if elapsed > 2*time.Second {
		t.Errorf("refusal took %v — it must shed well inside the subread deadline", elapsed)
	}
}

// Background work counts toward the ceiling but must never WAIT for it: the
// sidecar warmer alone runs 48 concurrent reads, and a user navigating must not
// queue behind warming nobody asked for.
func TestBackgroundAcquireNeverWaits(t *testing.T) {
	width := int(effectiveFUSEDataWidth())
	var held []func()
	for i := 0; i < width; i++ {
		r, ok := acquireFUSEData()
		if !ok {
			t.Fatalf("could not fill the ceiling at %d/%d", i, width)
		}
		held = append(held, r)
	}
	defer func() {
		for _, r := range held {
			r()
		}
	}()

	start := time.Now()
	r, ok := tryAcquireFUSEDataBackground()
	elapsed := time.Since(start)
	if ok {
		r()
		t.Fatal("background work was admitted past a full ceiling — it must count " +
			"toward the same budget as foreground work")
	}
	if r == nil {
		t.Fatal("nil release on the background refusal path")
	}
	if elapsed > 50*time.Millisecond {
		t.Errorf("background acquire waited %v — it must yield instantly so it can "+
			"never delay a foreground read", elapsed)
	}
}

// A panicking syscall must not cost a slot permanently, or the ceiling erodes
// to zero over time.
func TestFUSEDataGateReleasesOnPanic(t *testing.T) {
	before, _ := fuseDataGateDepth()
	func() {
		defer func() { _ = recover() }()
		release, ok := acquireFUSEData()
		if !ok {
			t.Fatal("could not acquire")
		}
		defer release()
		panic("simulated syscall panic")
	}()
	after, _ := fuseDataGateDepth()
	if after != before {
		t.Errorf("slot leaked through a panic: depth %d -> %d", before, after)
	}
}

// Refusals must be counted, or a JUKEBOX storm originating here is
// indistinguishable in the field from one originating in the spool.
func TestFUSEDataGateCountsRefusals(t *testing.T) {
	_, _, before := FUSEDataGateStats()
	noteFUSEDataRefused()
	_, _, after := FUSEDataGateStats()
	if after != before+1 {
		t.Errorf("refusal counter went %d -> %d, want +1", before, after)
	}
}
