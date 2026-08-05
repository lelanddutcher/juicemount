package nfs

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The ceiling must hold on EVERY link class, not just cellular.
//
// THE WHOLE POINT. On 2026-08-05 the machine kernel-panicked twice during a
// 7,000-file run. Every FUSE-concurrency protection we had was gated to
// slow/metered links — slowGate (cap 1), readQoS ("Inert on medium/fast") — so
// on a LAN the bulk data path had NO ceiling, 62 concurrent writes reached
// macFUSE, and the kext died. A gate that switches itself off for the link
// class the user actually has is not a gate.
func TestFUSEDataGateBoundsConcurrency(t *testing.T) {
	if fuseDataGate == nil {
		t.Fatal("the FUSE data gate is disabled by default — the ceiling must be ON")
	}
	width := cap(fuseDataGate)
	if width <= 0 || width > 32 {
		t.Fatalf("gate width %d is outside a sane range: 62 concurrent demonstrably "+
			"killed the macFUSE session, and 0 means no ceiling at all", width)
	}

	var concurrent, peak int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	// Far more callers than slots, so the gate is the only thing that can bound
	// observed concurrency.
	for i := 0; i < width*4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			release, ok := acquireFUSEData()
			if !ok {
				return
			}
			defer release()
			cur := atomic.AddInt64(&concurrent, 1)
			for {
				p := atomic.LoadInt64(&peak)
				if cur <= p || atomic.CompareAndSwapInt64(&peak, p, cur) {
					break
				}
			}
			time.Sleep(15 * time.Millisecond) // stand in for the syscall
			atomic.AddInt64(&concurrent, -1)
		}()
	}
	close(start)
	wg.Wait()

	if peak > int64(width) {
		t.Errorf("observed %d concurrent FUSE data ops with a gate of %d — the ceiling "+
			"did not hold, which is the condition that panicked the kernel", peak, width)
	}
	if peak == 0 {
		t.Error("no operation was admitted — a gate that admits nothing is not a pass")
	}
	if inUse, _ := fuseDataGateDepth(); inUse != 0 {
		t.Errorf("%d slots still held after every caller returned — a leaked slot "+
			"permanently shrinks the ceiling", inUse)
	}
}

// Every acquire must return a non-nil release, including the FAILURE path.
// Callers defer it unconditionally; a nil release on the timeout path would
// panic the RPC goroutine instead of degrading to a retry.
func TestFUSEDataGateAlwaysReturnsRelease(t *testing.T) {
	release, ok := acquireFUSEData()
	if release == nil {
		t.Fatal("acquire returned a nil release on the success path")
	}
	if !ok {
		t.Fatal("could not acquire on an idle gate")
	}
	release()

	// Saturate, then confirm the refused path still hands back a usable release.
	width := cap(fuseDataGate)
	var held []func()
	for i := 0; i < width; i++ {
		r, ok := acquireFUSEData()
		if !ok {
			t.Fatalf("failed to fill the gate at slot %d/%d", i, width)
		}
		held = append(held, r)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		r, ok := acquireFUSEData()
		if r == nil {
			t.Error("acquire returned a nil release on the TIMEOUT path — callers " +
				"defer it unconditionally and would panic")
		}
		if ok {
			t.Error("acquire succeeded against a full gate")
			r()
		}
	}()
	select {
	case <-done:
	case <-time.After(fuseDataGateWait + 5*time.Second):
		t.Fatal("acquire against a full gate never returned — it must time out and " +
			"degrade to a retry, not park the RPC forever")
	}
	for _, r := range held {
		r()
	}
	if inUse, _ := fuseDataGateDepth(); inUse != 0 {
		t.Errorf("%d slots leaked after release", inUse)
	}
}

// Releasing must be safe to defer even when the syscall panics — otherwise one
// panicking read permanently costs a slot and the ceiling erodes to zero.
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
