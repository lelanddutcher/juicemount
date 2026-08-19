package nfs

import (
	"sync"
	"testing"
)

// The high-water mark must survive a race. A load-then-store would let two
// goroutines both read the old maximum and the larger one lose its update,
// which is precisely the case where the number matters — a deep burst.
func TestInFlightHighWaterSurvivesRace(t *testing.T) {
	rpcInFlight.Store(0)
	rpcInFlightMax.Store(0)

	const n = 64
	var start, done sync.WaitGroup
	start.Add(1)
	done.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer done.Done()
			start.Wait()
			rpcInFlightAdd(1)
		}()
	}
	start.Done()
	done.Wait()

	cur, max := InFlightRPCs()
	if cur != n {
		t.Fatalf("current in-flight = %d, want %d", cur, n)
	}
	if max != n {
		t.Fatalf("high-water = %d, want %d — a lost update means the metric "+
			"under-reports exactly the deep bursts it exists to show", max, n)
	}
	for i := 0; i < n; i++ {
		rpcInFlightAdd(-1)
	}
	if cur, max = InFlightRPCs(); cur != 0 || max != n {
		t.Fatalf("after release cur=%d max=%d, want 0 and %d — the mark must not decay", cur, max, n)
	}
}
