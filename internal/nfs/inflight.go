package nfs

import "sync/atomic"

// In-flight RPC depth: how many RPCs have been dispatched and not yet finished,
// and the high-water mark since start.
//
// Kept in this package rather than in internal/metrics because internal/nfs
// deliberately does not import it (see the ObserverFunc hook in conn.go); the
// metrics package pulls these through a provider registered at wiring time.
var (
	rpcInFlight    atomic.Int64
	rpcInFlightMax atomic.Int64
)

func rpcInFlightAdd(delta int64) {
	n := rpcInFlight.Add(delta)
	if delta <= 0 {
		return
	}
	// CAS loop, not load-then-store: two goroutines racing here would both read
	// the old maximum and the larger one could lose its update.
	for {
		max := rpcInFlightMax.Load()
		if n <= max || rpcInFlightMax.CompareAndSwap(max, n) {
			return
		}
	}
}

// InFlightRPCs returns the current in-flight RPC count and its high-water mark.
func InFlightRPCs() (cur, max int64) {
	return rpcInFlight.Load(), rpcInFlightMax.Load()
}
