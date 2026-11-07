package nfs

import (
	"runtime"
	"strings"
	"testing"
	"time"
)

// spoolSweeperFrame identifies the goroutine SpoolStore.StartSweeper launches.
// TestMain fails the package if any such goroutine outlives the tests.
const spoolSweeperFrame = "(*SpoolStore).StartSweeper.func"

// leakedSpoolSweepers waits up to grace for every StartSweeper goroutine to
// exit and returns how many are still running, with their stacks.
func leakedSpoolSweepers(grace time.Duration) (int, string) {
	deadline := time.Now().Add(grace)
	for {
		buf := make([]byte, 1<<20)
		for {
			n := runtime.Stack(buf, true)
			if n < len(buf) {
				buf = buf[:n]
				break
			}
			buf = make([]byte, 2*len(buf))
		}
		var leaked []string
		for _, g := range strings.Split(string(buf), "\n\n") {
			if strings.Contains(g, spoolSweeperFrame) {
				leaked = append(leaked, g)
			}
		}
		if len(leaked) == 0 || time.Now().After(deadline) {
			return len(leaked), strings.Join(leaked, "\n\n")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// reattachSpoolForTest attaches a new spool to a handler that may already
// have one. SetSpool starts an idle sweeper and stores its stop function in
// h.spoolSweeperStop, overwriting any earlier one; the earlier sweeper would
// then run until the test binary exits. Join it first.
func reattachSpoolForTest(t *testing.T, h *JuiceMountHandler, spool *SpoolStore, d *Drainer) {
	t.Helper()
	h.spoolLifecycleMu.Lock()
	stop := h.spoolSweeperStop
	h.spoolSweeperStop = nil
	h.spoolLifecycleMu.Unlock()
	if stop != nil {
		stop()
	}
	h.SetSpool(spool, d)
}
