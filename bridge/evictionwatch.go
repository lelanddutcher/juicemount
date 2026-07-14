package main

import (
	"context"
	"os"
	"strconv"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

// Eviction watch (pin-integrity guarantee). See the wiring comment in
// NFSServerStart. Decision core is a pure function for tests.
const (
	evictionWatchInterval  = 60 * time.Second
	evictionRepairCooldown = 30 * time.Minute
)

// evictionDropThreshold returns the cache-usage drop that counts as an
// eviction event: max(2 GiB, 10% of the previous sample).
func evictionDropThreshold(prev int64) int64 {
	th := prev / 10
	if min := int64(2) << 30; th < min {
		th = min
	}
	return th
}

// evictionRepairDecision is the testable core: given the previous and
// current cache usage, whether the link is fast enough, the over-capacity
// verdict, and time since the last repair, should a repair run?
func evictionRepairDecision(prevUsage, curUsage int64, fastLink, overCapacity bool, sinceLastRepair time.Duration) bool {
	if prevUsage <= 0 || curUsage < 0 {
		return false
	}
	if overCapacity || !fastLink {
		return false
	}
	if sinceLastRepair < evictionRepairCooldown {
		return false
	}
	return prevUsage-curUsage >= evictionDropThreshold(prevUsage)
}

func evictionWatchLoop(ctx context.Context) {
	interval := evictionWatchInterval
	if raw := os.Getenv("JM_EVICTION_WATCH_SEC"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n >= 10 {
			interval = time.Duration(n) * time.Second
		}
	}
	if os.Getenv("JM_EVICTION_WATCH") == "0" {
		jmlog.Info("eviction watch: disabled (JM_EVICTION_WATCH=0)")
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	var prevUsage int64
	var lastRepair time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			v := pin.Capacity()
			if v.Computed.IsZero() {
				continue // CapacityLoop hasn't produced a verdict yet
			}
			cur := v.CacheUsageBytes
			c := netprofile.Default().Class()
			fast := c != netprofile.ClassMetered && c != netprofile.ClassSlow
			var since time.Duration = 24 * time.Hour
			if !lastRepair.IsZero() {
				since = time.Since(lastRepair)
			}
			if evictionRepairDecision(prevUsage, cur, fast, v.OverCapacity, since) {
				lastRepair = time.Now()
				globalMu.Lock()
				pfr := globalPrefetcher
				globalMu.Unlock()
				if pfr != nil {
					jmlog.Info("eviction watch: cache usage dropped — verifying pinned residency",
						"prev_bytes", prevUsage, "now_bytes", cur)
					ctx2, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
					if rep, err := pfr.VerifyAndRepair(ctx2); err == nil {
						jmlog.Info("eviction watch: pinned verify scheduled",
							"files", rep.TotalPinned, "reenqueued", rep.Reenqueued)
					} else {
						jmlog.Warn("eviction watch: verify failed", "error", err.Error())
					}
					cancel()
				}
			}
			prevUsage = cur
		}
	}
}
