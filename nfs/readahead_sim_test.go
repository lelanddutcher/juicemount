package nfs

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

// A before/after simulation of a cold sequential read over a slow link,
// against a fake backend with a per-request latency and a shared rate cap.
// It drives the real ReadaheadManager (policy, guards, gate, Read-QoS) and
// replaces only the FUSE ReadAt. Run it with:
//
//	JM_READAHEAD_SIM=1 go test -run TestReadaheadSlowLinkSimulation -v ./nfs/
//
// The model is deliberately simple. Each request waits the latency, then
// moves its bytes through one FIFO pipe at the link rate, so concurrent
// requests share the rate. The cache is tracked in 256 KiB units; a unit is
// cold, in flight, or cached. A foreground subread of a cached unit is free,
// of an in-flight unit waits for it, and of a cold unit pays a request. The
// reader is one serial stream of 256 KiB subreads (the Mac client keeps about
// three in flight, so real foreground throughput without prefetch is higher).

const simUnit = 256 << 10

type simLink struct {
	rate    float64 // bytes per second
	latency time.Duration

	mu   sync.Mutex
	next time.Time

	prefetchInflight atomic.Int32
}

func (l *simLink) transfer(n int) {
	time.Sleep(l.latency)
	for n > 0 {
		c := n
		if c > simUnit {
			c = simUnit
		}
		n -= c
		l.mu.Lock()
		now := time.Now()
		if l.next.Before(now) {
			l.next = now
		}
		l.next = l.next.Add(time.Duration(float64(c) / l.rate * float64(time.Second)))
		until := l.next
		l.mu.Unlock()
		time.Sleep(time.Until(until))
	}
}

type simCache struct {
	mu    sync.Mutex
	units map[int64]chan struct{} // closed when cached
	done  map[int64]bool
}

func newSimCache() *simCache {
	return &simCache{units: map[int64]chan struct{}{}, done: map[int64]bool{}}
}

// claim splits [off, off+n) into units this caller must fetch and units to
// wait for.
func (c *simCache) claim(off int64, n int) (mine []int64, wait []chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for u := off / simUnit; u*simUnit < off+int64(n); u++ {
		if c.done[u] {
			continue
		}
		if ch, ok := c.units[u]; ok {
			wait = append(wait, ch)
			continue
		}
		c.units[u] = make(chan struct{})
		mine = append(mine, u)
	}
	return mine, wait
}

func (c *simCache) finish(units []int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, u := range units {
		c.done[u] = true
		close(c.units[u])
	}
}

func (c *simCache) fetch(link *simLink, off int64, n int, prefetch bool) {
	mine, wait := c.claim(off, n)
	if len(mine) > 0 {
		if prefetch {
			link.prefetchInflight.Add(1)
		}
		link.transfer(len(mine) * simUnit)
		if prefetch {
			link.prefetchInflight.Add(-1)
		}
		c.finish(mine)
	}
	for _, ch := range wait {
		<-ch
	}
}

type simResult struct {
	mbps         float64
	avgInflight  float64
	peakInflight int32
	prefetched   int64
}

func runReadaheadSim(t *testing.T, scale float64, rateMbit float64, latency time.Duration, fileMiB int, override *netprofile.ReadaheadPolicy) simResult {
	t.Helper()
	dir, name := windowTestFile(t, fileMiB/4)
	link := &simLink{rate: rateMbit * 1e6 / 8 * scale, latency: time.Duration(float64(latency) / scale)}
	cache := newSimCache()
	rm := NewReadaheadManager(dir, nil, slowProfile(t))
	defer rm.Stop()
	if override != nil {
		pol := *override
		rm.policyOverride = func() netprofile.ReadaheadPolicy { return pol }
	}
	rm.prefetchReadAt = func(fd *os.File, buf []byte, off int64) (int, error) {
		cache.fetch(link, off, len(buf), true)
		return len(buf), nil
	}

	var samples, sum int64
	var peak int32
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				n := link.prefetchInflight.Load()
				samples++
				sum += int64(n)
				if n > peak {
					peak = n
				}
			}
		}
	}()

	size := int64(fileMiB) << 20
	t0 := time.Now()
	for off := int64(0); off < size; off += simUnit {
		cache.fetch(link, off, simUnit, false)
		rm.OnRead(600, off, simUnit, name)
	}
	elapsed := time.Since(t0)
	close(stop)
	<-sampled
	_, prefetched := rm.Stats()
	return simResult{
		mbps:         float64(size*8) / 1e6 / (elapsed.Seconds() * scale),
		avgInflight:  float64(sum) / float64(samples),
		peakInflight: peak,
		prefetched:   prefetched,
	}
}

func TestReadaheadSlowLinkSimulation(t *testing.T) {
	if os.Getenv("JM_READAHEAD_SIM") != "1" {
		t.Skip("set JM_READAHEAD_SIM=1 to run the slow-link simulation")
	}
	t.Setenv("JM_SERVER_READAHEAD", "")
	resetPrefetchOfflineState(t)
	forceDefaultClass(t, netprofile.ClassSlow)

	// The old slow policy and its gate: 2 blocks, 2 rounds, serial buffer,
	// FUSE data gate 4 (background 2).
	old := netprofile.ReadaheadPolicy{Enabled: true, SeqThreshold: 4, Blocks: 2, Workers: 2}
	const scale = 10 // run 10x faster than the real link; results are rescaled
	for _, sc := range []struct {
		name     string
		rateMbit float64
		latency  time.Duration
	}{
		// Request latency is about two round trips for a ranged GET on a warm
		// connection (TTFB measured ~1 s on the 200 ms lab profile was a cold
		// connection).
		{"abroad-noloss (200 ms RTT, 30 Mbit/s)", 30, 400 * time.Millisecond},
		// 20 Mbit/s classifies Metered in the product (unchanged by this
		// policy); this row shows the same link as if it were Slow.
		{"cellular (120 ms RTT, 20 Mbit/s) as Slow", 20, 240 * time.Millisecond},
		{"same-city (20 ms RTT, 200 Mbit/s)", 200, 40 * time.Millisecond},
	} {
		var before, after simResult
		func() {
			t.Setenv("JM_FUSE_DATA_GATE", "4")
			before = runReadaheadSim(t, scale, sc.rateMbit, sc.latency, 96, &old)
		}()
		t.Setenv("JM_FUSE_DATA_GATE", "")
		after = runReadaheadSim(t, scale, sc.rateMbit, sc.latency, 96, nil)
		t.Log(fmt.Sprintf("%-40s before: %5.1f Mbit/s, prefetch requests in flight avg %.2f peak %d (%d ReadAt calls) | after: %5.1f Mbit/s, avg %.2f peak %d (%d blocks)",
			sc.name, before.mbps, before.avgInflight, before.peakInflight, before.prefetched,
			after.mbps, after.avgInflight, after.peakInflight, after.prefetched))
	}
}
