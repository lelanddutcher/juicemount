package nfs

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
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
	mbps         float64 // all readers together, real-link units
	avgInflight  float64
	peakInflight int32
	prefetched   int64
	fgP50        time.Duration // foreground subread latency, real-link units
	fgP99        time.Duration
	fgMax        time.Duration
	jukebox      int64         // foreground gate refusals (JUKEBOX retries)
	drainMax     time.Duration // longest drain wait for a background slot
}

type simVariant struct {
	name     string
	policy   *netprofile.ReadaheadPolicy // nil = the product's Slow policy
	gate     string                      // JM_FUSE_DATA_GATE
	windowCp int                         // windowCapOverride (0 = product)
}

func percentile(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	return d[int(float64(len(d)-1)*p)]
}

// runReadaheadSim reads `readers` files of fileMiB each at the same time.
// Each foreground subread goes through the Read-QoS bulk lane and the FUSE
// data gate as the handler's does. With drainer set, a spool drain polls the
// background gate every 10 ms (1 ms scaled) and holds a slot 50 ms (5 ms).
func runReadaheadSim(t *testing.T, scale float64, rateMbit float64, latency time.Duration, fileMiB, readers int, drainer bool, v simVariant) simResult {
	t.Helper()
	t.Setenv("JM_FUSE_DATA_GATE", v.gate)
	fuseDataInFlight.Store(0)
	dir := t.TempDir()
	caches := map[string]*simCache{}
	names := make([]string, readers)
	for r := range names {
		names[r] = fmt.Sprintf("reel%d.mov", r)
		f, err := os.Create(filepath.Join(dir, names[r]))
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Truncate(int64(fileMiB) << 20); err != nil {
			t.Fatal(err)
		}
		f.Close()
		caches[filepath.Join(dir, names[r])] = newSimCache()
	}
	link := &simLink{rate: rateMbit * 1e6 / 8 * scale, latency: time.Duration(float64(latency) / scale)}
	rm := NewReadaheadManager(dir, nil, slowProfile(t))
	defer rm.Stop()
	if v.policy != nil {
		pol := *v.policy
		rm.policyOverride = func() netprofile.ReadaheadPolicy { return pol }
	}
	rm.windowCapOverride = v.windowCp
	rm.prefetchReadAt = func(fd *os.File, buf []byte, off int64) (int, error) {
		caches[fd.Name()].fetch(link, off, len(buf), true)
		return len(buf), nil
	}

	var samples, sum int64
	var peak int32
	stop := make(chan struct{})
	var bg sync.WaitGroup
	bg.Add(1)
	go func() {
		defer bg.Done()
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
	var drainMax time.Duration
	if drainer {
		bg.Add(1)
		go func() {
			defer bg.Done()
			for {
				t0 := time.Now()
				for {
					select {
					case <-stop:
						return
					default:
					}
					if rel, ok := tryAcquireFUSEDataBackground(); ok {
						if w := time.Since(t0); w > drainMax {
							drainMax = w
						}
						time.Sleep(5 * time.Millisecond)
						rel()
						break
					}
					time.Sleep(time.Millisecond)
				}
			}
		}()
	}

	size := int64(fileMiB) << 20
	var mu sync.Mutex
	var lat []time.Duration
	var jukebox atomic.Int64
	var rw sync.WaitGroup
	t0 := time.Now()
	for r := 0; r < readers; r++ {
		rw.Add(1)
		go func(r int) {
			defer rw.Done()
			cache := caches[filepath.Join(dir, names[r])]
			var mine []time.Duration
			for off := int64(0); off < size; off += simUnit {
				s0 := time.Now()
				qos := defaultReadQoS.acquire(off, simUnit)
				for {
					rel, ok := acquireFUSEData()
					if ok {
						cache.fetch(link, off, simUnit, false)
						rel()
						break
					}
					jukebox.Add(1) // the client retries the READ
				}
				qos()
				mine = append(mine, time.Duration(float64(time.Since(s0))*scale))
				rm.OnRead(uint64(600+r), off, simUnit, names[r])
			}
			mu.Lock()
			lat = append(lat, mine...)
			mu.Unlock()
		}(r)
	}
	rw.Wait()
	elapsed := time.Since(t0)
	close(stop)
	bg.Wait()
	_, prefetched := rm.Stats()
	return simResult{
		mbps:         float64(int64(readers)*size*8) / 1e6 / (elapsed.Seconds() * scale),
		avgInflight:  float64(sum) / float64(samples),
		peakInflight: peak,
		prefetched:   prefetched,
		fgP50:        percentile(lat, 0.5),
		fgP99:        percentile(lat, 0.99),
		fgMax:        percentile(lat, 1),
		jukebox:      jukebox.Load(),
		drainMax:     time.Duration(float64(drainMax) * scale),
	}
}

func TestReadaheadSlowLinkSimulation(t *testing.T) {
	if os.Getenv("JM_READAHEAD_SIM") != "1" {
		t.Skip("set JM_READAHEAD_SIM=1 to run the slow-link simulation")
	}
	t.Setenv("JM_SERVER_READAHEAD", "")
	resetPrefetchOfflineState(t)
	forceDefaultClass(t, netprofile.ClassSlow)
	t.Cleanup(func() { fuseDataInFlight.Store(0) })

	// The old slow policy and its gate: 2 blocks, 2 rounds, serial buffer,
	// FUSE data gate 4 (background 2).
	old := netprofile.ReadaheadPolicy{Enabled: true, SeqThreshold: 4, Blocks: 2, Workers: 2}
	variants := []simVariant{
		{name: "before", policy: &old, gate: "4"},
		{name: "after, 4 in flight", windowCp: 4},
		{name: "after, 3 in flight (shipped)"},
	}
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
		for _, load := range []struct {
			name    string
			readers int
			drainer bool
		}{
			{"1 reader", 1, false},
			{"2 readers + drain", 2, true},
		} {
			for _, v := range variants {
				r := runReadaheadSim(t, scale, sc.rateMbit, sc.latency, 96, load.readers, load.drainer, v)
				t.Logf("| %s | %s | %s | %.1f | %.2f / %d | %v / %v / %v | %d | %v |",
					sc.name, load.name, v.name, r.mbps, r.avgInflight, r.peakInflight,
					r.fgP50.Round(time.Millisecond), r.fgP99.Round(time.Millisecond), r.fgMax.Round(time.Millisecond),
					r.jukebox, r.drainMax.Round(time.Millisecond))
			}
		}
	}
}
