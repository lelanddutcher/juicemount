package chunk

// JuiceMount (patch 0021): demand spans, with --demand-first.
//
// A block-cache miss of an unencrypted object fetches the span around the
// read: from the 256 KiB part that holds the read, up to 1 MiB, but not past
// the block. Each 256 KiB part is its own ranged GET and they run in
// parallel. The parts stay in memory, so the other reads of that span wait
// only for their own part, which is already on its way.
//
// Why (2026-11-11 laptop lab, 200 ms RTT): macOS sends a 1 MiB read as
// 128 KiB FUSE reads, one after another. Each was its own ranged GET, and
// each also queued a whole-block prefetch of its block:
//   - with server BBR a scrub jump cost eight serial GET round trips (about
//     5.5 s) and fetched about 7 times the bytes it needed;
//   - with 1% loss and cubic each TCP stream carries about 0.7 Mbit/s, the
//     single prefetch worker downloaded whole 4 MiB blocks one after another
//     (about 50 s each), and demand reads queued behind them.
// Parallel parts use several streams, a jump costs about one part's GET,
// and nothing fetches a byte twice. With spans, a ranged read no longer
// queues a whole-block prefetch: a reader that reads a whole block through
// gets it from its parts, and a full block of parts goes to the disk cache.
//
// Object keys name immutable objects (slice id, index, size), so a part can
// never hold stale bytes: no invalidation is needed.

import (
	"container/list"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/utils"
)

const (
	spanPartSize = 256 << 10
	spanSize     = 1 << 20
	// spanMemMax bounds the bytes of finished parts kept in memory; the
	// oldest go first.
	spanMemMax = 128 << 20
	// spanTTL: a finished part older than this is fetched again. It is
	// longer than the macOS NFS client's longest JUKEBOX backoff (30 s), so
	// a retry still finds the bytes of the read it repeats.
	spanTTL = 2 * time.Minute
)

type spanPart struct {
	key  string
	off  int // offset in the block
	n    int
	done chan struct{} // closed when data or err is set
	data []byte
	err  error
	at   time.Time     // when it finished
	elem *list.Element // in spanCache.lru while it is kept
}

// spanCache holds the parts in flight and the finished ones.
type spanCache struct {
	mu    sync.Mutex
	parts map[string]map[int]*spanPart // key -> part offset -> part
	lru   *list.List                   // finished parts, oldest first
	bytes int
	max   int
	ttl   time.Duration
	// fetch downloads one part; tests replace it.
	fetch func(sp *spanPart) ([]byte, error)
	// whole is called once when every part of a block has finished, with
	// the parts in order (the store writes them to the disk cache).
	whole func(key string, parts [][]byte, size int)
	// cachedWhole remembers the blocks already handed to whole.
	cachedWhole map[string]bool
	// demand marks a read waiting for a part of key, for patch 0020's
	// demand signal; the returned func ends it. Parts nobody waits for are
	// not demand reads.
	demand func(key string) func()
	// sweeping is set while a sweep timer is armed.
	sweeping bool
}

func newSpanCache(max int, ttl time.Duration) *spanCache {
	return &spanCache{parts: make(map[string]map[int]*spanPart), lru: list.New(), max: max, ttl: ttl,
		cachedWhole: make(map[string]bool)}
}

// lookupLocked returns the live part at off of key, dropping an expired one.
func (c *spanCache) lookupLocked(key string, off int) *spanPart {
	sp := c.parts[key][off]
	if sp == nil {
		return nil
	}
	if sp.elem != nil && time.Since(sp.at) > c.ttl {
		c.removeLocked(sp)
		return nil
	}
	return sp
}

// sweepLocked drops finished parts older than ttl. The LRU is in finish
// order (a hit does not move a part), so it stops at the first fresh one.
func (c *spanCache) sweepLocked() {
	for e := c.lru.Front(); e != nil; e = c.lru.Front() {
		sp := e.Value.(*spanPart)
		if time.Since(sp.at) <= c.ttl {
			return
		}
		c.removeLocked(sp)
	}
}

// armSweepLocked makes sure kept parts are swept even when no read comes:
// a timer runs every ttl while any part is kept.
func (c *spanCache) armSweepLocked() {
	if c.sweeping || c.lru.Len() == 0 {
		return
	}
	c.sweeping = true
	time.AfterFunc(c.ttl, func() {
		c.mu.Lock()
		c.sweeping = false
		c.sweepLocked()
		c.armSweepLocked()
		c.mu.Unlock()
	})
}

func (c *spanCache) removeLocked(sp *spanPart) {
	if m := c.parts[sp.key]; m[sp.off] == sp {
		delete(m, sp.off)
		if len(m) == 0 {
			delete(c.parts, sp.key)
			delete(c.cachedWhole, sp.key)
		}
	}
	if sp.elem != nil {
		c.lru.Remove(sp.elem)
		sp.elem = nil
		c.bytes -= len(sp.data)
	}
}

// read serves p, at offset boff of the block key (bsize bytes), from parts.
// It starts the parts of the span that are missing and waits for the ones p
// needs. ok is false when a needed part failed; the caller then reads on its
// own.
func (c *spanCache) read(key string, boff int, p []byte, bsize int) (n int, started int, ok bool) {
	if len(p) == 0 || boff >= bsize {
		return 0, 0, true
	}
	end := boff + len(p)
	if end > bsize {
		end = bsize
	}
	first := boff / spanPartSize * spanPartSize
	var need []*spanPart
	var start []*spanPart
	c.mu.Lock()
	c.sweepLocked()
	missing := false
	for off := first; off < end; off += spanPartSize {
		if c.lookupLocked(key, off) == nil {
			missing = true
			break
		}
	}
	if missing {
		// Fetch the span from the read's part: up to spanSize, within the
		// block, skipping parts already here.
		limit := first + spanSize
		if limit > bsize {
			limit = bsize
		}
		if limit < end {
			limit = end
		}
		for off := first; off < limit; off += spanPartSize {
			if c.lookupLocked(key, off) != nil {
				continue
			}
			sz := spanPartSize
			if off+sz > bsize {
				sz = bsize - off
			}
			sp := &spanPart{key: key, off: off, n: sz, done: make(chan struct{})}
			m := c.parts[key]
			if m == nil {
				m = make(map[int]*spanPart)
				c.parts[key] = m
			}
			m[off] = sp
			start = append(start, sp)
		}
	}
	for off := first; off < end; off += spanPartSize {
		need = append(need, c.parts[key][off])
	}
	c.mu.Unlock()
	for _, sp := range start {
		go c.run(sp, bsize)
	}
	for _, sp := range need {
		select {
		case <-sp.done:
		default:
			end := func() {}
			if c.demand != nil {
				end = c.demand(key)
			}
			<-sp.done
			end()
		}
		if sp.err != nil {
			return 0, len(start), false
		}
	}
	for _, sp := range need {
		lo, hi := boff, end
		if lo < sp.off {
			lo = sp.off
		}
		if hi > sp.off+len(sp.data) {
			hi = sp.off + len(sp.data)
		}
		if lo >= hi {
			return 0, len(start), false // short part: let the caller read
		}
		n += copy(p[lo-boff:hi-boff], sp.data[lo-sp.off:hi-sp.off])
	}
	return n, len(start), true
}

// run fetches one part and publishes it.
func (c *spanCache) run(sp *spanPart, bsize int) {
	data, err := c.fetch(sp)
	if err == nil && len(data) != sp.n {
		err = fmt.Errorf("part %s@%d: %d of %d bytes", sp.key, sp.off, len(data), sp.n)
	}
	var whole [][]byte
	c.mu.Lock()
	sp.at = time.Now()
	if err != nil {
		sp.err = err
		if m := c.parts[sp.key]; m[sp.off] == sp {
			delete(m, sp.off) // a later read tries again
			if len(m) == 0 {
				delete(c.parts, sp.key)
			}
		}
	} else {
		sp.data = data
		if c.parts[sp.key][sp.off] == sp {
			sp.elem = c.lru.PushBack(sp)
			c.bytes += len(data)
			for c.bytes > c.max && c.lru.Len() > 0 {
				c.removeLocked(c.lru.Front().Value.(*spanPart))
			}
			c.armSweepLocked()
		}
		if c.whole != nil && !c.cachedWhole[sp.key] {
			whole = c.wholeLocked(sp.key, bsize)
		}
	}
	c.mu.Unlock()
	close(sp.done)
	if whole != nil {
		c.whole(sp.key, whole, bsize)
	}
}

// wholeLocked returns the block's parts in order when every part of it is
// here and finished, once per kept block. Finished parts are never written
// again, so the caller may read them after the lock is released.
func (c *spanCache) wholeLocked(key string, bsize int) [][]byte {
	m := c.parts[key]
	if len(m)*spanPartSize < bsize {
		return nil
	}
	var parts [][]byte
	for off := 0; off < bsize; off += spanPartSize {
		sp := m[off]
		if sp == nil || sp.elem == nil || len(sp.data) != min(spanPartSize, bsize-off) {
			return nil
		}
		parts = append(parts, sp.data)
	}
	c.cachedWhole[key] = true
	return parts
}

// spansOn reports whether a miss of this store reads through demand spans:
// --demand-first, an object store with ranged GETs, and no encryption
// (an encrypted object is decrypted whole for any range).
func (store *cachedStore) spansOn() bool {
	return store.spans != nil && store.seekable && !store.conf.Encrypted
}

// newStoreSpans wires the span cache to the store's object GETs and its
// disk cache.
func newStoreSpans(store *cachedStore) *spanCache {
	c := newSpanCache(spanMemMax, spanTTL)
	c.fetch = func(sp *spanPart) ([]byte, error) {
		if store.downLimit != nil {
			store.downLimit.Wait(int64(sp.n))
		}
		buf := make([]byte, sp.n)
		var (
			reqID     string
			sc        = object.DefaultStorageClass
			n         int
			firstByte time.Duration
		)
		st := time.Now()
		err := utils.WithTimeout(func() error {
			in, err := store.storage.Get(sp.key, int64(sp.off), int64(sp.n), object.WithRequestID(&reqID), object.WithStorageClass(&sc))
			firstByte = time.Since(st)
			if err == nil {
				n, err = io.ReadFull(in, buf)
				_ = in.Close()
			}
			return err
		}, store.conf.GetTimeout)
		used := time.Since(st)
		if readTimelineOn {
			readTimeline("rt jfs span part key=%s off=%d len=%d first_byte_ms=%d ms=%d err=%v", sp.key, sp.off, sp.n, firstByte.Milliseconds(), used.Milliseconds(), err)
		}
		logRequest("GET", sp.key, fmt.Sprintf("RANGE(%d,%d) ", sp.off, sp.n), reqID, err, used)
		store.objectDataBytes.WithLabelValues("GET", sc).Add(float64(n))
		store.objectReqsHistogram.WithLabelValues("GET", sc).Observe(used.Seconds())
		if err != nil {
			store.objectReqErrors.Add(1)
			return nil, err
		}
		store.demand.observeRanged(used, sp.n)
		return buf[:n], nil
	}
	c.whole = func(key string, parts [][]byte, size int) {
		if !store.conf.CacheEnabled() || !store.shouldCache(size) {
			return
		}
		if _, ok := store.bcache.exist(key); ok {
			return
		}
		p := NewOffPage(size)
		n := 0
		for _, part := range parts {
			n += copy(p.Data[n:], part)
		}
		store.bcache.cache(key, p, false, !store.conf.OSCache)
		p.Release()
		if readTimelineOn {
			readTimeline("rt jfs span block cached key=%s", key)
		}
	}
	c.demand = store.demand.beginDemand
	return c
}
