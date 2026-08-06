package nfs

import (
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	// Files under this size are candidates for memory buffering.
	DefaultMemBufThreshold = 128 * 1024 * 1024 // 128MB

	// Maximum total memory for all buffered files.
	DefaultMemBufBudget = 2 * 1024 * 1024 * 1024 // 2GB

	// Buffered files not accessed in this duration are evicted.
	memBufIdleTTL = 2 * time.Minute

	// Chunk size for async loading.
	memBufChunkSize = 4 * 1024 * 1024 // 4MB (matches JuiceFS block size)

	// Max time a concurrent reader will wait for an in-flight loadFile to
	// complete before falling through to the SSD-direct cache reader. A
	// wedged FUSE would otherwise park every reader of the same file
	// forever on the entry.ready channel.
	memBufLoadTimeout = 5 * time.Second

	// Default cap on concurrently-running loadFile goroutines. Each loadFile
	// does an os.Open on the FUSE mount; when the mount is wedged/slow that
	// open blocks in the kernel, pinning one OS thread for the duration.
	// Without a cap, a burst of distinct small-file reads (e.g. Spotlight or
	// QuickLook indexing a freshly-listed tree) spawns one blocked open per
	// file. The budget check is no real bound here — 2 GiB of ~1 KiB sidecar
	// files is ~2 million entries — so the process marches straight to the
	// macOS hard per-task thread limit (kern.num_taskthreads = 8192) and the
	// Go runtime aborts when pthread_create fails inside runtime.newm1
	// (crash 2026-06-14, 8178 threads parked in open()). This cap converts
	// that fan-out into a fixed worst-case thread cost. Tunable via
	// JM_MEMBUF_LOADERS.
	defaultMemBufLoaders = 16
)

// MemoryBuffer caches small files entirely in Go heap for zero-syscall reads.
// Good candidates: .prproj (1-50MB), .cube/.3dl LUTs (1-100KB),
// .xmp/.fcpxml sidecar files (small, read frequently).
// Bad candidates: media files (too large, one-pass sequential reads).
type MemoryBuffer struct {
	mu        sync.Mutex
	entries   map[string]*memBufEntry
	totalSize int64
	threshold int64 // max file size to buffer
	budget    int64 // max total heap usage

	stopCh chan struct{}

	// loadSem bounds the number of loadFile goroutines that can be blocked in
	// an os.Open on the FUSE mount at once. Buffered to defaultMemBufLoaders
	// (or JM_MEMBUF_LOADERS). Acquired non-blocking in Get before spawning a
	// loader; when saturated, Get declines to buffer and the caller falls
	// through to the per-RPC fd-pool read (already bounded by the NFS
	// server's rpcSem). This is the structural bound that keeps a FUSE wedge
	// from exhausting OS threads and aborting the process.
	loadSem chan struct{}

	// Stats
	statsMu     sync.Mutex
	hits        int64
	misses      int64
	evicts      int64
	loadSkipped int64 // loads declined because loadSem was saturated
}

type memBufEntry struct {
	data       []byte
	size       int64
	lastAccess time.Time
	loading    bool // true while async load is in progress
	ready      chan struct{} // closed when loading is complete
}

func NewMemoryBuffer(threshold, budget int64) *MemoryBuffer {
	if threshold <= 0 {
		threshold = DefaultMemBufThreshold
	}
	if budget <= 0 {
		budget = DefaultMemBufBudget
	}
	loaders := defaultMemBufLoaders
	if v := os.Getenv("JM_MEMBUF_LOADERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			loaders = n
		}
	}
	mb := &MemoryBuffer{
		entries:   make(map[string]*memBufEntry),
		threshold: threshold,
		budget:    budget,
		stopCh:    make(chan struct{}),
		loadSem:   make(chan struct{}, loaders),
	}
	go mb.evictLoop()
	return mb
}

// Get returns the buffered data for a file, or nil if not buffered.
// If the file is eligible but not yet buffered, it starts async loading
// and returns nil (caller should use FUSE/fd pool for this read).
func (mb *MemoryBuffer) Get(path string, fileSize int64, fusePath string) []byte {
	if fileSize > mb.threshold || fileSize <= 0 {
		return nil
	}

	mb.mu.Lock()
	entry, exists := mb.entries[path]

	if exists {
		if entry.loading {
			mb.mu.Unlock()
			// Wait for loading to complete, but bound it. A wedged FUSE
			// (MinIO unreachable, JuiceFS hung) would otherwise park every
			// subsequent reader of the same file forever on this receive,
			// cascading the loadFile goroutine's stall to every concurrent
			// NFS RPC touching this path. On timeout we return nil so the
			// caller falls through to the SSD-direct cacheReader (different
			// code path — reads JuiceFS chunk files via Redis slice lookup,
			// no FUSE involvement). If THAT also misses, the caller hits
			// the FUSE fd pool, where the stall is per-RPC rather than
			// cascading-shared.
			select {
			case <-entry.ready:
				// load finished — fall through
			case <-time.After(memBufLoadTimeout):
				return nil
			case <-mb.stopCh:
				return nil
			}
			mb.mu.Lock()
			entry = mb.entries[path]
			if entry == nil {
				mb.mu.Unlock()
				return nil
			}
		}
		entry.lastAccess = time.Now()
		data := entry.data
		mb.mu.Unlock()

		mb.statsMu.Lock()
		mb.hits++
		mb.statsMu.Unlock()

		return data
	}

	// Not buffered yet — check if we have budget
	if mb.totalSize+fileSize > mb.budget {
		mb.mu.Unlock()
		mb.statsMu.Lock()
		mb.misses++
		mb.statsMu.Unlock()
		return nil
	}

	// Bound concurrent background loads. Each loadFile blocks in os.Open on
	// the FUSE mount; under a wedge that pins an OS thread. Acquire a loader
	// slot non-blocking BEFORE inserting the entry — if the pool is
	// saturated we neither buffer nor spawn, and the caller falls through to
	// the per-RPC fd-pool read (bounded by rpcSem). Crucially we must not
	// insert a `loading` entry we can't load: that would park every
	// concurrent reader of this path on entry.ready until the 5s timeout for
	// no benefit. (Thread-exhaustion crash 2026-06-14.)
	select {
	case mb.loadSem <- struct{}{}:
		// acquired — proceed to insert + spawn below
	default:
		mb.mu.Unlock()
		mb.statsMu.Lock()
		mb.misses++
		mb.loadSkipped++
		mb.statsMu.Unlock()
		return nil
	}

	// Start async loading
	entry = &memBufEntry{
		size:       fileSize,
		lastAccess: time.Now(),
		loading:    true,
		ready:      make(chan struct{}),
	}
	mb.entries[path] = entry
	mb.totalSize += fileSize
	mb.mu.Unlock()

	mb.statsMu.Lock()
	mb.misses++
	mb.statsMu.Unlock()

	// Load in background (loadFile releases the loadSem slot on exit).
	go mb.loadFile(path, fusePath, fileSize, entry)

	return nil // caller uses FUSE for this first read
}

// ReadAt reads from a buffered file at the given offset.
// Returns bytes read and true if the file is buffered, or 0 and false if not.
func (mb *MemoryBuffer) ReadAt(path string, p []byte, off int64, fileSize int64, fusePath string) (int, bool) {
	data := mb.Get(path, fileSize, fusePath)
	if data == nil {
		return 0, false
	}

	if off >= int64(len(data)) {
		return 0, true
	}

	n := copy(p, data[off:])
	return n, true
}

func (mb *MemoryBuffer) loadFile(path, fusePath string, fileSize int64, entry *memBufEntry) {
	// Release the loader slot last (LIFO: close(entry.ready) runs first to
	// wake any waiters, then we free the slot for the next load).
	defer func() { <-mb.loadSem }()
	defer close(entry.ready)

	fd, err := os.Open(fusePath)
	if err != nil {
		// Loading failed — remove entry (identity-checked; a stale loader must
		// not clobber a replacement inserted after Invalidate + re-Get).
		mb.removeStaleEntry(path, entry)
		return
	}
	defer fd.Close()

	data := make([]byte, fileSize)
	totalRead := 0
	for totalRead < int(fileSize) {
		select {
		case <-mb.stopCh:
			mb.removeStaleEntry(path, entry)
			return
		default:
		}

		// FUSE data ceiling (2026-08-05 double kernel panic). This loop pulls a
		// WHOLE file through FUSE in the background, so it is exactly the kind
		// of bulk work that must yield to a user's read. Non-blocking acquire
		// for the same reason as readahead: speculative work never waits in the
		// admission window ahead of foreground.
		//
		// On refusal ABANDON the load rather than skipping the block. This loop
		// cannot tolerate a gap — the torn-read guard below rejects any short
		// load precisely because publishing data[:totalRead] would serve a
		// partial file as complete (the 2026-06-15 black-frame bug). Failing the
		// whole load is the safe outcome: membuf is a cache, and a miss costs a
		// re-read, while a hole costs corrupt bytes.
		gateRelease, gateOK := tryAcquireFUSEDataBackground()
		if !gateOK {
			mb.removeStaleEntry(path, entry)
			return
		}
		n, err := fd.ReadAt(data[totalRead:], int64(totalRead))
		gateRelease()
		totalRead += n
		if err == io.EOF {
			break
		}
		if err != nil {
			mb.removeStaleEntry(path, entry)
			return
		}
	}

	// [torn-read 2026-06-15] Reject a SHORT load. This background load runs
	// CONCURRENTLY with other loads (loadSem-bounded), so its sequential read
	// can hit the same premature-EOF transient that truncates the FUSE read
	// path. Publishing data[:totalRead] with totalRead < fileSize would make
	// memBuf serve a false EOF on every subsequent read of this file → the
	// client TRUNCATES it (silent data loss). Discard instead: reads fall
	// through to the retry-protected FUSE path (cachedFile.ReadAt P3), and a
	// later Get re-attempts a clean full load. (A genuinely short file would
	// have an accurate fileSize from the metadata cache, so totalRead==fileSize.)
	if totalRead < int(fileSize) {
		mb.removeStaleEntry(path, entry)
		return
	}

	mb.mu.Lock()
	entry.data = data[:totalRead]
	entry.size = int64(totalRead)
	entry.loading = false
	mb.mu.Unlock()
}

// removeStaleEntry drops path's entry and refunds its bytes ONLY if the map
// still points to THIS exact entry. A loadFile goroutine can sit blocked in
// os.Open on a wedged/slow FUSE mount long enough for Invalidate (fired on
// every write/rename/delete) to drop its entry AND a fresh Get to insert a NEW
// entry for the same path. Deleting by path key alone would then let the stale
// loader clobber the good replacement (a silent cache drop) and subtract the
// wrong byte count from totalSize — a monotonic leak that eventually makes the
// budget gate in Get refuse ALL buffering until the process restarts. The
// identity check (entries[path] == entry) makes stale cleanups no-ops.
func (mb *MemoryBuffer) removeStaleEntry(path string, entry *memBufEntry) {
	mb.mu.Lock()
	if mb.entries[path] == entry {
		delete(mb.entries, path)
		mb.totalSize -= entry.size
	}
	mb.mu.Unlock()
}

// Invalidate removes a file from the buffer (call on write/rename/delete).
func (mb *MemoryBuffer) Invalidate(path string) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	if entry, ok := mb.entries[path]; ok {
		mb.totalSize -= entry.size
		delete(mb.entries, path)
	}
}

// Stats returns buffer statistics.
func (mb *MemoryBuffer) Stats() (buffered int, totalMB float64, hits, misses, evicts int64) {
	mb.mu.Lock()
	buffered = len(mb.entries)
	totalMB = float64(mb.totalSize) / (1024 * 1024)
	mb.mu.Unlock()

	mb.statsMu.Lock()
	hits = mb.hits
	misses = mb.misses
	evicts = mb.evicts
	mb.statsMu.Unlock()
	return
}

func (mb *MemoryBuffer) evictLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-mb.stopCh:
			return
		case <-ticker.C:
			mb.evictStale()
		}
	}
}

func (mb *MemoryBuffer) evictStale() {
	now := time.Now()
	mb.mu.Lock()
	defer mb.mu.Unlock()

	for path, entry := range mb.entries {
		if !entry.loading && now.Sub(entry.lastAccess) > memBufIdleTTL {
			mb.totalSize -= entry.size
			delete(mb.entries, path)
			mb.statsMu.Lock()
			mb.evicts++
			mb.statsMu.Unlock()
		}
	}
}

// Stop clears all buffers and stops the eviction loop.
func (mb *MemoryBuffer) Stop() {
	close(mb.stopCh)
	mb.mu.Lock()
	mb.entries = nil
	mb.mu.Unlock()
	// Snapshot the counters under statsMu: a Get() racing shutdown (an NFS read
	// in flight) writes these under the same lock; an unlocked read here is a
	// data race (-race flags it).
	mb.statsMu.Lock()
	hits, misses, evicts, loadSkipped := mb.hits, mb.misses, mb.evicts, mb.loadSkipped
	mb.statsMu.Unlock()
	log.Printf("membuf: stopped (hits=%d, misses=%d, evicts=%d, loadSkipped=%d)", hits, misses, evicts, loadSkipped)
}
