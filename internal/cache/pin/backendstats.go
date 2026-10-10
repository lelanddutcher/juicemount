package pin

import (
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// backendstats.go — how much of a read actually crossed the link.
//
// THE GAP THIS CLOSES: our own /metrics `bytes_read` counts bytes SERVED TO THE
// NFS CLIENT. A byte served from the local SSD block cache and a byte dragged
// over a cellular uplink from MinIO are indistinguishable in it. So the single
// most important question about a cellular session — "when a file is opened,
// does it round-trip to the object store, or is the cached copy served?" — was
// not answerable from anything we published.
//
// It was, however, already answered on the wire. The JuiceFS mount daemon
// exposes its own Prometheus endpoint (the `--metrics` addr, see health/fuse.go)
// carrying exactly the counters needed:
//
//	juicefs_blockcache_hits / _miss              block reads served locally vs not
//	juicefs_blockcache_hit_bytes / _miss_bytes   the same in bytes
//	juicefs_object_request_data_bytes{method=…}  bytes that really moved to/from MinIO
//	juicefs_meta_ops_…_count                     metadata ops = Redis round trips
//
// We were already scraping that endpoint and taking exactly ONE key from it
// (juicefs_blockcache_bytes, see blockcache.go). This reads the rest from the
// SAME scrape body, so it costs no extra HTTP request.
//
// WHY object bytes AND miss bytes: they differ, and the difference is the
// finding. On a live LAN session: miss_bytes 726.1 GB against object GET
// 880.1 GB — the backend delivered 1.21x more than was actually missed. That
// gap is read amplification (readahead pulling past the request), the same
// effect measured at 4KB->53MB on cellular in [[project_wan_read_amplification]].
// One number cannot show it; the pair can.
//
// HOT-PATH DISCIPLINE: same as BlockCacheBytes — lazy, throttled, last-good
// cached, never per-RPC.

// backendStatLines maps a Prometheus metric name to the field it fills.
// juicefs emits each either bare or with a {label,...} block depending on
// version, so the label block is optional in the pattern.
var backendStatRe = regexp.MustCompile(`^(juicefs_[a-z_]+)(?:\{([^}]*)\})?\s+([0-9.eE+-]+)`)

// BackendStats reports how much work reached the object store and the metadata
// store, versus how much was served from the local block cache.
//
// Every field is a monotonically increasing counter since the JuiceFS daemon
// started, so a measurement is a DIFFERENCE across the window under test, not a
// single reading. Take one before the operation and one after.
type BackendStats struct {
	// CacheHits / CacheMiss count block reads, not bytes. The hit RATIO is the
	// headline answer to "was the cached copy served?".
	CacheHits int64 `json:"cache_hits"`
	CacheMiss int64 `json:"cache_miss"`
	// CacheHitBytes / CacheMissBytes are the same split in bytes.
	CacheHitBytes  int64 `json:"cache_hit_bytes"`
	CacheMissBytes int64 `json:"cache_miss_bytes"`
	// ObjectGetBytes / ObjectPutBytes are bytes that ACTUALLY crossed to MinIO.
	// Compare ObjectGetBytes against CacheMissBytes: the excess is read
	// amplification, and on a metered link it is the number that matters.
	ObjectGetBytes int64 `json:"object_get_bytes"`
	ObjectPutBytes int64 `json:"object_put_bytes"`
	// MetaOps counts metadata operations, i.e. Redis round trips. This is the
	// per-OPEN cost that dominates a high-RTT link: on the 2026-07-29 cellular
	// session, 67 minutes of cumulative metadata wait against only 77 object
	// GETs. Cost is per FILE, not per byte.
	MetaOps int64 `json:"meta_ops"`
}

type backendScraper struct {
	mu       sync.Mutex
	lastVal  BackendStats
	lastOK   bool
	lastTry  time.Time
	minEvery time.Duration
}

var backendStats = &backendScraper{minEvery: 2 * time.Second}

// BackendStatsSnapshot returns the JuiceFS daemon's backend/cache counters and
// whether a value is available.
//
// (zero, false) means no addr configured or no successful scrape yet. It NEVER
// returns zeros with ok=true from a failed scrape: a cellular run that reports
// "0 bytes from the backend" because the scrape failed would be read as a
// spectacular cache-hit result. That is the "harnesses must refuse to report"
// rule applied at the source rather than in the harness.
func BackendStatsSnapshot() (BackendStats, bool) {
	blockCache.mu.Lock()
	addr := blockCache.addr
	client := blockCache.client
	blockCache.mu.Unlock()
	if addr == "" {
		return BackendStats{}, false
	}

	backendStats.mu.Lock()
	now := time.Now()
	due := backendStats.lastTry.IsZero() || now.Sub(backendStats.lastTry) >= backendStats.minEvery
	lastVal, lastOK := backendStats.lastVal, backendStats.lastOK
	if !due {
		backendStats.mu.Unlock()
		return lastVal, lastOK
	}
	backendStats.lastTry = now
	backendStats.mu.Unlock()

	body, ok := scrapeBody(client, addr)
	if !ok {
		// Reuse the last good value across a transient miss, exactly as
		// BlockCacheBytes does. Never fabricate zeros.
		return lastVal, lastOK
	}
	val, ok := parseBackendStats(body)
	if !ok {
		return lastVal, lastOK
	}
	backendStats.mu.Lock()
	backendStats.lastVal = val
	backendStats.lastOK = true
	backendStats.mu.Unlock()
	return val, true
}

// parseBackendStats extracts the backend/cache counters from a /metrics body.
//
// Returns ok=false when NONE of the expected counters were present — a body
// that parsed but contained nothing we recognise means we scraped the wrong
// endpoint (or a juicefs version that renamed them), and reporting all-zeros
// from that would look identical to a perfect cache-hit session.
func parseBackendStats(body []byte) (BackendStats, bool) {
	var out BackendStats
	found := false
	for _, line := range strings.Split(string(body), "\n") {
		if !strings.HasPrefix(line, "juicefs_") {
			continue
		}
		m := backendStatRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		v, err := strconv.ParseFloat(m[3], 64)
		if err != nil || v < 0 {
			continue
		}
		n := int64(v)
		switch m[1] {
		case "juicefs_blockcache_hits":
			out.CacheHits, found = n, true
		case "juicefs_blockcache_miss":
			out.CacheMiss, found = n, true
		case "juicefs_blockcache_hit_bytes":
			out.CacheHitBytes, found = n, true
		case "juicefs_blockcache_miss_bytes":
			out.CacheMissBytes, found = n, true
		case "juicefs_meta_ops_durations_histogram_seconds_count":
			out.MetaOps, found = n, true
		case "juicefs_object_request_data_bytes":
			// Labelled by method; GET and PUT are separate lines.
			switch {
			case strings.Contains(m[2], `method="GET"`):
				out.ObjectGetBytes, found = n, true
			case strings.Contains(m[2], `method="PUT"`):
				out.ObjectPutBytes, found = n, true
			}
		}
	}
	return out, found
}
