package pin

import (
	"context"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// blockcache.go — the AUTHORITATIVE "total cache used" gauge.
//
// THE BUG this fixes: "cache used" historically reported the pinned-only
// aggregate (pin store SUM(bytes_cached)) — a SUBSET of the real on-disk LRU
// block cache. A second wrong source was a filepath.WalkDir du of the cache
// dir (capacity.go dirUsageBytes): approximate, racy, and it counts staging /
// temp chunk files that aren't part of the addressable cache.
//
// THE CORRECT VALUE is JuiceFS's own `juicefs_blockcache_bytes` gauge —
// "bytes currently in the on-disk block cache" — exposed on the FUSE daemon's
// Prometheus /metrics endpoint. We scrape it with the same proven pattern the
// sync path uses (internal/manager/sync.go pollJuicefsMetrics/parseJuicefsMetrics):
// a short-timeout HTTP GET, regex-parse one line, int64 it.
//
// HOT-PATH DISCIPLINE (NFS perf): never scrape per-RPC. BlockCacheBytes is
// pulled LAZILY on the /cache-status poll cadence (~2s) behind a min-interval
// throttle, and the last good value is cached so a transient scrape failure
// reuses it (and ultimately the bridge falls back to the dir-walk). The HTTP
// GET runs under a 3s-timeout context, so a wedged metrics endpoint can never
// block the popper.

// blockCacheLineRegex captures the value of the juicefs_blockcache_bytes gauge.
// JuiceFS emits it either bare (`juicefs_blockcache_bytes 1.234e+08`) or with
// labels (`juicefs_blockcache_bytes{...} 1.234e+08`) depending on version, so
// the `{...}` is optional. Value is a Prometheus float (may be in 1.2e+08 form).
var blockCacheLineRegex = regexp.MustCompile(`^juicefs_blockcache_bytes(?:\{[^}]*\})?\s+([0-9.eE+-]+)`)

// parseBlockCacheBytes extracts the juicefs_blockcache_bytes gauge from a
// /metrics scrape body. Returns (bytes, true) when the line is found and
// parses; (0, false) when the line is missing or malformed. Rounds the
// Prometheus float down to int64 for the wire format (mirrors the sync parser).
func parseBlockCacheBytes(body []byte) (int64, bool) {
	for _, line := range strings.Split(string(body), "\n") {
		if !strings.HasPrefix(line, "juicefs_blockcache_bytes") {
			continue
		}
		m := blockCacheLineRegex.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		val, err := strconv.ParseFloat(m[1], 64)
		if err != nil || val < 0 {
			return 0, false
		}
		return int64(val), true
	}
	return 0, false
}

// blockCacheScraper caches the last good juicefs_blockcache_bytes value and the
// addr it scrapes, so the /cache-status poll path can ask for the gauge cheaply
// and reuse the last value across a transient scrape miss.
type blockCacheScraper struct {
	mu       sync.Mutex
	addr     string // host:port of the FUSE daemon's prometheus metrics
	client   *http.Client
	lastVal  int64     // last successfully scraped gauge value
	lastOK   bool      // have we ever scraped successfully?
	lastTry  time.Time // last scrape attempt (throttle anchor)
	minEvery time.Duration
}

// blockCache is the process-wide scraper singleton. Address is set once at
// mount time via SetBlockCacheMetricsAddr; reads go through BlockCacheBytes.
var blockCache = &blockCacheScraper{
	client:   &http.Client{Timeout: 3 * time.Second},
	minEvery: 2 * time.Second,
}

// SetBlockCacheMetricsAddr records the FUSE daemon's prometheus metrics
// host:port (the value passed as `--metrics` to `juicefs mount`). Called once
// the mount addr is known. Empty addr disables scraping (BlockCacheBytes then
// reports unavailable and the bridge keeps using the dir-walk fallback).
// Changing the addr drops any cached value so we don't report a stale gauge
// from the previous daemon.
func SetBlockCacheMetricsAddr(addr string) {
	blockCache.mu.Lock()
	if addr != blockCache.addr {
		blockCache.addr = addr
		blockCache.lastVal = 0
		blockCache.lastOK = false
		blockCache.lastTry = time.Time{}
	}
	blockCache.mu.Unlock()
}

// BlockCacheBytes returns the JuiceFS on-disk block-cache size in bytes and
// whether a value is available. It scrapes the FUSE daemon's /metrics endpoint
// LAZILY — at most once per minEvery — and otherwise returns the last good
// value. (0, false) means no addr configured or no successful scrape yet AND no
// cached value; the caller must fall back. Never blocks longer than the HTTP
// client timeout; safe to call from the /cache-status poll path, NEVER per-RPC.
func BlockCacheBytes() (int64, bool) {
	return blockCache.bytes()
}

func (s *blockCacheScraper) bytes() (int64, bool) {
	s.mu.Lock()
	addr := s.addr
	now := time.Now()
	due := s.lastTry.IsZero() || now.Sub(s.lastTry) >= s.minEvery
	lastVal, lastOK := s.lastVal, s.lastOK
	if addr == "" {
		s.mu.Unlock()
		return 0, false
	}
	if !due {
		s.mu.Unlock()
		return lastVal, lastOK
	}
	s.lastTry = now
	client := s.client
	s.mu.Unlock()

	val, ok := scrapeBlockCache(client, addr)
	if !ok {
		// Reuse the last good value across a transient miss; if we never
		// scraped successfully, signal unavailable so the bridge falls back.
		return lastVal, lastOK
	}
	s.mu.Lock()
	s.lastVal = val
	s.lastOK = true
	s.mu.Unlock()
	return val, true
}

// scrapeBlockCache performs one bounded GET of http://addr/metrics and parses
// the gauge. Nil/err-safe: any transport error, non-200, or missing line
// returns (0, false). Mirrors internal/manager/sync.go's scrape closure.
func scrapeBlockCache(client *http.Client, addr string) (int64, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/metrics", nil)
	if err != nil {
		return 0, false
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, false
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return 0, false
	}
	return parseBlockCacheBytes(body)
}
