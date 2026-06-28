package pin

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// capturedJuicefsMetrics is a trimmed-but-representative slice of a real
// `juicefs mount` /metrics body. The line we care about is
// juicefs_blockcache_bytes; the surrounding lines are present so the parser is
// exercised against realistic noise (HELP/TYPE comments, labeled gauges,
// other juicefs_* families).
const capturedJuicefsMetrics = `# HELP juicefs_blockcache_bytes Total bytes of cached blocks
# TYPE juicefs_blockcache_bytes gauge
juicefs_blockcache_bytes{mp="/Users/x/.juicemount/fuse-internal",vol_name="zpool"} 1.10729625e+11
# HELP juicefs_blockcache_blocks Total number of cached blocks
# TYPE juicefs_blockcache_blocks gauge
juicefs_blockcache_blocks{mp="/Users/x/.juicemount/fuse-internal",vol_name="zpool"} 26421
# HELP juicefs_used_space Total used space in bytes
# TYPE juicefs_used_space gauge
juicefs_used_space{vol_name="zpool"} 4.5097156608e+11
go_goroutines 88
`

func TestParseBlockCacheBytes(t *testing.T) {
	cases := []struct {
		name   string
		body   string
		want   int64
		wantOK bool
	}{
		{
			name:   "captured juicefs metrics body, labeled gauge",
			body:   capturedJuicefsMetrics,
			want:   110729625000, // 1.10729625e+11
			wantOK: true,
		},
		{
			name:   "bare gauge, no labels, integer",
			body:   "juicefs_blockcache_bytes 4096\n",
			want:   4096,
			wantOK: true,
		},
		{
			name:   "bare gauge, no labels, float exponent",
			body:   "juicefs_blockcache_bytes 1.5e+09\n",
			want:   1500000000,
			wantOK: true,
		},
		{
			name:   "zero is a valid value",
			body:   "juicefs_blockcache_bytes{mp=\"/x\"} 0\n",
			want:   0,
			wantOK: true,
		},
		{
			name:   "missing line falls back",
			body:   "go_goroutines 12\njuicefs_used_space 999\n",
			want:   0,
			wantOK: false,
		},
		{
			name:   "empty body",
			body:   "",
			want:   0,
			wantOK: false,
		},
		{
			name:   "malformed value (not a number)",
			body:   "juicefs_blockcache_bytes{mp=\"/x\"} not_a_number\n",
			want:   0,
			wantOK: false,
		},
		{
			name:   "negative value rejected (defensive)",
			body:   "juicefs_blockcache_bytes -5\n",
			want:   0,
			wantOK: false,
		},
		{
			// A counter with a similar prefix must NOT be mistaken for the
			// gauge: the prefix guard + anchored regex require an exact name.
			name:   "similarly-named counter is not matched",
			body:   "juicefs_blockcache_bytes_total 777\n",
			want:   0,
			wantOK: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseBlockCacheBytes([]byte(tc.body))
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if got != tc.want {
				t.Fatalf("bytes = %d, want %d", got, tc.want)
			}
		})
	}
}

// freshScraper returns a scraper with no throttle so tests can drive each
// bytes() call deterministically without the 2s min-interval gate.
func freshScraper() *blockCacheScraper {
	return &blockCacheScraper{
		client:   &http.Client{Timeout: 2 * time.Second},
		minEvery: 0,
	}
}

func TestBlockCacheScraperHappyPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(capturedJuicefsMetrics))
	}))
	defer srv.Close()

	s := freshScraper()
	s.addr = hostPort(srv.URL)

	got, ok := s.bytes()
	if !ok {
		t.Fatal("expected a value, got unavailable")
	}
	if got != 110729625000 {
		t.Fatalf("bytes = %d, want 110729625000", got)
	}
}

// TestBlockCacheScraperReusesLastGood verifies that once a value is cached, a
// later transient failure (endpoint goes away) reuses the last good value
// rather than reporting unavailable — so a flapping endpoint doesn't make
// cache_used_bytes drop to the fallback mid-session.
func TestBlockCacheScraperReusesLastGood(t *testing.T) {
	body := "juicefs_blockcache_bytes 8192\n"
	fail := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	s := freshScraper()
	s.addr = hostPort(srv.URL)

	if got, ok := s.bytes(); !ok || got != 8192 {
		t.Fatalf("first scrape: got %d ok=%v, want 8192 true", got, ok)
	}
	fail = true
	if got, ok := s.bytes(); !ok || got != 8192 {
		t.Fatalf("after endpoint failure: got %d ok=%v, want last-good 8192 true", got, ok)
	}
}

// TestBlockCacheScraperUnavailable covers the two "no value" paths the bridge
// must fall back on: no addr configured, and an unreachable addr with no prior
// success. Neither must panic or block.
func TestBlockCacheScraperUnavailable(t *testing.T) {
	t.Run("no addr", func(t *testing.T) {
		s := freshScraper()
		if got, ok := s.bytes(); ok || got != 0 {
			t.Fatalf("no addr: got %d ok=%v, want 0 false", got, ok)
		}
	})
	t.Run("unreachable addr, no prior success", func(t *testing.T) {
		s := freshScraper()
		s.client = &http.Client{Timeout: 200 * time.Millisecond}
		s.addr = "127.0.0.1:0" // nothing listens here
		if got, ok := s.bytes(); ok || got != 0 {
			t.Fatalf("unreachable: got %d ok=%v, want 0 false", got, ok)
		}
	})
}

// TestSetBlockCacheMetricsAddrResetsCache verifies that pointing the singleton
// at a new daemon drops the previously cached gauge so /cache-status never
// reports a stale value from the prior mount.
func TestSetBlockCacheMetricsAddrResetsCache(t *testing.T) {
	// Operate on the package singleton; restore its value fields afterward so we
	// don't leak state into other tests in this package. (Copy the value fields,
	// not the whole struct — it carries a sync.Mutex.)
	blockCache.mu.Lock()
	sAddr, sVal, sOK, sTry := blockCache.addr, blockCache.lastVal, blockCache.lastOK, blockCache.lastTry
	blockCache.mu.Unlock()
	t.Cleanup(func() {
		blockCache.mu.Lock()
		blockCache.addr, blockCache.lastVal, blockCache.lastOK, blockCache.lastTry = sAddr, sVal, sOK, sTry
		blockCache.mu.Unlock()
	})

	blockCache.addr = "127.0.0.1:9568"
	blockCache.lastVal = 12345
	blockCache.lastOK = true
	blockCache.lastTry = time.Now()

	SetBlockCacheMetricsAddr("127.0.0.1:9999")
	if blockCache.lastOK || blockCache.lastVal != 0 {
		t.Fatalf("changing addr did not reset cache: lastVal=%d lastOK=%v", blockCache.lastVal, blockCache.lastOK)
	}
	if !blockCache.lastTry.IsZero() {
		t.Fatalf("changing addr did not reset lastTry")
	}

	// Same addr again must NOT reset (idempotent).
	blockCache.lastVal = 77
	blockCache.lastOK = true
	SetBlockCacheMetricsAddr("127.0.0.1:9999")
	if !blockCache.lastOK || blockCache.lastVal != 77 {
		t.Fatalf("re-setting same addr wrongly reset the cache")
	}
}

// hostPort strips the http:// scheme from an httptest server URL, leaving the
// host:port the scraper expects.
func hostPort(serverURL string) string {
	const p = "http://"
	if len(serverURL) > len(p) && serverURL[:len(p)] == p {
		return serverURL[len(p):]
	}
	return serverURL
}
