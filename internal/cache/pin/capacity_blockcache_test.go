package pin

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// The write spool's admission path may not do I/O (it runs under a TryLock with
// a path shard held), so it reads juicefs_blockcache_bytes out of the verdict
// snapshot instead of scraping. That only works if ComputeCapacity — which runs
// on the 60s CapacityLoop, off the hot path — actually refreshes and records the
// gauge. Before this, the ONLY thing that refreshed it was the /cache-status
// poll, i.e. it ticked only while the menu-bar popover happened to be open.
func TestComputeCapacityRecordsBlockCacheGauge(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte("juicefs_blockcache_bytes 107374182400\n"))
	}))
	defer srv.Close()

	// Restore the singleton's fields so we don't leak state into sibling tests.
	blockCache.mu.Lock()
	sAddr, sVal, sOK, sTry := blockCache.addr, blockCache.lastVal, blockCache.lastOK, blockCache.lastTry
	blockCache.mu.Unlock()
	t.Cleanup(func() {
		blockCache.mu.Lock()
		blockCache.addr, blockCache.lastVal, blockCache.lastOK, blockCache.lastTry = sAddr, sVal, sOK, sTry
		blockCache.mu.Unlock()
	})
	SetBlockCacheMetricsAddr(hostPort(srv.URL)) // also clears the throttle anchor

	v := ComputeCapacity(nil, t.TempDir())
	if v.BlockCacheBytes != 100*gib {
		t.Fatalf("verdict BlockCacheBytes = %d, want %d — the spool's headroom term "+
			"has no source and silently degrades to free-disk-only", v.BlockCacheBytes, 100*gib)
	}
	if snap := Capacity(); snap.BlockCacheBytes != 100*gib {
		t.Errorf("published verdict BlockCacheBytes = %d, want %d", snap.BlockCacheBytes, 100*gib)
	}
	if snap := Capacity(); snap.Computed.IsZero() {
		t.Error("published verdict has no Computed timestamp — consumers cannot age-gate it")
	}
}

// An unavailable gauge must leave the field at 0 (the caller then falls back to
// the cache-dir du) rather than blocking or poisoning the verdict.
func TestComputeCapacityToleratesMissingGauge(t *testing.T) {
	blockCache.mu.Lock()
	sAddr, sVal, sOK, sTry := blockCache.addr, blockCache.lastVal, blockCache.lastOK, blockCache.lastTry
	blockCache.mu.Unlock()
	t.Cleanup(func() {
		blockCache.mu.Lock()
		blockCache.addr, blockCache.lastVal, blockCache.lastOK, blockCache.lastTry = sAddr, sVal, sOK, sTry
		blockCache.mu.Unlock()
	})
	SetBlockCacheMetricsAddr("") // scraping disabled

	start := time.Now()
	v := ComputeCapacity(nil, t.TempDir())
	if v.BlockCacheBytes != 0 {
		t.Errorf("BlockCacheBytes = %d with no metrics addr, want 0", v.BlockCacheBytes)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("ComputeCapacity took %v with no metrics addr", elapsed)
	}
}
