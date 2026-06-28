package main

// cache_used_bytes contract: /cache-status MUST carry a top-level
// `cache_used_bytes` field = the TRUE on-disk JuiceFS block-cache size,
// preferring the scraped juicefs_blockcache_bytes gauge and FALLING BACK to the
// cache-dir du (Capacity.CacheUsageBytes) when the scrape is unavailable. This
// fixes the long-standing bug where the app/OpenLoupe reported the pinned-only
// aggregate (a subset) as "cache used".
//
// These tests pin (a) the wire key + type, and (b) the fallback behavior when
// no FUSE metrics addr is configured — the scrape returns unavailable and the
// field must equal Capacity.CacheUsageBytes, never panic, never the pinned
// aggregate.

import (
	"encoding/json"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

// cacheUsedBytesForTest mirrors the assignment in NFSServerCacheStatus so the
// fallback contract is unit-testable without the cgo export / live stores.
// Keep in lockstep with cbridge.go.
func cacheUsedBytesForTest(cs *CacheStatus) {
	cs.CacheUsedBytes = cs.Capacity.CacheUsageBytes // fallback: cache-dir du
	if bc, ok := pin.BlockCacheBytes(); ok {
		cs.CacheUsedBytes = bc // authoritative block-cache gauge
	}
}

func TestCacheUsedBytesFallsBackToDirUsage(t *testing.T) {
	// No metrics addr configured → the scraper is unavailable → cache_used_bytes
	// must fall back to the dir-walk usage, NOT the pinned aggregate, and must
	// not panic.
	pin.SetBlockCacheMetricsAddr("")

	cs := CacheStatus{}
	cs.Capacity.CacheUsageBytes = 103 << 30 // ~103 GiB, the real block cache du
	cs.Capacity.PinnedBytes = 3 << 30       // ~3 GiB pinned (the OLD wrong number)
	cs.Aggregate.CachedBytes = 3 << 30      // pinned-resident subset

	cacheUsedBytesForTest(&cs)

	if cs.CacheUsedBytes != 103<<30 {
		t.Fatalf("cache_used_bytes = %d, want fallback to CacheUsageBytes %d", cs.CacheUsedBytes, int64(103<<30))
	}
	if cs.CacheUsedBytes == cs.Aggregate.CachedBytes {
		t.Fatalf("cache_used_bytes must NOT equal the pinned-only aggregate (%d)", cs.Aggregate.CachedBytes)
	}

	// The capacity guard inputs must be left untouched by the cache_used path.
	if cs.Capacity.PinnedBytes != 3<<30 {
		t.Fatalf("PinnedBytes mutated: %d", cs.Capacity.PinnedBytes)
	}
}

func TestCacheUsedBytesWireKeyAndType(t *testing.T) {
	cs := CacheStatus{}
	cs.CacheUsedBytes = 110729625000

	blob, err := json.Marshal(cs)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(blob, &top); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	raw, ok := top["cache_used_bytes"]
	if !ok {
		t.Fatal("cache_used_bytes missing from /cache-status JSON")
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err != nil {
		t.Fatalf("cache_used_bytes is not an integer: %v", err)
	}
	if n != 110729625000 {
		t.Fatalf("cache_used_bytes = %d, want 110729625000", n)
	}
}
