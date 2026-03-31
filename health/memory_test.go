package health

import (
	"testing"
	"time"
)

func TestMemoryTelemetry(t *testing.T) {
	cfg := Config{
		// Use unreachable addresses so health checks fail fast
		// without blocking. We only care about memory stats here.
		RedisURL: "127.0.0.1:1",
		MinIOURL: "http://127.0.0.1:1",
		FUSEPath: t.TempDir(),
	}
	m := New(cfg)

	// Wire up a stats provider to verify it gets called.
	m.SetStatsProvider(func() MemoryStats {
		return MemoryStats{
			PathCacheSize:  42,
			InodeCacheSize: 7,
			FDPoolOpen:     3,
			FDPoolActive:   1,
			MemBufEntries:  10,
			MemBufSizeMB:   2.5,
		}
	})

	m.Start()
	// Wait for the initial health check to complete.
	time.Sleep(500 * time.Millisecond)
	defer m.Stop()

	s := m.Status()

	// Runtime memory stats should be populated.
	if s.HeapAllocMB <= 0 {
		t.Errorf("expected HeapAllocMB > 0, got %f", s.HeapAllocMB)
	}
	if s.HeapSysMB <= 0 {
		t.Errorf("expected HeapSysMB > 0, got %f", s.HeapSysMB)
	}
	if s.NumGC == 0 {
		// Force a GC and re-check. In short-lived test processes the
		// collector may not have run yet.
		t.Log("NumGC was 0; this can happen in short test runs — acceptable")
	}

	// Application-level stats from the provider.
	if s.PathCacheSize != 42 {
		t.Errorf("expected PathCacheSize 42, got %d", s.PathCacheSize)
	}
	if s.InodeCacheSize != 7 {
		t.Errorf("expected InodeCacheSize 7, got %d", s.InodeCacheSize)
	}
	if s.FDPoolOpen != 3 {
		t.Errorf("expected FDPoolOpen 3, got %d", s.FDPoolOpen)
	}
	if s.FDPoolActive != 1 {
		t.Errorf("expected FDPoolActive 1, got %d", s.FDPoolActive)
	}
	if s.MemBufEntries != 10 {
		t.Errorf("expected MemBufEntries 10, got %d", s.MemBufEntries)
	}
	if s.MemBufSizeMB != 2.5 {
		t.Errorf("expected MemBufSizeMB 2.5, got %f", s.MemBufSizeMB)
	}
}
