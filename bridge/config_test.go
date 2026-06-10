package main

// Tests for the ServerConfig JSON contract (LB-4, Phase 3b). The critical
// property is BACK-COMPAT: config JSON written by app builds that predate
// the tuning knobs (memory_buffer_mb / membuf_file_limit_mb /
// reconcile_seconds) must decode to zero values, and zero must mean "keep
// the previous hardcoded default" all the way down:
//   - nfs.NewMemoryBuffer maps <= 0 → DefaultMemBufThreshold/Budget
//     (covered by nfs/handler_options_test.go)
//   - metadata.SetReconcileInterval ignores <= 0
//     (covered by metadata/reconcile_interval_test.go)
// This file pins the JSON-decode half of that chain.

import (
	"encoding/json"
	"testing"
	"time"
)

// oldStyleConfigJSON is a config as the pre-Phase-3b app would send it —
// no tuning fields at all.
const oldStyleConfigJSON = `{
	"redis_url": "redis://127.0.0.1:6379/1",
	"fuse_path": "/tmp/fuse",
	"mount_point": "/Volumes/test-zpool",
	"listen_addr": "127.0.0.1:11049",
	"db_path": "/tmp/metadata.db",
	"cache_size": "102400",
	"metrics_addr": "127.0.0.1:11050",
	"log_file": "",
	"log_level": "info",
	"bucket_override": "",
	"spool_enable": false,
	"spool_size_gb": 50
}`

func TestServerConfigBackCompatTuningFieldsAbsent(t *testing.T) {
	var cfg ServerConfig
	if err := json.Unmarshal([]byte(oldStyleConfigJSON), &cfg); err != nil {
		t.Fatalf("unmarshal old-style config: %v", err)
	}
	if cfg.MemoryBufferMB != 0 || cfg.MemBufFileLimitMB != 0 || cfg.ReconcileSeconds != 0 {
		t.Fatalf("absent tuning fields must decode to 0 (defaults downstream), got membuf=%d limit=%d reconcile=%d",
			cfg.MemoryBufferMB, cfg.MemBufFileLimitMB, cfg.ReconcileSeconds)
	}
	if got := cfg.reconcileInterval(); got != 0 {
		t.Fatalf("reconcileInterval() = %v for absent field, want 0 (SetReconcileInterval no-ops on <= 0)", got)
	}
	// Sanity: the pre-existing fields still decode.
	if cfg.MountPoint != "/Volumes/test-zpool" || cfg.SpoolSizeGB != 50 {
		t.Fatalf("pre-existing fields broke: %+v", cfg)
	}
}

func TestServerConfigTuningFieldsDecode(t *testing.T) {
	raw := `{
		"redis_url": "redis://127.0.0.1:6379/1",
		"memory_buffer_mb": 4096,
		"membuf_file_limit_mb": 256,
		"reconcile_seconds": 45
	}`
	var cfg ServerConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if cfg.MemoryBufferMB != 4096 {
		t.Fatalf("MemoryBufferMB = %d, want 4096", cfg.MemoryBufferMB)
	}
	if cfg.MemBufFileLimitMB != 256 {
		t.Fatalf("MemBufFileLimitMB = %d, want 256", cfg.MemBufFileLimitMB)
	}
	if cfg.ReconcileSeconds != 45 {
		t.Fatalf("ReconcileSeconds = %d, want 45", cfg.ReconcileSeconds)
	}
	if got, want := cfg.reconcileInterval(), 45*time.Second; got != want {
		t.Fatalf("reconcileInterval() = %v, want %v", got, want)
	}
}
