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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

// Exact pre-option ServerConfig encoding from a234b96. Keep this literal:
// deriving it from the current encoder would miss a breaking field addition.
const historicalCanonicalRecoveryOffConfig = `{"redis_url":"redis://mac-edit2@nas.local:30179/1","fuse_path":"","mount_point":"","listen_addr":"","db_path":"","cache_size":"","metrics_addr":"","log_file":"","log_level":"","bucket_override":"","net_control_url":"","net_authkey":"","net_hostname":"","net_nas_addr":"","spool_enable":true,"spool_size_gb":50,"native_preview_warm":false,"open_cache_ttl":"","memory_buffer_mb":0,"membuf_file_limit_mb":0,"reconcile_seconds":0}`

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

func TestServerConfigNativePreviewWarmPresence(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		value     bool
	}{
		{"legacy-absent", `{}`, false},
		{"legacy-null", `{"native_preview_warm":null}`, false},
		{"explicit-off", `{"native_preview_warm":false}`, false},
		{"explicit-on", `{"native_preview_warm":true}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cfg ServerConfig
			if err := json.Unmarshal([]byte(tc.raw), &cfg); err != nil {
				t.Fatal(err)
			}
			if cfg.NativePreviewWarm != tc.value {
				t.Fatalf("value=%t, want %t", cfg.NativePreviewWarm, tc.value)
			}
			encoded, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			var roundTrip map[string]any
			if err := json.Unmarshal(encoded, &roundTrip); err != nil {
				t.Fatal(err)
			}
			value, present := roundTrip["native_preview_warm"]
			if !present || value != tc.value {
				t.Fatalf("round-trip presence=%t value=%v; want %t", present, value, tc.value)
			}
			var independent ServerConfig
			if err := json.Unmarshal([]byte(tc.raw), &independent); err != nil {
				t.Fatal(err)
			}
			if independent != cfg {
				t.Fatal("separately decoded identical configs compare unequal")
			}
		})
	}
}

func TestServerConfigReplacementRecoveryPresence(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		want      bool
	}{
		{"legacy-absent", `{}`, false},
		{"legacy-null", `{"replacement_recovery":null}`, false},
		{"explicit-off", `{"replacement_recovery":false}`, false},
		{"explicit-on", `{"replacement_recovery":true}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cfg ServerConfig
			if err := json.Unmarshal([]byte(tc.raw), &cfg); err != nil {
				t.Fatal(err)
			}
			if cfg.ReplacementRecovery != tc.want {
				t.Fatalf("replacement recovery=%t, want %t", cfg.ReplacementRecovery, tc.want)
			}
			encoded, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			var roundTrip map[string]any
			if err := json.Unmarshal(encoded, &roundTrip); err != nil {
				t.Fatal(err)
			}
			got, present := roundTrip["replacement_recovery"]
			if present != tc.want || (present && got != true) {
				t.Fatalf("round trip presence=%t value=%v, want true only when enabled", present, got)
			}
		})
	}
	for _, raw := range []string{`{"replacement_recovery":"true"}`, `{"replacement_recovery":1}`} {
		var cfg ServerConfig
		if err := json.Unmarshal([]byte(raw), &cfg); err == nil {
			t.Fatalf("accepted non-Boolean %s", raw)
		}
	}
}

func TestReplacementRecoveryPreservesHistoricalConfigDigest(t *testing.T) {
	cfg := ServerConfig{RedisURL: "redis://mac-edit2@nas.local:30179/1", SpoolEnable: true, SpoolSizeGB: 50}
	encoded, err := json.Marshal(persistableServerConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != historicalCanonicalRecoveryOffConfig {
		t.Fatalf("disabled option changed historical canonical config:\n%s", encoded)
	}
	historical := sha256.Sum256([]byte(historicalCanonicalRecoveryOffConfig))
	digest, err := servingConfigDigest(cfg)
	if err != nil || digest != fmt.Sprintf("%x", historical) {
		t.Fatalf("historical digest changed: %s %v", digest, err)
	}
	cfg.ReplacementRecovery = true
	active, err := servingConfigDigest(cfg)
	if err != nil || active == digest {
		t.Fatalf("enabled option did not change digest: %s %v", active, err)
	}
}

func TestReplacementRecoveryRequiresEffectiveSpool(t *testing.T) {
	for _, tc := range []struct {
		name                             string
		recovery, configSpool, legacyEnv bool
		wantAllowed                      bool
	}{
		{"default-off", false, false, false, true},
		{"recovery-without-spool", true, false, false, false},
		{"configured-spool", true, true, false, true},
		{"legacy-env-spool", true, false, true, true},
		{"both-spool-inputs", true, true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := ServerConfig{ReplacementRecovery: tc.recovery, SpoolEnable: tc.configSpool}
			configured := cfg.spoolConfigured(tc.legacyEnv)
			if configured != (tc.configSpool || tc.legacyEnv) {
				t.Fatalf("effective spool=%t", configured)
			}
			if allowed := cfg.validateReplacementRecoveryConfig(configured) == nil; allowed != tc.wantAllowed {
				t.Fatalf("allowed=%t, want %t", allowed, tc.wantAllowed)
			}
		})
	}
}
