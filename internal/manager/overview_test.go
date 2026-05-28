package manager

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestCollectOverview exercises the happy-path fan-out aggregator with
// every probe mocked to succeed. Confirms (a) all sections populated,
// (b) per-section Error fields empty, (c) CollectedAt set to a recent
// millisecond value.
func TestCollectOverview(t *testing.T) {
	mgr := NewJobManager("/dev/null", RunSyncSpec{Mode: ModeEmbedded, FUSEMount: "/mnt/juicefs"})
	mgr.SetRunner(runnerSucceedImmediately)
	defer mgr.StopAll()

	// Seed a couple of jobs so the recent-jobs section has something
	// concrete to surface. Submitting through the real Submit path
	// validates the GetSnapshot/duration math too.
	_, _ = mgr.Submit("/tmp/a", "/jfs/a", DefaultSyncOptions(), 100, "")
	_, _ = mgr.Submit("/tmp/b", "/jfs/b", DefaultSyncOptions(), 200, "")
	// Tiny sleep so the runners finish — otherwise the recent-jobs
	// duration math fires the "still running" branch which is fine
	// but worth covering the FinishedAt branch too.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		all := mgr.List()
		done := 0
		for _, j := range all {
			if j.GetState() == JobDone {
				done++
			}
		}
		if done == len(all) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	src := &overviewSource{
		mgr: mgr,
		probeVolume: func(ctx context.Context) VolumeStatusSection {
			return VolumeStatusSection{Name: "zpool", UsedBytes: 123_456_789, Files: 1024}
		},
		probeRedis: func(ctx context.Context) RedisStatusSection {
			return RedisStatusSection{Reachable: true, LatencyMs: 1, Version: "7.4.0", UptimeSec: 3600, UsedMemoryMB: 12}
		},
		probeMinIO: func(ctx context.Context) MinIOStatusSection {
			return MinIOStatusSection{Reachable: true, LatencyMs: 5, Endpoint: "http://127.0.0.1:9000"}
		},
		probeCache: func(ctx context.Context) CacheStatsSection {
			return CacheStatsSection{Available: true, HitRatePct: 99.5, ReadOpsPerS: 10, WriteOpsPerS: 2}
		},
		probeJobs: func(ctx context.Context) RecentJobsSection {
			// Run the REAL realProbeJobs against the mgr — this is the
			// path we actually care about and the easiest mock is
			// "no mock". Mirrors the prod wiring exactly.
			return (&overviewSource{mgr: mgr}).realProbeJobs(ctx)
		},
	}

	before := time.Now().UnixMilli()
	snap := src.collectOverview(context.Background())
	after := time.Now().UnixMilli()

	if snap.CollectedAt < before || snap.CollectedAt > after {
		t.Errorf("CollectedAt %d outside [%d,%d]", snap.CollectedAt, before, after)
	}
	if snap.Volume.Name != "zpool" {
		t.Errorf("Volume.Name = %q want %q", snap.Volume.Name, "zpool")
	}
	if snap.Volume.UsedBytes != 123_456_789 {
		t.Errorf("Volume.UsedBytes = %d want 123456789", snap.Volume.UsedBytes)
	}
	if snap.Volume.Error != "" {
		t.Errorf("Volume.Error = %q, want empty on success", snap.Volume.Error)
	}
	if !snap.Redis.Reachable {
		t.Errorf("Redis.Reachable = false, want true")
	}
	if snap.Redis.Error != "" {
		t.Errorf("Redis.Error = %q, want empty on success", snap.Redis.Error)
	}
	if !snap.MinIO.Reachable {
		t.Errorf("MinIO.Reachable = false, want true")
	}
	if snap.MinIO.Error != "" {
		t.Errorf("MinIO.Error = %q, want empty on success", snap.MinIO.Error)
	}
	if !snap.Cache.Available {
		t.Errorf("Cache.Available = false, want true")
	}
	if snap.Cache.HitRatePct != 99.5 {
		t.Errorf("Cache.HitRatePct = %v want 99.5", snap.Cache.HitRatePct)
	}
	if snap.Jobs.Error != "" {
		t.Errorf("Jobs.Error = %q, want empty when mgr is configured", snap.Jobs.Error)
	}
	if len(snap.Jobs.Items) != 2 {
		t.Errorf("Jobs.Items len = %d want 2", len(snap.Jobs.Items))
	}
	// Reverse-chronological — most recently submitted job is first.
	if len(snap.Jobs.Items) == 2 {
		if snap.Jobs.Items[0].Source != "/tmp/b" {
			t.Errorf("Jobs.Items[0].Source = %q want /tmp/b (reverse insertion order)", snap.Jobs.Items[0].Source)
		}
		if snap.Jobs.Items[1].Source != "/tmp/a" {
			t.Errorf("Jobs.Items[1].Source = %q want /tmp/a", snap.Jobs.Items[1].Source)
		}
	}
}

// TestCollectOverviewDegraded mocks Redis to fail. Confirms the per-
// section Error field is populated AND every other section still
// reports success — the partial-data contract that keeps the dashboard
// useful when one backend is sick.
//
// Also verifies the HTTP handler returns 200 (not 5xx) for the
// degraded snapshot, which is the user-facing guarantee.
func TestCollectOverviewDegraded(t *testing.T) {
	mgr := NewJobManager("/dev/null", RunSyncSpec{Mode: ModeEmbedded, FUSEMount: "/mnt/juicefs"})
	mgr.SetRunner(runnerSucceedImmediately)
	defer mgr.StopAll()

	src := &overviewSource{
		mgr: mgr,
		probeVolume: func(ctx context.Context) VolumeStatusSection {
			return VolumeStatusSection{Name: "zpool", UsedBytes: 42, Files: 7}
		},
		probeRedis: func(ctx context.Context) RedisStatusSection {
			// Simulate Redis unreachable. Reachable stays false; Error
			// surfaces the cause. Other fields stay zero (no spoofed
			// version / uptime / memory).
			return RedisStatusSection{Reachable: false, Error: "dial tcp 127.0.0.1:6379: connect: connection refused"}
		},
		probeMinIO: func(ctx context.Context) MinIOStatusSection {
			return MinIOStatusSection{Reachable: true, LatencyMs: 5, Endpoint: "http://127.0.0.1:9000"}
		},
		probeCache: func(ctx context.Context) CacheStatsSection {
			return CacheStatsSection{Available: true, HitRatePct: 80, ReadOpsPerS: 1, WriteOpsPerS: 0}
		},
		probeJobs: func(ctx context.Context) RecentJobsSection {
			return RecentJobsSection{Items: []RecentJob{}}
		},
	}

	snap := src.collectOverview(context.Background())

	// Redis section should carry the error...
	if snap.Redis.Reachable {
		t.Errorf("Redis.Reachable = true on simulated failure, want false")
	}
	if snap.Redis.Error == "" {
		t.Errorf("Redis.Error empty on simulated failure")
	}
	// ...but every OTHER section should still report success — the
	// partial-data contract.
	if snap.Volume.Error != "" {
		t.Errorf("Volume.Error = %q, expected empty when only Redis fails", snap.Volume.Error)
	}
	if snap.Volume.Name != "zpool" {
		t.Errorf("Volume.Name lost: got %q want zpool", snap.Volume.Name)
	}
	if snap.MinIO.Error != "" {
		t.Errorf("MinIO.Error = %q, expected empty when only Redis fails", snap.MinIO.Error)
	}
	if !snap.MinIO.Reachable {
		t.Errorf("MinIO.Reachable = false despite mock returning true")
	}
	if snap.Cache.Error != "" {
		t.Errorf("Cache.Error = %q, expected empty when only Redis fails", snap.Cache.Error)
	}
	if !snap.Cache.Available {
		t.Errorf("Cache.Available = false despite mock returning true")
	}
	if snap.Jobs.Error != "" {
		t.Errorf("Jobs.Error = %q, expected empty when only Redis fails", snap.Jobs.Error)
	}

	// Now wire the snapshot through the HTTP handler and verify the
	// status code stays 200. Build a minimal API by hand (no Register
	// — that wires real probes).
	a := &API{overview: src}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/overview", nil)
	a.handleOverview(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("degraded handleOverview status = %d, want 200 (graceful degradation)", rec.Code)
	}
	// Verify the JSON body actually parses and carries the Redis error.
	var got OverviewSnapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.Redis.Error == "" {
		t.Errorf("Redis.Error empty in JSON response, want populated")
	}
	if got.Volume.Name != "zpool" {
		t.Errorf("Volume.Name lost in JSON round-trip: got %q", got.Volume.Name)
	}
}

// TestParseJuicefsStatusJSON locks in the tolerant field-alias parsing
// — protects against juicefs status's JSON shape drifting in a way
// that silently zeros out the dashboard's volume card.
func TestParseJuicefsStatusJSON(t *testing.T) {
	cases := []struct {
		name      string
		in        string
		wantName  string
		wantUsed  int64
		wantFiles int64
		wantErr   bool
	}{
		{
			name:      "current 1.3.x shape",
			in:        `{"Setting":{"Name":"zpool"},"UsedSpace":1024,"FilesCount":42}`,
			wantName:  "zpool",
			wantUsed:  1024,
			wantFiles: 42,
		},
		{
			name:      "1.2.x snake_case shape",
			in:        `{"Setting":{"Name":"zpool"},"used_space":2048,"files_count":99}`,
			wantName:  "zpool",
			wantUsed:  2048,
			wantFiles: 99,
		},
		{
			name:    "malformed JSON → Error populated",
			in:      `{not json`,
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseJuicefsStatusJSON([]byte(tc.in))
			if tc.wantErr {
				if got.Error == "" {
					t.Errorf("expected Error populated for %q", tc.in)
				}
				return
			}
			if got.Name != tc.wantName {
				t.Errorf("Name = %q want %q", got.Name, tc.wantName)
			}
			if got.UsedBytes != tc.wantUsed {
				t.Errorf("UsedBytes = %d want %d", got.UsedBytes, tc.wantUsed)
			}
			if got.Files != tc.wantFiles {
				t.Errorf("Files = %d want %d", got.Files, tc.wantFiles)
			}
		})
	}
}

// TestParseRedisAddr exercises the redis:// URL parser. Defensive
// coverage — the real code path constructs go-redis with the parsed
// addr/db, and a silently-wrong parse would manifest as a "connection
// refused" with no operator-actionable hint.
func TestParseRedisAddr(t *testing.T) {
	cases := []struct {
		in       string
		wantAddr string
		wantDB   int
		wantErr  bool
	}{
		{in: "redis://127.0.0.1:6379/1", wantAddr: "127.0.0.1:6379", wantDB: 1},
		{in: "redis://localhost/0", wantAddr: "localhost:6379", wantDB: 0},
		{in: "redis://host:30179/", wantAddr: "host:30179", wantDB: 0},
		{in: "http://example/0", wantErr: true},
		{in: "redis://host/abc", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			addr, db, err := parseRedisAddr(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error, got addr=%q db=%d", addr, db)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if addr != tc.wantAddr {
				t.Errorf("addr = %q want %q", addr, tc.wantAddr)
			}
			if db != tc.wantDB {
				t.Errorf("db = %d want %d", db, tc.wantDB)
			}
		})
	}
}
