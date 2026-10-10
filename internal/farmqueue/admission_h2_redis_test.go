package farmqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestRedisH2BlockedRenderHeartbeatCannotClaimUntilMeasuredAdmission(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	info, err := q.rdb.Info(ctx, "server").Result()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(info, "redis_version:7.4.11\r\n") {
		t.Fatalf("acceptance requires Redis 7.4.11, server info did not match: %q", info)
	}

	const workerID = "h2-stable-render"
	blocked := Worker{ID: workerID, Name: "render-node", Role: QueueClassRender, RequireStoragePermit: true,
		AdmissionBlocked: true, Benchmarks: WorkerBenchmarks{ProbeError: "accelerator unavailable"}}
	if err := q.Heartbeat(ctx, blocked); err != nil {
		t.Fatal(err)
	}
	assertH2WorkerHeartbeat(t, q, ctx, workerID, true)

	job := NewJob("/acceptance/h2/proxy-input.mov", []string{KindProxy}, "h2-redis-acceptance")
	job.QueueClass = QueueClassRender
	job.RequiredCapabilities = []string{"encoder:hevc_vaapi"}
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	if claim, ok, err := q.ClaimForWorker(ctx, time.Second, blocked); err != nil || ok {
		t.Fatalf("blocked live worker claimed work: %+v, ok=%v err=%v", claim, ok, err)
	}
	assertH2JobStillReady(t, q, ctx, job)

	// Force an early expiry, then refresh through the real Heartbeat API. The
	// refreshed worker must stay visible without becoming eligible to claim.
	workerKey := WorkerPrefix + workerID
	if err := q.rdb.PExpire(ctx, workerKey, 150*time.Millisecond).Err(); err != nil {
		t.Fatal(err)
	}
	if err := q.Heartbeat(ctx, blocked); err != nil {
		t.Fatal(err)
	}
	if ttl, err := q.rdb.TTL(ctx, workerKey).Result(); err != nil || ttl < WorkerTTL/2 {
		t.Fatalf("refreshed blocked heartbeat TTL=%v err=%v", ttl, err)
	}
	assertH2WorkerHeartbeat(t, q, ctx, workerID, true)
	if claim, ok, err := q.ClaimForWorker(ctx, time.Second, blocked); err != nil || ok {
		t.Fatalf("TTL-refreshed blocked worker claimed work: %+v, ok=%v err=%v", claim, ok, err)
	}
	assertH2JobStillReady(t, q, ctx, job)

	if err := q.rdb.PExpire(ctx, workerKey, 100*time.Millisecond).Err(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		workers, err := q.ActiveWorkers(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(workers) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expired worker heartbeat remained visible: %+v", workers)
		}
		time.Sleep(10 * time.Millisecond)
	}
	assertH2JobStillReady(t, q, ctx, job)
	if claim, ok, err := q.ClaimForWorker(ctx, 0, blocked); err != nil || ok {
		t.Fatalf("expired blocked worker claimed work: %+v, ok=%v err=%v", claim, ok, err)
	}

	admitted := Worker{ID: workerID, Name: "render-node", Role: QueueClassRender, RequireStoragePermit: true,
		Encoders: []string{"hevc_vaapi"}, Benchmarks: WorkerBenchmarks{
			EncodeFPS: 24.5, ProbedAt: nowISO(),
		}}
	if err := q.Heartbeat(ctx, admitted); err != nil {
		t.Fatal(err)
	}
	if err := q.PublishStoragePermit(ctx, StoragePermit{Source: "h2-test", AvailableBytes: 2, RequiredBytes: 1}); err != nil {
		t.Fatal(err)
	}
	workers, err := q.ActiveWorkers(ctx)
	if err != nil || len(workers) != 1 || workers[0].ID != workerID || workers[0].AdmissionBlocked ||
		workers[0].Benchmarks.EncodeFPS != admitted.Benchmarks.EncodeFPS ||
		workers[0].Benchmarks.ProbedAt != admitted.Benchmarks.ProbedAt {
		t.Fatalf("measured admitted heartbeat = %+v err=%v", workers, err)
	}
	claim, ok, err := q.ClaimForWorker(ctx, time.Second, admitted)
	if err != nil || !ok || claim.Job.ID != job.ID || claim.Job.QueueClass != QueueClassRender {
		t.Fatalf("same-ID admitted render claim = %+v ok=%v err=%v", claim, ok, err)
	}
	if claim, ok, err := q.ClaimForWorker(ctx, 0, admitted); err != nil || ok {
		t.Fatalf("render job was delivered more than once: %+v ok=%v err=%v", claim, ok, err)
	}
}

func TestRedisH2RenderClaimUsesLiveHeartbeatPermitAndCurrentProfile(t *testing.T) {
	t.Run("missing permit denies and valid permit allows", func(t *testing.T) {
		q, ctx, admitted, job := setupH2AdmittedRenderJob(t)
		if claim, ok, err := q.ClaimForWorker(ctx, 0, admitted); err != nil || ok {
			t.Fatalf("render claim without permit = %+v ok=%v err=%v", claim, ok, err)
		}
		assertH2JobStillReady(t, q, ctx, job)
		publishH2Permit(t, q, ctx)
		claim, ok, err := q.ClaimForWorker(ctx, time.Second, admitted)
		if err != nil || !ok || claim.Job.ID != job.ID {
			t.Fatalf("render claim with valid permit = %+v ok=%v err=%v", claim, ok, err)
		}
	})

	t.Run("expired heartbeat denies stale local profile", func(t *testing.T) {
		q, ctx, admitted, job := setupH2AdmittedRenderJob(t)
		publishH2Permit(t, q, ctx)
		if err := q.rdb.PExpire(ctx, WorkerPrefix+admitted.ID, 80*time.Millisecond).Err(); err != nil {
			t.Fatal(err)
		}
		waitH2KeyExpiry(t, q, ctx, WorkerPrefix+admitted.ID)
		claim, ok, err := q.ClaimForWorker(ctx, 0, admitted)
		if err != nil || ok {
			t.Errorf("expired-heartbeat worker claimed with stale local profile: %+v ok=%v err=%v", claim, ok, err)
		}
		assertH2JobStillReady(t, q, ctx, job)
		if n, err := q.rdb.LLen(ctx, ProcessingPrefix+admitted.ID).Result(); err != nil || n != 0 {
			t.Errorf("expired-heartbeat processing receipt depth=%d err=%v; want 0", n, err)
		}
	})

	t.Run("blocked replacement denies stale admitted profile", func(t *testing.T) {
		q, ctx, admitted, job := setupH2AdmittedRenderJob(t)
		publishH2Permit(t, q, ctx)
		blocked := Worker{ID: admitted.ID, Name: admitted.Name, Role: QueueClassRender,
			RequireStoragePermit: true, AdmissionBlocked: true,
			Benchmarks: WorkerBenchmarks{ProbeError: "accelerator probe failed"}}
		if err := q.Heartbeat(ctx, blocked); err != nil {
			t.Fatal(err)
		}
		claim, ok, err := q.ClaimForWorker(ctx, 0, admitted)
		if err != nil || ok {
			t.Errorf("blocked replacement claimed through stale admitted profile: %+v ok=%v err=%v", claim, ok, err)
		}
		assertH2JobStillReady(t, q, ctx, job)
		if n, err := q.rdb.LLen(ctx, ProcessingPrefix+admitted.ID).Result(); err != nil || n != 0 {
			t.Errorf("blocked replacement processing receipt depth=%d err=%v; want 0", n, err)
		}
	})

	t.Run("current profile gates unqualified render lane", func(t *testing.T) {
		q, ctx, admitted, _ := setupH2AdmittedRenderJob(t)
		publishH2Permit(t, q, ctx)
		unqualified := NewJob("/acceptance/h2/unqualified.mov", []string{KindProxy}, "h2-redis-acceptance")
		unqualified.QueueClass = QueueClassRender
		if err := q.Enqueue(ctx, unqualified); err != nil {
			t.Fatal(err)
		}
		admitted.Encoders = nil
		admitted.Benchmarks = WorkerBenchmarks{ProbedAt: nowISO()}
		if err := q.Heartbeat(ctx, admitted); err != nil {
			t.Fatal(err)
		}
		staleProfile := admitted
		staleProfile.Encoders = []string{"hevc_vaapi"}
		staleProfile.Benchmarks.EncodeFPS = 24.5
		claim, ok, err := q.ClaimForWorker(ctx, 0, staleProfile)
		if err != nil || ok {
			t.Errorf("unqualified proxy claimed through stale render lane: %+v ok=%v err=%v", claim, ok, err)
		}
		if depth, err := q.rdb.LLen(ctx, QueueKeyFor(&unqualified)).Result(); err != nil || depth != 2 {
			t.Errorf("stale render lane ready depth=%d err=%v; want both jobs untouched", depth, err)
		}
		if n, err := q.rdb.LLen(ctx, ProcessingPrefix+admitted.ID).Result(); err != nil || n != 0 {
			t.Errorf("stale render lane processing receipt depth=%d err=%v; want 0", n, err)
		}
	})
}

func TestRedisH2AuthoritativeProfileHonorsStableWorkerPin(t *testing.T) {
	t.Run("matching stable name claims", func(t *testing.T) {
		q, ctx, worker := setupH2AdmittedRenderWorker(t, "render-node")
		publishH2Permit(t, q, ctx)
		job := NewJob("/acceptance/h2/pinned-match.mov", []string{KindProxy}, "h2-redis-acceptance")
		job.QueueClass = QueueClassRender
		job.RequiredCapabilities = []string{"worker-name:render-node"}
		if err := q.Enqueue(ctx, job); err != nil {
			t.Fatal(err)
		}
		claim, ok, err := q.ClaimForWorker(ctx, time.Second, worker)
		if err != nil || !ok || claim.Job.ID != job.ID {
			t.Fatalf("matching stable-name pin claim = %+v ok=%v err=%v", claim, ok, err)
		}
	})

	t.Run("different current stable name denies", func(t *testing.T) {
		q, ctx, stale := setupH2AdmittedRenderWorker(t, "render-node")
		publishH2Permit(t, q, ctx)
		job := NewJob("/acceptance/h2/pinned-mismatch.mov", []string{KindProxy}, "h2-redis-acceptance")
		job.QueueClass = QueueClassRender
		job.RequiredCapabilities = []string{"worker-name:render-node"}
		if err := q.Enqueue(ctx, job); err != nil {
			t.Fatal(err)
		}
		current := stale
		current.Name = "replacement-render"
		if err := q.Heartbeat(ctx, current); err != nil {
			t.Fatal(err)
		}
		claim, ok, err := q.ClaimForWorker(ctx, 0, stale)
		if err != nil || ok {
			t.Errorf("different stable name claimed pinned job: %+v ok=%v err=%v", claim, ok, err)
		}
		assertH2JobStillReady(t, q, ctx, job)
		if n, err := q.rdb.LLen(ctx, ProcessingPrefix+stale.ID).Result(); err != nil || n != 0 {
			t.Errorf("different-name processing depth=%d err=%v; want 0", n, err)
		}
	})
}

func TestRedisH2MalformedDecodeProfileCannotLoseReadyJob(t *testing.T) {
	q, ctx, worker := setupH2AdmittedRenderWorker(t, "decode-render")
	worker.Decoders = []string{"hevc_vaapi"}
	worker.DecodeLimits = map[string]VideoDecodeLimit{
		"hevc_vaapi": {MaxWidth: 4096, MaxHeight: 2160},
	}
	worker.Benchmarks.DecodeFPS = 18.0
	if err := q.Heartbeat(ctx, worker); err != nil {
		t.Fatal(err)
	}
	job := NewJob("/acceptance/h2/dimensioned.mov", []string{KindDerivatives}, "h2-redis-acceptance")
	job.QueueClass = QueueClassRender
	job.RequiredCapabilities = []string{"decoder:hevc_vaapi"}
	job.SourceVideoWidth, job.SourceVideoHeight = 3840, 2160
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	publishH2Permit(t, q, ctx)

	heartbeatRaw, err := q.rdb.Get(ctx, WorkerPrefix+worker.ID).Result()
	if err != nil {
		t.Fatal(err)
	}
	var heartbeat map[string]any
	if err := json.Unmarshal([]byte(heartbeatRaw), &heartbeat); err != nil {
		t.Fatal(err)
	}
	heartbeat["decode_limits"] = map[string]any{"hevc_vaapi": 1}
	malformed, err := json.Marshal(heartbeat)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.Set(ctx, WorkerPrefix+worker.ID, malformed, WorkerTTL).Err(); err != nil {
		t.Fatal(err)
	}

	if claim, ok, err := q.ClaimForWorker(ctx, 0, worker); err == nil || ok {
		t.Errorf("malformed decode profile claim = %+v ok=%v err=%v; want rejection", claim, ok, err)
	}
	if n, err := q.rdb.LLen(ctx, QueueKeyFor(&job)).Result(); err != nil || n != 1 {
		t.Errorf("malformed-profile ready depth=%d err=%v; want 1", n, err)
	}
	if n, err := q.rdb.LLen(ctx, ProcessingPrefix+worker.ID).Result(); err != nil || n != 0 {
		t.Errorf("malformed-profile processing depth=%d err=%v; want 0", n, err)
	}
}

func TestRedisH2RenderClaimPermitAndTranscriptAuthority(t *testing.T) {
	for _, tracked := range []bool{false, true} {
		for _, requirePermit := range []bool{false, true} {
			for _, backends := range [][]string{{"cpu"}, {""}, {"vulkan"}, {"cpu", "vulkan"}} {
				label := strings.Join(backends, "+")
				t.Run(label+"/tracked="+strconv.FormatBool(tracked)+"/callerPermit="+strconv.FormatBool(requirePermit), func(t *testing.T) {
					q, ctx := openWatchEpochTestQueue(t)
					worker := Worker{ID: "h2-transcript-authority", Name: "transcript-node", Role: QueueClassRender,
						RequireStoragePermit: requirePermit, TranscriptBackends: backends,
						Benchmarks: WorkerBenchmarks{ProbedAt: nowISO(), TranscriptXReal: 2.4}}
					if err := q.Heartbeat(ctx, worker); err != nil {
						t.Fatal(err)
					}
					publishH2Permit(t, q, ctx)
					job := NewJob("/acceptance/h2/transcript.wav", []string{KindTranscript}, "h2-redis-acceptance")
					job.QueueClass = QueueClassRender
					if strings.Contains(strings.Join(backends, "|"), "vulkan") {
						job.RequiredCapabilities = []string{"transcript:vulkan"}
					}
					if tracked {
						job.TrackedEpoch = "h2-epoch"
					}
					if err := q.Enqueue(ctx, job); err != nil {
						t.Fatal(err)
					}
					// The caller profile supplies a candidate key, but never authority.
					caller := worker
					caller.TranscriptBackends = []string{"vulkan"}
					claim, ok, err := q.ClaimForWorker(ctx, 0, caller)
					wantClaim := strings.Contains(strings.Join(backends, "|"), "vulkan")
					if err != nil || ok != wantClaim || (ok && claim.Job.ID != job.ID) {
						t.Fatalf("transcript authority %v claimed=%v want=%v err=%v", backends, ok, wantClaim, err)
					}
					readyDepth, err := q.rdb.LLen(ctx, QueueKeyFor(&job)).Result()
					if err != nil || readyDepth != int64(1-btoi(ok)) {
						t.Fatalf("ready custody depth=%d err=%v claimed=%v", readyDepth, err, ok)
					}
					processing := ProcessingPrefix + worker.ID
					if tracked {
						processing = TrackedProcessingPrefix + worker.ID
					}
					processingDepth, err := q.rdb.LLen(ctx, processing).Result()
					if err != nil || processingDepth != int64(btoi(ok)) {
						t.Fatalf("processing custody depth=%d err=%v claimed=%v", processingDepth, err, ok)
					}
				})
			}
		}
	}

	for _, permitCase := range []string{"malformed", "unsafe", "nonexpiring", "fractional-numeric-after-precheck"} {
		t.Run("permit/"+permitCase, func(t *testing.T) {
			q, ctx, worker, job := setupH2AdmittedRenderJob(t)
			switch permitCase {
			case "malformed":
				if err := q.rdb.Set(ctx, StoragePermitKey, "{", StoragePermitTTL).Err(); err != nil {
					t.Fatal(err)
				}
			case "unsafe":
				raw, _ := json.Marshal(StoragePermit{Source: "unsafe", CheckedAt: nowISO(), CheckedAtUnix: time.Now().Unix(), AvailableBytes: 1, RequiredBytes: 2})
				if err := q.rdb.Set(ctx, StoragePermitKey, raw, StoragePermitTTL).Err(); err != nil {
					t.Fatal(err)
				}
			case "nonexpiring":
				raw, _ := json.Marshal(StoragePermit{Source: "nonexpiring", CheckedAt: nowISO(), CheckedAtUnix: time.Now().Unix(), AvailableBytes: 2, RequiredBytes: 1})
				if err := q.rdb.Set(ctx, StoragePermitKey, raw, 0).Err(); err != nil {
					t.Fatal(err)
				}
			case "fractional-numeric-after-precheck":
				publishH2Permit(t, q, ctx)
				if _, err := q.GetStoragePermit(ctx); err != nil {
					t.Fatalf("precheck did not see valid permit: %v", err)
				}
				raw := map[string]any{"source": "replacement", "checked_at": nowISO(),
					"checked_at_unix": float64(time.Now().Unix()) + 0.5,
					"available_bytes": float64(2), "required_bytes": 1.5}
				encoded, _ := json.Marshal(raw)
				if err := q.rdb.Set(ctx, StoragePermitKey, encoded, StoragePermitTTL).Err(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := q.GetStoragePermit(ctx); err == nil {
				t.Fatalf("Go permit reader accepted invalid %s permit", permitCase)
			}
			claim, ok, err := q.ClaimForWorker(ctx, 0, worker)
			if err != nil || ok {
				t.Fatalf("invalid permit %s claim=%+v ok=%v err=%v", permitCase, claim, ok, err)
			}
			assertH2JobStillReady(t, q, ctx, job)
			if n, err := q.rdb.LLen(ctx, ProcessingPrefix+worker.ID).Result(); err != nil || n != 0 {
				t.Fatalf("processing depth=%d err=%v", n, err)
			}
		})
	}
}

func TestRedisH2StaleGenericCapabilityDoesNotClearRecoveryGrace(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	worker := Worker{ID: "h2-recovery-profile", Name: "recovery-node", Role: QueueClassRender,
		Capabilities: []string{"encoder:hevc_vaapi"}, Benchmarks: WorkerBenchmarks{ProbedAt: nowISO()}}
	if err := q.Heartbeat(ctx, worker); err != nil {
		t.Fatal(err)
	}
	job := NewJob("/acceptance/h2/stale-generic-recovery.mov", []string{KindProxy}, "h2-redis-acceptance")
	job.QueueClass = QueueClassRender
	job.RequiredCapabilities = []string{"encoder:hevc_vaapi"}
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	marker := time.Now().UTC().Format(time.RFC3339Nano)
	if err := q.rdb.HSet(ctx, JobHashPrefix+job.ID, readyFallbackSinceField, marker).Err(); err != nil {
		t.Fatal(err)
	}
	if n, err := q.RecoverUnserviceableReady(ctx); err != nil || n != 0 {
		t.Fatalf("recovery count=%d err=%v; want grace hold", n, err)
	}
	got, err := q.rdb.HGet(ctx, JobHashPrefix+job.ID, readyFallbackSinceField).Result()
	if err != nil || got != marker {
		t.Fatalf("stale capability cleared/replaced grace marker=%q err=%v", got, err)
	}
	assertH2JobStillReady(t, q, ctx, job)
}

func TestRedisH2CPUTranscriptHeartbeatPreservesAndEventuallyFallsBack(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	worker := Worker{ID: "h2-cpu-transcript", Name: "cpu-transcript", Role: QueueClassRender,
		TranscriptBackends: []string{"cpu"}, Benchmarks: WorkerBenchmarks{ProbedAt: nowISO(), TranscriptXReal: 1.2}}
	if err := q.Heartbeat(ctx, worker); err != nil {
		t.Fatal(err)
	}
	job := NewJob("/acceptance/h2/cpu-transcript.wav", []string{KindTranscript}, "h2-redis-acceptance")
	job.QueueClass = QueueClassRender
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	if claim, ok, err := q.ClaimForWorker(ctx, 0, worker); err != nil || ok {
		t.Fatalf("CPU-only render heartbeat claimed transcript: %+v ok=%v err=%v", claim, ok, err)
	}
	assertH2JobStillReady(t, q, ctx, job)
	if n, err := q.rdb.LLen(ctx, ProcessingPrefix+worker.ID).Result(); err != nil || n != 0 {
		t.Fatalf("CPU-only processing receipt depth=%d err=%v; want 0", n, err)
	}

	marker := time.Now().Add(-readyFallbackGrace - time.Minute).UTC().Format(time.RFC3339Nano)
	jobKey := JobHashPrefix + job.ID
	if err := q.rdb.HSet(ctx, jobKey, readyFallbackSinceField, marker).Err(); err != nil {
		t.Fatal(err)
	}
	if n, err := q.RecoverUnserviceableReady(ctx); err != nil || n != 1 {
		t.Fatalf("expired-grace transcript recovery count=%d err=%v; want 1", n, err)
	}
	var fallback Job
	raw, err := q.rdb.LIndex(ctx, classQueue(KindTranscript, QueueClassCPU), -1).Bytes()
	if err != nil || json.Unmarshal(raw, &fallback) != nil || fallback.ID != job.ID || fallback.QueueClass != QueueClassCPU {
		t.Fatalf("CPU fallback job=%+v raw=%q err=%v", fallback, raw, err)
	}
	cpuWorker := Worker{ID: "h2-cpu-fallback", Role: QueueClassServer, Capabilities: []string{"cpu"}}
	claim, ok, err := q.ClaimForWorker(ctx, time.Second, cpuWorker)
	if err != nil || !ok || claim.Job.ID != job.ID || claim.Job.QueueClass != QueueClassCPU {
		t.Fatalf("CPU fallback claim=%+v ok=%v err=%v", claim, ok, err)
	}
}

func TestRedisH2ProxySourceDecodeLaneMatchesGoVerdict(t *testing.T) {
	for _, tc := range []struct {
		name        string
		fps         float64
		profileOnly bool
		want        bool
	}{
		{name: "missing decode rate"},
		{name: "zero decode rate", fps: 0},
		{name: "profile-only zero decode rate", profileOnly: true},
		{name: "measured decode rate", fps: 16, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, ctx := openWatchEpochTestQueue(t)
			worker := Worker{ID: "h2-proxy-decode-rate", Name: "proxy-decode", Role: QueueClassRender,
				RequireStoragePermit: true, Encoders: []string{"hevc_vaapi"}, Decoders: []string{"h264_vaapi"},
				Capabilities: []string{"decoder:h264_vaapi:profile:high"},
				Benchmarks:   WorkerBenchmarks{ProbedAt: nowISO(), EncodeFPS: 24, DecodeFPS: tc.fps}}
			if err := q.Heartbeat(ctx, worker); err != nil {
				t.Fatal(err)
			}
			publishH2Permit(t, q, ctx)
			job := NewJob("/acceptance/h2/source-aware-proxy.mov", []string{KindProxy}, "h2-redis-acceptance")
			job.QueueClass = QueueClassRender
			job.RequiredCapabilities = []string{"encoder:hevc_vaapi", "decoder:h264_vaapi"}
			if tc.profileOnly {
				job.RequiredCapabilities[1] = "decoder:h264_vaapi:profile:high"
			}
			if err := q.Enqueue(ctx, job); err != nil {
				t.Fatal(err)
			}
			if got := workerCanClaimJob(worker, job); got != tc.want {
				t.Fatalf("Go render verdict=%v; want %v", got, tc.want)
			}
			claim, ok, err := q.ClaimForWorker(ctx, 0, worker)
			if err != nil || ok != tc.want || (ok && claim.Job.ID != job.ID) {
				t.Fatalf("Lua claim=%v want=%v claim=%+v err=%v", ok, tc.want, claim, err)
			}
			if !tc.want {
				assertH2JobStillReady(t, q, ctx, job)
			}
			if tc.profileOnly {
				marker := time.Now().Add(-readyFallbackGrace - time.Minute).UTC().Format(time.RFC3339Nano)
				if err := q.rdb.HSet(ctx, JobHashPrefix+job.ID, readyFallbackSinceField, marker).Err(); err != nil {
					t.Fatal(err)
				}
				if n, err := q.RecoverUnserviceableReady(ctx); err != nil || n != 1 {
					t.Fatalf("profile-only expired-grace recovery count=%d err=%v; want 1", n, err)
				}
			}
		})
	}
}

func TestRedisH2ImpossiblePermitDisplayDateKeepsNumericAuthority(t *testing.T) {
	for _, tc := range []struct {
		name string
		date any
		omit bool
	}{
		{name: "impossible-calendar", date: "2026-99-99T99:99:99Z"},
		{name: "missing", omit: true},
		{name: "non-string", date: float64(42)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, ctx, worker, job := setupH2AdmittedRenderJob(t)
			publishH2Permit(t, q, ctx)
			now := time.Now().Unix()
			permit := map[string]any{"source": "replacement", "checked_at_unix": now,
				"available_bytes": 2, "required_bytes": 1}
			if !tc.omit {
				permit["checked_at"] = tc.date
			}
			raw, err := json.Marshal(permit)
			if err != nil {
				t.Fatal(err)
			}
			if err := q.rdb.Set(ctx, StoragePermitKey, raw, StoragePermitTTL).Err(); err != nil {
				t.Fatal(err)
			}
			got, err := q.GetStoragePermit(ctx)
			wantDate := time.Unix(now, 0).UTC().Format(time.RFC3339)
			if err != nil || got.CheckedAt != wantDate {
				t.Fatalf("Go permit reader = %+v err=%v; want canonical checked_at %q", got, err, wantDate)
			}
			claim, ok, err := q.ClaimForWorker(ctx, time.Second, worker)
			if err != nil || !ok || claim.Job.ID != job.ID {
				t.Fatalf("Lua claim disagreed with valid numeric permit authority: %+v ok=%v err=%v", claim, ok, err)
			}
		})
	}
}

func TestRedisH2GenericRoleCapabilitiesCannotReplaceMeasuredBackends(t *testing.T) {
	for _, tc := range []struct {
		name, kind, required string
		caller, current      Worker
	}{
		{name: "encoder", kind: KindProxy, required: "encoder:hevc_vaapi",
			caller:  Worker{Encoders: []string{"hevc_vaapi"}},
			current: Worker{Encoders: []string{"av1_nvenc"}, Benchmarks: WorkerBenchmarks{EncodeFPS: 12, ProbedAt: nowISO()}}},
		{name: "decoder", kind: KindDerivatives, required: "decoder:hevc_vaapi",
			caller:  Worker{Decoders: []string{"hevc_vaapi"}},
			current: Worker{Decoders: []string{"av1_nvdec"}, Benchmarks: WorkerBenchmarks{DecodeFPS: 12, ProbedAt: nowISO()}}},
		{name: "transcript", kind: KindTranscript, required: "transcript:vulkan",
			caller:  Worker{TranscriptBackends: []string{"vulkan"}},
			current: Worker{TranscriptBackends: []string{"whisper"}, Benchmarks: WorkerBenchmarks{TranscriptXReal: 1.2, ProbedAt: nowISO()}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, ctx := openWatchEpochTestQueue(t)
			current := Worker{ID: "h2-generic-cap", Name: "measured-node", Role: QueueClassRender,
				Capabilities: []string{tc.required}, RequireStoragePermit: false}
			current.Encoders = tc.current.Encoders
			current.Decoders = tc.current.Decoders
			current.TranscriptBackends = tc.current.TranscriptBackends
			current.Benchmarks = tc.current.Benchmarks
			if err := q.Heartbeat(ctx, current); err != nil {
				t.Fatal(err)
			}
			publishH2Permit(t, q, ctx)
			job := NewJob("/acceptance/h2/stale-generic-"+tc.name, []string{tc.kind}, "h2-redis-acceptance")
			job.QueueClass = QueueClassRender
			job.RequiredCapabilities = []string{tc.required}
			if err := q.Enqueue(ctx, job); err != nil {
				t.Fatal(err)
			}
			caller := current
			caller.Encoders, caller.Decoders, caller.TranscriptBackends = tc.caller.Encoders, tc.caller.Decoders, tc.caller.TranscriptBackends
			if claim, ok, err := q.ClaimForWorker(ctx, 0, caller); err != nil || ok {
				t.Fatalf("stale generic %s capability claimed: %+v ok=%v err=%v", tc.name, claim, ok, err)
			}
			assertH2JobStillReady(t, q, ctx, job)
		})
	}
}

func btoi(value bool) int {
	if value {
		return 1
	}
	return 0
}

func setupH2AdmittedRenderWorker(t *testing.T, name string) (*Client, context.Context, Worker) {
	t.Helper()
	q, ctx := openWatchEpochTestQueue(t)
	worker := Worker{ID: "h2-profile-" + name, Name: name, Role: QueueClassRender,
		RequireStoragePermit: true, Encoders: []string{"hevc_vaapi"}, Benchmarks: WorkerBenchmarks{
			EncodeFPS: 24.5, ProbedAt: nowISO(),
		}}
	if err := q.Heartbeat(ctx, worker); err != nil {
		t.Fatal(err)
	}
	return q, ctx, worker
}

func setupH2AdmittedRenderJob(t *testing.T) (*Client, context.Context, Worker, Job) {
	t.Helper()
	q, ctx := openWatchEpochTestQueue(t)
	worker := Worker{ID: "h2-profile-authority", Name: "render-node", Role: QueueClassRender,
		RequireStoragePermit: true, Encoders: []string{"hevc_vaapi"}, Benchmarks: WorkerBenchmarks{
			EncodeFPS: 24.5, ProbedAt: nowISO(),
		}}
	if err := q.Heartbeat(ctx, worker); err != nil {
		t.Fatal(err)
	}
	job := NewJob("/acceptance/h2/authority.mov", []string{KindProxy}, "h2-redis-acceptance")
	job.QueueClass = QueueClassRender
	job.RequiredCapabilities = []string{"encoder:hevc_vaapi"}
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	return q, ctx, worker, job
}

func publishH2Permit(t *testing.T, q *Client, ctx context.Context) {
	t.Helper()
	if err := q.PublishStoragePermit(ctx, StoragePermit{Source: "h2-test", AvailableBytes: 2, RequiredBytes: 1}); err != nil {
		t.Fatal(err)
	}
}

func waitH2KeyExpiry(t *testing.T, q *Client, ctx context.Context, key string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		exists, err := q.rdb.Exists(ctx, key).Result()
		if err != nil {
			t.Fatal(err)
		}
		if exists == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("key %s did not expire", key)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func assertH2WorkerHeartbeat(t *testing.T, q *Client, ctx context.Context, id string, blocked bool) {
	t.Helper()
	workers, err := q.ActiveWorkers(ctx)
	if err != nil || len(workers) != 1 || workers[0].ID != id || workers[0].Role != QueueClassRender ||
		workers[0].AdmissionBlocked != blocked {
		t.Fatalf("worker heartbeat id=%q blocked=%v: %+v err=%v", id, blocked, workers, err)
	}
}

func assertH2JobStillReady(t *testing.T, q *Client, ctx context.Context, job Job) {
	t.Helper()
	depth, err := q.rdb.LLen(ctx, QueueKeyFor(&job)).Result()
	if err != nil || depth != 1 {
		t.Fatalf("render job %s ready depth=%d err=%v; want one untouched copy", job.ID, depth, err)
	}
	status, err := q.rdb.HGet(ctx, JobHashPrefix+job.ID, "status").Result()
	if err != nil || status != StatusQueued {
		t.Fatalf("render job %s status=%q err=%v; want queued", job.ID, status, err)
	}
}

// A render worker upgraded before the server worker sees permits written by the
// pre-H-2 publisher: canonical checked_at only, no checked_at_unix. This is the
// exact shape read from production Redis on 2026-10-06. Both the Go reader and
// the Lua claim must accept it, and must still refuse a non-canonical date.
func TestRedisH2LegacyPermitWithoutUnixSecondsAdmitsClaim(t *testing.T) {
	for _, tc := range []struct {
		name string
		date func() string
		ok   bool
	}{
		{name: "canonical", date: func() string { return time.Now().UTC().Truncate(time.Second).Format(time.RFC3339) }, ok: true},
		{name: "offset-form", date: func() string { return time.Now().In(time.FixedZone("x", 3600)).Format(time.RFC3339) }, ok: false},
		{name: "stale", date: func() string { return time.Now().UTC().Add(-time.Minute).Format(time.RFC3339) }, ok: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, ctx, worker, job := setupH2AdmittedRenderJob(t)
			raw := fmt.Sprintf(`{"source":"server-worker","checked_at":%q,"total_bytes":27315992002560,"available_bytes":7441157062656,"required_bytes":273159920025}`, tc.date())
			if err := q.rdb.Set(ctx, StoragePermitKey, raw, StoragePermitTTL).Err(); err != nil {
				t.Fatal(err)
			}
			_, readErr := q.GetStoragePermit(ctx)
			claim, claimed, claimErr := q.ClaimForWorker(ctx, time.Second, worker)
			if tc.ok {
				if readErr != nil {
					t.Fatalf("Go reader rejected a valid legacy permit: %v", readErr)
				}
				if claimErr != nil || !claimed || claim.Job.ID != job.ID {
					t.Fatalf("Lua claim rejected a valid legacy permit: %+v ok=%v err=%v", claim, claimed, claimErr)
				}
				return
			}
			if readErr == nil {
				t.Fatal("Go reader accepted an invalid legacy permit")
			}
			if claimed {
				t.Fatal("Lua claim accepted an invalid legacy permit")
			}
		})
	}
}
