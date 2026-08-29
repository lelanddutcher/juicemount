package farmqueue

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// TestRedisClaimRecovery exercises the queue's Lua claim/recovery path against
// a real, disposable Redis. It is intentionally opt-in because the test clears
// its selected Redis DB; CI/local callers must provide an isolated instance.
func TestRedisClaimRecovery(t *testing.T) {
	metaURL := os.Getenv("JM_FARM_TEST_REDIS_URL")
	if metaURL == "" || os.Getenv("JM_FARM_TEST_REDIS_FLUSH") != "1" {
		t.Skip("set JM_FARM_TEST_REDIS_URL to an isolated Redis and JM_FARM_TEST_REDIS_FLUSH=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	q, err := Open(metaURL)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err := q.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	server := Worker{ID: "redis-test-server", Name: "server", Role: QueueClassServer,
		Capabilities: []string{"cpu", "metadata"}}
	render := Worker{ID: "redis-test-render", Name: "render", Role: QueueClassRender,
		Capabilities: []string{"cpu", "vaapi", "encoder:hevc_vaapi"},
		Encoders:     []string{"hevc_vaapi"},
		Benchmarks:   WorkerBenchmarks{EncodeFPS: 180}}
	if err := q.Heartbeat(ctx, server); err != nil {
		t.Fatal(err)
	}
	if err := q.Heartbeat(ctx, render); err != nil {
		t.Fatal(err)
	}

	job := NewJob("/jfs/incoming/clip.mov", []string{KindProxy}, "manager")
	// This test exercises execution-claim recovery, not parent discovery.
	// Model the bounded child that the server planner publishes.
	job.ShardIndex, job.ShardCount = 1, 1
	job.RetryTargets = []string{job.Path}
	q.RouteJob(ctx, &job)
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := q.ClaimForWorker(ctx, 0, server); err != nil || ok {
		t.Fatalf("server claimed render work: ok=%v err=%v", ok, err)
	}
	claim, ok, err := q.ClaimForWorker(ctx, time.Second, render)
	if err != nil || !ok {
		t.Fatalf("render claim: ok=%v err=%v", ok, err)
	}
	if claim.Job.SelectedBackend != "hevc_vaapi" {
		t.Fatalf("backend = %q, want hevc_vaapi", claim.Job.SelectedBackend)
	}
	if claim.Job.SelectedWorker != "render" {
		t.Fatalf("selected worker = %q, want render", claim.Job.SelectedWorker)
	}
	if err := q.MarkClaimRunning(ctx, claim, render); err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.Del(ctx, WorkerPrefix+render.ID).Err(); err != nil {
		t.Fatal(err)
	}
	if n, err := q.RecoverAbandonedWorkers(ctx); err != nil || n != 1 {
		t.Fatalf("recover abandoned = %d, %v; want 1, nil", n, err)
	}

	fallback, ok, err := q.ClaimForWorker(ctx, time.Second, server)
	if err != nil || !ok {
		t.Fatalf("server fallback claim: ok=%v err=%v", ok, err)
	}
	if fallback.Job.SelectedBackend != "libx264" || fallback.Job.QueueClass != QueueClassCPU {
		t.Fatalf("fallback = backend %q class %q", fallback.Job.SelectedBackend, fallback.Job.QueueClass)
	}
	if fallback.Job.SelectedWorker != "" {
		t.Fatalf("CPU fallback retained selected worker %q", fallback.Job.SelectedWorker)
	}
	if err := q.MarkDone(ctx, fallback.Job.ID, 1, 0); err != nil {
		t.Fatal(err)
	}
	if err := q.AckClaim(ctx, fallback); err != nil {
		t.Fatal(err)
	}

	control := DefaultFarmControl()
	control.Paused = true
	if _, err := q.StoreControl(ctx, control, 0); err != nil {
		t.Fatal(err)
	}
	got, err := q.GetControl(ctx)
	if err != nil || !got.Paused || !got.WatchEnabled {
		t.Fatalf("control = %+v, %v", got, err)
	}
	pausedJob := NewJob("/jfs/incoming/paused.mov", []string{KindDerivatives}, "manager")
	if err := q.Enqueue(ctx, pausedJob); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := q.ClaimForWorker(ctx, 100*time.Millisecond, server); err != nil || ok {
		t.Fatalf("paused farm allowed claim: ok=%v err=%v", ok, err)
	}
	control.Paused = false
	if _, err := q.StoreControl(ctx, control, 0); err != nil {
		t.Fatal(err)
	}
	pausedClaim, ok, err := q.ClaimForWorker(ctx, time.Second, server)
	if err != nil || !ok || pausedClaim.Job.ID != pausedJob.ID {
		t.Fatalf("resumed claim = %+v, ok=%v err=%v", pausedClaim.Job, ok, err)
	}
	if err := q.AckClaim(ctx, pausedClaim); err != nil {
		t.Fatal(err)
	}
}

func TestRedisClaimRecoveryPrefersAnotherRenderWorker(t *testing.T) {
	metaURL := os.Getenv("JM_FARM_TEST_REDIS_URL")
	if metaURL == "" || os.Getenv("JM_FARM_TEST_REDIS_FLUSH") != "1" {
		t.Skip("set JM_FARM_TEST_REDIS_URL to an isolated Redis and JM_FARM_TEST_REDIS_FLUSH=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	q, err := Open(metaURL)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err := q.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	server := Worker{ID: "redis-test-server", Name: "server", Role: QueueClassServer,
		Capabilities: []string{"cpu", "metadata"}}
	renderA := Worker{ID: "redis-test-render-a", Name: "render-a", Role: QueueClassRender,
		Capabilities: []string{"vaapi", "encoder:hevc_vaapi", "decoder:hevc_vaapi"},
		Encoders:     []string{"hevc_vaapi"}, Decoders: []string{"hevc_vaapi"},
		Benchmarks: WorkerBenchmarks{EncodeFPS: 180}}
	renderB := Worker{ID: "redis-test-render-b", Name: "render-b", Role: QueueClassRender,
		Capabilities: []string{"vaapi", "encoder:hevc_vaapi", "decoder:hevc_vaapi"},
		Encoders:     []string{"hevc_vaapi"}, Decoders: []string{"hevc_vaapi"},
		Benchmarks: WorkerBenchmarks{EncodeFPS: 160}}
	if err := q.Heartbeat(ctx, server); err != nil {
		t.Fatal(err)
	}
	if err := q.Heartbeat(ctx, renderA); err != nil {
		t.Fatal(err)
	}

	job := NewJob("/jfs/incoming/recover-to-gpu.mov", []string{KindProxy}, "manager")
	job.ShardIndex, job.ShardCount = 1, 1
	job.RetryTargets = []string{job.Path}
	q.RouteJob(ctx, &job)
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	claim, ok, err := q.ClaimForWorker(ctx, time.Second, renderA)
	if err != nil || !ok {
		t.Fatalf("first render claim: ok=%v err=%v", ok, err)
	}
	if err := q.MarkClaimRunning(ctx, claim, renderA); err != nil {
		t.Fatal(err)
	}

	// Bring a second verified GPU online only after A owns the durable claim,
	// then simulate A disappearing. Recovery must re-run scheduling and select B.
	if err := q.Heartbeat(ctx, renderB); err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.Del(ctx, WorkerPrefix+renderA.ID).Err(); err != nil {
		t.Fatal(err)
	}
	if n, err := q.RecoverAbandonedWorkers(ctx); err != nil || n != 1 {
		t.Fatalf("recover abandoned = %d, %v; want 1, nil", n, err)
	}
	// Recovery is exactly-once for the durable receipt. A delayed second reaper
	// must not duplicate the job in the replacement worker's ready lane.
	if err := q.RequeueClaim(ctx, claim, false, "duplicate recovery"); err == nil {
		t.Fatal("already recovered claim was requeued a second time")
	}
	if _, ok, err := q.ClaimForWorker(ctx, 100*time.Millisecond, server); err != nil || ok {
		t.Fatalf("server claimed work while replacement GPU was online: ok=%v err=%v", ok, err)
	}
	recovered, ok, err := q.ClaimForWorker(ctx, time.Second, renderB)
	if err != nil || !ok {
		t.Fatalf("replacement render claim: ok=%v err=%v", ok, err)
	}
	if recovered.Job.SelectedBackend != "hevc_vaapi" || recovered.Job.QueueClass != QueueClassRender {
		t.Fatalf("recovered route = backend %q class %q", recovered.Job.SelectedBackend, recovered.Job.QueueClass)
	}
	if recovered.Job.SelectedWorker != renderB.Name || recovered.Job.Attempts != 1 {
		t.Fatalf("recovered target = %q attempts=%d, want %q/1",
			recovered.Job.SelectedWorker, recovered.Job.Attempts, renderB.Name)
	}
	if err := q.AckClaim(ctx, recovered); err != nil {
		t.Fatal(err)
	}
}

// A farm-wide pause stops new claims, not durable queue maintenance. This is
// the exact restart-during-pause case that left a live validation transcript
// status:"running" on an expired worker receipt indefinitely.
func TestRedisPausedFarmRecoversAbandonedClaimWithoutExecutingIt(t *testing.T) {
	metaURL := os.Getenv("JM_FARM_TEST_REDIS_URL")
	if metaURL == "" || os.Getenv("JM_FARM_TEST_REDIS_FLUSH") != "1" {
		t.Skip("set JM_FARM_TEST_REDIS_URL to an isolated Redis and JM_FARM_TEST_REDIS_FLUSH=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	q, err := Open(metaURL)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err := q.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	server := Worker{ID: "paused-recovery-server", Name: "server", Role: QueueClassServer,
		Capabilities: []string{"cpu", "metadata"}}
	render := Worker{ID: "paused-recovery-render", Name: "render", Role: QueueClassRender,
		Capabilities: []string{"encoder:hevc_vaapi"}, Encoders: []string{"hevc_vaapi"}}
	if err := q.Heartbeat(ctx, server); err != nil {
		t.Fatal(err)
	}
	if err := q.Heartbeat(ctx, render); err != nil {
		t.Fatal(err)
	}
	job := NewJob("/jfs/incoming/paused-recovery.mov", []string{KindProxy}, "manager")
	job.ShardIndex, job.ShardCount = 1, 1
	job.RetryTargets = []string{job.Path}
	q.RouteJob(ctx, &job)
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	claim, ok, err := q.ClaimForWorker(ctx, time.Second, render)
	if err != nil || !ok {
		t.Fatalf("initial render claim: ok=%v err=%v", ok, err)
	}
	if err := q.MarkClaimRunning(ctx, claim, render); err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.Del(ctx, WorkerPrefix+render.ID).Err(); err != nil {
		t.Fatal(err)
	}
	control := DefaultFarmControl()
	control.Paused = true
	if _, err := q.StoreControl(ctx, control, 0); err != nil {
		t.Fatal(err)
	}

	if n, err := q.RecoverAbandonedWorkers(ctx); err != nil || n != 1 {
		t.Fatalf("paused recovery = %d, %v; want 1, nil", n, err)
	}
	if _, ok, err := q.ClaimForWorker(ctx, 100*time.Millisecond, server); err != nil || ok {
		t.Fatalf("paused farm executed recovered work: ok=%v err=%v", ok, err)
	}
	status, err := q.rdb.HGetAll(ctx, JobHashPrefix+job.ID).Result()
	if err != nil {
		t.Fatal(err)
	}
	if status["status"] != StatusQueued || status["attempts"] != "1" || status["backend"] != "libx264" {
		t.Fatalf("recovered status = %+v, want queued attempt 1 on explicit CPU fallback", status)
	}

	control.Paused = false
	if _, err := q.StoreControl(ctx, control, 0); err != nil {
		t.Fatal(err)
	}
	recovered, ok, err := q.ClaimForWorker(ctx, time.Second, server)
	if err != nil || !ok || recovered.Job.ID != job.ID {
		t.Fatalf("resumed claim = %+v ok=%v err=%v", recovered.Job, ok, err)
	}
}

func TestRedisReadyCPUPromotionWhenVerifiedRenderReturns(t *testing.T) {
	metaURL := os.Getenv("JM_FARM_TEST_REDIS_URL")
	if metaURL == "" || os.Getenv("JM_FARM_TEST_REDIS_FLUSH") != "1" {
		t.Skip("set JM_FARM_TEST_REDIS_URL to an isolated Redis and JM_FARM_TEST_REDIS_FLUSH=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	q, err := Open(metaURL)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err := q.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	server := Worker{ID: "promotion-server", Name: "server", Role: QueueClassServer,
		Capabilities: []string{"cpu", "metadata"}}
	if err := q.Heartbeat(ctx, server); err != nil {
		t.Fatal(err)
	}

	temporaryProxy := NewJob("/jfs/incoming/temporary.mp4", []string{KindProxy}, "manager")
	temporaryProxy.ShardIndex, temporaryProxy.ShardCount = 1, 1
	temporaryProxy.RetryTargets = []string{temporaryProxy.Path}
	q.RouteJob(ctx, &temporaryProxy)
	if temporaryProxy.QueueClass != QueueClassCPU || temporaryProxy.CPUFallbackLocked {
		t.Fatalf("temporary proxy route = %+v, want promotable CPU", temporaryProxy)
	}
	if err := q.Enqueue(ctx, temporaryProxy); err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.HSet(ctx, JobHashPrefix+temporaryProxy.ID,
		"error", "recovered after worker old-render disappeared; compatible worker selection rerun").Err(); err != nil {
		t.Fatal(err)
	}

	// Deployed-RC migration case: an old deterministic -cpu child was promoted,
	// retained its stale lock bit, then moved back to CPU when that ephemeral
	// render identity disappeared. Availability provenance must repair the stale
	// lock; the returning worker's live admission probe remains the safety gate.
	staleOutageLock := temporaryProxy
	staleOutageLock.ID = NewID() + "-cpu"
	staleOutageLock.ParentID = strings.TrimSuffix(staleOutageLock.ID, "-cpu")
	staleOutageLock.Path = "/jfs/incoming/stale-outage-lock.mp4"
	staleOutageLock.RetryTargets = []string{staleOutageLock.Path}
	staleOutageLock.QueueClass = QueueClassCPU
	staleOutageLock.SelectedBackend = "libx264"
	staleOutageLock.SelectedWorker = ""
	staleOutageLock.RequiredCapabilities = []string{"cpu"}
	staleOutageLock.CPUFallbackLocked = true
	staleOutageLock.RoutingReason = "render capability went offline before claim; queued CPU fallback"
	if err := q.Enqueue(ctx, staleOutageLock); err != nil {
		t.Fatal(err)
	}

	parent := NewJob("/jfs/incoming/incompatible.mov", []string{KindProxy}, "manager")
	lockedProxy, err := newCPUFallbackSubset(parent, []string{parent.Path})
	if err != nil {
		t.Fatal(err)
	}
	if !lockedProxy.CPUFallbackLocked {
		t.Fatal("source-incompatible CPU subset was not locked")
	}
	if err := q.Enqueue(ctx, lockedProxy); err != nil {
		t.Fatal(err)
	}

	temporaryPreview := NewJob("/jfs/incoming/temporary-high.mp4", []string{KindDerivatives}, "manager")
	temporaryPreview.ShardIndex, temporaryPreview.ShardCount = 1, 1
	temporaryPreview.DerivativePass = DerivativePassPreviews
	temporaryPreview.SourceVideoCodec = "h264"
	temporaryPreview.SourceVideoProfile = "high"
	temporaryPreview.SourceBitDepth = 8
	temporaryPreview.RetryTargets = []string{temporaryPreview.Path}
	q.RouteJob(ctx, &temporaryPreview)
	if temporaryPreview.QueueClass != QueueClassCPU || temporaryPreview.CPUFallbackLocked {
		t.Fatalf("temporary preview route = %+v, want promotable CPU", temporaryPreview)
	}
	if err := q.Enqueue(ctx, temporaryPreview); err != nil {
		t.Fatal(err)
	}

	lockedPreview := temporaryPreview
	lockedPreview.ID = NewID()
	lockedPreview.Path = "/jfs/incoming/yuv422.mp4"
	lockedPreview.RetryTargets = []string{lockedPreview.Path}
	lockedPreview.RoutingReason = "pixel format yuv422p10le requires a non-4:2:0 decode path"
	lockedPreview.CPUFallbackLocked = true
	if err := q.Enqueue(ctx, lockedPreview); err != nil {
		t.Fatal(err)
	}

	render := Worker{ID: "promotion-render", Name: "render", Role: QueueClassRender,
		Capabilities: []string{
			"encoder:hevc_vaapi", "decoder:h264_vaapi", "decoder:h264_vaapi:profile:high",
		},
		Encoders: []string{"hevc_vaapi"}, Decoders: []string{"h264_vaapi"},
		Benchmarks: WorkerBenchmarks{EncodeFPS: 200, DecodeFPS: 300}}
	if err := q.Heartbeat(ctx, render); err != nil {
		t.Fatal(err)
	}

	if n, err := q.PromoteServiceableReady(ctx); err != nil || n != 3 {
		t.Fatalf("promote ready CPU = %d, %v; want 3, nil", n, err)
	}
	if n, err := q.PromoteServiceableReady(ctx); err != nil || n != 0 {
		t.Fatalf("idempotent promotion = %d, %v; want 0, nil", n, err)
	}

	wantRender := map[string]bool{
		temporaryProxy.ID: true, temporaryPreview.ID: true, staleOutageLock.ID: true,
	}
	for i := 0; i < 3; i++ {
		claim, ok, err := q.ClaimForWorker(ctx, time.Second, render)
		if err != nil || !ok {
			t.Fatalf("render claim after promotion: ok=%v err=%v", ok, err)
		}
		if !wantRender[claim.Job.ID] || claim.Job.QueueClass != QueueClassRender || claim.Job.CPUFallbackLocked {
			t.Fatalf("unexpected promoted claim: %+v", claim.Job)
		}
		delete(wantRender, claim.Job.ID)
		status, err := q.rdb.HGetAll(ctx, JobHashPrefix+claim.Job.ID).Result()
		if err != nil || !strings.Contains(status["error"], "promoted queued CPU fallback") {
			t.Fatalf("promotion status = %+v, %v", status, err)
		}
		if err := q.AckClaim(ctx, claim); err != nil {
			t.Fatal(err)
		}
	}
	if len(wantRender) != 0 {
		t.Fatalf("promoted render jobs not claimed: %v", wantRender)
	}

	wantCPU := map[string]bool{lockedProxy.ID: true, lockedPreview.ID: true}
	for i := 0; i < 2; i++ {
		claim, ok, err := q.ClaimForWorker(ctx, time.Second, server)
		if err != nil || !ok {
			t.Fatalf("server claim for locked fallback: ok=%v err=%v", ok, err)
		}
		if !wantCPU[claim.Job.ID] || claim.Job.QueueClass != QueueClassCPU || !claim.Job.CPUFallbackLocked {
			t.Fatalf("unexpected locked CPU claim: %+v", claim.Job)
		}
		delete(wantCPU, claim.Job.ID)
		if err := q.AckClaim(ctx, claim); err != nil {
			t.Fatal(err)
		}
	}
}

// Portable source-aware jobs intentionally omit a worker pin so another
// compatible accelerator can take over. Capability names alone are not enough:
// the atomic claim must also enforce that worker's measured frame envelope.
func TestRedisClaimEnforcesMeasuredDecodeGeometry(t *testing.T) {
	metaURL := os.Getenv("JM_FARM_TEST_REDIS_URL")
	if metaURL == "" || os.Getenv("JM_FARM_TEST_REDIS_FLUSH") != "1" {
		t.Skip("set JM_FARM_TEST_REDIS_URL to an isolated Redis and JM_FARM_TEST_REDIS_FLUSH=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	q, err := Open(metaURL)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err := q.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	capabilities := []string{
		"encoder:hevc_vaapi", "decoder:h264_vaapi",
		"decoder:h264_vaapi:profile:high", "decoder:h264_vaapi:pixfmt:yuv420p",
	}
	server := Worker{ID: "geometry-server", Name: "server", Role: QueueClassServer,
		Capabilities: []string{"cpu", "metadata"}}
	low := Worker{ID: "geometry-low", Name: "low", Role: QueueClassRender,
		Capabilities: capabilities, Encoders: []string{"hevc_vaapi"}, Decoders: []string{"h264_vaapi"},
		DecodeLimits: map[string]VideoDecodeLimit{"h264_vaapi": {MaxWidth: 4096, MaxHeight: 4096}}}
	high := Worker{ID: "geometry-high", Name: "high", Role: QueueClassRender,
		Capabilities: capabilities, Encoders: []string{"hevc_vaapi"}, Decoders: []string{"h264_vaapi"},
		DecodeLimits: map[string]VideoDecodeLimit{"h264_vaapi": {MaxWidth: 7680, MaxHeight: 4320}}}
	for _, worker := range []Worker{server, low, high} {
		if err := q.Heartbeat(ctx, worker); err != nil {
			t.Fatal(err)
		}
	}

	newOversizedJob := func(path string) Job {
		job := NewJob(path, []string{KindProxy}, "manager")
		job.ShardIndex, job.ShardCount = 1, 1
		job.RetryTargets = []string{path}
		job.QueueClass = QueueClassRender
		job.VCodec = "hevc_vaapi"
		job.SelectedBackend = "hevc_vaapi"
		job.RequiredCapabilities = append([]string(nil), capabilities...)
		job.SourceVideoCodec = "h264"
		job.SourceVideoProfile = "high"
		job.SourcePixelFormat = "yuv420p"
		job.SourceVideoWidth, job.SourceVideoHeight = 5760, 2880
		return job
	}

	job := newOversizedJob("/jfs/incoming/geometry-portable.mp4")
	if !WorkerSupports(low, job.RequiredCapabilities) || workerCanClaimJob(low, job) {
		t.Fatal("lower-capacity worker was not distinguished by measured geometry")
	}
	if !workerCanClaimJob(high, job) {
		t.Fatal("verified 8K worker rejected the source")
	}
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := q.ClaimForWorker(ctx, 100*time.Millisecond, low); err != nil || ok {
		t.Fatalf("lower-capacity worker claimed 5760-wide source: ok=%v err=%v", ok, err)
	}
	claim, ok, err := q.ClaimForWorker(ctx, time.Second, high)
	if err != nil || !ok || claim.Job.ID != job.ID {
		t.Fatalf("verified worker claim=%+v ok=%v err=%v", claim.Job, ok, err)
	}
	if err := q.AckClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}

	// With the high-capacity worker gone, ready-queue recovery must observe
	// geometry too and move the source to an explicit, visible CPU route.
	if err := q.rdb.Del(ctx, WorkerPrefix+high.ID).Err(); err != nil {
		t.Fatal(err)
	}
	fallback := newOversizedJob("/jfs/incoming/geometry-fallback.mp4")
	if err := q.Enqueue(ctx, fallback); err != nil {
		t.Fatal(err)
	}
	if n, err := q.RecoverUnserviceableReady(ctx); err != nil || n != 0 {
		t.Fatalf("first geometry recovery=%d, %v; want grace hold", n, err)
	}
	if class, err := q.rdb.HGet(ctx, JobHashPrefix+fallback.ID, "queue_class").Result(); err != nil || class != QueueClassRender {
		t.Fatalf("grace moved render work early: class=%q err=%v", class, err)
	}
	if err := q.rdb.HSet(ctx, JobHashPrefix+fallback.ID, readyFallbackSinceField,
		time.Now().Add(-readyFallbackGrace-time.Second).UTC().Format(time.RFC3339Nano)).Err(); err != nil {
		t.Fatal(err)
	}
	if n, err := q.RecoverUnserviceableReady(ctx); err != nil || n != 1 {
		t.Fatalf("geometry recovery=%d, %v; want one CPU reroute", n, err)
	}
	cpuClaim, ok, err := q.ClaimForWorker(ctx, time.Second, server)
	if err != nil || !ok || cpuClaim.Job.ID != fallback.ID {
		t.Fatalf("CPU fallback claim=%+v ok=%v err=%v", cpuClaim.Job, ok, err)
	}
	if cpuClaim.Job.QueueClass != QueueClassCPU || cpuClaim.Job.SelectedBackend != "libx264" ||
		cpuClaim.Job.CPUFallbackLocked {
		t.Fatalf("geometry fallback route=%+v", cpuClaim.Job)
	}
	if !temporaryAvailabilityFallback(cpuClaim.Job, "") {
		t.Fatalf("geometry outage lost temporary provenance: %+v", cpuClaim.Job)
	}
	if err := q.AckClaim(ctx, cpuClaim); err != nil {
		t.Fatal(err)
	}
}
