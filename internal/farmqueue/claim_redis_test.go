package farmqueue

import (
	"context"
	"os"
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
