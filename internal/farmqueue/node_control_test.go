package farmqueue

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestValidWorkerControlName(t *testing.T) {
	for _, good := range []string{"render-a", "b70-render-rc0.5", "nas_server"} {
		if !ValidWorkerControlName(good) {
			t.Errorf("valid name %q rejected", good)
		}
	}
	for _, bad := range []string{"", "Render-A", "../render", "render a", "render/a"} {
		if ValidWorkerControlName(bad) {
			t.Errorf("invalid name %q accepted", bad)
		}
	}
}

// TestRedisWorkerLifecycleControl exercises the one-shot restart + durable
// disable protocol against the same isolated Redis gate as claim recovery.
func TestRedisWorkerLifecycleControl(t *testing.T) {
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
	if err := q.rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	first, err := q.RequestWorkerRestart(ctx, "render-a", "manager")
	if err != nil {
		t.Fatal(err)
	}
	second, err := q.RequestWorkerRestart(ctx, "render-a", "manager")
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := q.WaitWorkerCommand(ctx, "render-a", 0)
	if err != nil || !ok || got.ID != first.ID {
		t.Fatalf("first FIFO command = %+v ok=%v err=%v", got, ok, err)
	}
	if err := q.AcknowledgeWorkerCommand(ctx, got, "runtime-1"); err != nil {
		t.Fatal(err)
	}
	status, found, err := q.WorkerCommandStatusByID(ctx, first.ID)
	if err != nil || !found || status.State != "acknowledged" || status.WorkerID != "runtime-1" {
		t.Fatalf("ack status = %+v found=%v err=%v", status, found, err)
	}
	got, ok, err = q.WaitWorkerCommand(ctx, "render-a", 0)
	if err != nil || !ok || got.ID != second.ID {
		t.Fatalf("second FIFO command = %+v ok=%v err=%v", got, ok, err)
	}
	if _, ok, err := q.WaitWorkerCommand(ctx, "render-a", 0); err != nil || ok {
		t.Fatalf("empty command queue ok=%v err=%v", ok, err)
	}

	if err := q.SetWorkerDisabled(ctx, "render-a", true); err != nil {
		t.Fatal(err)
	}
	if disabled, err := q.WorkerDisabled(ctx, "render-a"); err != nil || !disabled {
		t.Fatalf("disabled=%v err=%v", disabled, err)
	}
	if err := q.SetWorkerDisabled(ctx, "render-a", false); err != nil {
		t.Fatal(err)
	}
	if disabled, err := q.WorkerDisabled(ctx, "render-a"); err != nil || disabled {
		t.Fatalf("after enable disabled=%v err=%v", disabled, err)
	}

	base := &FarmConfig{Defaults: map[string]any{}, Overrides: map[string]map[string]any{}}
	if rev, err := q.StoreConfig(ctx, base, 0); err != nil || rev != 1 {
		t.Fatalf("initial config revision=%d err=%v", rev, err)
	}
	firstConfig, err := q.GetConfig(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stale, err := q.GetConfig(ctx)
	if err != nil {
		t.Fatal(err)
	}
	firstConfig.Defaults["paused"] = true
	if rev, err := q.StoreConfig(ctx, firstConfig, 0); err != nil || rev != 2 {
		t.Fatalf("second config revision=%d err=%v", rev, err)
	}
	stale.Defaults["drain"] = true
	if _, err := q.StoreConfig(ctx, stale, 0); !errors.Is(err, ErrFarmConfigConflict) {
		t.Fatalf("stale config write error=%v, want conflict", err)
	}
}

// TestRedisJobControl proves cancellation is durable, failed-job requeue keeps
// the exact route/fallback payload, repeated requeue clicks are idempotent, and
// the worker log remains a bounded newest-first 200-line tail.
func TestRedisJobControl(t *testing.T) {
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
	if err := q.rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	job := NewJob("/safe/media", []string{KindProxy}, "test")
	job.QueueClass = QueueClassCPU
	job.RequiredCapabilities = []string{"cpu"}
	job.SelectedBackend = "libx264"
	job.VCodec = "libx264"
	job.CPUFallbackLocked = true
	job.RetryTargets = []string{"clip-a.mov"}
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := q.RequestJobCancel(ctx, job.ID, "manager"); err != nil {
		t.Fatal(err)
	}
	if requested, err := q.JobCancellationRequested(ctx, job.ID); err != nil || !requested {
		t.Fatalf("cancel requested=%v err=%v", requested, err)
	}
	if err := q.MarkFailed(ctx, job.ID, "fixture failure"); err != nil {
		t.Fatal(err)
	}
	if err := q.RequestJobCancel(ctx, job.ID, "manager"); !errors.Is(err, ErrJobTerminal) {
		t.Fatalf("terminal cancel error = %v", err)
	}

	retry, created, err := q.RequeueFailed(ctx, job.ID, "manager")
	if err != nil || !created {
		t.Fatalf("first requeue = %+v created=%v err=%v", retry, created, err)
	}
	if retry.ID == job.ID || retry.RequeueOf != job.ID || retry.SelectedBackend != "libx264" ||
		!retry.CPUFallbackLocked || len(retry.RetryTargets) != 1 || retry.RetryTargets[0] != "clip-a.mov" {
		t.Fatalf("retry did not preserve exact payload: %+v", retry)
	}
	again, created, err := q.RequeueFailed(ctx, job.ID, "manager")
	if err != nil || created || again.ID != retry.ID {
		t.Fatalf("idempotent requeue = %+v created=%v err=%v", again, created, err)
	}

	for i := 0; i < 205; i++ {
		if err := q.AppendWorkerLog(ctx, "runtime-a", fmt.Sprintf("line %03d", i)); err != nil {
			t.Fatal(err)
		}
	}
	lines, err := q.WorkerLog(ctx, "runtime-a", 200)
	if err != nil || len(lines) != 200 {
		t.Fatalf("bounded log len=%d err=%v", len(lines), err)
	}
	if lines[0].Line != "line 204" || lines[199].Line != "line 005" {
		t.Fatalf("bounded log first=%+v last=%+v", lines[0], lines[199])
	}
}
