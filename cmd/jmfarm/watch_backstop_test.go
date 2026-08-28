package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/farmqueue"
	"github.com/redis/go-redis/v9"
)

// These tests exercise the durable half of automatic discovery against a real,
// isolated Redis. The keyspace-event watcher has its own live integration test;
// this suite proves that a restart, pause, or bounded scan cannot silently skip
// media while the push path is unavailable.
func openBackstopTestQueue(t *testing.T) (*farmqueue.Client, context.Context) {
	t.Helper()
	metaURL := os.Getenv("JM_FARM_TEST_REDIS_URL")
	if metaURL == "" || os.Getenv("JM_FARM_TEST_REDIS_FLUSH") != "1" {
		t.Skip("set JM_FARM_TEST_REDIS_URL to an isolated Redis and JM_FARM_TEST_REDIS_FLUSH=1")
	}
	opt, err := redis.ParseURL(metaURL)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opt)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		t.Fatal(err)
	}
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		rdb.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Second)
		defer cleanupCancel()
		_ = rdb.FlushDB(cleanupCtx).Err()
		_ = rdb.Close()
	})

	q, err := farmqueue.Open(metaURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })
	return q, ctx
}

func runBackstopStartup(t *testing.T, cfg queueConfig, q *farmqueue.Client, workerID string, kinds []string, ready func() bool) {
	t.Helper()
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runWatchBackstop(runCtx, cfg, q, workerID, kinds)
		close(done)
	}()

	deadline := time.Now().Add(3 * time.Second)
	for !ready() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("watch backstop did not stop after cancellation")
	}
	if !ready() {
		t.Fatal("watch backstop startup scan did not reach the expected state")
	}
}

func TestWatchBackstopInitialCatchupSurvivesPausedStartup(t *testing.T) {
	q, ctx := openBackstopTestQueue(t)
	t.Setenv("JM_FARM_WATCH_BACKSTOP_SEC", "60")
	mount := t.TempDir()
	cfg := queueConfig{mount: mount}

	control := farmqueue.DefaultFarmControl()
	control.Paused = true
	if _, err := q.StoreControl(ctx, control, 0); err != nil {
		t.Fatal(err)
	}

	// A paused restart must neither enqueue nor advance the durable cursor. If
	// the cursor moved here, resuming later would falsely declare old media seen.
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runWatchBackstop(runCtx, cfg, q, "paused-server", []string{farmqueue.KindDerivatives})
		close(done)
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done
	if cursor, err := q.GetWatchCursor(ctx); err != nil || !cursor.IsZero() {
		t.Fatalf("paused startup cursor = %v, %v; want zero", cursor, err)
	}
	if depth, err := q.QueueDepth(ctx); err != nil || depth != 0 {
		t.Fatalf("paused startup queue depth = %d, %v; want zero", depth, err)
	}

	control.Paused = false
	if _, err := q.StoreControl(ctx, control, 0); err != nil {
		t.Fatal(err)
	}
	runBackstopStartup(t, cfg, q, "resumed-server", []string{farmqueue.KindDerivatives}, func() bool {
		cursor, err := q.GetWatchCursor(ctx)
		return err == nil && !cursor.IsZero()
	})
	if depth, err := q.QueueDepth(ctx); err != nil || depth != 1 {
		t.Fatalf("resumed initial catch-up depth = %d, %v; want one recursive root job", depth, err)
	}

	server := farmqueue.Worker{ID: "claim-server", Name: "claim-server", Role: farmqueue.QueueClassServer,
		Capabilities: []string{"cpu", "metadata"}}
	claim, ok, err := q.ClaimForWorker(ctx, time.Second, server)
	if err != nil || !ok {
		t.Fatalf("claim initial catch-up: ok=%v err=%v", ok, err)
	}
	if claim.Job.Path != mount || claim.Job.Producer != "farm-watch-catchup" {
		t.Fatalf("initial catch-up job = %+v, want recursive mount root", claim.Job)
	}
	if err := q.AckClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
}

func TestWatchBackstopSaturationQueuesLosslessRootCatchup(t *testing.T) {
	q, ctx := openBackstopTestQueue(t)
	t.Setenv("JM_FARM_WATCH_BACKSTOP_SEC", "60")
	t.Setenv("JM_FARM_WATCH_BACKSTOP_MAX", "1")
	mount := t.TempDir()
	cursor := time.Now().Add(-time.Minute)
	if err := q.StoreWatchCursor(ctx, cursor); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{"camera-a", "camera-b"} {
		if err := os.Mkdir(filepath.Join(mount, dir), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	render := farmqueue.Worker{ID: "render", Name: "render", Role: farmqueue.QueueClassRender,
		Capabilities: []string{"encoder:hevc_vaapi"}, Encoders: []string{"hevc_vaapi"}}
	if err := q.Heartbeat(ctx, render); err != nil {
		t.Fatal(err)
	}

	cfg := queueConfig{mount: mount}
	runBackstopStartup(t, cfg, q, "saturated-server", []string{farmqueue.KindProxy}, func() bool {
		depth, err := q.QueueDepth(ctx)
		return err == nil && depth == 1
	})
	advanced, err := q.GetWatchCursor(ctx)
	if err != nil || !advanced.After(cursor) {
		t.Fatalf("saturated scan cursor = %v, %v; want advancement after root catch-up enqueue", advanced, err)
	}

	server := farmqueue.Worker{ID: "plan-server", Name: "plan-server", Role: farmqueue.QueueClassServer,
		Capabilities: []string{"cpu", "metadata"}}
	claim, ok, err := q.ClaimForWorker(ctx, time.Second, server)
	if err != nil || !ok {
		t.Fatalf("server claim saturated root catch-up planner: ok=%v err=%v", ok, err)
	}
	if claim.Job.Path != mount || claim.Job.Producer != "farm-watch-catchup" ||
		claim.Job.Kinds[0] != farmqueue.KindProxy || !claim.Job.PlanOnly ||
		claim.Job.QueueClass != farmqueue.QueueClassServer {
		t.Fatalf("saturated catch-up job = %+v, want one recursive server-planned proxy root job", claim.Job)
	}
	if err := q.AckClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
}

func TestWatchBackstopCursorPreventsReplayAcrossLeaderRestart(t *testing.T) {
	q, ctx := openBackstopTestQueue(t)
	t.Setenv("JM_FARM_WATCH_BACKSTOP_SEC", "60")
	mount := t.TempDir()
	if err := os.Mkdir(filepath.Join(mount, "new-card"), 0o755); err != nil {
		t.Fatal(err)
	}
	initialCursor := time.Now().Add(-time.Minute)
	if err := q.StoreWatchCursor(ctx, initialCursor); err != nil {
		t.Fatal(err)
	}
	cfg := queueConfig{mount: mount}

	runBackstopStartup(t, cfg, q, "server-a", []string{farmqueue.KindDerivatives}, func() bool {
		depth, err := q.QueueDepth(ctx)
		return err == nil && depth == 1
	})
	firstCursor, err := q.GetWatchCursor(ctx)
	if err != nil || !firstCursor.After(initialCursor) {
		t.Fatalf("first leader cursor = %v, %v; want durable advancement", firstCursor, err)
	}
	server := farmqueue.Worker{ID: "restart-claim-server", Name: "restart-claim-server", Role: farmqueue.QueueClassServer,
		Capabilities: []string{"cpu", "metadata"}}
	claim, ok, err := q.ClaimForWorker(ctx, time.Second, server)
	if err != nil || !ok {
		t.Fatalf("claim modified directory: ok=%v err=%v", ok, err)
	}
	if claim.Job.Path != filepath.Join(mount, "new-card") || claim.Job.Producer != "farm-watch-backstop" {
		t.Fatalf("modified-directory job = %+v", claim.Job)
	}
	if err := q.AckClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if err := q.ReleaseWatchLeadership(ctx, "server-a"); err != nil {
		t.Fatal(err)
	}

	// A replacement leader starts immediately. It reads the stored cursor,
	// advances it after an empty scan, and does not replay the old directory.
	runBackstopStartup(t, cfg, q, "server-b", []string{farmqueue.KindDerivatives}, func() bool {
		cursor, err := q.GetWatchCursor(ctx)
		return err == nil && cursor.After(firstCursor)
	})
	if depth, err := q.QueueDepth(ctx); err != nil || depth != 0 {
		t.Fatalf("replacement leader replayed old work: depth=%d err=%v", depth, err)
	}
}
