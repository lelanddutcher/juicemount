package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
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
		state, err := q.GetWatchEpoch(ctx, mount)
		return err == nil && state.Active != ""
	})
	if cursor, err := q.GetWatchCursor(ctx); err != nil || !cursor.IsZero() {
		t.Fatalf("initial cursor = %v, %v; want retained until complete plan", cursor, err)
	}
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
	claim.Job.TrackedClaim = claim.Token
	if err := q.CompleteTrackedEmptyPlanForClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if err := q.AckClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	scanWatchBackstop(ctx, cfg, q, "resumed-server", []string{farmqueue.KindDerivatives}, time.Now())
	if cursor, err := q.GetWatchCursor(ctx); err != nil || cursor.IsZero() {
		t.Fatalf("completed initial cursor = %v, %v; want committed cutoff", cursor, err)
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
		state, err := q.GetWatchEpoch(ctx, mount)
		return err == nil && state.Active != ""
	})
	retained, err := q.GetWatchCursor(ctx)
	if err != nil || !retained.Equal(cursor) {
		t.Fatalf("saturated scan cursor = %v, %v; want retained before full plan", retained, err)
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
	claim.Job.TrackedClaim = claim.Token
	if err := q.CompleteTrackedEmptyPlanForClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if err := q.AckClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	scanWatchBackstop(ctx, cfg, q, "saturated-server", []string{farmqueue.KindProxy}, time.Now())
	advanced, err := q.GetWatchCursor(ctx)
	if err != nil || !advanced.After(cursor) {
		t.Fatalf("completed saturated cursor = %v, %v; want committed cutoff", advanced, err)
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
		state, err := q.GetWatchEpoch(ctx, mount)
		return err == nil && state.Active != ""
	})
	firstCursor, err := q.GetWatchCursor(ctx)
	if err != nil || !firstCursor.Equal(initialCursor) {
		t.Fatalf("first leader cursor = %v, %v; want retained v1 cursor", firstCursor, err)
	}
	server := farmqueue.Worker{ID: "restart-claim-server", Name: "restart-claim-server", Role: farmqueue.QueueClassServer,
		Capabilities: []string{"cpu", "metadata"}}
	claim, ok, err := q.ClaimForWorker(ctx, time.Second, server)
	if err != nil || !ok {
		t.Fatalf("claim modified directory: ok=%v err=%v", ok, err)
	}
	if claim.Job.Path != mount || claim.Job.Producer != "farm-watch-catchup" {
		t.Fatalf("tracked root job = %+v", claim.Job)
	}
	claim.Job.TrackedClaim = claim.Token
	if err := q.CompleteTrackedEmptyPlanForClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if err := q.AckClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if err := q.ReleaseWatchLeadership(ctx, "server-a"); err != nil {
		t.Fatal(err)
	}

	// A replacement leader commits the prior complete epoch exactly once.
	runBackstopStartup(t, cfg, q, "server-b", []string{farmqueue.KindDerivatives}, func() bool {
		cursor, err := q.GetWatchCursor(ctx)
		return err == nil && cursor.After(firstCursor)
	})
	if err := q.ReleaseWatchLeadership(ctx, "server-b"); err != nil {
		t.Fatal(err)
	}
	scanWatchBackstop(ctx, cfg, q, "server-c", []string{farmqueue.KindDerivatives}, time.Now())
	if depth, err := q.QueueDepth(ctx); err != nil || depth != 0 {
		t.Fatalf("replacement leader replayed old work: depth=%d err=%v", depth, err)
	}
}

func TestWatchBackstopRevisitsMutationDespiteLegacyDedupe(t *testing.T) {
	q, ctx := openBackstopTestQueue(t)
	mount := t.TempDir()
	dir := filepath.Join(mount, "camera-card")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := queueConfig{mount: mount}
	kinds := []string{farmqueue.KindDerivatives}
	firstScan := time.Now()
	scanWatchBackstop(ctx, cfg, q, "server-a", kinds, firstScan)
	server := farmqueue.Worker{ID: "claim-server", Name: "claim-server", Role: farmqueue.QueueClassServer,
		Capabilities: []string{"cpu", "metadata"}}
	claim, ok, err := q.ClaimForWorker(ctx, time.Second, server)
	if err != nil || !ok {
		t.Fatalf("claim first tracked root: ok=%v err=%v", ok, err)
	}
	claim.Job.TrackedClaim = claim.Token
	if err := q.CompleteTrackedEmptyPlanForClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if err := q.AckClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	scanWatchBackstop(ctx, cfg, q, "server-a", kinds, firstScan.Add(time.Second))
	firstCursor, err := q.GetWatchCursor(ctx)
	if err != nil || !firstCursor.Equal(firstScan) {
		t.Fatalf("completed first cursor = %v, %v; want %v", firstCursor, err, firstScan)
	}
	opt, err := redis.ParseURL(os.Getenv("JM_FARM_TEST_REDIS_URL"))
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opt)
	defer rdb.Close()
	sum := sha256.Sum256([]byte(dir))
	key := fmt.Sprintf("juicefarm:watch:dedupe:%x", sum[:12])
	if err := rdb.Set(ctx, key, "legacy", 10*time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "new-clip.mov"), []byte("clip"), 0o644); err != nil {
		t.Fatal(err)
	}
	modified := firstScan.Add(2 * time.Second)
	if err := os.Chtimes(dir, modified, modified); err != nil {
		t.Fatal(err)
	}
	scanWatchBackstop(ctx, cfg, q, "server-a", kinds, firstScan.Add(3*time.Second))
	retained, err := q.GetWatchCursor(ctx)
	if err != nil || !retained.Equal(firstCursor) {
		t.Fatalf("changed root cursor = %v, %v; want retained %v", retained, err, firstCursor)
	}
	claim, ok, err = q.ClaimForWorker(ctx, time.Second, server)
	if err != nil || !ok {
		t.Fatalf("claim changed tracked root: ok=%v err=%v", ok, err)
	}
	if claim.Job.Path != mount || claim.Job.TrackedEpoch == "" {
		t.Fatalf("changed root job = %+v; want tracked root despite legacy dedupe", claim.Job)
	}
}

func TestWatchBackstopDedupeDoesNotAdvanceRootCursors(t *testing.T) {
	for _, tc := range []struct {
		name      string
		saturated bool
	}{
		{name: "initial"},
		{name: "saturated", saturated: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, ctx := openBackstopTestQueue(t)
			mount := t.TempDir()
			cfg := queueConfig{mount: mount}
			kinds := []string{farmqueue.KindDerivatives}
			var before time.Time
			if tc.saturated {
				t.Setenv("JM_FARM_WATCH_BACKSTOP_MAX", "1")
				before = time.Now().Add(-time.Minute)
				if err := q.StoreWatchCursor(ctx, before); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(filepath.Join(mount, "camera-card"), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := q.EnqueueDiscovered(ctx, mount, kinds, "farm-watch-catchup"); err != nil {
				t.Fatal(err)
			}
			scanWatchBackstop(ctx, cfg, q, "server-a", kinds, time.Now())
			after, err := q.GetWatchCursor(ctx)
			if err != nil || !after.Equal(before) {
				t.Fatalf("deduped root cursor = %v, %v; want %v", after, err, before)
			}
		})
	}
}

func TestWatchBackstopDedupedFirstCandidateDoesNotStarveLaterPath(t *testing.T) {
	mount := t.TempDir()
	for _, name := range []string{"a", "b"} {
		if err := os.Mkdir(filepath.Join(mount, name), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	cursor := time.Now().Add(-time.Hour)
	initialCursor := cursor
	queued := make(map[string]int)
	rootDeduped := true
	bFresh := true
	bEnqueues := 0
	enqueue := func(_ context.Context, path string, _ []string, _ string) ([]string, error) {
		queued[path]++
		if path == filepath.Join(mount, "a") || path == mount && rootDeduped {
			return nil, nil
		}
		if path == filepath.Join(mount, "b") {
			if !bFresh {
				return nil, nil
			}
			bFresh = false
			bEnqueues++
		}
		return []string{"queued"}, nil
	}
	store := func(_ context.Context, next time.Time) error {
		cursor = next
		return nil
	}
	for i := 0; i < 3; i++ {
		refresh := cursor.Add(time.Duration(i+1) * time.Minute)
		if err := os.Chtimes(filepath.Join(mount, "a"), refresh, refresh); err != nil {
			t.Fatal(err)
		}
		if err := scanWatchBackstopCandidates(context.Background(), queueConfig{mount: mount}, []string{farmqueue.KindDerivatives}, cursor, cursor.Add(time.Duration(i+1)*time.Minute), enqueue, store); err != nil {
			t.Fatal(err)
		}
	}
	if queued[filepath.Join(mount, "b")] == 0 {
		t.Fatal("later path b was never attempted behind deduped a")
	}
	if bEnqueues != 1 {
		t.Fatalf("later path b was freshly enqueued %d times; want one despite retained cursor", bEnqueues)
	}
	if !cursor.Equal(initialCursor) {
		t.Fatalf("cursor advanced past deduped a: %v", cursor)
	}
	rootDeduped = false
	if err := scanWatchBackstopCandidates(context.Background(), queueConfig{mount: mount}, []string{farmqueue.KindDerivatives}, cursor, cursor.Add(4*time.Minute), enqueue, store); err != nil {
		t.Fatal(err)
	}
	if queued[mount] != 4 {
		t.Fatalf("root fallback attempts = %d, want one per scan until queued", queued[mount])
	}
	if !cursor.Equal(initialCursor) {
		t.Fatalf("root enqueue advanced cursor without a coverage receipt: %v", cursor)
	}
}

func TestCatchupTraversalErrorCannotCompletePartialRoot(t *testing.T) {
	mount := t.TempDir()
	for _, name := range []string{"a", "b"} {
		path := filepath.Join(mount, name, "clip.mov")
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("media"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	blocked := errors.New("injected unreadable subtree")
	walk := func(root string, visit filepath.WalkFunc) error {
		return filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if path == filepath.Join(mount, "b") {
				if visitErr := visit(path, nil, blocked); visitErr != nil {
					return visitErr
				}
				return filepath.SkipDir
			}
			return visit(path, info, err)
		})
	}
	if targets, err := collectTargetsWithWalk(mount, "", 0, 0, walk); !errors.Is(err, blocked) {
		t.Fatalf("partial root traversal = %v, %v; want unreadable subtree error", targets, err)
	}
	job := farmqueue.NewJob(mount, []string{farmqueue.KindDerivatives}, "farm-watch-catchup")
	cfg := queueConfig{mount: mount, collectTargets: func(_ context.Context, root, files string, limit int, minBytes int64) ([]string, error) {
		return collectTargetsWithWalk(root, files, limit, minBytes, walk)
	}}
	_, _, _, err := runJob(context.Background(), nil, nil, cfg, farmqueue.Worker{}, job, nil)
	if !errors.Is(err, blocked) {
		t.Fatalf("partial root job returned %v; want traversal error instead of success", err)
	}
	targets, err := collectTargets(mount, "", 0, 0)
	if err != nil || len(targets) != 2 {
		t.Fatalf("restored root traversal = %v, %v; want both eligible media", targets, err)
	}
}
