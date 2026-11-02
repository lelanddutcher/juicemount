//go:build darwin || linux

package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/internal/farmqueue"
	"github.com/redis/go-redis/v9"
)

const dispatchRetryChildEnv = "JM_FARM_DISPATCH_RETRY_CHILD"

// dispatchRetryPause holds Redis writes longer than go-redis's own retries
// (4 attempts with a 3s read timeout, about 12-13s) but ends before the
// worker's second publication attempt would time out (about 15s).
const dispatchRetryPause = 14 * time.Second

// TestRunQueueRetriesDispatchedPublicationThroughTransientFault runs the real
// worker loop on a directory parent that fans out into shards. Redis writes
// stall just as the parent publishes "dispatched". The worker must retry the
// dispatched mark and its ACK through the stall instead of stopping on the
// first timeout and leaving the parent to the reaper.
func TestRunQueueRetriesDispatchedPublicationThroughTransientFault(t *testing.T) {
	if os.Getenv(dispatchRetryChildEnv) == "1" {
		started, release := os.Getenv("JM_FARM_DISPATCH_STARTED"), os.Getenv("JM_FARM_DISPATCH_RELEASE")
		targets := strings.Split(os.Getenv("JM_FARM_DISPATCH_TARGETS"), "\n")
		cfg := queueConfig{
			meta: os.Getenv("JM_FARM_DISPATCH_META"), dbPath: os.Getenv("JM_FARM_DISPATCH_DB"),
			mount: os.Getenv("JM_FARM_DISPATCH_MOUNT"), producer: "dispatch-retry", version: 1,
			conc: 1, pConc: 1, vcodec: "libx264", preset: "ultrafast", role: farmqueue.QueueClassServer,
		}
		cfg.probeProfile = func(context.Context, queueConfig, *derivatives.Store) (workerProfile, error) {
			return workerProfile{Role: farmqueue.QueueClassServer, Capabilities: []string{"cpu", "metadata"}}, nil
		}
		// Hold the parent in planning until the test has stalled Redis writes.
		cfg.collectTargets = func(ctx context.Context, _ string, _ string, _ int, _ int64) ([]string, error) {
			_ = os.WriteFile(started, nil, 0o644)
			for {
				if _, err := os.Stat(release); err == nil {
					return targets, nil
				}
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(20 * time.Millisecond):
				}
			}
		}
		os.Exit(runQueue(cfg))
	}

	metaURL := os.Getenv("JM_FARM_TEST_REDIS_URL")
	if metaURL == "" || os.Getenv("JM_FARM_TEST_REDIS_FLUSH") != "1" {
		t.Skip("set JM_FARM_TEST_REDIS_URL to isolated Redis and JM_FARM_TEST_REDIS_FLUSH=1")
	}
	redisOptions, err := redis.ParseURL(metaURL)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(redisOptions)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		rdb.Close()
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_ = rdb.Do(cleanupCtx, "CLIENT", "UNPAUSE").Err()
		_ = rdb.FlushDB(cleanupCtx).Err()
		_ = rdb.Close()
	}()
	q, err := farmqueue.Open(metaURL)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	root := t.TempDir()
	targets := []string{filepath.Join(root, "a.wav"), filepath.Join(root, "b.wav")}
	for _, target := range targets {
		if err := os.WriteFile(target, []byte("audio"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	parent := farmqueue.NewJob(root, []string{farmqueue.KindTranscript}, "manager")
	parent.QueueClass = farmqueue.QueueClassServer
	parent.RequiredCapabilities = []string{"cpu"}
	// Publish the shards the parent will plan, so its own shard publication
	// is a read-only replay that a write stall cannot fail. Keep the children
	// off the ready lane so the worker does not run them afterwards.
	children, created, err := q.EnqueueTargetShards(ctx, parent, targets, targetShardSize(parent))
	if err != nil || created != len(targets) {
		t.Fatalf("pre-publish shards: created=%d err=%v", created, err)
	}
	if err := rdb.Del(ctx, farmqueue.QueueKeyFor(&children[0])).Err(); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(ctx, parent); err != nil {
		t.Fatal(err)
	}

	startedPath, releasePath := filepath.Join(root, "planning.started"), filepath.Join(root, "planning.release")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(executable, "-test.run=^TestRunQueueRetriesDispatchedPublicationThroughTransientFault$", "-test.timeout=80s")
	cmd.Env = append(os.Environ(),
		dispatchRetryChildEnv+"=1",
		"JM_FARM_DISPATCH_META="+metaURL,
		"JM_FARM_DISPATCH_DB="+filepath.Join(t.TempDir(), "worker.db"),
		"JM_FARM_DISPATCH_MOUNT="+root,
		"JM_FARM_DISPATCH_STARTED="+startedPath,
		"JM_FARM_DISPATCH_RELEASE="+releasePath,
		"JM_FARM_DISPATCH_TARGETS="+strings.Join(targets, "\n"),
		"JM_FARM_WATCH=0",
	)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var output lockedBuffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	childDone := make(chan error, 1)
	go func() { childDone <- cmd.Wait() }()
	childRunning := true
	defer func() {
		if childRunning {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-childDone
		}
	}()

	waitForFile(t, startedPath, 20*time.Second, childDone, &output)
	statusKey := farmqueue.JobHashPrefix + parent.ID
	if status, err := rdb.HGet(ctx, statusKey, "status").Result(); err != nil || status != farmqueue.StatusRunning {
		t.Fatalf("parent before the stall = %q, %v; want running", status, err)
	}
	if err := rdb.Do(ctx, "CLIENT", "PAUSE", dispatchRetryPause.Milliseconds(), "WRITE").Err(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(releasePath, nil, 0o644); err != nil {
		t.Fatal(err)
	}

	// Reads still work while writes are paused.
	deadline := time.After(dispatchRetryPause + 20*time.Second)
	for published := false; !published; {
		select {
		case waitErr := <-childDone:
			childRunning = false
			t.Fatalf("worker stopped on a transient fault instead of retrying (exit %v); parent left %q; output:\n%s",
				waitErr, rdb.HGet(ctx, statusKey, "status").Val(), output.String())
		case <-deadline:
			t.Fatalf("parent was not published dispatched; status %q; output:\n%s",
				rdb.HGet(ctx, statusKey, "status").Val(), output.String())
		case <-time.After(100 * time.Millisecond):
		}
		status := rdb.HGet(ctx, statusKey, "status").Val()
		receipts := rdb.SCard(ctx, farmqueue.ProcessingIndexKey).Val()
		published = status == farmqueue.StatusDispatched && receipts == 0
	}
	select {
	case waitErr := <-childDone:
		childRunning = false
		t.Fatalf("worker exited after publishing (exit %v); output:\n%s", waitErr, output.String())
	case <-time.After(time.Second):
	}
}
