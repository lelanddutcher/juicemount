//go:build darwin || linux

package main

import (
	"bytes"
	"context"
	"errors"
	"net/url"
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

const publishFailureChildEnv = "JM_FARM_PUBLISH_FAILURE_CHILD"

// TestRunQueueStopsWhenLegacyOutcomeCannotBePublished runs the real worker loop
// against a Redis user that loses EVAL permission mid-job. Heartbeats (plain
// SET) keep working, so the worker looks alive while every terminal write
// fails. The worker must retry, then stop with its receipt intact so the reaper
// can recover the job. Before the fix it logged the failure and kept polling
// with a fresh heartbeat, leaving the job running indefinitely.
func TestRunQueueStopsWhenLegacyOutcomeCannotBePublished(t *testing.T) {
	if os.Getenv(publishFailureChildEnv) == "1" {
		cfg := queueConfig{
			meta: os.Getenv("JM_FARM_PUBLISH_META"), dbPath: os.Getenv("JM_FARM_PUBLISH_DB"),
			mount: os.Getenv("JM_FARM_PUBLISH_MOUNT"), producer: "publish-failure", version: 1,
			conc: 1, pConc: 1, vcodec: "libx264", preset: "ultrafast", role: farmqueue.QueueClassServer,
		}
		cfg.probeProfile = func(context.Context, queueConfig, *derivatives.Store) (workerProfile, error) {
			return workerProfile{Role: farmqueue.QueueClassServer, Capabilities: []string{"cpu", "metadata"}}, nil
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
	const aclUser = "jm-publish-failure-worker"
	const aclPass = "publish-failure-test"
	if err := rdb.Do(ctx, "ACL", "SETUSER", aclUser, "reset", "on", ">"+aclPass, "~*", "&*", "+@all").Err(); err != nil {
		rdb.Close()
		t.Skipf("Redis ACL unavailable: %v", err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cleanupCancel()
		_ = rdb.Do(cleanupCtx, "ACL", "DELUSER", aclUser).Err()
		_ = rdb.FlushDB(cleanupCtx).Err()
		_ = rdb.Close()
	}()
	workerURL, err := url.Parse(metaURL)
	if err != nil {
		t.Fatal(err)
	}
	workerURL.User = url.UserPassword(aclUser, aclPass)
	q, err := farmqueue.Open(metaURL)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	root := t.TempDir()
	mediaPath := filepath.Join(root, "clip.mov")
	if err := os.WriteFile(mediaPath, bytes.Repeat([]byte("m"), 64*1024), 0o644); err != nil {
		t.Fatal(err)
	}
	toolDir := filepath.Join(root, "tools")
	if err := os.Mkdir(toolDir, 0o755); err != nil {
		t.Fatal(err)
	}
	probe := `#!/bin/sh
printf '%s\n' '{"streams":[{"codec_type":"video","codec_name":"h264","width":1920,"height":1080,"r_frame_rate":"24/1","avg_frame_rate":"24/1"}],"format":{"format_name":"mov","duration":"10","size":"65536"}}'
`
	if err := os.WriteFile(filepath.Join(toolDir, "ffprobe"), []byte(probe), 0o755); err != nil {
		t.Fatal(err)
	}
	// The fake encoder holds the job open until the test has revoked EVAL, then
	// fails, so the worker reaches its terminal publication with Redis refusing.
	startedPath := filepath.Join(root, "ffmpeg.started")
	releasePath := filepath.Join(root, "ffmpeg.release")
	ffmpeg := "#!/bin/sh\nprintf '%s\\n' \"$$\" > \"" + startedPath + "\"\n" +
		"while [ ! -e \"" + releasePath + "\" ]; do sleep 0.05; done\nexit 1\n"
	if err := os.WriteFile(filepath.Join(toolDir, "ffmpeg"), []byte(ffmpeg), 0o755); err != nil {
		t.Fatal(err)
	}

	job := farmqueue.NewJob(root, []string{farmqueue.KindProxy}, "manager")
	job.ShardIndex, job.ShardCount = 1, 1
	job.RetryTargets = []string{mediaPath}
	job.QueueClass = farmqueue.QueueClassCPU
	job.RequiredCapabilities = []string{"cpu"}
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}

	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(executable, "-test.run=^TestRunQueueStopsWhenLegacyOutcomeCannotBePublished$", "-test.timeout=60s")
	cmd.Env = append(os.Environ(),
		publishFailureChildEnv+"=1",
		"JM_FARM_PUBLISH_META="+workerURL.String(),
		"JM_FARM_PUBLISH_DB="+filepath.Join(root, "worker.db"),
		"JM_FARM_PUBLISH_MOUNT="+root,
		"JM_FARM_WATCH=0",
		"PATH="+toolDir+string(os.PathListSeparator)+os.Getenv("PATH"),
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
		_ = os.WriteFile(releasePath, nil, 0o644)
		if childRunning {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-childDone
		}
	}()

	waitForFile(t, startedPath, 20*time.Second, childDone, &output)
	statusKey := farmqueue.JobHashPrefix + job.ID
	if status, err := rdb.HGet(ctx, statusKey, "status").Result(); err != nil || status != farmqueue.StatusRunning {
		t.Fatalf("job before fault = %q, %v; want running", status, err)
	}
	if err := rdb.Do(ctx, "ACL", "SETUSER", aclUser, "-eval", "-evalsha").Err(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(releasePath, nil, 0o644); err != nil {
		t.Fatal(err)
	}

	select {
	case waitErr := <-childDone:
		childRunning = false
		var exitErr *exec.ExitError
		if !errors.As(waitErr, &exitErr) || exitErr.ExitCode() != 1 {
			t.Fatalf("runQueue child exit = %v, want 1; output:\n%s", waitErr, output.String())
		}
	case <-time.After(40 * time.Second):
		t.Fatalf("worker kept running after its outcome could not be published; job left %q; output:\n%s",
			rdb.HGet(ctx, statusKey, "status").Val(), output.String())
	}
	if !strings.Contains(output.String(), "stopping with durable claim intact") {
		t.Fatalf("worker did not report a retained claim: %s", output.String())
	}

	// The stopped worker's heartbeat and delivery lease run out; the reaper
	// then returns the job to its ready lane.
	workers, err := rdb.SMembers(ctx, farmqueue.ProcessingIndexKey).Result()
	if err != nil || len(workers) != 1 {
		t.Fatalf("processing workers = %v, %v; want the stopped worker's receipt", workers, err)
	}
	if err := rdb.Del(ctx, farmqueue.WorkerPrefix+workers[0]).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.HSet(ctx, statusKey, "delivery_expires_unix", time.Now().Add(-time.Minute).Unix()).Err(); err != nil {
		t.Fatal(err)
	}
	if n, err := q.RecoverAbandonedWorkers(ctx); err != nil || n != 1 {
		t.Fatalf("recover stopped worker = %d, %v; want 1", n, err)
	}
	if status, err := rdb.HGet(ctx, statusKey, "status").Result(); err != nil || status != farmqueue.StatusQueued {
		t.Fatalf("recovered job = %q, %v; want queued", status, err)
	}
	if depth, err := q.QueueDepth(ctx); err != nil || depth != 1 {
		t.Fatalf("ready depth = %d, %v; want the recovered job", depth, err)
	}
}
