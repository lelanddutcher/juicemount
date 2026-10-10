//go:build darwin || linux

package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/internal/farmqueue"
	"github.com/redis/go-redis/v9"
)

const h1RunQueueChildEnv = "JM_FARM_H1_RUNQUEUE_CHILD"

// TestH1RunQueueLeaseLossStopsMediaProcessGroup runs the real worker loop in a
// child test process so its signal context and media process group are isolated
// from the Redis-owning test process.
func TestH1RunQueueLeaseLossStopsMediaProcessGroup(t *testing.T) {
	if os.Getenv(h1RunQueueChildEnv) == "1" {
		cfg := queueConfig{
			meta: os.Getenv("JM_FARM_H1_META"), dbPath: os.Getenv("JM_FARM_H1_DB"),
			mount: os.Getenv("JM_FARM_H1_MOUNT"), producer: "h1-runqueue", version: 1,
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
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		t.Fatal(err)
	}
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		rdb.Close()
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cleanupCancel()
		_ = rdb.FlushDB(cleanupCtx).Err()
		_ = rdb.Close()
	}()
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
	startedPath := filepath.Join(root, "ffmpeg.started")
	ffmpeg := "#!/bin/sh\nprintf '%s\\n' \"$$\" > \"" + startedPath + "\"\nexec tail -f /dev/null\n"
	if err := os.WriteFile(filepath.Join(toolDir, "ffmpeg"), []byte(ffmpeg), 0o755); err != nil {
		t.Fatal(err)
	}

	planner := farmqueue.Worker{ID: "h1-runqueue-planner", Name: "h1-runqueue-planner",
		Role: farmqueue.QueueClassServer, Capabilities: []string{"cpu", "metadata"}}
	if err := q.Heartbeat(ctx, planner); err != nil {
		t.Fatal(err)
	}
	if leader, err := q.AcquireWatchLeadership(ctx, planner.ID, time.Minute); err != nil || !leader {
		t.Fatalf("acquire watch leadership: leader=%v err=%v", leader, err)
	}
	epoch, err := q.StartWatchEpoch(ctx, root, planner.ID, []string{farmqueue.KindProxy}, time.Now())
	if err != nil || epoch.Active == "" {
		t.Fatalf("start tracked epoch: state=%+v err=%v", epoch, err)
	}
	plannerClaim, ok, err := q.ClaimForWorker(ctx, time.Second, planner)
	if err != nil || !ok || plannerClaim.Job.TrackedEpoch != epoch.Active {
		t.Fatalf("claim tracked planner: claim=%+v ok=%v err=%v", plannerClaim, ok, err)
	}
	child := farmqueue.Job{
		ID: plannerClaim.Job.ID + "-exec-0001", ParentID: plannerClaim.Job.ID,
		Path: root, Kinds: []string{farmqueue.KindProxy}, Producer: plannerClaim.Job.Producer,
		EnqueuedAt: plannerClaim.Job.EnqueuedAt, TrackedEpoch: epoch.Active,
		TrackedClaim: plannerClaim.Token, ShardIndex: 1, ShardCount: 1,
		RetryTargets: []string{mediaPath}, QueueClass: farmqueue.QueueClassCPU,
		RequiredCapabilities: []string{"cpu"},
	}
	if created, err := q.EnqueuePlannedChildrenForClaim(ctx, plannerClaim, []farmqueue.Job{child}); err != nil || created != 1 {
		t.Fatalf("publish execution child: created=%d err=%v", created, err)
	}
	if committed, err := q.CommitWatchEpoch(ctx, root, planner.ID, epoch.Active); err != nil || !committed {
		t.Fatalf("commit tracked epoch: committed=%v err=%v", committed, err)
	}
	if err := q.AckClaim(ctx, plannerClaim); err != nil {
		t.Fatalf("ack tracked planner: %v", err)
	}

	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(executable, "-test.run=^TestH1RunQueueLeaseLossStopsMediaProcessGroup$", "-test.timeout=30s")
	cmd.Env = append(os.Environ(),
		h1RunQueueChildEnv+"=1",
		"JM_FARM_H1_META="+metaURL,
		"JM_FARM_H1_DB="+filepath.Join(root, "worker.db"),
		"JM_FARM_H1_MOUNT="+root,
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
		if pidBytes, err := os.ReadFile(startedPath); err == nil {
			if mediaPID, err := strconv.Atoi(strings.TrimSpace(string(pidBytes))); err == nil && mediaPID > 0 {
				_ = syscall.Kill(-mediaPID, syscall.SIGKILL)
			}
		}
		if childRunning {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-childDone
		}
	}()

	waitForFile(t, startedPath, 20*time.Second, childDone, &output)
	pidBytes, err := os.ReadFile(startedPath)
	if err != nil {
		t.Fatal(err)
	}
	mediaPID, err := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
	if err != nil || mediaPID <= 0 {
		t.Fatalf("fake ffmpeg PID %q: %v", strings.TrimSpace(string(pidBytes)), err)
	}
	statusKey := farmqueue.JobHashPrefix + child.ID
	status, err := rdb.HGetAll(ctx, statusKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	if status["status"] != farmqueue.StatusRunning || status["claim_phase"] != "running" || status["claim_token"] == "" {
		t.Fatalf("tracked media claim did not reach running state: %+v", status)
	}
	oldToken := status["claim_token"]
	if err := rdb.HSet(ctx, statusKey, "lease_expires_unix", time.Now().Add(-time.Minute).Unix()).Err(); err != nil {
		t.Fatal(err)
	}
	if recovered, err := q.RecoverAbandonedWorkers(ctx); err != nil || recovered != 1 {
		t.Fatalf("recover expired execution claim: recovered=%d err=%v", recovered, err)
	}

	select {
	case waitErr := <-childDone:
		childRunning = false
		var exitErr *exec.ExitError
		if !errors.As(waitErr, &exitErr) || exitErr.ExitCode() != 1 {
			t.Fatalf("runQueue child exit = %v, want lease-loss exit code 1; output:\n%s", waitErr, output.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("runQueue child did not terminate after tracked lease loss; output:\n%s", output.String())
	}
	if !strings.Contains(output.String(), "lost during execution; stopping without outcome publication") {
		t.Fatalf("runQueue did not report the lease-loss outcome: %s", output.String())
	}
	assertProcessGroupGone(t, mediaPID, 3*time.Second)

	status, err = rdb.HGetAll(ctx, statusKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	if status["status"] != farmqueue.StatusQueued || status["claim_token"] != "" || status["ack_token"] != "" {
		t.Fatalf("stale worker published terminal state or ACK after recovery: %+v", status)
	}
	if ready, err := rdb.LLen(ctx, farmqueue.QueueKeyFor(&child)).Result(); err != nil || ready != 1 {
		t.Fatalf("recovered execution obligations = %d err=%v; want exactly one ready child", ready, err)
	}
	if depth, err := q.QueueDepth(ctx); err != nil || depth != 1 {
		t.Fatalf("aggregate recovered queue depth = %d err=%v; want one obligation", depth, err)
	}
	if processing, err := rdb.SMembers(ctx, farmqueue.TrackedProcessingIndexKey).Result(); err != nil || len(processing) != 0 {
		t.Fatalf("stale worker processing receipts = %v err=%v; want none", processing, err)
	}
	jobs, err := q.ListJobs(ctx, 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 2 {
		t.Fatalf("tracked planner/execution outcomes = %d jobs: %+v; want no fallback child", len(jobs), jobs)
	}
	for _, job := range jobs {
		if strings.HasPrefix(job.ID, child.ID+"-") {
			t.Fatalf("stale worker published fallback child: %+v", job)
		}
	}

	successor := farmqueue.Worker{ID: "h1-runqueue-successor", Name: "h1-runqueue-successor",
		Role: farmqueue.QueueClassServer, Capabilities: []string{"cpu", "metadata"}}
	if err := q.Heartbeat(ctx, successor); err != nil {
		t.Fatal(err)
	}
	next, ok, err := q.ClaimForWorker(ctx, time.Second, successor)
	if err != nil || !ok || next.Job.ID != child.ID || next.Token == "" || next.Token == oldToken {
		t.Fatalf("successor claim=%+v ok=%v err=%v; want one same-job obligation with a fresh token", next, ok, err)
	}
	if err := q.MarkClaimRunning(ctx, next, successor); err != nil {
		t.Fatal(err)
	}
	status, err = rdb.HGetAll(ctx, statusKey).Result()
	if err != nil || status["status"] != farmqueue.StatusRunning || status["claim_token"] != next.Token {
		t.Fatalf("successor did not own fresh running token: status=%+v err=%v", status, err)
	}
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func waitForFile(t *testing.T, path string, timeout time.Duration, childDone chan error, output *lockedBuffer) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer deadline.Stop()
	defer ticker.Stop()
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		select {
		case waitErr := <-childDone:
			childDone <- waitErr
			t.Fatalf("runQueue child exited before fake ffmpeg started: %v\n%s", waitErr, output.String())
		case <-deadline.C:
			t.Fatalf("fake ffmpeg did not start within %s\n%s", timeout, output.String())
		case <-ticker.C:
		}
	}
}

func assertProcessGroupGone(t *testing.T, pid int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		err := syscall.Kill(-pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		if err != nil && !errors.Is(err, syscall.EPERM) {
			t.Fatalf("check media process group %d: %v", pid, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("media process group %d still exists after runQueue cancellation", pid)
}
