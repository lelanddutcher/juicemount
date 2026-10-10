//go:build darwin || linux

package farmqueue

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"
)

// TestRedisMixedVersionWatchQueue runs the archived pre-watch-v2 worker against
// a tracked watch planner in isolated Redis, then checks current-worker receipt
// routing and recovery. The legacy process gets an empty temporary mount and
// watch is disabled, so it cannot scan or modify a host volume.
func TestRedisMixedVersionWatchQueue(t *testing.T) {
	redisURL := os.Getenv("JM_FARM_TEST_REDIS_URL")
	if redisURL == "" || os.Getenv("JM_FARM_TEST_REDIS_FLUSH") != "1" {
		t.Skip("set JM_FARM_TEST_REDIS_URL to isolated Redis and JM_FARM_TEST_REDIS_FLUSH=1")
	}
	legacyBinary := os.Getenv("JM_FARM_LEGACY_WORKER_BINARY")
	legacySHA256 := os.Getenv("JM_FARM_LEGACY_WORKER_SHA256")
	if legacyBinary == "" || legacySHA256 == "" {
		t.Skip("set JM_FARM_LEGACY_WORKER_BINARY and JM_FARM_LEGACY_WORKER_SHA256 to the archived pre-watch-v2 jmfarm artifact")
	}
	if info, err := os.Stat(legacyBinary); err != nil || info.IsDir() || info.Mode()&0111 == 0 {
		t.Fatalf("legacy worker binary %q is not an executable file: %v", legacyBinary, err)
	}
	if err := verifyLegacyWorkerSHA256(legacyBinary, legacySHA256); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	q, err := Open(redisURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })
	if err := q.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	root := filepath.Join(t.TempDir(), "empty-mount")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	if leader, err := q.AcquireWatchLeadership(ctx, "mixed-version-oracle", time.Minute); err != nil || !leader {
		t.Fatalf("acquire watch leadership: leader=%v err=%v", leader, err)
	}
	epoch, err := q.StartWatchEpoch(ctx, root, "mixed-version-oracle", []string{KindProxy}, time.Now())
	if err != nil || epoch.Active == "" {
		t.Fatalf("start tracked watch epoch: state=%+v err=%v", epoch, err)
	}
	trackedID := "watch-v2-" + epoch.Active + "-" + KindProxy
	trackedLane := TrackedQueuePrefix + QueueClassServer + ":" + KindProxy
	if depth, err := q.rdb.LLen(ctx, trackedLane).Result(); err != nil || depth != 1 {
		t.Fatalf("tracked planner lane depth=%d err=%v; want one", depth, err)
	}

	legacyName := "h1-mixed-legacy-" + NewID()
	legacyProcess, err := startLegacyQueueWorker(legacyBinary, redisURL, root, legacyName)
	if err != nil {
		t.Fatalf("start legacy worker: %v", err)
	}
	t.Cleanup(func() { _ = legacyProcess.stop() })
	firstHeartbeat, err := waitForWorkerName(ctx, q, legacyName)
	if err != nil {
		t.Fatalf("legacy worker did not heartbeat: %v", err)
	}
	if firstHeartbeat.Role != QueueClassServer {
		t.Fatalf("legacy worker role=%q; want server", firstHeartbeat.Role)
	}
	if err := waitForWorkerHeartbeatChange(ctx, q, legacyProcess, legacyName, firstHeartbeat.LastSeen); err != nil {
		t.Fatalf("legacy worker did not complete an idle claim cycle: %v", err)
	}
	if status, err := q.rdb.HGet(ctx, JobHashPrefix+trackedID, "status").Result(); err != nil || status != StatusQueued {
		t.Fatalf("legacy worker changed tracked planner status=%q err=%v; want queued", status, err)
	}
	if depth, err := q.rdb.LLen(ctx, trackedLane).Result(); err != nil || depth != 1 {
		t.Fatalf("tracked planner lane after legacy poll depth=%d err=%v; want one", depth, err)
	}
	if depth, err := q.rdb.LLen(ctx, ProcessingPrefix+firstHeartbeat.ID).Result(); err != nil || depth != 0 {
		t.Fatalf("legacy worker legacy receipt depth=%d err=%v; want none", depth, err)
	}
	if depth, err := q.rdb.LLen(ctx, TrackedProcessingPrefix+firstHeartbeat.ID).Result(); err != nil || depth != 0 {
		t.Fatalf("legacy worker tracked receipt depth=%d err=%v; want none", depth, err)
	}
	oldConsumedJob := NewJob(root, []string{KindAll}, "mixed-version-old-consumer")
	if err := q.Enqueue(ctx, oldConsumedJob); err != nil {
		t.Fatal(err)
	}
	if err := waitForLegacyJobOutcome(ctx, q, legacyProcess, oldConsumedJob.ID, legacyName); err != nil {
		t.Fatalf("archived worker did not consume legacy work: %v", err)
	}
	if depth, err := q.rdb.LLen(ctx, trackedLane).Result(); err != nil || depth != 1 {
		t.Fatalf("tracked planner lane after legacy execution depth=%d err=%v; want one", depth, err)
	}
	if err := legacyProcess.stop(); err != nil {
		t.Fatalf("stop idle legacy worker: %v", err)
	}

	first := Worker{ID: "h1-mixed-current-a-" + NewID(), Name: "current-a", Role: QueueClassServer,
		Capabilities: []string{"cpu", "metadata"}}
	if err := q.Heartbeat(ctx, first); err != nil {
		t.Fatal(err)
	}
	trackedClaim, ok, err := q.ClaimForWorker(ctx, time.Second, first)
	if err != nil || !ok || trackedClaim.Job.ID != trackedID || trackedClaim.Token == "" {
		t.Fatalf("current worker tracked claim=%+v ok=%v err=%v", trackedClaim, ok, err)
	}
	if depth, err := q.rdb.LLen(ctx, TrackedProcessingPrefix+first.ID).Result(); err != nil || depth != 1 {
		t.Fatalf("current tracked receipt depth=%d err=%v; want one", depth, err)
	}
	if members, err := q.rdb.SMembers(ctx, TrackedProcessingIndexKey).Result(); err != nil || !slices.Contains(members, first.ID) {
		t.Fatalf("tracked processing index=%v err=%v; want %s", members, err, first.ID)
	}
	if depth, err := q.rdb.LLen(ctx, ProcessingPrefix+first.ID).Result(); err != nil || depth != 0 {
		t.Fatalf("tracked claim leaked into legacy receipt depth=%d err=%v", depth, err)
	}

	if err := q.rdb.HSet(ctx, JobHashPrefix+trackedID, "lease_expires_unix", time.Now().Add(-time.Second).Unix()).Err(); err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.Del(ctx, WorkerPrefix+first.ID).Err(); err != nil {
		t.Fatal(err)
	}
	if recovered, err := q.RecoverAbandonedWorkers(ctx); err != nil || recovered != 1 {
		t.Fatalf("tracked worker-loss recovery=%d err=%v; want one", recovered, err)
	}
	if depth, err := q.rdb.LLen(ctx, TrackedProcessingPrefix+first.ID).Result(); err != nil || depth != 0 {
		t.Fatalf("tracked receipt after recovery depth=%d err=%v; want none", depth, err)
	}
	if exists, err := q.rdb.SIsMember(ctx, TrackedProcessingIndexKey, first.ID).Result(); err != nil || exists {
		t.Fatalf("tracked processing index retained lost worker=%v err=%v", exists, err)
	}
	if promoted, err := q.PromoteWatchRetries(ctx, time.Now().Add(2*time.Hour), 128); err != nil || promoted != 1 {
		t.Fatalf("promote recovered watch planner=%d err=%v; want one", promoted, err)
	}

	restarted := Worker{ID: "h1-mixed-current-b-" + NewID(), Name: "current-b", Role: QueueClassServer,
		Capabilities: []string{"cpu", "metadata"}}
	if err := q.Heartbeat(ctx, restarted); err != nil {
		t.Fatal(err)
	}
	restartedClaim, ok, err := q.ClaimForWorker(ctx, time.Second, restarted)
	if err != nil || !ok || restartedClaim.Job.ID != trackedID || restartedClaim.Generation <= trackedClaim.Generation {
		t.Fatalf("restarted current worker claim=%+v ok=%v err=%v", restartedClaim, ok, err)
	}
	if depth, err := q.rdb.LLen(ctx, TrackedProcessingPrefix+restarted.ID).Result(); err != nil || depth != 1 {
		t.Fatalf("restarted tracked receipt depth=%d err=%v; want one", depth, err)
	}
	if err := q.DeferTrackedClaim(ctx, restartedClaim, "mixed-version oracle cleanup", time.Now()); err != nil {
		t.Fatalf("retire restarted planner receipt: %v", err)
	}

	legacyJob := NewJob(root, []string{KindAll}, "mixed-version-oracle")
	if err := q.Enqueue(ctx, legacyJob); err != nil {
		t.Fatal(err)
	}
	legacyWorker := Worker{ID: "h1-mixed-legacy-claim-" + NewID(), Name: "legacy-claim", Role: QueueClassServer,
		Capabilities: []string{"cpu", "metadata"}}
	if err := q.Heartbeat(ctx, legacyWorker); err != nil {
		t.Fatal(err)
	}
	legacyClaim, ok, err := q.ClaimForWorker(ctx, time.Second, legacyWorker)
	if err != nil || !ok || legacyClaim.Job.ID != legacyJob.ID || legacyClaim.Job.TrackedEpoch != "" {
		t.Fatalf("current worker legacy claim=%+v ok=%v err=%v", legacyClaim, ok, err)
	}
	if depth, err := q.rdb.LLen(ctx, ProcessingPrefix+legacyWorker.ID).Result(); err != nil || depth != 1 {
		t.Fatalf("legacy processing receipt depth=%d err=%v; want one", depth, err)
	}
	if depth, err := q.rdb.LLen(ctx, TrackedProcessingPrefix+legacyWorker.ID).Result(); err != nil || depth != 0 {
		t.Fatalf("legacy claim leaked into tracked receipt depth=%d err=%v", depth, err)
	}
	if members, err := q.rdb.SMembers(ctx, ProcessingIndexKey).Result(); err != nil || !slices.Contains(members, legacyWorker.ID) {
		t.Fatalf("legacy processing index=%v err=%v; want %s", members, err, legacyWorker.ID)
	}

	if err := q.rdb.HSet(ctx, JobHashPrefix+legacyJob.ID, "lease_expires_unix", time.Now().Add(-time.Second).Unix()).Err(); err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.Del(ctx, WorkerPrefix+legacyWorker.ID).Err(); err != nil {
		t.Fatal(err)
	}
	if recovered, err := q.RecoverAbandonedWorkers(ctx); err != nil || recovered != 1 {
		t.Fatalf("legacy worker-loss recovery=%d err=%v; want one", recovered, err)
	}
	replacement := Worker{ID: "h1-mixed-legacy-restart-" + NewID(), Name: "legacy-restart", Role: QueueClassServer,
		Capabilities: []string{"cpu", "metadata"}}
	if err := q.Heartbeat(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	recoveredLegacy, ok, err := q.ClaimForWorker(ctx, time.Second, replacement)
	if err != nil || !ok || recoveredLegacy.Job.ID != legacyJob.ID || recoveredLegacy.Job.TrackedEpoch != "" {
		t.Fatalf("replacement legacy claim=%+v ok=%v err=%v", recoveredLegacy, ok, err)
	}
	if depth, err := q.rdb.LLen(ctx, ProcessingPrefix+replacement.ID).Result(); err != nil || depth != 1 {
		t.Fatalf("replacement legacy receipt depth=%d err=%v; want one", depth, err)
	}
	if err := q.MarkDone(ctx, legacyJob.ID, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := q.AckClaim(ctx, recoveredLegacy); err != nil {
		t.Fatal(err)
	}
}

func verifyLegacyWorkerSHA256(path, expected string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	if got := fmt.Sprintf("%x", h.Sum(nil)); got != expected {
		return fmt.Errorf("legacy worker SHA-256 = %s, want %s", got, expected)
	}
	return nil
}

type legacyQueueWorkerProcess struct {
	cmd  *exec.Cmd
	done chan struct{}
	err  error
}

func startLegacyQueueWorker(binary, redisURL, mount, name string) (*legacyQueueWorkerProcess, error) {
	dbPath := filepath.Join(mount, "legacy-worker.sqlite")
	cmd := exec.Command(binary, "-queue", "-meta", redisURL, "-db", dbPath, "-mount", mount,
		"-role", QueueClassServer, "-name", name, "-kinds", KindAll,
		"-min-size-mb", "0", "-concurrency", "1", "-ffmpeg-threads", "1")
	cmd.Env = envWith("JM_FARM_WATCH", "0")
	p := &legacyQueueWorkerProcess{cmd: cmd, done: make(chan struct{})}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	go func() {
		p.err = cmd.Wait()
		close(p.done)
	}()
	return p, nil
}

func (p *legacyQueueWorkerProcess) stop() error {
	select {
	case <-p.done:
		return p.err
	default:
	}
	if p.cmd.Process != nil {
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
	}
	select {
	case <-p.done:
		return p.err
	case <-time.After(3 * time.Second):
	}
	if p.cmd.Process != nil {
		_ = p.cmd.Process.Kill()
	}
	select {
	case <-p.done:
		return p.err
	case <-time.After(3 * time.Second):
		return fmt.Errorf("legacy worker did not exit after SIGTERM and kill")
	}
}

func waitForWorkerName(ctx context.Context, q *Client, name string) (Worker, error) {
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		ids, err := q.rdb.SMembers(ctx, WorkerIndexKey).Result()
		if err != nil {
			return Worker{}, err
		}
		for _, id := range ids {
			raw, err := q.rdb.Get(ctx, WorkerPrefix+id).Result()
			if err != nil {
				continue
			}
			var worker Worker
			if json.Unmarshal([]byte(raw), &worker) == nil && worker.Name == name {
				return worker, nil
			}
		}
		select {
		case <-ctx.Done():
			return Worker{}, ctx.Err()
		case <-tick.C:
		}
	}
}

func waitForWorkerHeartbeatChange(ctx context.Context, q *Client, p *legacyQueueWorkerProcess, name, previous string) error {
	timer := time.NewTimer(9 * time.Second)
	defer timer.Stop()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-p.done:
			return fmt.Errorf("legacy worker exited during claim cycle: %v", p.err)
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return fmt.Errorf("legacy worker heartbeat did not advance")
		case <-tick.C:
			worker, err := waitForWorkerName(ctx, q, name)
			if err != nil {
				continue
			}
			if worker.LastSeen != previous {
				return nil
			}
		}
	}
}

func waitForLegacyJobOutcome(ctx context.Context, q *Client, p *legacyQueueWorkerProcess, jobID, workerName string) error {
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		status, err := q.rdb.HGetAll(ctx, JobHashPrefix+jobID).Result()
		if err != nil {
			return err
		}
		if status["worker"] == workerName && status["status"] == StatusDone {
			return nil
		}
		select {
		case <-p.done:
			return fmt.Errorf("archived worker exited before terminal legacy outcome: %v (status=%v)", p.err, status)
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return fmt.Errorf("timed out waiting for archived worker outcome: status=%v", status)
		case <-tick.C:
		}
	}
}

func envWith(key, value string) []string {
	env := os.Environ()
	out := make([]string, 0, len(env)+1)
	for _, item := range env {
		if len(item) > len(key) && item[:len(key)] == key && item[len(key)] == '=' {
			continue
		}
		out = append(out, item)
	}
	return append(out, key+"="+value)
}
