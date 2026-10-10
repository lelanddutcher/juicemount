package farmqueue

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func openWatchEpochTestQueue(t *testing.T) (*Client, context.Context) {
	t.Helper()
	url := os.Getenv("JM_FARM_TEST_REDIS_URL")
	if url == "" || os.Getenv("JM_FARM_TEST_REDIS_FLUSH") != "1" {
		t.Skip("set JM_FARM_TEST_REDIS_URL to isolated Redis and JM_FARM_TEST_REDIS_FLUSH=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	q, err := Open(url)
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
	return q, ctx
}

func watchPlannerWorker() Worker {
	return Worker{ID: "watch-planner-test", Name: "watch-planner-test", Role: QueueClassServer,
		Capabilities: []string{"cpu", "metadata"}}
}

func trackedTestChild(parent Claim, target string) Job {
	return Job{
		ID: parent.Job.ID + "-shard-0001", ParentID: parent.Job.ID,
		Path: parent.Job.Path, Kinds: append([]string(nil), parent.Job.Kinds...),
		Producer: parent.Job.Producer, EnqueuedAt: parent.Job.EnqueuedAt,
		TrackedEpoch: parent.Job.TrackedEpoch, TrackedClaim: parent.Token,
		PlanOnly: false, ShardIndex: 1, ShardCount: 1, RetryTargets: []string{target},
		QueueClass: QueueClassCPU, RequiredCapabilities: []string{"cpu"},
	}
}

func TestTrackedWatchEpochMultiKindReceiptAndCustody(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	root := t.TempDir()
	old := time.Now().Add(-time.Hour)
	if err := q.StoreWatchCursor(ctx, old); err != nil {
		t.Fatal(err)
	}
	if leader, err := q.AcquireWatchLeadership(ctx, "leader", time.Minute); err != nil || !leader {
		t.Fatalf("watch leader: %v %v", leader, err)
	}
	cutoff := time.Now()
	kinds := []string{KindDerivatives, KindProxy, KindTranscript}
	state, err := q.StartWatchEpoch(ctx, root, "leader", kinds, cutoff)
	if err != nil || state.Active == "" || !state.Cursor.IsZero() || state.Pending != len(kinds) {
		t.Fatalf("bootstrap state = %+v, %v", state, err)
	}
	if cursor, err := q.GetWatchCursor(ctx); err != nil || !cursor.Equal(old) {
		t.Fatalf("old cursor advanced at enqueue: %v, %v", cursor, err)
	}
	if n, err := q.rdb.SCard(ctx, ProcessingIndexKey).Result(); err != nil || n != 0 {
		t.Fatalf("legacy processing index = %d, %v", n, err)
	}
	worker := watchPlannerWorker()
	children := make([]Job, 0, len(kinds))
	for i := range kinds {
		claim, ok, err := q.ClaimForWorker(ctx, time.Second, worker)
		if err != nil || !ok || claim.Token == "" {
			t.Fatalf("tracked parent %d claim = %+v, %v, %v", i, claim, ok, err)
		}
		if n, err := q.rdb.LLen(ctx, TrackedProcessingPrefix+worker.ID).Result(); err != nil || n != 1 {
			t.Fatalf("isolated processing receipt = %d, %v", n, err)
		}
		child := trackedTestChild(claim, filepath.Join(root, "clip.mov"))
		children = append(children, child)
		created, err := q.EnqueuePlannedChildrenForClaim(ctx, claim, []Job{child})
		if err != nil || created != 1 {
			t.Fatalf("publish child %d: created=%d err=%v", i, created, err)
		}
		if created, err := q.EnqueuePlannedChildrenForClaim(ctx, claim, []Job{child}); err != nil || created != 0 {
			t.Fatalf("lost-response replay child %d: created=%d err=%v", i, created, err)
		}
		if committed, err := q.CommitWatchEpoch(ctx, root, "leader", state.Active); err != nil || committed != (i == len(kinds)-1) {
			t.Fatalf("commit after %d/%d kinds = %v, %v", i+1, len(kinds), committed, err)
		}
		if err := q.AckClaim(ctx, claim); err != nil {
			t.Fatal(err)
		}
	}
	childWorker := Worker{ID: "watch-child-test", Name: "watch-child-test", Role: QueueClassServer,
		Capabilities: []string{"cpu"}}
	for _, child := range children {
		ttl, err := q.rdb.TTL(ctx, JobHashPrefix+child.ID).Result()
		if err != nil || ttl != -1 {
			t.Fatalf("pending child TTL = %v, %v; want no expiry beyond seven days", ttl, err)
		}
		if lane := QueueKeyFor(&child); lane != TrackedQueuePrefix+QueueClassCPU+":"+child.Kinds[0] ||
			!slices.Contains(WorkerQueueKeys(childWorker), lane) {
			t.Fatalf("tracked child lane %q is not claimable by server worker", lane)
		}
		childClaim, claimed, err := q.ClaimForWorker(ctx, time.Second, childWorker)
		if err != nil || !claimed || childClaim.Job.ID != child.ID {
			t.Fatalf("claim child: %+v ok=%v err=%v", childClaim, claimed, err)
		}
		if err := q.MarkClaimRunning(ctx, childClaim, childWorker); err != nil {
			t.Fatal(err)
		}
		if err := q.CompleteClaim(ctx, childClaim, StatusDone, 1, 0, ""); err != nil {
			t.Fatal(err)
		}
		if err := q.AckClaim(ctx, childClaim); err != nil {
			t.Fatal(err)
		}
		ttl, err = q.rdb.TTL(ctx, JobHashPrefix+child.ID).Result()
		if err != nil || ttl != -1 {
			t.Fatalf("terminal child TTL = %v, %v; want retained proof until coverage retirement", ttl, err)
		}
	}
	if cursor, err := q.GetWatchCursor(ctx); err != nil || !cursor.Equal(cutoff) {
		t.Fatalf("completed epoch cursor = %v, %v; want %v", cursor, err, cutoff)
	}
	for attempt := 0; attempt < 16; attempt++ {
		if _, err := q.RetireTrackedTerminals(ctx, time.Now().Add(time.Hour), 128); err != nil {
			t.Fatal(err)
		}
	}
	for _, child := range children {
		ttl, err := q.rdb.TTL(ctx, JobHashPrefix+child.ID).Result()
		if err != nil || ttl <= 0 {
			t.Fatalf("drained child TTL = %v, %v; want bounded retirement", ttl, err)
		}
		if exists, err := q.rdb.HExists(ctx, watchEpochKey(root), "receipt:"+child.ParentID).Result(); err != nil || exists {
			t.Fatalf("drained planner receipt for %s remains: %v, %v", child.ParentID, exists, err)
		}
	}
}

func TestTrackedWatchExpiredLiveClaimIsFencedAndDeferred(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	root := t.TempDir()
	worker := watchPlannerWorker()
	if err := q.Heartbeat(ctx, worker); err != nil {
		t.Fatal(err)
	}
	if leader, err := q.AcquireWatchLeadership(ctx, "leader", time.Minute); err != nil || !leader {
		t.Fatalf("watch leader: %v %v", leader, err)
	}
	state, err := q.StartWatchEpoch(ctx, root, "leader", []string{KindTranscript}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	claim, ok, err := q.ClaimForWorker(ctx, time.Second, worker)
	if err != nil || !ok {
		t.Fatalf("claim = %+v, %v, %v", claim, ok, err)
	}
	if err := q.rdb.HSet(ctx, JobHashPrefix+claim.Job.ID, "lease_expires_unix", time.Now().Add(-time.Minute).Unix()).Err(); err != nil {
		t.Fatal(err)
	}
	if recovered, err := q.RecoverAbandonedWorkers(ctx); err != nil || recovered != 1 {
		t.Fatalf("expired live-claim recovery = %d, %v", recovered, err)
	}
	if n, err := q.rdb.SCard(ctx, ProcessingIndexKey).Result(); err != nil || n != 0 {
		t.Fatalf("old reaper index acquired tracked work: %d, %v", n, err)
	}
	child := trackedTestChild(claim, filepath.Join(root, "clip.mov"))
	if _, err := q.EnqueuePlannedChildrenForClaim(ctx, claim, []Job{child}); err == nil {
		t.Fatal("stale worker published plan after its claim was deferred")
	}
	if committed, err := q.CommitWatchEpoch(ctx, root, "leader", state.Active); err != nil || committed {
		t.Fatalf("cursor committed blocked epoch: %v, %v", committed, err)
	}
	if promoted, err := q.PromoteWatchRetries(ctx, time.Now().Add(2*time.Minute), 128); err != nil || promoted != 1 {
		t.Fatalf("scheduled retry promotion = %d, %v", promoted, err)
	}
	newClaim, ok, err := q.ClaimForWorker(ctx, time.Second, worker)
	if err != nil || !ok || newClaim.Token == claim.Token {
		t.Fatalf("new fenced claim = %+v, %v, %v", newClaim, ok, err)
	}
}

func TestTrackedWatchRepairsUnpublishedStoredRoute(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	root := t.TempDir()
	if leader, err := q.AcquireWatchLeadership(ctx, "leader", time.Minute); err != nil || !leader {
		t.Fatalf("watch leader: %v %v", leader, err)
	}
	state, err := q.StartWatchEpoch(ctx, root, "leader", []string{KindProxy}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	claim, ok, err := q.ClaimForWorker(ctx, time.Second, watchPlannerWorker())
	if err != nil || !ok {
		t.Fatalf("claim = %+v, %v, %v", claim, ok, err)
	}
	child := trackedTestChild(claim, filepath.Join(root, "clip.mov"))
	stored := child
	stored.QueueClass = QueueClassRender
	stored.SelectedBackend = "hevc_vaapi"
	stored.RequiredCapabilities = []string{"encoder:hevc_vaapi"}
	stored.TrackedClaim = ""
	raw, err := json.Marshal(stored)
	if err != nil {
		t.Fatal(err)
	}
	statusKey := JobHashPrefix + child.ID
	if err := q.rdb.HSet(ctx, statusKey, "status", StatusQueued, "payload", raw,
		"parent_id", claim.Job.ID, "tracked_epoch", state.Active, "published", "0").Err(); err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.HSet(ctx, coverageLedgerKey(claim.Job.ID),
		"parent_id", claim.Job.ID, "epoch", state.Active, "root", root,
		"coverage_protocol", "1", "state", "open", "open_count", "1",
		"committed", "0", "planner_ack", "0").Err(); err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.ZAdd(ctx, coverageMembersKey(claim.Job.ID), redis.Z{Score: 0, Member: child.ID}).Err(); err != nil {
		t.Fatal(err)
	}
	if created, err := q.EnqueuePlannedChildrenForClaim(ctx, claim, []Job{child}); err != nil || created != 1 {
		t.Fatalf("repair stored render child = %d, %v", created, err)
	}
	if n, err := q.rdb.LLen(ctx, TrackedQueuePrefix+QueueClassRender+":"+KindProxy).Result(); err != nil || n != 1 {
		t.Fatalf("stored render lane = %d, %v", n, err)
	}
	if n, err := q.rdb.LLen(ctx, TrackedQueuePrefix+QueueClassCPU+":"+KindProxy).Result(); err != nil || n != 0 {
		t.Fatalf("rebuilt CPU lane unexpectedly owns stored child: %d, %v", n, err)
	}
}

func TestWatchRetryDelayBounded(t *testing.T) {
	for attempt, want := range []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute, time.Hour, 6 * time.Hour, 6 * time.Hour} {
		if got := WatchRetryDelay(attempt + 1); got != want {
			t.Fatalf("attempt %d delay = %v, want %v", attempt+1, got, want)
		}
	}
}
