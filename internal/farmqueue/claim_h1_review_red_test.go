package farmqueue

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func h1ReviewTrackedChild(t *testing.T, q *Client, ctx context.Context, worker Worker) (Claim, Job) {
	t.Helper()
	if err := q.Heartbeat(ctx, worker); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if leader, err := q.AcquireWatchLeadership(ctx, "leader", time.Minute); err != nil || !leader {
		t.Fatalf("watch leader: %v %v", leader, err)
	}
	if _, err := q.StartWatchEpoch(ctx, root, "leader", []string{KindProxy}, time.Now()); err != nil {
		t.Fatal(err)
	}
	parent, ok, err := q.ClaimForWorker(ctx, 0, worker)
	if err != nil || !ok || parent.Token == "" {
		t.Fatalf("claim tracked planner: ok=%v token=%q err=%v", ok, parent.Token, err)
	}
	child := trackedTestChild(parent, root+"/clip.mov")
	if created, err := q.EnqueuePlannedChildrenForClaim(ctx, parent, []Job{child}); err != nil || created != 1 {
		t.Fatalf("enqueue tracked child: created=%d err=%v", created, err)
	}
	return parent, child
}

func h1AssertPlannerAcked(t *testing.T, q *Client, ctx context.Context, parent Claim) {
	t.Helper()
	fields, err := q.rdb.HGetAll(ctx, JobHashPrefix+parent.Job.ID).Result()
	if err != nil || fields["status"] != StatusDispatched || fields["ack_token"] != parent.Token ||
		fields["claim_token"] != "" {
		t.Fatalf("recovered planner ACK=%+v err=%v", fields, err)
	}
	receipts, err := q.rdb.LRange(ctx, claimProcessingKey(parent), 0, -1).Result()
	if err != nil || slices.Contains(receipts, parent.Raw) {
		t.Fatalf("recovered planner receipt still processing=%q err=%v", receipts, err)
	}
}

func h1ReviewInjectClaimPostMoveFailure(t *testing.T, q *Client, ctx context.Context, worker Worker) {
	t.Helper()
	keys := append([]string{ControlKey, StoragePermitKey, ProcessingIndexKey,
		ProcessingPrefix + worker.ID, TrackedProcessingIndexKey,
		TrackedProcessingPrefix + worker.ID, LeaseIndexKey}, WorkerQueueKeys(worker)...)
	capabilities, _ := json.Marshal(workerCapabilitySet(worker))
	decodeLimits, _ := json.Marshal(worker.DecodeLimits)
	if string(decodeLimits) == "null" {
		decodeLimits = []byte("{}")
	}
	token := NewID()
	expires := time.Now().Add(claimLeaseTTL)
	faultScript := strings.Replace(claimForWorkerScript,
		"local moved = redis.call('LMOVE', KEYS[i], processing, 'RIGHT', 'LEFT')",
		"local moved = redis.call('LMOVE', KEYS[i], processing, 'RIGHT', 'LEFT')\nredis.call('HSET', processing, 'injected-fault', '1')", 1)
	if faultScript == claimForWorkerScript {
		t.Fatal("failed to install post-LMOVE claim fault")
	}
	if _, err := q.rdb.Eval(ctx, faultScript, keys, worker.ID,
		string(capabilities), string(decodeLimits), workerPinCapability(worker),
		workerDisplayName(worker), "0", token,
		expires.UTC().Format(time.RFC3339), expires.Unix(), worker.Role).Result(); err == nil {
		t.Fatal("injected post-LMOVE Redis failure unexpectedly succeeded")
	}
}

func TestH1TrackedClaimWrongTypePrevalidationDoesNotDuplicateReadyRaw(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	worker := watchPlannerWorker()
	_, child := h1ReviewTrackedChild(t, q, ctx, worker)
	queueKey := QueueKeyFor(&child)
	statusKey := JobHashPrefix + child.ID
	processingKey := TrackedProcessingPrefix + worker.ID
	if err := q.rdb.Del(ctx, TrackedProcessingIndexKey).Err(); err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.Set(ctx, TrackedProcessingIndexKey, "wrong type", 0).Err(); err != nil {
		t.Fatal(err)
	}
	wantReady, err := q.rdb.LRange(ctx, queueKey, 0, -1).Result()
	if err != nil || len(wantReady) != 1 {
		t.Fatalf("initial ready raws = %v, %v; want one", wantReady, err)
	}
	for attempt := 1; attempt <= 3; attempt++ {
		if _, ok, err := q.ClaimForWorker(ctx, 0, worker); err == nil || ok {
			t.Fatalf("claim attempt %d with wrong-type processing index = ok %v, err %v; want an error and no claim", attempt, ok, err)
		}
		ready, err := q.rdb.LRange(ctx, queueKey, 0, -1).Result()
		if err != nil || len(ready) != len(wantReady) || ready[0] != wantReady[0] {
			t.Fatalf("wrong-type prevalidation attempt %d changed ready raws: got %q err=%v want exact snapshot %q", attempt, ready, err, wantReady)
		}
	}

	ready, err := q.rdb.LRange(ctx, queueKey, 0, -1).Result()
	if err != nil || len(ready) != len(wantReady) || ready[0] != wantReady[0] {
		t.Fatalf("wrong-type prevalidation left ready raws %q, err %v; want exact original snapshot %q", ready, err, wantReady)
	}
	var got Job
	if err := json.Unmarshal([]byte(ready[0]), &got); err != nil || got.ID != child.ID {
		t.Fatalf("remaining ready raw = %+v, decode error %v; want child %q", got, err, child.ID)
	}
	status, err := q.rdb.HGetAll(ctx, statusKey).Result()
	if err != nil || status["status"] != StatusQueued || status["claim_phase"] != "" ||
		status["claim_token"] != "" || status["lease_owner"] != "" || status["lease_expires_unix"] != "" {
		t.Fatalf("wrong-type prevalidation status = %+v, %v; want unclaimed queued child", status, err)
	}
	processing, err := q.rdb.LRange(ctx, processingKey, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range processing {
		var got Job
		if json.Unmarshal([]byte(raw), &got) == nil && got.ID == child.ID {
			t.Fatalf("wrong-type prevalidation also moved child into processing: %v", processing)
		}
	}
	if score, err := q.rdb.ZScore(ctx, LeaseIndexKey, child.ID).Result(); !errors.Is(err, redis.Nil) || score != 0 {
		t.Fatalf("wrong-type prevalidation lease index score = %v, err %v; want absent", score, err)
	}
	if typ, err := q.rdb.Type(ctx, TrackedProcessingIndexKey).Result(); err != nil || typ != "string" {
		t.Fatalf("injected processing index type = %q, %v; want string", typ, err)
	}
}

func TestH1TrackedReclaimInterruptedOnSecondWorkerRemainsDiscoverable(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	workerA := watchPlannerWorker()
	parent, child := h1ReviewTrackedChild(t, q, ctx, workerA)
	workerB := watchPlannerWorker()
	workerB.ID = "watch-planner-replacement"
	workerB.Name = workerB.ID
	if err := q.Heartbeat(ctx, workerB); err != nil {
		t.Fatal(err)
	}
	queueKey := QueueKeyFor(&child)
	statusKey := JobHashPrefix + child.ID
	first, ok, err := q.ClaimForWorker(ctx, 0, workerA)
	if err != nil || !ok || first.Job.ID != child.ID {
		t.Fatalf("first worker child claim = %+v, %v, %v", first, ok, err)
	}
	if err := q.rdb.HSet(ctx, statusKey, "lease_expires_unix", time.Now().Add(-time.Minute).Unix()).Err(); err != nil {
		t.Fatal(err)
	}
	if recovered, err := q.RecoverAbandonedWorkers(ctx); err != nil || recovered != 2 {
		t.Fatalf("first worker planner ACK and expired reservation recovery = %d, %v; want two", recovered, err)
	}
	h1AssertPlannerAcked(t, q, ctx, parent)
	h1ReviewInjectClaimPostMoveFailure(t, q, ctx, workerB)
	processingB := TrackedProcessingPrefix + workerB.ID
	receipts, err := q.rdb.LRange(ctx, processingB, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	childReceipts := 0
	for _, raw := range receipts {
		var got Job
		if json.Unmarshal([]byte(raw), &got) == nil && got.ID == child.ID {
			childReceipts++
		}
	}
	if childReceipts != 1 {
		t.Fatalf("replacement worker processing has %d child receipts immediately after interrupted move: %v; want exactly one", childReceipts, receipts)
	}

	if _, err := q.RecoverAbandonedWorkers(ctx); err != nil {
		t.Fatalf("replacement worker interrupted-reclaim recovery: %v", err)
	}
	claim, ok, err := q.ClaimForWorker(ctx, 0, workerB)
	if err != nil || !ok || claim.Job.ID != child.ID || claim.Token == "" {
		t.Fatalf("replacement worker could not reclaim child after interrupted transfer: claim=%+v ok=%v err=%v", claim, ok, err)
	}

	ready, err := q.rdb.LRange(ctx, queueKey, 0, -1).Result()
	if err != nil || len(ready) != 0 {
		t.Fatalf("reclaimed child ready raws = %v, %v; want none", ready, err)
	}
	status, err := q.rdb.HGetAll(ctx, statusKey).Result()
	if err != nil || status["status"] != StatusQueued || status["claim_phase"] != "reserved" ||
		status["lease_owner"] != workerB.ID || status["claim_token"] != claim.Token {
		t.Fatalf("replacement claim status = %+v, %v", status, err)
	}
	if err := q.MarkClaimRunning(ctx, first, workerA); err == nil {
		t.Fatal("stale first-worker claim started after replacement worker reclaimed the child")
	}
	processing, err := q.rdb.LRange(ctx, processingB, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, raw := range processing {
		var got Job
		if json.Unmarshal([]byte(raw), &got) == nil && got.ID == child.ID {
			found = true
		}
	}
	if !found {
		t.Fatalf("replacement worker processing has no child receipt: %v", processing)
	}
	if score, err := q.rdb.ZScore(ctx, LeaseIndexKey, child.ID).Result(); err != nil || score <= float64(time.Now().Unix()) {
		t.Fatalf("replacement claim lease score = %v, %v; want future lease", score, err)
	}
	if members, err := q.rdb.SMembers(ctx, TrackedProcessingIndexKey).Result(); err != nil || !containsString(members, workerB.ID) {
		t.Fatalf("tracked processing index = %v, %v; want replacement worker", members, err)
	}
}

func TestH1UnreservedPlannerRecoversAfterHeartbeatExpiry(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	worker := watchPlannerWorker()
	if err := q.Heartbeat(ctx, worker); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if leader, err := q.AcquireWatchLeadership(ctx, "leader", time.Minute); err != nil || !leader {
		t.Fatalf("watch leader: %v %v", leader, err)
	}
	if _, err := q.StartWatchEpoch(ctx, root, "leader", []string{KindProxy}, time.Now()); err != nil {
		t.Fatal(err)
	}
	processingKey := TrackedProcessingPrefix + worker.ID
	h1ReviewInjectClaimPostMoveFailure(t, q, ctx, worker)
	processing, err := q.rdb.LRange(ctx, processingKey, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	var planner Job
	for _, raw := range processing {
		var candidate Job
		if json.Unmarshal([]byte(raw), &candidate) == nil && candidate.ParentID == "" && candidate.TrackedEpoch != "" {
			planner = candidate
			break
		}
	}
	if planner.ID == "" {
		t.Fatalf("injected post-LMOVE processing has no tracked planner: %v", processing)
	}
	queueKey := QueueKeyFor(&planner)
	statusKey := JobHashPrefix + planner.ID
	if err := q.rdb.Del(ctx, WorkerPrefix+worker.ID).Err(); err != nil {
		t.Fatal(err)
	}

	if recovered, err := q.RecoverAbandonedWorkers(ctx); err != nil || recovered != 1 {
		t.Fatalf("expired-heartbeat unreserved planner recovery = %d, %v; want one recoverable planner", recovered, err)
	}
	ready, err := q.rdb.LRange(ctx, queueKey, 0, -1).Result()
	if err != nil || len(ready) != 1 {
		t.Fatalf("expired-heartbeat planner ready raws = %v, %v; want exactly one", ready, err)
	}
	status, err := q.rdb.HGetAll(ctx, statusKey).Result()
	if err != nil || status["status"] != StatusQueued ||
		(status["claim_phase"] != "" && status["claim_phase"] != "reclaiming") ||
		status["claim_token"] != "" || status["lease_expires_unix"] != "" || status["lease_expires_at"] != "" {
		t.Fatalf("expired-heartbeat planner status = %+v, %v; want queued and unreserved", status, err)
	}
	processing, err = q.rdb.LRange(ctx, processingKey, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range processing {
		var got Job
		if json.Unmarshal([]byte(raw), &got) == nil && got.ID == planner.ID {
			t.Fatalf("expired-heartbeat planner remains in processing: %v", processing)
		}
	}
	if score, err := q.rdb.ZScore(ctx, LeaseIndexKey, planner.ID).Result(); !errors.Is(err, redis.Nil) || score != 0 {
		t.Fatalf("expired-heartbeat planner lease index score = %v, err %v; want absent", score, err)
	}
	if members, err := q.rdb.SMembers(ctx, TrackedProcessingIndexKey).Result(); err != nil || containsString(members, worker.ID) {
		t.Fatalf("tracked processing index after planner recovery = %v, %v; want expired worker removed", members, err)
	}
	if err := q.Heartbeat(ctx, worker); err != nil {
		t.Fatal(err)
	}
	claim, ok, err := q.ClaimForWorker(ctx, 0, worker)
	if err != nil || !ok || claim.Job.ID != planner.ID || claim.Token == "" {
		t.Fatalf("planner was not deliverable after heartbeat recovery: claim=%+v ok=%v err=%v", claim, ok, err)
	}
}

func TestH1SameWorkerSuccessiveGenerationsRejectOldStartAndRenew(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	worker := watchPlannerWorker()
	parent, child := h1ReviewTrackedChild(t, q, ctx, worker)
	first, ok, err := q.ClaimForWorker(ctx, 0, worker)
	if err != nil || !ok || first.Job.ID != child.ID {
		t.Fatalf("first claim: %+v %v %v", first, ok, err)
	}
	statusKey := JobHashPrefix + child.ID
	if err := q.rdb.HSet(ctx, statusKey, "lease_expires_unix", time.Now().Add(-time.Minute).Unix()).Err(); err != nil {
		t.Fatal(err)
	}
	if n, err := q.RecoverAbandonedWorkers(ctx); err != nil || n != 2 {
		t.Fatalf("first recovery: %d %v", n, err)
	}
	h1AssertPlannerAcked(t, q, ctx, parent)
	second, ok, err := q.ClaimForWorker(ctx, 0, worker)
	if err != nil || !ok || second.Job.ID != child.ID || second.Generation <= first.Generation {
		t.Fatalf("second claim: %+v %v %v; first generation %d", second, ok, err, first.Generation)
	}
	if err := q.MarkClaimRunning(ctx, first, worker); err == nil {
		t.Fatal("old generation started")
	}
	if err := q.RenewClaim(ctx, first); err == nil {
		t.Fatal("old generation renewed")
	}
	if err := q.MarkClaimRunning(ctx, second, worker); err != nil {
		t.Fatalf("new generation start: %v", err)
	}
}

func TestH1TransferBeforeMoveRecoversIndexedReadyRaw(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	worker := watchPlannerWorker()
	parent, child := h1ReviewTrackedChild(t, q, ctx, worker)
	keys := append([]string{ControlKey, StoragePermitKey, ProcessingIndexKey,
		ProcessingPrefix + worker.ID, TrackedProcessingIndexKey,
		TrackedProcessingPrefix + worker.ID, LeaseIndexKey}, WorkerQueueKeys(worker)...)
	capabilities, _ := json.Marshal(workerCapabilitySet(worker))
	decodeLimits, _ := json.Marshal(worker.DecodeLimits)
	if string(decodeLimits) == "null" {
		decodeLimits = []byte("{}")
	}
	expires := time.Now().Add(claimLeaseTTL)
	faultScript := strings.Replace(claimForWorkerScript,
		"local moved = redis.call('LMOVE', KEYS[i], processing, 'RIGHT', 'LEFT')",
		"redis.call('HSET', processing, 'injected-fault', '1')\nlocal moved = redis.call('LMOVE', KEYS[i], processing, 'RIGHT', 'LEFT')", 1)
	if faultScript == claimForWorkerScript {
		t.Fatal("failed to install pre-LMOVE fault")
	}
	if _, err := q.rdb.Eval(ctx, faultScript, keys, worker.ID,
		string(capabilities), string(decodeLimits), workerPinCapability(worker),
		workerDisplayName(worker), "0", NewID(),
		expires.UTC().Format(time.RFC3339), expires.Unix(), worker.Role).Result(); err == nil {
		t.Fatal("pre-LMOVE fault unexpectedly succeeded")
	}
	statusKey := JobHashPrefix + child.ID
	if phase, err := q.rdb.HGet(ctx, statusKey, "claim_phase").Result(); err != nil || phase != "transfer" {
		t.Fatalf("pre-LMOVE phase %q %v", phase, err)
	}
	if n, err := q.RecoverAbandonedWorkers(ctx); err != nil || n != 2 {
		t.Fatalf("transfer recovery: %d %v", n, err)
	}
	h1AssertPlannerAcked(t, q, ctx, parent)
	ready, err := q.rdb.LRange(ctx, QueueKeyFor(&child), 0, -1).Result()
	if err != nil || len(ready) != 1 {
		t.Fatalf("ready raws %v %v", ready, err)
	}
	claim, ok, err := q.ClaimForWorker(ctx, 0, worker)
	if err != nil || !ok || claim.Job.ID != child.ID || claim.Generation < 2 {
		t.Fatalf("successor claim %+v %v %v", claim, ok, err)
	}
}
