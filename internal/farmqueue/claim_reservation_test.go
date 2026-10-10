package farmqueue

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestRedisTrackedExecutionClaimLostReplyReclaimsOnlyReservation(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	worker := watchPlannerWorker()
	if err := q.Heartbeat(ctx, worker); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if leader, err := q.AcquireWatchLeadership(ctx, "leader", time.Minute); err != nil || !leader {
		t.Fatalf("watch leader: %v %v", leader, err)
	}
	_, err := q.StartWatchEpoch(ctx, root, "leader", []string{KindProxy}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	parent, ok, err := q.ClaimForWorker(ctx, time.Second, worker)
	if err != nil || !ok || parent.Token == "" {
		t.Fatalf("claim tracked parent: ok=%v token=%q err=%v", ok, parent.Token, err)
	}
	child := trackedTestChild(parent, root+"/clip.mov")
	if created, err := q.EnqueuePlannedChildrenForClaim(ctx, parent, []Job{child}); err != nil || created != 1 {
		t.Fatalf("enqueue tracked child: created=%d err=%v", created, err)
	}

	keys := append([]string{ControlKey, StoragePermitKey, ProcessingIndexKey,
		ProcessingPrefix + worker.ID, TrackedProcessingIndexKey,
		TrackedProcessingPrefix + worker.ID, LeaseIndexKey}, WorkerQueueKeys(worker)...)
	capabilities, _ := json.Marshal(workerCapabilitySet(worker))
	decodeLimits, _ := json.Marshal(worker.DecodeLimits)
	if string(decodeLimits) == "null" {
		decodeLimits = []byte("{}")
	}
	claimedToken := NewID()
	expires := time.Now().Add(claimLeaseTTL)
	// The invalid score raises a real Redis runtime error after the claim has
	// moved into processing and its reservation fields have been written.
	suppressedReplyScript := strings.Replace(claimForWorkerScript,
		"redis.call('ZADD', KEYS[7], leaseExpires, job.id)",
		"redis.call('ZADD', KEYS[7], leaseExpires, job.id)\nredis.call('ZADD', KEYS[7], 'not-a-score', job.id)", 1)
	if suppressedReplyScript == claimForWorkerScript {
		t.Fatal("failed to install lost-reply test fault")
	}
	if _, err := q.rdb.Eval(ctx, suppressedReplyScript, keys, worker.ID,
		string(capabilities), string(decodeLimits), workerPinCapability(worker),
		workerDisplayName(worker), "0", claimedToken,
		expires.UTC().Format(time.RFC3339), expires.Unix()).Result(); err == nil {
		t.Fatal("injected claim reply suppression unexpectedly succeeded")
	}

	statusKey := JobHashPrefix + child.ID
	status, err := q.rdb.HGetAll(ctx, statusKey).Result()
	if err != nil || status["claim_token"] != claimedToken || status["claim_phase"] != "reserved" ||
		status["lease_owner"] != worker.ID {
		t.Fatalf("lost-reply reservation = %+v, %v", status, err)
	}
	processingKey := TrackedProcessingPrefix + worker.ID
	processing, err := q.rdb.LRange(ctx, processingKey, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	var childRaw string
	for _, raw := range processing {
		var got Job
		if json.Unmarshal([]byte(raw), &got) == nil && got.ID == child.ID {
			childRaw = raw
		}
	}
	if childRaw == "" {
		t.Fatalf("lost-reply child missing from processing receipt: %v", processing)
	}
	if err := q.rdb.HSet(ctx, statusKey, "lease_expires_unix", time.Now().Add(-time.Minute).Unix()).Err(); err != nil {
		t.Fatal(err)
	}
	queueKey := QueueKeyFor(&child)
	if err := q.rdb.Set(ctx, queueKey, "wrong type", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := q.RecoverAbandonedWorkers(ctx); err == nil {
		t.Fatal("reaper accepted wrong-type destination lane")
	}
	if n, err := q.rdb.LLen(ctx, processingKey).Result(); err != nil || n != 2 {
		t.Fatalf("failed prevalidation changed processing custody: len=%d err=%v", n, err)
	}
	if err := q.rdb.Del(ctx, queueKey).Err(); err != nil {
		t.Fatal(err)
	}
	// Fail after the destination copy and reclaiming marker are committed, but
	// before the final source LREM. The old receipt must remain authoritative.
	faultScript := strings.Replace(reclaimTrackedReservationScript,
		"if redis.call('LREM', KEYS[1], 1, ARGV[3]) ~= 1 then return 0 end",
		"redis.call('ZADD', KEYS[4], 'not-a-score', ARGV[5])\nif redis.call('LREM', KEYS[1], 1, ARGV[3]) ~= 1 then return 0 end", 1)
	if faultScript == reclaimTrackedReservationScript {
		t.Fatal("failed to install pre-final-LREM reaper fault")
	}
	if _, err := q.rdb.Eval(ctx, faultScript, []string{
		processingKey, queueKey, statusKey, LeaseIndexKey, TrackedProcessingIndexKey,
	}, claimedToken, worker.ID, childRaw, time.Now().Unix(), child.ID, status["claim_generation"]).Result(); err == nil {
		t.Fatal("injected reaper failure unexpectedly succeeded")
	}
	status, err = q.rdb.HGetAll(ctx, statusKey).Result()
	if err != nil || status["claim_phase"] != "reclaiming" {
		t.Fatalf("failed reaper handoff phase = %+v, %v", status, err)
	}
	if n, err := q.rdb.LLen(ctx, queueKey).Result(); err != nil || n != 1 {
		t.Fatalf("interrupted handoff ready depth=%d err=%v; want 1", n, err)
	}
	if n, err := q.rdb.LLen(ctx, processingKey).Result(); err != nil || n != 2 {
		t.Fatalf("interrupted handoff processing depth=%d err=%v; want 2", n, err)
	}
	if raced, claimed, err := q.ClaimForWorker(ctx, 0, worker); err != nil || claimed {
		t.Fatalf("claim racing old source custody got %+v claimed=%v err=%v", raced, claimed, err)
	}
	if n, err := q.rdb.LLen(ctx, queueKey).Result(); err != nil || n != 1 {
		t.Fatalf("racing claim changed ready depth=%d err=%v; want 1", n, err)
	}
	if n, err := q.RecoverAbandonedWorkers(ctx); err != nil || n != 2 {
		t.Fatalf("planner ACK and expired reservation recovery = %d, %v; want 2", n, err)
	}
	h1AssertPlannerAcked(t, q, ctx, parent)
	if n, err := q.rdb.LLen(ctx, queueKey).Result(); err != nil || n != 1 {
		t.Fatalf("recovered child ready depth=%d err=%v; want 1", n, err)
	}
	if n, err := q.rdb.LLen(ctx, processingKey).Result(); err != nil || n != 0 {
		t.Fatalf("processing depth after planner ACK and reservation recovery=%d err=%v; want none", n, err)
	}
	status, err = q.rdb.HGetAll(ctx, statusKey).Result()
	if err != nil || status["claim_phase"] != "" || status["claim_token"] != "" || status["status"] != StatusQueued {
		t.Fatalf("reclaimed reservation status = %+v, %v", status, err)
	}
	staleClaim := Claim{Job: child, Raw: childRaw, QueueKey: QueueKeyFor(&child), WorkerID: worker.ID, Token: claimedToken}
	if err := q.MarkClaimRunning(ctx, staleClaim, worker); err == nil {
		t.Fatal("stale owner started execution after reservation reclaim")
	}

	newClaim, ok, err := q.ClaimForWorker(ctx, time.Second, worker)
	if err != nil || !ok || newClaim.Job.ID != child.ID || newClaim.Token == "" || newClaim.Token == claimedToken {
		t.Fatalf("reclaimed child claim = %+v, %v, %v", newClaim, ok, err)
	}
	if n, err := q.rdb.LRem(ctx, processingKey, 1, newClaim.Raw).Result(); err != nil || n != 1 {
		t.Fatalf("remove raw to test start custody: removed=%d err=%v", n, err)
	}
	if err := q.MarkClaimRunning(ctx, newClaim, worker); err == nil {
		t.Fatal("tracked child started without its exact processing-list raw")
	}
	if err := q.rdb.LPush(ctx, processingKey, newClaim.Raw).Err(); err != nil {
		t.Fatal(err)
	}
	if err := q.MarkClaimRunning(ctx, newClaim, worker); err != nil {
		t.Fatalf("start reclaimed child: %v", err)
	}
	if err := q.rdb.HSet(ctx, statusKey, "lease_expires_unix", time.Now().Add(-time.Minute).Unix()).Err(); err != nil {
		t.Fatal(err)
	}
	if n, err := q.RecoverAbandonedWorkers(ctx); err != nil || n != 1 {
		t.Fatalf("expired running child was not reclaimed: recovered=%d err=%v", n, err)
	}
	if n, err := q.rdb.LLen(ctx, queueKey).Result(); err != nil || n != 1 {
		t.Fatalf("expired child ready depth=%d err=%v", n, err)
	}
	status, err = q.rdb.HGetAll(ctx, statusKey).Result()
	if err != nil || status["claim_phase"] != "" || status["claim_token"] != "" || status["status"] != StatusQueued {
		t.Fatalf("expired claim retained execution authority: %+v, %v", status, err)
	}
}

func TestRedisTrackedExecutionClaimLMOVEFailureLeavesRecoverableObligation(t *testing.T) {
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
	parent, ok, err := q.ClaimForWorker(ctx, time.Second, worker)
	if err != nil || !ok || parent.Token == "" {
		t.Fatalf("claim tracked parent: ok=%v token=%q err=%v", ok, parent.Token, err)
	}
	child := trackedTestChild(parent, root+"/clip.mov")
	if created, err := q.EnqueuePlannedChildrenForClaim(ctx, parent, []Job{child}); err != nil || created != 1 {
		t.Fatalf("enqueue tracked child: created=%d err=%v", created, err)
	}

	keys := append([]string{ControlKey, StoragePermitKey, ProcessingIndexKey,
		ProcessingPrefix + worker.ID, TrackedProcessingIndexKey,
		TrackedProcessingPrefix + worker.ID, LeaseIndexKey}, WorkerQueueKeys(worker)...)
	capabilities, _ := json.Marshal(workerCapabilitySet(worker))
	decodeLimits, _ := json.Marshal(worker.DecodeLimits)
	if string(decodeLimits) == "null" {
		decodeLimits = []byte("{}")
	}
	claimedToken := NewID()
	expires := time.Now().Add(claimLeaseTTL)
	// Make Redis fail after the atomic ready-to-processing transfer but before
	// the reservation fields are written. The worker stays live throughout.
	faultScript := strings.Replace(claimForWorkerScript,
		"local moved = redis.call('LMOVE', KEYS[i], processing, 'RIGHT', 'LEFT')",
		"local moved = redis.call('LMOVE', KEYS[i], processing, 'RIGHT', 'LEFT')\nredis.call('HSET', processing, 'injected-fault', '1')", 1)
	if faultScript == claimForWorkerScript {
		t.Fatal("failed to install post-LMOVE claim fault")
	}
	if _, err := q.rdb.Eval(ctx, faultScript, keys, worker.ID,
		string(capabilities), string(decodeLimits), workerPinCapability(worker),
		workerDisplayName(worker), "0", claimedToken,
		expires.UTC().Format(time.RFC3339), expires.Unix()).Result(); err == nil {
		t.Fatal("injected post-LPUSH Redis failure unexpectedly succeeded")
	}

	if _, err := q.RecoverAbandonedWorkers(ctx); err != nil {
		t.Fatalf("recover after failed tracked claim: %v", err)
	}
	queueKey := QueueKeyFor(&child)
	ready, err := q.rdb.LRange(ctx, queueKey, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(ready) != 1 {
		t.Fatalf("failed claim left %d ready obligations, want exactly 1: %v", len(ready), ready)
	}
	var readyJob Job
	if err := json.Unmarshal([]byte(ready[0]), &readyJob); err != nil || readyJob.ID != child.ID {
		t.Fatalf("ready obligation = %+v, decode error %v; want child %q", readyJob, err, child.ID)
	}
	status, err := q.rdb.HGetAll(ctx, JobHashPrefix+child.ID).Result()
	if err != nil || status["status"] != StatusQueued || status["claim_phase"] != "" ||
		status["claim_token"] != "" || status["lease_expires_unix"] != "" {
		t.Fatalf("recovered pre-reservation child status = %+v, %v", status, err)
	}
	processing, err := q.rdb.LRange(ctx, TrackedProcessingPrefix+worker.ID, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range processing {
		var got Job
		if json.Unmarshal([]byte(raw), &got) == nil && got.ID == child.ID {
			t.Fatalf("stranded unreserved live-worker receipt for child %q: %v", child.ID, processing)
		}
	}

	claim, ok, err := q.ClaimForWorker(ctx, time.Second, worker)
	if err != nil || !ok || claim.Job.ID != child.ID || claim.Token == "" {
		t.Fatalf("recovered child eventual delivery = %+v, %v, %v", claim, ok, err)
	}
	if n, err := q.rdb.LLen(ctx, queueKey).Result(); err != nil || n != 0 {
		t.Fatalf("recovered child remained queued after delivery: depth=%d err=%v", n, err)
	}
}

func TestRedisTrackedExecutionClaimPreIndexFailureKeepsReadyRaw(t *testing.T) {
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
	parent, ok, err := q.ClaimForWorker(ctx, time.Second, worker)
	if err != nil || !ok || parent.Token == "" {
		t.Fatalf("claim tracked parent: ok=%v token=%q err=%v", ok, parent.Token, err)
	}
	child := trackedTestChild(parent, root+"/clip.mov")
	if created, err := q.EnqueuePlannedChildrenForClaim(ctx, parent, []Job{child}); err != nil || created != 1 {
		t.Fatalf("enqueue tracked child: created=%d err=%v", created, err)
	}
	keys := append([]string{ControlKey, StoragePermitKey, ProcessingIndexKey,
		ProcessingPrefix + worker.ID, TrackedProcessingIndexKey,
		TrackedProcessingPrefix + worker.ID, LeaseIndexKey}, WorkerQueueKeys(worker)...)
	capabilities, _ := json.Marshal(workerCapabilitySet(worker))
	decodeLimits, _ := json.Marshal(worker.DecodeLimits)
	if string(decodeLimits) == "null" {
		decodeLimits = []byte("{}")
	}
	claimedToken := NewID()
	expires := time.Now().Add(claimLeaseTTL)
	faultScript := strings.Replace(claimForWorkerScript,
		"redis.call('SADD', 'juicefarm:watch:v2:custody:jobs', job.id)",
		"redis.call('SADD', 'juicefarm:watch:v2:custody:jobs', job.id)\nredis.call('HSET', processing, 'injected-fault', '1')", 1)
	if faultScript == claimForWorkerScript {
		t.Fatal("failed to install pre-transfer claim fault")
	}
	if _, err := q.rdb.Eval(ctx, faultScript, keys, worker.ID,
		string(capabilities), string(decodeLimits), workerPinCapability(worker),
		workerDisplayName(worker), "0", claimedToken,
		expires.UTC().Format(time.RFC3339), expires.Unix()).Result(); err == nil {
		t.Fatal("injected pre-transfer Redis failure unexpectedly succeeded")
	}
	queueKey := QueueKeyFor(&child)
	ready, err := q.rdb.LRange(ctx, queueKey, 0, -1).Result()
	if err != nil || len(ready) != 1 {
		t.Fatalf("pre-transfer failure ready raws=%v err=%v; want one", ready, err)
	}
	var got Job
	if err := json.Unmarshal([]byte(ready[0]), &got); err != nil || got.ID != child.ID {
		t.Fatalf("pre-transfer ready raw = %+v, decode error %v; want child %q", got, err, child.ID)
	}
	status, err := q.rdb.HGetAll(ctx, JobHashPrefix+child.ID).Result()
	if err != nil || status["status"] != StatusQueued || status["claim_phase"] != "" ||
		status["claim_token"] != "" || status["lease_expires_unix"] != "" {
		t.Fatalf("pre-transfer child status = %+v, %v", status, err)
	}
	processing, err := q.rdb.LRange(ctx, TrackedProcessingPrefix+worker.ID, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range processing {
		var queued Job
		if json.Unmarshal([]byte(raw), &queued) == nil && queued.ID == child.ID {
			t.Fatalf("pre-transfer failure moved child into processing: %v", processing)
		}
	}
}

func TestRedisTrackedPlannerClaimLMOVEFailureIsRecoverable(t *testing.T) {
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
	keys := append([]string{ControlKey, StoragePermitKey, ProcessingIndexKey,
		ProcessingPrefix + worker.ID, TrackedProcessingIndexKey,
		TrackedProcessingPrefix + worker.ID, LeaseIndexKey}, WorkerQueueKeys(worker)...)
	capabilities, _ := json.Marshal(workerCapabilitySet(worker))
	decodeLimits, _ := json.Marshal(worker.DecodeLimits)
	if string(decodeLimits) == "null" {
		decodeLimits = []byte("{}")
	}
	claimedToken := NewID()
	expires := time.Now().Add(claimLeaseTTL)
	faultScript := strings.Replace(claimForWorkerScript,
		"local moved = redis.call('LMOVE', KEYS[i], processing, 'RIGHT', 'LEFT')",
		"local moved = redis.call('LMOVE', KEYS[i], processing, 'RIGHT', 'LEFT')\nredis.call('HSET', processing, 'injected-fault', '1')", 1)
	if faultScript == claimForWorkerScript {
		t.Fatal("failed to install tracked planner post-LMOVE fault")
	}
	if _, err := q.rdb.Eval(ctx, faultScript, keys, worker.ID,
		string(capabilities), string(decodeLimits), workerPinCapability(worker),
		workerDisplayName(worker), "0", claimedToken,
		expires.UTC().Format(time.RFC3339), expires.Unix()).Result(); err == nil {
		t.Fatal("injected tracked planner Redis failure unexpectedly succeeded")
	}
	if n, err := q.RecoverAbandonedWorkers(ctx); err != nil || n != 1 {
		t.Fatalf("recover unreserved live planner = %d, %v; want 1", n, err)
	}
	planner, ok, err := q.ClaimForWorker(ctx, time.Second, worker)
	if err != nil || !ok || planner.Job.ParentID != "" || planner.Job.TrackedEpoch == "" || planner.Token == "" {
		t.Fatalf("recovered planner claim = %+v, %v, %v", planner, ok, err)
	}
	if n, err := q.rdb.LLen(ctx, QueueKeyFor(&planner.Job)).Result(); err != nil || n != 0 {
		t.Fatalf("planner remained ready after delivery: depth=%d err=%v", n, err)
	}
}
