package farmqueue

import (
	"slices"
	"strconv"
	"testing"
	"time"
)

func TestH1TrackedReaperUsesRedisTimeForLeaseExpiry(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	worker := watchPlannerWorker()
	_, child := h1ReviewTrackedChild(t, q, ctx, worker)
	claim, ok, err := q.ClaimForWorker(ctx, 0, worker)
	if err != nil || !ok || claim.Job.ID != child.ID {
		t.Fatalf("claim tracked child: claim=%+v ok=%v err=%v", claim, ok, err)
	}
	if err := q.MarkClaimRunning(ctx, claim, worker); err != nil {
		t.Fatalf("mark child running: %v", err)
	}

	redisNow, err := q.rdb.Time(ctx).Result()
	if err != nil {
		t.Fatalf("read Redis clock: %v", err)
	}
	expires, err := q.rdb.HGet(ctx, JobHashPrefix+child.ID, "lease_expires_unix").Int64()
	if err != nil {
		t.Fatalf("read child lease expiry: %v", err)
	}
	if expires <= redisNow.Unix() {
		t.Fatalf("fixture lease is not valid by Redis time: expiry=%d Redis now=%d", expires, redisNow.Unix())
	}
	beforeProcessing, err := q.rdb.LRange(ctx, claimProcessingKey(claim), 0, -1).Result()
	if err != nil || !slices.Contains(beforeProcessing, claim.Raw) {
		t.Fatalf("missing exact child receipt before recovery: %q err=%v", beforeProcessing, err)
	}

	// Model a reaper host whose clock is ahead. Recovery must compare the lease
	// with Redis TIME atomically; the timestamp passed by the caller is untrusted.
	futureHostNow := redisNow.Add(time.Hour).Unix()
	n, err := q.rdb.Eval(ctx, reclaimTrackedReservationScript, []string{
		claimProcessingKey(claim), QueueKeyFor(&claim.Job), JobHashPrefix + child.ID,
		LeaseIndexKey, TrackedProcessingIndexKey,
	}, claim.Token, worker.ID, claim.Raw, futureHostNow, child.ID,
		strconv.FormatInt(claim.Generation, 10)).Int()
	if err != nil {
		t.Fatalf("run tracked recovery script with skewed caller clock: %v", err)
	}
	if n != 0 {
		t.Fatalf("reaper reclaimed child with valid Redis lease: recovered=%d expiry=%d Redis now=%d caller now=%d; want 0",
			n, expires, redisNow.Unix(), futureHostNow)
	}

	status, err := q.rdb.HGetAll(ctx, JobHashPrefix+child.ID).Result()
	if err != nil {
		t.Fatalf("read child status after recovery: %v", err)
	}
	if status["status"] != StatusRunning || status["claim_token"] != claim.Token ||
		status["lease_owner"] != worker.ID || status["lease_expires_unix"] != strconv.FormatInt(expires, 10) {
		t.Fatalf("valid child lease changed after skewed recovery: status=%+v", status)
	}
	processing, err := q.rdb.LRange(ctx, claimProcessingKey(claim), 0, -1).Result()
	if err != nil {
		t.Fatalf("read tracked processing receipt: %v", err)
	}
	if !slices.Equal(processing, beforeProcessing) {
		t.Fatalf("valid processing receipts changed after skewed recovery: before=%q after=%q", beforeProcessing, processing)
	}
}
