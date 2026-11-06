package farmqueue

import (
	"context"
	"errors"
	"testing"
)

func legacyFenceWorker(id string) Worker {
	return Worker{ID: id, Name: id, Role: QueueClassServer, Capabilities: []string{"cpu", "metadata"}}
}

func legacyFenceJob(path string) Job {
	job := NewJob(path, []string{KindDerivatives}, "manager")
	job.QueueClass = QueueClassServer
	job.RequiredCapabilities = []string{"cpu"}
	return job
}

// legacyFenceStart heartbeats w, claims the next legacy job and starts it.
func legacyFenceStart(t *testing.T, q *Client, ctx context.Context, w Worker) Claim {
	t.Helper()
	if err := q.Heartbeat(ctx, w); err != nil {
		t.Fatal(err)
	}
	claim, ok, err := q.ClaimForWorker(ctx, 0, w)
	if err != nil || !ok {
		t.Fatalf("%s claim: ok=%v err=%v", w.ID, ok, err)
	}
	if _, err := q.MarkClaimRunningWithDeadline(ctx, claim, w); err != nil {
		t.Fatalf("%s start: %v", w.ID, err)
	}
	return claim
}

// A worker partitioned past its delivery lease is reaped and replaced. When it
// returns, its terminal write and ACK must be refused instead of overwriting the
// new owner's running job and stripping the new owner's lease.
func TestLegacyStaleWorkerCannotCompleteOverNewOwner(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	a, b := legacyFenceWorker("legacy-fence-a"), legacyFenceWorker("legacy-fence-b")
	job := legacyFenceJob("/jfs/legacy-fence/stale.mov")
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	stale := legacyFenceStart(t, q, ctx, a)
	if err := q.rdb.Del(ctx, WorkerPrefix+a.ID).Err(); err != nil {
		t.Fatal(err)
	}
	expireLegacyDelivery(t, ctx, q, job.ID)
	if n, err := q.RecoverAbandonedWorkers(ctx); err != nil || n != 1 {
		t.Fatalf("recover expired delivery = %d, %v; want 1", n, err)
	}
	owner := legacyFenceStart(t, q, ctx, b)
	if owner.Job.ID != job.ID || owner.Token == stale.Token {
		t.Fatalf("replacement claim = %q token %q; want %q with a fresh token", owner.Job.ID, owner.Token, job.ID)
	}

	for _, status := range []string{StatusDone, StatusFailed, StatusCanceled} {
		if err := q.CompleteClaim(ctx, stale, status, 1, 0, "stale"); !errors.Is(err, ErrClaimLost) {
			t.Fatalf("stale CompleteClaim(%s) = %v; want ErrClaimLost", status, err)
		}
	}
	if err := q.AckClaim(ctx, stale); !errors.Is(err, ErrClaimLost) {
		t.Fatalf("stale AckClaim = %v; want ErrClaimLost", err)
	}
	status, err := q.rdb.HGetAll(ctx, JobHashPrefix+job.ID).Result()
	if err != nil {
		t.Fatal(err)
	}
	if status["status"] != StatusRunning || status["delivery_token"] != owner.Token ||
		status["lease_owner"] != b.ID || status["finished_at"] != "" {
		t.Fatalf("stale worker changed the new owner's record: %+v", status)
	}
	if score, err := q.rdb.ZScore(ctx, LeaseIndexKey, job.ID).Result(); err != nil || score == 0 {
		t.Fatalf("new owner's lease index entry = %v, %v; want kept", score, err)
	}
	if _, err := q.RenewClaimWithDeadline(ctx, owner); err != nil {
		t.Fatalf("new owner renew after stale publication: %v", err)
	}

	if err := q.CompleteClaim(ctx, owner, StatusDone, 3, 0, ""); err != nil {
		t.Fatalf("owner complete: %v", err)
	}
	if err := q.AckClaim(ctx, owner); err != nil {
		t.Fatalf("owner ack: %v", err)
	}
	status, err = q.rdb.HGetAll(ctx, JobHashPrefix+job.ID).Result()
	if err != nil || status["status"] != StatusDone || status["processed"] != "3" || status["delivery_token"] != "" {
		t.Fatalf("owner outcome = %+v, %v", status, err)
	}
	if n, err := q.rdb.LLen(ctx, ProcessingPrefix+b.ID).Result(); err != nil || n != 0 {
		t.Fatalf("owner receipt depth = %d, %v; want 0", n, err)
	}
}

// A dispatched parent is published through the same delivery fence.
func TestLegacyStaleWorkerCannotMarkDispatched(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	a, b := legacyFenceWorker("legacy-dispatch-a"), legacyFenceWorker("legacy-dispatch-b")
	job := legacyFenceJob("/jfs/legacy-fence/dispatch")
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	stale := legacyFenceStart(t, q, ctx, a)
	if err := q.rdb.Del(ctx, WorkerPrefix+a.ID).Err(); err != nil {
		t.Fatal(err)
	}
	expireLegacyDelivery(t, ctx, q, job.ID)
	if n, err := q.RecoverAbandonedWorkers(ctx); err != nil || n != 1 {
		t.Fatalf("recover expired delivery = %d, %v; want 1", n, err)
	}
	owner := legacyFenceStart(t, q, ctx, b)
	if err := q.MarkClaimDispatched(ctx, stale); !errors.Is(err, ErrClaimLost) {
		t.Fatalf("stale MarkClaimDispatched = %v; want ErrClaimLost", err)
	}
	if err := q.MarkClaimDispatched(ctx, owner); err != nil {
		t.Fatalf("owner MarkClaimDispatched: %v", err)
	}
	if err := q.AckClaim(ctx, owner); err != nil {
		t.Fatalf("owner ack: %v", err)
	}
	key := JobHashPrefix + job.ID
	if status, err := q.rdb.HGet(ctx, key, "status").Result(); err != nil || status != StatusDispatched {
		t.Fatalf("status = %q, %v; want dispatched", status, err)
	}
	if ttl, err := q.rdb.TTL(ctx, key).Result(); err != nil || ttl <= 0 {
		t.Fatalf("dispatched record TTL = %v, %v; want JobTTL", ttl, err)
	}
}

// Legacy start and renew stamp delivery_expires_unix. A lapsed heartbeat must
// not release a delivery whose lease is still being renewed.
func TestLegacyReaperHonoursDeliveryLease(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	a := legacyFenceWorker("legacy-lease-a")
	job := legacyFenceJob("/jfs/legacy-fence/lease.mov")
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	claim := legacyFenceStart(t, q, ctx, a)
	if err := q.rdb.Del(ctx, WorkerPrefix+a.ID).Err(); err != nil {
		t.Fatal(err)
	}
	if n, err := q.RecoverAbandonedWorkers(ctx); err != nil || n != 0 {
		t.Fatalf("recovery with a fresh delivery lease = %d, %v; want 0", n, err)
	}
	if _, err := q.RenewClaimWithDeadline(ctx, claim); err != nil {
		t.Fatalf("renew after skipped recovery: %v", err)
	}
	if err := q.CompleteClaim(ctx, claim, StatusDone, 1, 0, ""); err != nil {
		t.Fatalf("complete after skipped recovery: %v", err)
	}
	if err := q.AckClaim(ctx, claim); err != nil {
		t.Fatalf("ack after skipped recovery: %v", err)
	}
	if depth, err := q.QueueDepth(ctx); err != nil || depth != 0 {
		t.Fatalf("queue depth = %d, %v; the live delivery was requeued", depth, err)
	}
}

// The reaper's requeue re-checks the lease atomically: a renewal that lands
// after the scan read an expired deadline keeps its owner.
func TestLegacyReaperRequeueRechecksLeaseAtomically(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	a := legacyFenceWorker("legacy-race-a")
	job := legacyFenceJob("/jfs/legacy-fence/race.mov")
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	claim := legacyFenceStart(t, q, ctx, a)
	token, err := q.rdb.HGet(ctx, JobHashPrefix+job.ID, "delivery_token").Result()
	if err != nil {
		t.Fatal(err)
	}
	reaped := Claim{Job: claim.Job, Raw: claim.Raw, WorkerID: a.ID, Token: token, reapExpired: true}
	if err := q.RequeueClaim(ctx, reaped, false, "reaper"); !errors.Is(err, ErrClaimLost) {
		t.Fatalf("reaper requeue of a renewed delivery = %v; want ErrClaimLost", err)
	}
	expireLegacyDelivery(t, ctx, q, job.ID)
	if err := q.CompleteClaim(ctx, claim, StatusDone, 1, 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := q.RequeueClaim(ctx, reaped, false, "reaper"); !errors.Is(err, ErrClaimLost) {
		t.Fatalf("reaper requeue of a committed outcome = %v; want ErrClaimLost", err)
	}
	if depth, err := q.QueueDepth(ctx); err != nil || depth != 0 {
		t.Fatalf("queue depth = %d, %v; want no rerun", depth, err)
	}
}

// Worker publication retries transient failures, so a legacy terminal write
// and ACK must be idempotent when Redis committed them but the reply was lost.
func TestLegacyTerminalPublicationSurvivesLostReplies(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	a := legacyFenceWorker("legacy-reply-a")
	job := legacyFenceJob("/jfs/legacy-fence/reply.mov")
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	claim := legacyFenceStart(t, q, ctx, a)

	complete := h1InstallCommittedReplyLoss(t, q,
		h1ReplyLossOnEvalContaining("if status ~= 'running' and status ~= ARGV[4] then return 0 end"))
	if err := q.CompleteClaim(ctx, claim, StatusFailed, 2, 1, "boom"); err == nil || errors.Is(err, ErrClaimLost) {
		t.Fatalf("complete with lost reply = %v; want a transport error", err)
	}
	if !complete.fired.Load() {
		t.Fatal("terminal EVAL did not commit and lose its reply")
	}
	if err := q.CompleteClaim(ctx, claim, StatusFailed, 2, 1, "boom"); err != nil {
		t.Fatalf("retried complete: %v", err)
	}
	ack := h1InstallCommittedReplyLoss(t, q,
		h1ReplyLossOnEvalContaining("if not redis.call('LPOS', KEYS[1], ARGV[1]) then return 1 end"))
	if err := q.AckClaim(ctx, claim); err == nil || errors.Is(err, ErrClaimLost) {
		t.Fatalf("ack with lost reply = %v; want a transport error", err)
	}
	if !ack.fired.Load() {
		t.Fatal("ACK EVAL did not commit and lose its reply")
	}
	if err := q.AckClaim(ctx, claim); err != nil {
		t.Fatalf("retried ack: %v", err)
	}
	status, err := q.rdb.HGetAll(ctx, JobHashPrefix+job.ID).Result()
	if err != nil || status["status"] != StatusFailed || status["processed"] != "2" ||
		status["failed"] != "1" || status["error"] != "boom" || status["delivery_token"] != "" {
		t.Fatalf("published outcome = %+v, %v", status, err)
	}
	if n, err := q.rdb.LLen(ctx, ProcessingPrefix+a.ID).Result(); err != nil || n != 0 {
		t.Fatalf("receipt depth = %d, %v; want 0", n, err)
	}
	// A different status for the same delivery is not a retry.
	if err := q.CompleteClaim(ctx, claim, StatusDone, 2, 0, ""); !errors.Is(err, ErrClaimLost) {
		t.Fatalf("complete after ACK = %v; want ErrClaimLost", err)
	}
}

// The Manager's "Clear finished" can delete a done record between the
// worker's terminal write and its ACK. The ACK must still release the
// worker's own receipt; refusing it stopped the worker, and the reaper then
// requeued and re-ran completed work.
func TestLegacyAckAfterClearFinishedReleasesOwnReceipt(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	a := legacyFenceWorker("legacy-cleared-a")
	job := legacyFenceJob("/jfs/legacy-fence/cleared.mov")
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	claim := legacyFenceStart(t, q, ctx, a)
	if err := q.CompleteClaim(ctx, claim, StatusDone, 1, 0, ""); err != nil {
		t.Fatal(err)
	}
	if n, err := q.ClearFinished(ctx); err != nil || n != 1 {
		t.Fatalf("clear finished = %d, %v; want the done record removed", n, err)
	}
	if err := q.AckClaim(ctx, claim); err != nil {
		t.Fatalf("ack after clear finished = %v; want the own receipt released", err)
	}
	if n, err := q.rdb.LLen(ctx, ProcessingPrefix+a.ID).Result(); err != nil || n != 0 {
		t.Fatalf("receipt depth = %d, %v; want 0", n, err)
	}
	if _, err := q.rdb.ZScore(ctx, LeaseIndexKey, job.ID).Result(); err == nil {
		t.Fatal("lease index entry survived the ACK")
	}
	if n, err := q.rdb.Exists(ctx, JobHashPrefix+job.ID).Result(); err != nil || n != 0 {
		t.Fatalf("ACK recreated the cleared record: exists=%d err=%v", n, err)
	}
	if err := q.rdb.Del(ctx, WorkerPrefix+a.ID).Err(); err != nil {
		t.Fatal(err)
	}
	if n, err := q.RecoverAbandonedWorkers(ctx); err != nil || n != 0 {
		t.Fatalf("recovery after the ACK = %d, %v; want nothing to rerun", n, err)
	}
	if depth, err := q.QueueDepth(ctx); err != nil || depth != 0 {
		t.Fatalf("queue depth = %d, %v; completed work was requeued", depth, err)
	}
}
