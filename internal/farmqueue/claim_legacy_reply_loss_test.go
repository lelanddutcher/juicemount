package farmqueue

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9"
)

// h1InstallEvalResendReplyLoss drops the first reply to script, as
// h1InstallEvalTransportReplyLoss does, but keeps go-redis's retries, so the
// client resends the EVAL on a new connection and the caller sees no error.
func h1InstallEvalResendReplyLoss(t *testing.T, q *Client, script string) *atomic.Bool {
	t.Helper()
	options := *q.rdb.Options()
	if options.MaxRetries <= 0 {
		t.Fatalf("fixture client MaxRetries = %d; want go-redis's resend", options.MaxRetries)
	}
	var armed, lost atomic.Bool
	armed.Store(true)
	baseDialer := &net.Dialer{}
	options.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := baseDialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &h1TransportEvalReplyLossConn{Conn: conn, script: []byte(script), armed: &armed, lost: &lost}, nil
	}
	if err := q.rdb.Close(); err != nil {
		t.Fatalf("close fixture Redis client before installing transport: %v", err)
	}
	q.rdb = redis.NewClient(&options)
	return &lost
}

func legacyReplyLossFixture(t *testing.T) (*Client, context.Context, Worker, Job, Job) {
	t.Helper()
	q, ctx := openWatchEpochTestQueue(t)
	worker := Worker{ID: "legacy-reply-loss", Name: "legacy-reply-loss", Role: QueueClassServer,
		Capabilities: []string{"cpu", "metadata"}}
	if err := q.Heartbeat(ctx, worker); err != nil {
		t.Fatal(err)
	}
	first := NewJob("/mnt/first.mov", []string{KindDerivatives}, "test")
	second := NewJob("/mnt/second.mov", []string{KindDerivatives}, "test")
	for _, job := range []Job{first, second} {
		if err := q.Enqueue(ctx, job); err != nil {
			t.Fatal(err)
		}
	}
	return q, ctx, worker, first, second
}

// routedLane is the ready lane Enqueue routed job to.
func routedLane(t *testing.T, q *Client, ctx context.Context, job Job) string {
	t.Helper()
	payload, err := q.rdb.HGet(ctx, JobHashPrefix+job.ID, "payload").Result()
	if err != nil {
		t.Fatalf("read %s payload: %v", job.ID, err)
	}
	var routed Job
	if err := json.Unmarshal([]byte(payload), &routed); err != nil {
		t.Fatal(err)
	}
	return QueueKeyFor(&routed)
}

// assertLegacyDelivery checks that claim is the only receipt in the worker's
// processing list, fenced by its own token, and that other is still ready.
func assertLegacyDelivery(t *testing.T, q *Client, ctx context.Context, claim Claim, other Job) {
	t.Helper()
	receipts, err := q.rdb.LRange(ctx, ProcessingPrefix+claim.WorkerID, 0, -1).Result()
	if err != nil || len(receipts) != 1 || receipts[0] != claim.Raw {
		t.Fatalf("receipts = %q err=%v; want only the returned claim", receipts, err)
	}
	if token, err := q.rdb.HGet(ctx, JobHashPrefix+claim.Job.ID, "delivery_token").Result(); err != nil || token != claim.Token {
		t.Fatalf("delivery_token = %q err=%v; want the returned token %q", token, err, claim.Token)
	}
	if token, _ := q.rdb.HGet(ctx, JobHashPrefix+other.ID, "delivery_token").Result(); token != "" {
		t.Fatalf("job %s was also delivered (token %q)", other.ID, token)
	}
	ready, err := q.rdb.LRange(ctx, routedLane(t, q, ctx, other), 0, -1).Result()
	if err != nil || len(ready) != 1 {
		t.Fatalf("ready lane = %q err=%v; want only %s", ready, err, other.ID)
	}
}

func runLegacyClaimToAck(t *testing.T, q *Client, ctx context.Context, claim Claim, worker Worker) {
	t.Helper()
	if err := q.MarkClaimRunning(ctx, claim, worker); err != nil {
		t.Fatalf("start replayed claim: %v", err)
	}
	if err := q.CompleteClaim(ctx, claim, StatusDone, 1, 0, ""); err != nil {
		t.Fatalf("complete replayed claim: %v", err)
	}
	if err := q.AckClaim(ctx, claim); err != nil {
		t.Fatalf("ack replayed claim: %v", err)
	}
}

// A legacy claim has no lease and the reaper leaves a live worker's legacy
// receipts alone. When the claim's reply is lost and the call fails, the
// worker's next claim must return that job, not claim another one.
func TestLegacyClaimLostReplyErrorIsReturnedByTheNextClaim(t *testing.T) {
	q, ctx, worker, first, second := legacyReplyLossFixture(t)
	lost := h1InstallEvalTransportReplyLoss(t, q, claimForWorkerScript)
	if _, ok, err := q.ClaimForWorker(ctx, 0, worker); err == nil || ok || !lost.Load() {
		t.Fatalf("claim: ok=%v err=%v lost=%v; want the reply dropped and an error", ok, err, lost.Load())
	}
	claim, ok, err := q.ClaimForWorker(ctx, 0, worker)
	if err != nil || !ok || claim.Job.ID != first.ID {
		t.Fatalf("next claim: job=%q ok=%v err=%v; want %s, whose reply was lost", claim.Job.ID, ok, err, first.ID)
	}
	if lane := routedLane(t, q, ctx, first); claim.QueueKey != lane {
		t.Fatalf("replayed claim lane = %q; want %q", claim.QueueKey, lane)
	}
	assertLegacyDelivery(t, q, ctx, claim, second)
	runLegacyClaimToAck(t, q, ctx, claim, worker)
	next, ok, err := q.ClaimForWorker(ctx, 0, worker)
	if err != nil || !ok || next.Job.ID != second.ID || next.Token == claim.Token {
		t.Fatalf("claim after ack: job=%q token reused=%v ok=%v err=%v; want %s under a new token",
			next.Job.ID, next.Token == claim.Token, ok, err, second.ID)
	}
}

// go-redis resends an EVAL whose reply was lost to EOF. The resend runs the
// claim script again with the same token; it must return the job the first
// run claimed instead of claiming a second one under that token.
func TestLegacyClaimLostReplyResendReturnsTheSameJob(t *testing.T) {
	q, ctx, worker, first, second := legacyReplyLossFixture(t)
	lost := h1InstallEvalResendReplyLoss(t, q, claimForWorkerScript)
	claim, ok, err := q.ClaimForWorker(ctx, 0, worker)
	if !lost.Load() {
		t.Fatal("test did not drop the claim EVAL reply")
	}
	if err != nil || !ok || claim.Job.ID != first.ID {
		t.Fatalf("claim: job=%q ok=%v err=%v; want %s, claimed by the run whose reply was lost", claim.Job.ID, ok, err, first.ID)
	}
	assertLegacyDelivery(t, q, ctx, claim, second)
	runLegacyClaimToAck(t, q, ctx, claim, worker)
}

// A worker that stops claiming after a failed claim (disabled, paused,
// draining) gives back the job that claim may have taken.
func TestUnconfirmedLegacyClaimIsReleasedWhileTheWorkerIsNotClaiming(t *testing.T) {
	q, ctx, worker, first, second := legacyReplyLossFixture(t)
	if released, err := q.ReleaseUnconfirmedClaim(ctx, worker); err != nil || released {
		t.Fatalf("release before any claim: released=%v err=%v; want nothing", released, err)
	}
	lost := h1InstallEvalTransportReplyLoss(t, q, claimForWorkerScript)
	if _, ok, err := q.ClaimForWorker(ctx, 0, worker); err == nil || ok || !lost.Load() {
		t.Fatalf("claim: ok=%v err=%v lost=%v; want the reply dropped and an error", ok, err, lost.Load())
	}
	if released, err := q.ReleaseUnconfirmedClaim(ctx, worker); err != nil || !released {
		t.Fatalf("release: released=%v err=%v; want the unconfirmed claim released", released, err)
	}
	if released, err := q.ReleaseUnconfirmedClaim(ctx, worker); err != nil || released {
		t.Fatalf("second release: released=%v err=%v; want nothing left", released, err)
	}
	if n, err := q.rdb.LLen(ctx, ProcessingPrefix+worker.ID).Result(); err != nil || n != 0 {
		t.Fatalf("receipts after release = %d err=%v; want none", n, err)
	}
	fields, err := q.rdb.HGetAll(ctx, JobHashPrefix+first.ID).Result()
	if err != nil || fields["status"] != StatusQueued || fields["delivery_token"] != "" || fields["attempts"] != "0" {
		t.Fatalf("released record = %+v err=%v; want queued, undelivered, no attempt spent", fields, err)
	}
	ready, err := q.rdb.LRange(ctx, routedLane(t, q, ctx, first), 0, -1).Result()
	if err != nil || len(ready) != 2 {
		t.Fatalf("ready lane after release = %q err=%v; want both jobs", ready, err)
	}
	// Like the reaper, the release requeues at the far end of the lane.
	for _, want := range []Job{second, first} {
		claim, ok, err := q.ClaimForWorker(ctx, 0, worker)
		if err != nil || !ok || claim.Job.ID != want.ID {
			t.Fatalf("claim after release: job=%q ok=%v err=%v; want %s", claim.Job.ID, ok, err, want.ID)
		}
		runLegacyClaimToAck(t, q, ctx, claim, worker)
	}
}

// The token lives in the process. A restarted worker has a new ID and a new
// client; the old worker's receipt is the reaper's once its heartbeat lapses.
func TestUnconfirmedLegacyClaimIsNotInheritedByARestartedWorker(t *testing.T) {
	q, ctx, worker, first, second := legacyReplyLossFixture(t)
	lost := h1InstallEvalTransportReplyLoss(t, q, claimForWorkerScript)
	if _, ok, err := q.ClaimForWorker(ctx, 0, worker); err == nil || ok || !lost.Load() {
		t.Fatalf("claim: ok=%v err=%v lost=%v; want the reply dropped and an error", ok, err, lost.Load())
	}
	restarted, err := Open(os.Getenv("JM_FARM_TEST_REDIS_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	successor := worker
	successor.ID = "legacy-reply-loss-restarted"
	if err := restarted.Heartbeat(ctx, successor); err != nil {
		t.Fatal(err)
	}
	claim, ok, err := restarted.ClaimForWorker(ctx, 0, successor)
	if err != nil || !ok || claim.Job.ID != second.ID {
		t.Fatalf("restarted worker claim: job=%q ok=%v err=%v; want %s", claim.Job.ID, ok, err, second.ID)
	}
	if err := q.rdb.Del(ctx, WorkerPrefix+worker.ID).Err(); err != nil {
		t.Fatal(err)
	}
	if n, err := restarted.RecoverAbandonedWorkers(ctx); err != nil || n != 1 {
		t.Fatalf("recover the old worker's receipt: n=%d err=%v; want one", n, err)
	}
	if token, _ := q.rdb.HGet(ctx, JobHashPrefix+first.ID, "delivery_token").Result(); token != "" {
		t.Fatalf("recovered job still carries delivery token %q", token)
	}
}
