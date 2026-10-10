package farmqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type h1CommittedReplyLossHook struct {
	match func(redis.Cmder) bool
	fired atomic.Bool
	err   error
}

func (h *h1CommittedReplyLossHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (h *h1CommittedReplyLossHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if err := next(ctx, cmd); err != nil {
			return err
		}
		if h.match(cmd) && h.fired.CompareAndSwap(false, true) {
			cmd.SetErr(h.err)
			return h.err
		}
		return nil
	}
}

func (h *h1CommittedReplyLossHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func h1ReplyLossOnEvalScript(script string) func(redis.Cmder) bool {
	return func(cmd redis.Cmder) bool {
		args := cmd.Args()
		return cmd.Name() == "eval" && len(args) > 1 && fmt.Sprint(args[1]) == script
	}
}

func h1ReplyLossOnEvalContaining(fragment string) func(redis.Cmder) bool {
	return func(cmd redis.Cmder) bool {
		args := cmd.Args()
		return cmd.Name() == "eval" && len(args) > 1 && strings.Contains(fmt.Sprint(args[1]), fragment)
	}
}

func h1InstallCommittedReplyLoss(t *testing.T, q *Client, match func(redis.Cmder) bool) *h1CommittedReplyLossHook {
	t.Helper()
	hook := &h1CommittedReplyLossHook{
		match: match,
		err:   errors.New("injected transport error after committed Redis EVAL"),
	}
	q.rdb.AddHook(hook)
	return hook
}

func h1ReplyLossClaimTrackedChild(t *testing.T, q *Client, ctx context.Context, child Job) (Claim, Worker) {
	t.Helper()
	worker := Worker{ID: "h1-committed-reply-worker", Name: "h1-committed-reply-worker", Role: QueueClassServer,
		Capabilities: []string{"cpu"}}
	if err := q.Heartbeat(ctx, worker); err != nil {
		t.Fatal(err)
	}
	claim, ok, err := q.ClaimForWorker(ctx, 0, worker)
	if err != nil || !ok || claim.Job.ID != child.ID {
		t.Fatalf("claim tracked child: ok=%v job=%q err=%v", ok, claim.Job.ID, err)
	}
	if err := q.MarkClaimRunning(ctx, claim, worker); err != nil {
		t.Fatalf("mark child running: %v", err)
	}
	return claim, worker
}

func TestH1TrackedClaimCommittedReplyLossIsRecoveredWithoutExecution(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	plannerWorker := watchPlannerWorker()
	parent, child := h1ReviewTrackedChild(t, q, ctx, plannerWorker)
	if committed, err := q.CommitWatchEpoch(ctx, parent.Job.Path, "leader", parent.Job.TrackedEpoch); err != nil || !committed {
		t.Fatalf("commit planner coverage: committed=%v err=%v", committed, err)
	}
	if err := q.AckClaim(ctx, parent); err != nil {
		t.Fatalf("acknowledge planner: %v", err)
	}
	worker := Worker{ID: "h1-claim-reply-loss-worker", Name: "h1-claim-reply-loss-worker", Role: QueueClassServer,
		Capabilities: []string{"cpu"}}
	if err := q.Heartbeat(ctx, worker); err != nil {
		t.Fatal(err)
	}
	hook := h1InstallCommittedReplyLoss(t, q, h1ReplyLossOnEvalScript(claimForWorkerScript))
	claim, ok, err := q.ClaimForWorker(ctx, 0, worker)
	if err == nil || ok || claim.Token != "" || claim.Job.ID != "" {
		t.Fatalf("claim after committed reply loss: claim=%+v ok=%v err=%v; want no returned claim and an error", claim, ok, err)
	}
	if !hook.fired.Load() {
		t.Fatal("target tracked-claim EVAL did not commit and lose its response")
	}

	statusKey := JobHashPrefix + child.ID
	status, err := q.rdb.HGetAll(ctx, statusKey).Result()
	if err != nil || status["claim_phase"] != "reserved" || status["lease_owner"] != worker.ID || status["claim_token"] == "" {
		t.Fatalf("committed claim reservation = %+v err=%v; want its durable reservation", status, err)
	}
	processingKey := TrackedProcessingPrefix + worker.ID
	processing, err := q.rdb.LRange(ctx, processingKey, 0, -1).Result()
	if err != nil || len(processing) != 1 || processing[0] == "" {
		t.Fatalf("committed claim processing receipt = %q err=%v; want exactly one receipt", processing, err)
	}
	var received Job
	if err := json.Unmarshal([]byte(processing[0]), &received); err != nil || received.ID != child.ID {
		t.Fatalf("processing receipt job = %+v err=%v; want %q", received, err, child.ID)
	}
	if err := q.rdb.HSet(ctx, statusKey, "lease_expires_unix", time.Now().Add(-time.Minute).Unix()).Err(); err != nil {
		t.Fatal(err)
	}
	if heartbeat, err := q.rdb.Exists(ctx, WorkerPrefix+worker.ID).Result(); err != nil || heartbeat != 1 {
		t.Fatalf("worker heartbeat before recovery = %d err=%v; want live", heartbeat, err)
	}
	if recovered, err := q.RecoverAbandonedWorkers(ctx); err != nil || recovered != 1 {
		t.Fatalf("recover committed but unobserved claim: recovered=%d err=%v; want one", recovered, err)
	}
	processing, err = q.rdb.LRange(ctx, processingKey, 0, -1).Result()
	if err != nil || len(processing) != 0 {
		t.Fatalf("old processing custody after recovery = %q err=%v; want none", processing, err)
	}
	ready, err := q.rdb.LRange(ctx, QueueKeyFor(&child), 0, -1).Result()
	if err != nil || len(ready) != 1 {
		t.Fatalf("ready child after recovery = %q err=%v; want exactly one", ready, err)
	}
	var restored Job
	if err := json.Unmarshal([]byte(ready[0]), &restored); err != nil || restored.ID != child.ID {
		t.Fatalf("restored ready job = %+v err=%v; want %q", restored, err, child.ID)
	}
	status, err = q.rdb.HGetAll(ctx, statusKey).Result()
	if err != nil || status["status"] != StatusQueued || status["claim_token"] != "" || status["claim_phase"] != "" {
		t.Fatalf("recovered child state = %+v err=%v; want unclaimed queued state", status, err)
	}
	if heartbeat, err := q.rdb.Exists(ctx, WorkerPrefix+worker.ID).Result(); err != nil || heartbeat != 1 {
		t.Fatalf("recovery changed live worker heartbeat = %d err=%v", heartbeat, err)
	}
}

func TestH1TrackedCPUFallbackCommittedReplyLossReplaysImmutableChild(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	plannerWorker := watchPlannerWorker()
	_, plannedChild := h1ReviewTrackedChild(t, q, ctx, plannerWorker)
	parentClaim, _ := h1ReplyLossClaimTrackedChild(t, q, ctx, plannedChild)
	targets := []string{plannedChild.RetryTargets[0]}
	childIdentity, err := newCPUFallbackSubset(parentClaim.Job, targets)
	if err != nil {
		t.Fatal(err)
	}
	hook := h1InstallCommittedReplyLoss(t, q, h1ReplyLossOnEvalScript(enqueueCPUFallbackSubsetScript))
	if _, _, err := q.EnqueueCPUFallbackSubsetForClaim(ctx, parentClaim, targets, "decoder unavailable"); err == nil {
		t.Fatal("tracked fallback publish did not report injected reply loss")
	}
	if !hook.fired.Load() {
		t.Fatal("target tracked-fallback EVAL did not commit and lose its response")
	}
	firstPayload, err := q.rdb.HGet(ctx, JobHashPrefix+childIdentity.ID, "payload").Result()
	if err != nil {
		t.Fatalf("read committed fallback payload: %v", err)
	}
	replayed, created, err := q.EnqueueCPUFallbackSubsetForClaim(ctx, parentClaim, targets, "decoder unavailable")
	if err != nil || created {
		t.Fatalf("same-claim fallback replay: created=%v err=%v; want existing immutable child", created, err)
	}
	replayPayload, err := json.Marshal(replayed)
	if err != nil || string(replayPayload) != firstPayload {
		t.Fatalf("fallback replay payload differs: replay=%s stored=%s err=%v", replayPayload, firstPayload, err)
	}
	if published, err := q.rdb.HGet(ctx, JobHashPrefix+childIdentity.ID, "cpu_fallback_published").Result(); err != nil || published != "1" {
		t.Fatalf("fallback publication receipt = %q err=%v; want durable publication", published, err)
	}
	queued, err := q.rdb.LRange(ctx, QueueKeyFor(&replayed), 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	occurrences := 0
	for _, raw := range queued {
		var job Job
		if json.Unmarshal([]byte(raw), &job) == nil && job.ID == replayed.ID {
			occurrences++
		}
	}
	if occurrences != 1 {
		t.Fatalf("fallback queue contains %d copies of %s in %q; want one", occurrences, replayed.ID, queued)
	}
}

func TestH1TrackedAckCommittedReplyLossSettlesLedgerOnce(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	plannerWorker := watchPlannerWorker()
	planner, child := h1ReviewTrackedChild(t, q, ctx, plannerWorker)
	claim, worker := h1ReplyLossClaimTrackedChild(t, q, ctx, child)
	if err := q.CompleteClaim(ctx, claim, StatusDone, 1, 0, ""); err != nil {
		t.Fatalf("complete tracked child: %v", err)
	}
	hook := h1InstallCommittedReplyLoss(t, q,
		h1ReplyLossOnEvalContaining("tracked acknowledgement lacks an open coverage ledger"))
	if err := q.AckClaim(ctx, claim); err == nil {
		t.Fatal("tracked ACK did not report injected reply loss")
	}
	if !hook.fired.Load() {
		t.Fatal("target tracked-ACK EVAL did not commit and lose its response")
	}
	if err := q.AckClaim(ctx, claim); err != nil {
		t.Fatalf("retry same committed ACK: %v", err)
	}

	status, err := q.rdb.HGetAll(ctx, JobHashPrefix+claim.Job.ID).Result()
	if err != nil || status["ack_token"] != claim.Token || status["ack_owner"] != worker.ID {
		t.Fatalf("ACK proof after reply-loss retry = %+v err=%v", status, err)
	}
	processing, err := q.rdb.LRange(ctx, claimProcessingKey(claim), 0, -1).Result()
	if err != nil || len(processing) != 0 {
		t.Fatalf("child processing custody after ACK retry = %q err=%v; want none", processing, err)
	}
	if member, err := q.rdb.SIsMember(ctx, claimProcessingIndex(claim), worker.ID).Result(); err != nil || member {
		t.Fatalf("child remains in processing-worker index = %v err=%v", member, err)
	}
	ledger := coverageLedgerKey(planner.Job.ID)
	if count, err := q.rdb.HGet(ctx, ledger, "open_count").Result(); err != nil || count != "0" {
		t.Fatalf("coverage ledger open_count = %q err=%v; want one settled child", count, err)
	}
	if marker, err := q.rdb.HGet(ctx, ledger, "settling:"+claim.Job.ID).Result(); err != nil || marker != "done:"+claim.Token {
		t.Fatalf("coverage settlement marker = %q err=%v; want exactly this claim settled", marker, err)
	}
	if score, err := q.rdb.ZScore(ctx, coverageMembersKey(planner.Job.ID), claim.Job.ID).Result(); err != nil || score != 1 {
		t.Fatalf("settled child membership score = %v err=%v; want one terminal ledger entry", score, err)
	}
}
