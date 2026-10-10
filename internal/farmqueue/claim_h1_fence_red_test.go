package farmqueue

import (
	"context"
	"slices"
	"strconv"
	"testing"
	"time"
)

func h1FenceRunningReplacement(t *testing.T, q *Client, ctx context.Context) (Claim, Claim, Worker) {
	t.Helper()
	worker := watchPlannerWorker()
	parent, child := h1ReviewTrackedChild(t, q, ctx, worker)
	old, ok, err := q.ClaimForWorker(ctx, 0, worker)
	if err != nil || !ok || old.Job.ID != child.ID {
		t.Fatalf("claim old child: %+v ok=%v err=%v", old, ok, err)
	}
	if err := q.MarkClaimRunning(ctx, old, worker); err != nil {
		t.Fatalf("start old child: %v", err)
	}
	if err := q.rdb.HSet(ctx, JobHashPrefix+child.ID, "lease_expires_unix", time.Now().Add(-time.Minute).Unix()).Err(); err != nil {
		t.Fatal(err)
	}
	if status, err := q.rdb.HGet(ctx, JobHashPrefix+parent.Job.ID, "status").Result(); err != nil || status != StatusDispatched {
		t.Fatalf("planner before recovery status=%q err=%v", status, err)
	}
	if receipts, err := q.rdb.LRange(ctx, claimProcessingKey(parent), 0, -1).Result(); err != nil ||
		!slices.Contains(receipts, parent.Raw) || !slices.Contains(receipts, old.Raw) {
		t.Fatalf("planner and child receipts before recovery=%q err=%v", receipts, err)
	}
	if n, err := q.RecoverAbandonedWorkers(ctx); err != nil || n != 2 {
		t.Fatalf("recover dispatched planner and expired child: %d, %v", n, err)
	}
	if fields, err := q.rdb.HGetAll(ctx, JobHashPrefix+parent.Job.ID).Result(); err != nil ||
		fields["ack_token"] != parent.Token || fields["claim_token"] != "" {
		t.Fatalf("planner ACK after recovery=%+v err=%v", fields, err)
	}
	if receipts, err := q.rdb.LRange(ctx, claimProcessingKey(parent), 0, -1).Result(); err != nil || len(receipts) != 0 {
		t.Fatalf("processing after planner ACK and child recovery=%q err=%v", receipts, err)
	}
	newClaim, ok, err := q.ClaimForWorker(ctx, 0, worker)
	if err != nil || !ok || newClaim.Job.ID != child.ID || newClaim.Generation <= old.Generation {
		t.Fatalf("replacement claim: %+v ok=%v err=%v; old generation=%d", newClaim, ok, err, old.Generation)
	}
	if err := q.MarkClaimRunning(ctx, newClaim, worker); err != nil {
		t.Fatalf("start replacement child: %v", err)
	}
	if old.Raw != newClaim.Raw {
		t.Fatalf("replacement receipt raw changed: old=%q new=%q", old.Raw, newClaim.Raw)
	}
	return old, newClaim, worker
}

func TestH1StaleChildMutationsCannotChangeReplacement(t *testing.T) {
	mutations := []struct {
		name string
		run  func(context.Context, *Client, Claim) error
	}{
		{"mark-done", func(ctx context.Context, q *Client, claim Claim) error {
			return q.MarkDone(ctx, claim.Job.ID, 9, 0)
		}},
		{"mark-failed", func(ctx context.Context, q *Client, claim Claim) error {
			return q.MarkFailed(ctx, claim.Job.ID, "stale failure")
		}},
		{"mark-canceled", func(ctx context.Context, q *Client, claim Claim) error {
			return q.MarkCanceled(ctx, claim.Job.ID, 0, 0)
		}},
		{"ack", func(ctx context.Context, q *Client, claim Claim) error {
			return q.AckClaim(ctx, claim)
		}},
		{"requeue", func(ctx context.Context, q *Client, claim Claim) error {
			return q.RequeueClaim(ctx, claim, false, "stale retry")
		}},
	}
	for _, mutation := range mutations {
		mutation := mutation
		t.Run(mutation.name, func(t *testing.T) {
			q, ctx := openWatchEpochTestQueue(t)
			old, current, _ := h1FenceRunningReplacement(t, q, ctx)
			beforeProcessing, err := q.rdb.LRange(ctx, claimProcessingKey(current), 0, -1).Result()
			if err != nil {
				t.Fatal(err)
			}
			currentReceipts := 0
			for _, raw := range beforeProcessing {
				if raw == current.Raw {
					currentReceipts++
				}
			}
			if currentReceipts != 1 {
				t.Fatalf("replacement has %d exact processing receipts: %q", currentReceipts, beforeProcessing)
			}
			if err := mutation.run(ctx, q, old); err != nil {
				// Claim-bound operations should reject stale ownership. ID-only terminal
				// methods currently cannot express that contract and are expected REDs.
				t.Logf("stale operation returned error: %v", err)
			}
			fields, err := q.rdb.HGetAll(ctx, JobHashPrefix+current.Job.ID).Result()
			if err != nil {
				t.Fatal(err)
			}
			if fields["status"] != StatusRunning || fields["claim_token"] != current.Token ||
				fields["claim_generation"] != strconv.FormatInt(current.Generation, 10) {
				t.Fatalf("stale %s changed replacement status/token/generation: %+v", mutation.name, fields)
			}
			if fields["lease_expires_unix"] == "" || fields["lease_expires_at"] == "" {
				t.Fatalf("stale %s removed replacement lease: %+v", mutation.name, fields)
			}
			processing, err := q.rdb.LRange(ctx, claimProcessingKey(current), 0, -1).Result()
			if err != nil || !slices.Equal(processing, beforeProcessing) {
				t.Fatalf("stale %s changed processing receipts: before=%q after=%q err=%v", mutation.name, beforeProcessing, processing, err)
			}
			if score, err := q.rdb.ZScore(ctx, LeaseIndexKey, current.Job.ID).Result(); err != nil || score <= float64(time.Now().Unix()) {
				t.Fatalf("stale %s changed replacement lease index: score=%v err=%v", mutation.name, score, err)
			}
			ready, err := q.rdb.LRange(ctx, QueueKeyFor(&current.Job), 0, -1).Result()
			if err != nil || len(ready) != 0 {
				t.Fatalf("stale %s published ready work: %q err=%v", mutation.name, ready, err)
			}
		})
	}
}

func TestH1PlannerExpiryReadThenReplacementDeferralIsFenced(t *testing.T) {
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
	old, ok, err := q.ClaimForWorker(ctx, 0, worker)
	if err != nil || !ok || old.Job.ID == "" {
		t.Fatalf("claim planner: %+v ok=%v err=%v", old, ok, err)
	}
	if err := q.rdb.HSet(ctx, JobHashPrefix+old.Job.ID, "lease_expires_unix", time.Now().Add(-time.Minute).Unix()).Err(); err != nil {
		t.Fatal(err)
	}
	// This is the reaper's read phase: observe the expired owner and token, then
	// let a replacement generation win before invoking its actual defer mutation.
	observed, err := q.rdb.HGetAll(ctx, JobHashPrefix+old.Job.ID).Result()
	if err != nil || observed["claim_token"] != old.Token || observed["status"] != StatusRunning {
		t.Fatalf("reaper pre-read = %+v err=%v", observed, err)
	}
	if n, err := q.RecoverAbandonedWorkers(ctx); err != nil || n != 1 {
		t.Fatalf("recover planner: %d, %v", n, err)
	}
	if n, err := q.PromoteWatchRetries(ctx, time.Now().Add(24*time.Hour), 1); err != nil || n != 1 {
		t.Fatalf("promote recovered planner retry: %d, %v", n, err)
	}
	current, ok, err := q.ClaimForWorker(ctx, 0, worker)
	if err != nil || !ok || current.Job.ID != old.Job.ID || current.Generation <= old.Generation {
		t.Fatalf("replacement planner: %+v ok=%v err=%v", current, ok, err)
	}
	before, err := q.rdb.HGetAll(ctx, JobHashPrefix+current.Job.ID).Result()
	if err != nil {
		t.Fatal(err)
	}
	if err := q.DeferTrackedClaim(ctx, old, "stale reaper", time.Now()); err == nil {
		t.Error("stale planner defer accepted after replacement ownership")
	}
	child := trackedTestChild(old, old.Job.Path+"/stale-plan.mov")
	if _, err := q.EnqueuePlannedChildrenForClaim(ctx, old, []Job{child}); err == nil {
		t.Error("stale planner published a child after replacement ownership")
	}
	if err := q.CompleteTrackedEmptyPlanForClaim(ctx, old); err == nil {
		t.Error("stale planner published empty coverage after replacement ownership")
	}
	if exists, err := q.rdb.Exists(ctx, JobHashPrefix+child.ID).Result(); err != nil || exists != 0 {
		t.Fatalf("stale planner child exists=%d err=%v", exists, err)
	}
	after, err := q.rdb.HGetAll(ctx, JobHashPrefix+current.Job.ID).Result()
	if err != nil {
		t.Fatal(err)
	}
	if after["status"] != before["status"] || after["claim_token"] != before["claim_token"] ||
		after["claim_generation"] != before["claim_generation"] || after["lease_expires_unix"] != before["lease_expires_unix"] {
		t.Fatalf("stale defer changed replacement planner: before=%+v after=%+v", before, after)
	}
	// No production hook exists between RecoverAbandonedWorkers' read and defer;
	// this deterministic simulation does not exercise that internal ordering.
}

func TestH1LeaseIndexWrongTypeFailsBeforeClaimDeadlineMutation(t *testing.T) {
	for _, mode := range []string{"planner-start", "planner-renew", "child-renew"} {
		mode := mode
		t.Run(mode, func(t *testing.T) {
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
			planner, ok, err := q.ClaimForWorker(ctx, 0, worker)
			if err != nil || !ok {
				t.Fatalf("claim planner: %+v ok=%v err=%v", planner, ok, err)
			}
			var claim Claim
			switch mode {
			case "planner-start", "planner-renew":
				claim = planner
			case "child-renew":
				_, child := h1ReviewTrackedChild(t, q, ctx, worker)
				claim, ok, err = q.ClaimForWorker(ctx, 0, worker)
				if err != nil || !ok || claim.Job.ID != child.ID {
					t.Fatalf("claim child: %+v ok=%v err=%v", claim, ok, err)
				}
				if err := q.MarkClaimRunning(ctx, claim, worker); err != nil {
					t.Fatal(err)
				}
				if err := q.rdb.HSet(ctx, JobHashPrefix+claim.Job.ID, "lease_expires_unix", time.Now().Add(10*time.Second).Unix(), "lease_expires_at", "sentinel").Err(); err != nil {
					t.Fatal(err)
				}
			}
			if err := q.rdb.HSet(ctx, JobHashPrefix+claim.Job.ID, "lease_expires_unix", time.Now().Add(10*time.Second).Unix(), "lease_expires_at", "sentinel").Err(); err != nil {
				t.Fatal(err)
			}
			if err := q.rdb.Del(ctx, LeaseIndexKey).Err(); err != nil {
				t.Fatal(err)
			}
			if err := q.rdb.Set(ctx, LeaseIndexKey, "wrong type", 0).Err(); err != nil {
				t.Fatal(err)
			}
			statusKey := JobHashPrefix + claim.Job.ID
			before, err := q.rdb.HGetAll(ctx, statusKey).Result()
			if err != nil {
				t.Fatal(err)
			}
			var callErr error
			switch mode {
			case "planner-start":
				callErr = q.MarkClaimRunning(ctx, claim, worker)
			default:
				callErr = q.RenewClaim(ctx, claim)
			}
			if callErr == nil {
				t.Errorf("%s succeeded with wrong-type lease index", mode)
			}
			after, err := q.rdb.HGetAll(ctx, statusKey).Result()
			if err != nil {
				t.Fatal(err)
			}
			if after["lease_expires_unix"] != before["lease_expires_unix"] || after["lease_expires_at"] != before["lease_expires_at"] {
				t.Errorf("%s changed hash deadline before failing: before=%+v after=%+v", mode, before, after)
			}
		})
	}
}

func TestH1StaleChildCannotPublishCPUFallback(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	old, current, _ := h1FenceRunningReplacement(t, q, ctx)
	targets := []string{old.Job.Path + "/fallback.mov"}
	child, err := newCPUFallbackSubset(old.Job, targets)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := q.EnqueueCPUFallbackSubsetForClaim(ctx, old, targets, "stale fallback"); err == nil {
		t.Fatal("stale child published CPU fallback after replacement")
	}
	if exists, err := q.rdb.Exists(ctx, JobHashPrefix+child.ID).Result(); err != nil || exists != 0 {
		t.Fatalf("stale fallback status exists=%d err=%v", exists, err)
	}
	if raw, err := q.rdb.LRange(ctx, QueueKeyFor(&child), 0, -1).Result(); err != nil || len(raw) != 0 {
		t.Fatalf("stale fallback queued=%q err=%v", raw, err)
	}
	if status, err := q.rdb.HGet(ctx, JobHashPrefix+current.Job.ID, "status").Result(); err != nil || status != StatusRunning {
		t.Fatalf("replacement child status=%q err=%v", status, err)
	}
}

func TestH1TrackedCompletionACKIsIdempotentAndRetiresCustody(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	worker := watchPlannerWorker()
	_, child := h1ReviewTrackedChild(t, q, ctx, worker)
	claim, ok, err := q.ClaimForWorker(ctx, 0, worker)
	if err != nil || !ok || claim.Job.ID != child.ID {
		t.Fatalf("claim child: %+v ok=%v err=%v", claim, ok, err)
	}
	if err := q.MarkClaimRunning(ctx, claim, worker); err != nil {
		t.Fatal(err)
	}
	if err := q.CompleteClaim(ctx, claim, StatusDone, 1, 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := q.AckClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if err := q.AckClaim(ctx, claim); err != nil {
		t.Fatalf("lost ACK response retry: %v", err)
	}
	fields, err := q.rdb.HGetAll(ctx, JobHashPrefix+child.ID).Result()
	if err != nil || fields["status"] != StatusDone || fields["terminal_token"] != claim.Token ||
		fields["ack_token"] != claim.Token || fields["claim_generation"] != strconv.FormatInt(claim.Generation, 10) {
		t.Fatalf("terminal receipt=%+v err=%v", fields, err)
	}
	if n, err := q.rdb.SCard(ctx, trackedCustodyIndexKey).Result(); err != nil || n != 0 {
		t.Fatalf("transfer custody index retained %d jobs: %v", n, err)
	}
	if n, err := q.rdb.LLen(ctx, claimProcessingKey(claim)).Result(); err != nil || n != 1 {
		// The planner still owns its own processing receipt; only the child was ACKed.
		t.Fatalf("processing receipts after child ACK=%d err=%v", n, err)
	}
}
