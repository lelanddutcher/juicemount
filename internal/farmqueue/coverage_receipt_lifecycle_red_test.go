package farmqueue

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func newCoverageReceiptLifecycleFixture(t *testing.T) (*Client, context.Context, string, WatchEpoch, Claim, Job) {
	t.Helper()
	q, ctx := openWatchEpochTestQueue(t)
	root := t.TempDir()
	if leader, err := q.AcquireWatchLeadership(ctx, "leader", time.Minute); err != nil || !leader {
		t.Fatalf("watch leader: %v %v", leader, err)
	}
	state, err := q.StartWatchEpoch(ctx, root, "leader", []string{KindProxy}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	planner, ok, err := q.ClaimForWorker(ctx, 0, watchPlannerWorker())
	if err != nil || !ok || planner.Token == "" {
		t.Fatalf("claim tracked planner: ok=%v token=%q err=%v", ok, planner.Token, err)
	}
	child := trackedTestChild(planner, root+"/clip.mov")
	if created, err := q.EnqueuePlannedChildrenForClaim(ctx, planner, []Job{child}); err != nil || created != 1 {
		t.Fatalf("publish planned child: created=%d err=%v", created, err)
	}
	if committed, err := q.CommitWatchEpoch(ctx, root, "leader", state.Active); err != nil || !committed {
		t.Fatalf("commit planner coverage: committed=%v err=%v", committed, err)
	}
	if err := q.AckClaim(ctx, planner); err != nil {
		t.Fatalf("acknowledge planner: %v", err)
	}
	return q, ctx, root, state, planner, child
}

func claimCoverageLifecycleChild(t *testing.T, q *Client, ctx context.Context, child Job) Claim {
	t.Helper()
	worker := Worker{ID: "coverage-lifecycle-cpu", Name: "coverage-lifecycle-cpu", Role: QueueClassServer,
		Capabilities: []string{"cpu"}}
	if err := q.Heartbeat(ctx, worker); err != nil {
		t.Fatal(err)
	}
	claim, ok, err := q.ClaimForWorker(ctx, 0, worker)
	if err != nil || !ok || claim.Job.ID != child.ID {
		t.Fatalf("claim tracked child: ok=%v job=%q err=%v", ok, claim.Job.ID, err)
	}
	if err := q.MarkClaimRunning(ctx, claim, worker); err != nil {
		t.Fatalf("mark %s running: %v", child.ID, err)
	}
	return claim
}

func TestH1AckedCPUFallbackRetainsHashWhileRenderParentRuns(t *testing.T) {
	q, ctx, _, _, _, child := newCoverageReceiptLifecycleFixture(t)
	parentClaim := claimCoverageLifecycleChild(t, q, ctx, child)
	targets := []string{child.RetryTargets[0]}
	fallback, created, err := q.EnqueueCPUFallbackSubsetForClaim(ctx, parentClaim, targets, "decoder unavailable")
	if err != nil || !created {
		t.Fatalf("publish CPU fallback grandchild: created=%v err=%v", created, err)
	}
	fallbackClaim := claimCoverageLifecycleChild(t, q, ctx, fallback)
	if err := q.CompleteClaim(ctx, fallbackClaim, StatusDone, 1, 0, ""); err != nil {
		t.Fatalf("complete CPU fallback grandchild: %v", err)
	}
	if err := q.AckClaim(ctx, fallbackClaim); err != nil {
		t.Fatalf("acknowledge CPU fallback grandchild: %v", err)
	}

	parentFields, err := q.rdb.HGetAll(ctx, JobHashPrefix+child.ID).Result()
	if err != nil || parentFields["status"] != StatusRunning {
		t.Fatalf("render parent state = %+v err=%v; want still running", parentFields, err)
	}
	ttl, err := q.rdb.TTL(ctx, JobHashPrefix+fallback.ID).Result()
	if err != nil || ttl != -1 {
		t.Fatalf("ACKed fallback hash TTL = %v err=%v; want persistent while its parent remains running", ttl, err)
	}
	if retired, err := q.RetireTrackedTerminals(ctx, time.Now().Add(2*time.Hour), 1); err != nil || retired != 0 {
		t.Fatalf("maintenance retired fallback before DAG drain: count=%d err=%v", retired, err)
	}
	ttl, err = q.rdb.TTL(ctx, JobHashPrefix+fallback.ID).Result()
	if err != nil || ttl != -1 {
		t.Fatalf("fallback hash TTL after premature-retirement scan = %v err=%v; want persistent", ttl, err)
	}
}

func TestH1BoundedRetirementRemovesDrainedEpochPlannerReceipt(t *testing.T) {
	q, ctx, root, state, planner, child := newCoverageReceiptLifecycleFixture(t)
	plannerReceipt := "receipt:" + planner.Job.ID
	if exists, err := q.rdb.HExists(ctx, watchEpochKey(root), plannerReceipt).Result(); err != nil || !exists {
		t.Fatalf("planner receipt before drain: exists=%v err=%v", exists, err)
	}

	parentClaim := claimCoverageLifecycleChild(t, q, ctx, child)
	fallback, created, err := q.EnqueueCPUFallbackSubsetForClaim(ctx, parentClaim,
		[]string{child.RetryTargets[0]}, "decoder unavailable")
	if err != nil || !created {
		t.Fatalf("publish CPU fallback grandchild: created=%v err=%v", created, err)
	}
	fallbackClaim := claimCoverageLifecycleChild(t, q, ctx, fallback)
	if err := q.CompleteClaim(ctx, fallbackClaim, StatusDone, 1, 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := q.AckClaim(ctx, fallbackClaim); err != nil {
		t.Fatal(err)
	}
	if err := q.CompleteClaim(ctx, parentClaim, StatusDone, 0, 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := q.AckClaim(ctx, parentClaim); err != nil {
		t.Fatal(err)
	}
	if exists, err := q.rdb.HExists(ctx, watchEpochKey(root), plannerReceipt).Result(); err != nil || !exists {
		t.Fatalf("planner receipt disappeared before bounded maintenance: exists=%v err=%v", exists, err)
	}

	for attempt := 0; attempt < 16; attempt++ {
		retired, err := q.RetireTrackedTerminals(ctx, time.Now().Add(time.Duration(attempt+2)*time.Hour), 1)
		if err != nil {
			t.Fatalf("bounded retirement pass %d: %v", attempt+1, err)
		}
		if retired > 1 {
			t.Fatalf("bounded retirement pass %d processed %d jobs; limit was one", attempt+1, retired)
		}
		exists, err := q.rdb.HExists(ctx, watchEpochKey(root), plannerReceipt).Result()
		if err != nil {
			t.Fatal(err)
		}
		if !exists {
			allBounded := true
			for _, id := range []string{child.ID, fallback.ID} {
				ttl, err := q.rdb.TTL(ctx, JobHashPrefix+id).Result()
				if err != nil || ttl > JobTTL+time.Minute {
					t.Fatalf("drained terminal %s TTL = %v err=%v", id, ttl, err)
				}
				allBounded = allBounded && ttl > 0
			}
			if allBounded {
				return
			}
		}
	}
	t.Fatalf("drained committed epoch %s kept its planner receipt or unbounded descendant hashes after 16 bounded passes", state.Active)
}

func TestH1SlowEpochRetainsCoverageAcrossLaterCommits(t *testing.T) {
	q, ctx, root, _, plannerA, childA := newCoverageReceiptLifecycleFixture(t)
	for _, label := range []string{"B", "C"} {
		state, err := q.StartWatchEpoch(ctx, root, "leader", []string{KindProxy}, time.Now())
		if err != nil {
			t.Fatalf("start epoch %s: %v", label, err)
		}
		planner, ok, err := q.ClaimForWorker(ctx, 0, watchPlannerWorker())
		if err != nil || !ok {
			t.Fatalf("claim epoch %s planner: ok=%v err=%v", label, ok, err)
		}
		if err := q.CompleteTrackedEmptyPlanForClaim(ctx, planner); err != nil {
			t.Fatalf("complete epoch %s empty plan: %v", label, err)
		}
		if committed, err := q.CommitWatchEpoch(ctx, root, "leader", state.Active); err != nil || !committed {
			t.Fatalf("commit epoch %s: committed=%v err=%v", label, committed, err)
		}
		if err := q.AckClaim(ctx, planner); err != nil {
			t.Fatalf("ack epoch %s planner: %v", label, err)
		}
	}
	for attempt := 0; attempt < 8; attempt++ {
		if _, err := q.RetireTrackedTerminals(ctx, time.Now().Add(time.Duration(attempt+2)*time.Hour), 16); err != nil {
			t.Fatalf("retire after later commits: %v", err)
		}
	}
	if exists, err := q.rdb.HExists(ctx, watchEpochKey(root), "receipt:"+plannerA.Job.ID).Result(); err != nil || !exists {
		t.Fatalf("slow epoch A lost coverage after B/C: exists=%v err=%v", exists, err)
	}
	if ttl, err := q.rdb.TTL(ctx, JobHashPrefix+childA.ID).Result(); err != nil || ttl != -1 {
		t.Fatalf("slow epoch A child TTL = %v err=%v; want persistent", ttl, err)
	}
}

func TestH1EmptyPlanRetiresOnlyAfterCommitAndPlannerAck(t *testing.T) {
	for _, ackFirst := range []bool{false, true} {
		name := "commit-first"
		if ackFirst {
			name = "ack-first"
		}
		t.Run(name, func(t *testing.T) {
			q, ctx := openWatchEpochTestQueue(t)
			root := t.TempDir()
			if leader, err := q.AcquireWatchLeadership(ctx, "leader", time.Minute); err != nil || !leader {
				t.Fatalf("watch leader: %v %v", leader, err)
			}
			state, err := q.StartWatchEpoch(ctx, root, "leader", []string{KindProxy}, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			planner, ok, err := q.ClaimForWorker(ctx, 0, watchPlannerWorker())
			if err != nil || !ok {
				t.Fatalf("claim empty planner: ok=%v err=%v", ok, err)
			}
			if err := q.CompleteTrackedEmptyPlanForClaim(ctx, planner); err != nil {
				t.Fatal(err)
			}
			commit := func() {
				t.Helper()
				if committed, err := q.CommitWatchEpoch(ctx, root, "leader", state.Active); err != nil || !committed {
					t.Fatalf("commit empty epoch: committed=%v err=%v", committed, err)
				}
			}
			ack := func() {
				t.Helper()
				if err := q.AckClaim(ctx, planner); err != nil {
					t.Fatal(err)
				}
			}
			if ackFirst {
				ack()
			} else {
				commit()
			}
			if _, err := q.RetireTrackedTerminals(ctx, time.Now().Add(time.Hour), 4); err != nil {
				t.Fatal(err)
			}
			receipt := "receipt:" + planner.Job.ID
			if exists, err := q.rdb.HExists(ctx, watchEpochKey(root), receipt).Result(); err != nil || !exists {
				t.Fatalf("empty plan retired before both conditions: exists=%v err=%v", exists, err)
			}
			if ackFirst {
				commit()
			} else {
				ack()
			}
			for attempt := 0; attempt < 4; attempt++ {
				if _, err := q.RetireTrackedTerminals(ctx, time.Now().Add(time.Duration(attempt+2)*time.Hour), 4); err != nil {
					t.Fatal(err)
				}
			}
			if exists, err := q.rdb.HExists(ctx, watchEpochKey(root), receipt).Result(); err != nil || exists {
				t.Fatalf("complete empty plan kept receipt: exists=%v err=%v", exists, err)
			}
			if ttl, err := q.rdb.TTL(ctx, JobHashPrefix+planner.Job.ID).Result(); err != nil || ttl <= 0 || ttl > JobTTL+time.Minute {
				t.Fatalf("drained planner TTL = %v err=%v; want bounded status history", ttl, err)
			}
		})
	}
}

func TestH1LegacyChildCanAckAfterNewEpochStarts(t *testing.T) {
	q, ctx, root, _, plannerA, childA := newCoverageReceiptLifecycleFixture(t)
	childClaim := claimCoverageLifecycleChild(t, q, ctx, childA)
	if err := q.CompleteClaim(ctx, childClaim, StatusDone, 1, 0, ""); err != nil {
		t.Fatal(err)
	}
	// Model a pre-ledger v2 family carried through activation. Its receipt is
	// real, but it has no per-family coverage ledger or protocol marker.
	if err := q.rdb.Del(ctx, coverageLedgerKey(plannerA.Job.ID), coverageMembersKey(plannerA.Job.ID)).Err(); err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.HDel(ctx, watchEpochKey(root), "coverage_protocol", "committed:"+plannerA.Job.ID).Err(); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{plannerA.Job.ID, childA.ID} {
		if err := q.rdb.HDel(ctx, JobHashPrefix+id, "coverage_protocol").Err(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := q.StartWatchEpoch(ctx, root, "leader", []string{KindProxy}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := q.AckClaim(ctx, childClaim); err != nil {
		t.Fatalf("legacy child ACK stranded by new root protocol: %v", err)
	}
	if exists, err := q.rdb.HExists(ctx, watchEpochKey(root), "receipt:"+plannerA.Job.ID).Result(); err != nil || !exists {
		t.Fatalf("unresolved legacy receipt retired: exists=%v err=%v", exists, err)
	}
}

func TestH1CompletionReplayDoesNotClearDrainingRetention(t *testing.T) {
	q, ctx, root, _, planner, child := newCoverageReceiptLifecycleFixture(t)
	parentClaim := claimCoverageLifecycleChild(t, q, ctx, child)
	fallback, created, err := q.EnqueueCPUFallbackSubsetForClaim(ctx, parentClaim,
		[]string{child.RetryTargets[0]}, "decoder unavailable")
	if err != nil || !created {
		t.Fatalf("publish fallback: created=%v err=%v", created, err)
	}
	fallbackClaim := claimCoverageLifecycleChild(t, q, ctx, fallback)
	for _, claim := range []Claim{fallbackClaim, parentClaim} {
		if err := q.CompleteClaim(ctx, claim, StatusDone, 1, 0, ""); err != nil {
			t.Fatal(err)
		}
		if err := q.AckClaim(ctx, claim); err != nil {
			t.Fatal(err)
		}
	}
	// A later epoch keeps this worker's processing list alive, so duplicate
	// completion can reach the terminal-idempotency branch during A's GC.
	if _, err := q.StartWatchEpoch(ctx, root, "leader", []string{KindProxy}, time.Now()); err != nil {
		t.Fatal(err)
	}
	worker := Worker{ID: "coverage-lifecycle-cpu", Name: "coverage-lifecycle-cpu", Role: QueueClassServer,
		Capabilities: []string{"cpu", "metadata"}}
	if _, ok, err := q.ClaimForWorker(ctx, 0, worker); err != nil || !ok {
		t.Fatalf("claim unrelated planner: ok=%v err=%v", ok, err)
	}
	if _, err := q.retireCoverageLedgerOnce(ctx, planner.Job.ID, time.Now().Add(time.Hour), 1); err != nil {
		t.Fatal(err)
	}
	if ttl, err := q.rdb.TTL(ctx, JobHashPrefix+child.ID).Result(); err != nil || ttl <= 0 {
		t.Fatalf("first drained member TTL = %v err=%v", ttl, err)
	}
	if err := q.CompleteClaim(ctx, parentClaim, StatusDone, 1, 0, ""); err != nil {
		t.Fatalf("replay completed child: %v", err)
	}
	if _, err := q.retireCoverageLedgerOnce(ctx, planner.Job.ID, time.Now().Add(2*time.Hour), 1); err != nil {
		t.Fatal(err)
	}
	if ttl, err := q.rdb.TTL(ctx, JobHashPrefix+child.ID).Result(); err != nil || ttl <= 0 || ttl > JobTTL+time.Minute {
		t.Fatalf("replayed completed child TTL = %v err=%v; want bounded retention", ttl, err)
	}
}

func TestH1RegistrationPrefixReplaysWithoutDuplicateChild(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	root := t.TempDir()
	if leader, err := q.AcquireWatchLeadership(ctx, "leader", time.Minute); err != nil || !leader {
		t.Fatalf("watch leader: %v %v", leader, err)
	}
	state, err := q.StartWatchEpoch(ctx, root, "leader", []string{KindProxy}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	planner, ok, err := q.ClaimForWorker(ctx, 0, watchPlannerWorker())
	if err != nil || !ok {
		t.Fatalf("claim planner: ok=%v err=%v", ok, err)
	}
	child := trackedTestChild(planner, root+"/clip.mov")
	if err := q.rdb.HSet(ctx, coverageLedgerKey(planner.Job.ID),
		"parent_id", planner.Job.ID, "epoch", state.Active, "root", root,
		"coverage_protocol", "1", "state", "open", "open_count", "0",
		"committed", "0", "planner_ack", "0").Err(); err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.ZAdd(ctx, coverageMembersKey(planner.Job.ID), redis.Z{Score: 0, Member: child.ID}).Err(); err != nil {
		t.Fatal(err)
	}
	if created, err := q.EnqueuePlannedChildrenForClaim(ctx, planner, []Job{child}); err != nil || created != 1 {
		t.Fatalf("replay partial registration: created=%d err=%v", created, err)
	}
	if n, err := q.rdb.LLen(ctx, QueueKeyFor(&child)).Result(); err != nil || n != 1 {
		t.Fatalf("child ready depth after repair = %d err=%v", n, err)
	}
	if count, err := q.rdb.HGet(ctx, coverageLedgerKey(planner.Job.ID), "open_count").Int(); err != nil || count != 1 {
		t.Fatalf("repaired open count = %d err=%v", count, err)
	}
	if _, err := q.rdb.ZScore(ctx, watchRetireKey, coverageRetirePrefix+planner.Job.ID).Result(); err != nil {
		t.Fatalf("receipted family has no durable retirement index: %v", err)
	}
	if created, err := q.EnqueuePlannedChildrenForClaim(ctx, planner, []Job{child}); err != nil || created != 0 {
		t.Fatalf("replay after repair: created=%d err=%v", created, err)
	}
}

func TestH1AckPrefixReconcilesSettledMemberCount(t *testing.T) {
	q, ctx, root, _, planner, child := newCoverageReceiptLifecycleFixture(t)
	claim := claimCoverageLifecycleChild(t, q, ctx, child)
	if err := q.CompleteClaim(ctx, claim, StatusDone, 1, 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := q.AckClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	// Recreate the durable prefix after ZADD member=settled, before the count
	// update. The exact ACK proof and processing removal have already landed.
	if err := q.rdb.HSet(ctx, coverageLedgerKey(planner.Job.ID),
		"open_count", "1", "settling:"+child.ID, "pending:"+claim.Token).Err(); err != nil {
		t.Fatal(err)
	}
	if err := q.AckClaim(ctx, claim); err != nil {
		t.Fatalf("replay partial ACK: %v", err)
	}
	if count, err := q.rdb.HGet(ctx, coverageLedgerKey(planner.Job.ID), "open_count").Int(); err != nil || count != 0 {
		t.Fatalf("reconciled open count = %d err=%v", count, err)
	}
	for attempt := 0; attempt < 4; attempt++ {
		if _, err := q.RetireTrackedTerminals(ctx, time.Now().Add(time.Duration(attempt+2)*time.Hour), 8); err != nil {
			t.Fatal(err)
		}
	}
	if exists, err := q.rdb.HExists(ctx, watchEpochKey(root), "receipt:"+planner.Job.ID).Result(); err != nil || exists {
		t.Fatalf("repaired ACK kept drained receipt: exists=%v err=%v", exists, err)
	}
}

func TestH1AckPrefixRecoversAfterCallerDisappears(t *testing.T) {
	q, ctx, root, _, planner, child := newCoverageReceiptLifecycleFixture(t)
	claim := claimCoverageLifecycleChild(t, q, ctx, child)
	if err := q.CompleteClaim(ctx, claim, StatusDone, 1, 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := q.AckClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.HSet(ctx, coverageLedgerKey(planner.Job.ID),
		"open_count", "1", "settling:"+child.ID, "pending:"+claim.Token).Err(); err != nil {
		t.Fatal(err)
	}
	// No AckClaim replay follows: the worker is gone and its processing receipt
	// was already removed before the interrupted ledger-count update.
	for attempt := 0; attempt < 8; attempt++ {
		if _, err := q.RecoverAbandonedWorkers(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := q.RetireTrackedTerminals(ctx, time.Now().Add(time.Duration(attempt+2)*time.Hour), 8); err != nil {
			t.Fatal(err)
		}
	}
	if exists, err := q.rdb.HExists(ctx, watchEpochKey(root), "receipt:"+planner.Job.ID).Result(); err != nil || exists {
		t.Fatalf("orphaned ACK prefix kept drained receipt: exists=%v err=%v", exists, err)
	}
	if count, err := q.rdb.HGet(ctx, coverageLedgerKey(planner.Job.ID), "open_count").Int(); err != nil || count != 0 {
		t.Fatalf("orphaned ACK prefix open count = %d err=%v", count, err)
	}
}

func TestH1AckPrefixRejectsMismatchedPendingIdentity(t *testing.T) {
	q, ctx, root, _, planner, child := newCoverageReceiptLifecycleFixture(t)
	claim := claimCoverageLifecycleChild(t, q, ctx, child)
	if err := q.CompleteClaim(ctx, claim, StatusDone, 1, 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := q.AckClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.HSet(ctx, coverageLedgerKey(planner.Job.ID),
		"open_count", "1", "settling:"+child.ID, "pending:not-the-ack-token").Err(); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 4; attempt++ {
		if _, err := q.RetireTrackedTerminals(ctx, time.Now().Add(time.Duration(attempt+2)*time.Hour), 8); err != nil {
			t.Fatal(err)
		}
	}
	if exists, err := q.rdb.HExists(ctx, watchEpochKey(root), "receipt:"+planner.Job.ID).Result(); err != nil || !exists {
		t.Fatalf("mismatched ACK prefix retired coverage: exists=%v err=%v", exists, err)
	}
	if count, err := q.rdb.HGet(ctx, coverageLedgerKey(planner.Job.ID), "open_count").Int(); err != nil || count != 1 {
		t.Fatalf("mismatched ACK prefix changed open count: count=%d err=%v", count, err)
	}
}

func TestH1PlannerAckPrefixRecoversWithoutCaller(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	root := t.TempDir()
	if leader, err := q.AcquireWatchLeadership(ctx, "leader", time.Minute); err != nil || !leader {
		t.Fatalf("watch leader: %v %v", leader, err)
	}
	state, err := q.StartWatchEpoch(ctx, root, "leader", []string{KindProxy}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	planner, ok, err := q.ClaimForWorker(ctx, 0, watchPlannerWorker())
	if err != nil || !ok {
		t.Fatalf("claim empty planner: ok=%v err=%v", ok, err)
	}
	if err := q.CompleteTrackedEmptyPlanForClaim(ctx, planner); err != nil {
		t.Fatal(err)
	}
	if committed, err := q.CommitWatchEpoch(ctx, root, "leader", state.Active); err != nil || !committed {
		t.Fatalf("commit empty plan: committed=%v err=%v", committed, err)
	}
	if err := q.AckClaim(ctx, planner); err != nil {
		t.Fatal(err)
	}
	// Recreate the committed ACK prefix before the ledger flag update. There
	// is no worker processing receipt left to replay this ACK after a crash.
	if err := q.rdb.HSet(ctx, coverageLedgerKey(planner.Job.ID), "planner_ack", "0").Err(); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 8; attempt++ {
		if _, err := q.RecoverAbandonedWorkers(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := q.RetireTrackedTerminals(ctx, time.Now().Add(time.Duration(attempt+2)*time.Hour), 8); err != nil {
			t.Fatal(err)
		}
	}
	if exists, err := q.rdb.HExists(ctx, watchEpochKey(root), "receipt:"+planner.Job.ID).Result(); err != nil || exists {
		t.Fatalf("orphaned planner ACK kept drained receipt: exists=%v err=%v", exists, err)
	}
}
