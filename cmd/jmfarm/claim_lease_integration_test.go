package main

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/farmqueue"
	"github.com/redis/go-redis/v9"
)

func TestTrackedPlannerLeaseLossStopsDiscoveryBeforePublication(t *testing.T) {
	q, ctx := openBackstopTestQueue(t)
	root := t.TempDir()
	if leader, err := q.AcquireWatchLeadership(ctx, "lease-loss-leader", time.Minute); err != nil || !leader {
		t.Fatalf("acquire watch leadership: leader=%v err=%v", leader, err)
	}
	if _, err := q.StartWatchEpoch(ctx, root, "lease-loss-leader", []string{farmqueue.KindDerivatives}, time.Now()); err != nil {
		t.Fatal(err)
	}
	worker := farmqueue.Worker{
		ID: "lease-loss-planner", Name: "lease-loss-planner", Role: farmqueue.QueueClassServer,
		Capabilities: []string{"cpu", "metadata"},
	}
	if err := q.Heartbeat(ctx, worker); err != nil {
		t.Fatal(err)
	}
	claim, ok, err := q.ClaimForWorker(ctx, 0, worker)
	if err != nil || !ok || claim.Job.TrackedEpoch == "" || !claim.Job.PlanOnly {
		t.Fatalf("tracked planner claim = %+v ok=%v err=%v", claim, ok, err)
	}
	deadline, err := q.MarkClaimRunningWithDeadline(ctx, claim, worker)
	if err != nil {
		t.Fatal(err)
	}

	renewStarted := make(chan struct{})
	releaseRenew := make(chan struct{})
	var renewOnce sync.Once
	guard := startClaimLeaseGuard(context.Background(), deadline, time.Millisecond, nil,
		func(renewCtx context.Context) (time.Time, error) {
			renewOnce.Do(func() { close(renewStarted) })
			select {
			case <-releaseRenew:
				return q.RenewClaimWithDeadline(renewCtx, claim)
			case <-renewCtx.Done():
				return time.Time{}, renewCtx.Err()
			}
		},
		func(err error) bool { return errors.Is(err, farmqueue.ErrClaimLost) })
	t.Cleanup(func() {
		select {
		case <-releaseRenew:
		default:
			close(releaseRenew)
		}
		guard.Stop()
	})

	walkStarted := make(chan struct{})
	var walkOnce sync.Once
	cfg := queueConfig{mount: root, collectTargets: func(collectCtx context.Context, _, _ string, _ int, _ int64) ([]string, error) {
		walkOnce.Do(func() { close(walkStarted) })
		<-collectCtx.Done()
		return nil, collectCtx.Err()
	}}
	jobDone := make(chan error, 1)
	go func() {
		_, _, _, runErr := runJob(guard.Context(), q, nil, cfg, worker, claim.Job, &claim)
		jobDone <- runErr
	}()
	select {
	case <-walkStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("planner did not enter discovery")
	}
	select {
	case <-renewStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("lease renewal did not reach fault gate")
	}

	opt, err := redis.ParseURL(os.Getenv("JM_FARM_TEST_REDIS_URL"))
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opt)
	defer rdb.Close()
	statusKey := farmqueue.JobHashPrefix + claim.Job.ID
	if err := rdb.HSet(ctx, statusKey, "lease_expires_unix", time.Now().Add(-time.Minute).Unix()).Err(); err != nil {
		t.Fatal(err)
	}
	if recovered, err := q.RecoverAbandonedWorkers(ctx); err != nil || recovered != 1 {
		t.Fatalf("recover expired planner: recovered=%d err=%v", recovered, err)
	}
	close(releaseRenew)
	select {
	case <-guard.Context().Done():
	case <-time.After(3 * time.Second):
		t.Fatal("replaced planner kept executing after lease loss")
	}
	select {
	case runErr := <-jobDone:
		if !errors.Is(runErr, context.Canceled) {
			t.Fatalf("stale planner returned %v; want cancellation", runErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stale planner did not stop after discovery resumed")
	}
	if !guard.Lost() {
		t.Fatal("lease guard did not record ownership loss")
	}
	if complete, err := q.TrackedPlanComplete(ctx, claim.Job); err != nil || complete {
		t.Fatalf("stale planner published coverage: complete=%v err=%v", complete, err)
	}
	status, err := rdb.HGetAll(ctx, statusKey).Result()
	if err != nil || status["status"] != "blocked" || status["ack_token"] != "" {
		t.Fatalf("recovered planner status = %+v err=%v; want deferred without stale ACK", status, err)
	}
	if ready, err := rdb.LLen(ctx, farmqueue.QueueKeyFor(&claim.Job)).Result(); err != nil || ready != 0 {
		t.Fatalf("deferred planner ready count = %d err=%v; want zero until promotion", ready, err)
	}
	if processing, err := rdb.LLen(ctx, farmqueue.TrackedProcessingPrefix+worker.ID).Result(); err != nil || processing != 0 {
		t.Fatalf("old planner processing count = %d err=%v; want zero", processing, err)
	}
	if promoted, err := q.PromoteWatchRetries(ctx, time.Now().Add(2*time.Minute), 1); err != nil || promoted != 1 {
		t.Fatalf("promote deferred planner: promoted=%d err=%v; want one", promoted, err)
	}
	if ready, err := rdb.LLen(ctx, farmqueue.QueueKeyFor(&claim.Job)).Result(); err != nil || ready != 1 {
		t.Fatalf("promoted planner ready count = %d err=%v; want one", ready, err)
	}
	if depth, err := q.QueueDepth(ctx); err != nil || depth != 1 {
		t.Fatalf("queue depth after stale planner cancellation = %d err=%v; want one parent only", depth, err)
	}
	successor := farmqueue.Worker{
		ID: "lease-loss-successor", Name: "lease-loss-successor", Role: farmqueue.QueueClassServer,
		Capabilities: []string{"cpu", "metadata"},
	}
	if err := q.Heartbeat(ctx, successor); err != nil {
		t.Fatal(err)
	}
	next, ok, err := q.ClaimForWorker(ctx, 0, successor)
	if err != nil || !ok || next.Job.ID != claim.Job.ID || next.Token == claim.Token {
		t.Fatalf("successor claim = %+v ok=%v err=%v; want same planner with new token", next, ok, err)
	}
}
