package farmqueue

import (
	"testing"
	"time"
)

// A transient Redis error during ACK must not leave completed tracked work
// stuck merely because its worker heartbeat is still present.
func TestH1LiveWorkerTerminalAckFailureIsRecoveredWithoutReplay(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	worker := watchPlannerWorker()
	parent, child := h1ReviewTrackedChild(t, q, ctx, worker)
	if committed, err := q.CommitWatchEpoch(ctx, parent.Job.Path, "leader", parent.Job.TrackedEpoch); err != nil || !committed {
		t.Fatalf("commit planner coverage: committed=%v err=%v", committed, err)
	}
	if err := q.AckClaim(ctx, parent); err != nil {
		t.Fatalf("acknowledge planner: %v", err)
	}

	claim, ok, err := q.ClaimForWorker(ctx, 0, worker)
	if err != nil || !ok || claim.Job.ID != child.ID {
		t.Fatalf("claim child: ok=%v job=%q err=%v", ok, claim.Job.ID, err)
	}
	if err := q.MarkClaimRunning(ctx, claim, worker); err != nil {
		t.Fatalf("mark child running: %v", err)
	}
	if err := q.CompleteClaim(ctx, claim, StatusDone, 7, 2, ""); err != nil {
		t.Fatalf("complete child: %v", err)
	}
	if ttl, err := q.rdb.TTL(ctx, JobHashPrefix+claim.Job.ID).Result(); err != nil || ttl != -1 {
		t.Errorf("unacknowledged terminal child TTL = %v, %v; want no expiry while custody remains", ttl, err)
	}

	processingKey := claimProcessingKey(claim)
	indexKey := claimProcessingIndex(claim)
	if err := q.rdb.Del(ctx, indexKey).Err(); err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.Set(ctx, indexKey, "injected transient ACK fault", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := q.AckClaim(ctx, claim); err == nil {
		t.Fatal("ACK unexpectedly succeeded while its processing index had the wrong type")
	}
	if heartbeat, err := q.rdb.Exists(ctx, WorkerPrefix+worker.ID).Result(); err != nil || heartbeat != 1 {
		t.Fatalf("worker heartbeat = %d, %v; want live", heartbeat, err)
	}
	processing, err := q.rdb.LRange(ctx, processingKey, 0, -1).Result()
	if err != nil || len(processing) != 1 || processing[0] != claim.Raw {
		t.Fatalf("failed ACK processing receipts = %q, %v; want exact claim receipt", processing, err)
	}

	if err := q.rdb.Del(ctx, indexKey).Err(); err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.SAdd(ctx, indexKey, worker.ID).Err(); err != nil {
		t.Fatal(err)
	}
	if recovered, err := q.RecoverAbandonedWorkers(ctx); err != nil || recovered != 1 {
		t.Fatalf("live worker terminal ACK recovery = %d, %v; want one receipt retired", recovered, err)
	}
	processing, err = q.rdb.LRange(ctx, processingKey, 0, -1).Result()
	if err != nil || len(processing) != 0 {
		t.Fatalf("processing receipts after recovery = %q, %v; want none", processing, err)
	}
	depth, err := q.rdb.LLen(ctx, QueueKeyFor(&claim.Job)).Result()
	if err != nil || depth != 0 {
		t.Fatalf("ready depth after recovery = %d, %v; completed child was replayed", depth, err)
	}
	fields, err := q.rdb.HGetAll(ctx, JobHashPrefix+claim.Job.ID).Result()
	if err != nil || fields["status"] != StatusDone || fields["processed"] != "7" || fields["failed"] != "2" ||
		fields["terminal_token"] != claim.Token || fields["terminal_owner"] != worker.ID {
		t.Fatalf("terminal proof after recovery = %+v, %v; want original completion and counters", fields, err)
	}
	if heartbeat, err := q.rdb.Exists(ctx, WorkerPrefix+worker.ID).Result(); err != nil || heartbeat != 1 {
		t.Fatalf("recovery changed worker heartbeat = %d, %v; want still live", heartbeat, err)
	}
	if ttl, err := q.rdb.TTL(ctx, JobHashPrefix+claim.Job.ID).Result(); err != nil || ttl != -1 {
		t.Fatalf("terminal child TTL after recovery = %v, %v; want proof retained until coverage retirement", ttl, err)
	}
	for attempt := 0; attempt < 4; attempt++ {
		if _, err := q.RetireTrackedTerminals(ctx, time.Now().Add(time.Duration(attempt+2)*time.Hour), 8); err != nil {
			t.Fatal(err)
		}
	}
	if ttl, err := q.rdb.TTL(ctx, JobHashPrefix+claim.Job.ID).Result(); err != nil || ttl <= 0 || ttl > JobTTL+time.Minute {
		t.Fatalf("terminal child TTL after coverage retirement = %v, %v; want bounded retention", ttl, err)
	}
}
