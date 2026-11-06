package farmqueue

import (
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// A legacy record must not expire while queued or running. Retention starts at
// the terminal write. A record that already expired under an older producer's
// enqueue-time TTL is rebuilt and re-indexed by the claim instead of leaking as
// a bare, unindexed hash with no TTL.
func TestLegacyRetentionStartsAtTerminalState(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	a := legacyFenceWorker("legacy-retention-a")
	job := legacyFenceJob("/jfs/legacy-retention/queued.mov")
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	key := JobHashPrefix + job.ID
	if ttl, err := q.rdb.TTL(ctx, key).Result(); err != nil || ttl != -1 {
		t.Fatalf("queued record TTL = %v, %v; want none", ttl, err)
	}

	// An older producer's record expired while the job was still queued, and a
	// Manager refresh pruned its index member.
	if err := q.rdb.Del(ctx, key).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := q.ListJobs(ctx, 50); err != nil {
		t.Fatal(err)
	}
	if _, err := q.rdb.ZScore(ctx, JobIndexKey, job.ID).Result(); err != redis.Nil {
		t.Fatalf("expired record still indexed: %v", err)
	}
	claim := legacyFenceStart(t, q, ctx, a)
	status, err := q.rdb.HGetAll(ctx, key).Result()
	if err != nil || status["id"] != job.ID || status["path"] != job.Path ||
		status["kinds"] != KindDerivatives || status["status"] != StatusRunning {
		t.Fatalf("running record = %+v, %v; want rebuilt display fields", status, err)
	}
	if ttl, err := q.rdb.TTL(ctx, key).Result(); err != nil || ttl != -1 {
		t.Fatalf("running record TTL = %v, %v; want none", ttl, err)
	}
	if _, err := q.rdb.ZScore(ctx, JobIndexKey, job.ID).Result(); err != nil {
		t.Fatalf("running record not re-indexed: %v", err)
	}

	if err := q.CompleteClaim(ctx, claim, StatusDone, 1, 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := q.AckClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if ttl, err := q.rdb.TTL(ctx, key).Result(); err != nil || ttl <= JobTTL-time.Minute || ttl > JobTTL {
		t.Fatalf("terminal record TTL = %v, %v; want JobTTL from completion", ttl, err)
	}
	jobs, err := q.ListJobs(ctx, 50)
	if err != nil || len(jobs) != 1 || jobs[0].ID != job.ID || jobs[0].Status != StatusDone || jobs[0].Path != job.Path {
		t.Fatalf("listed jobs = %+v, %v; want the finished job", jobs, err)
	}
}

// A record created by an older producer still carries its enqueue-time TTL.
// Claiming it clears that TTL so a long run cannot outlive it.
func TestLegacyClaimClearsOlderProducerTTL(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	a := legacyFenceWorker("legacy-retention-old")
	job := legacyFenceJob("/jfs/legacy-retention/old.mov")
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	key := JobHashPrefix + job.ID
	if err := q.rdb.Expire(ctx, key, time.Hour).Err(); err != nil {
		t.Fatal(err)
	}
	claim := legacyFenceStart(t, q, ctx, a)
	if ttl, err := q.rdb.TTL(ctx, key).Result(); err != nil || ttl != -1 {
		t.Fatalf("claimed record TTL = %v, %v; want cleared", ttl, err)
	}
	if err := q.RequeueClaimSameRoute(ctx, claim, "test requeue"); err != nil {
		t.Fatal(err)
	}
	if ttl, err := q.rdb.TTL(ctx, key).Result(); err != nil || ttl != -1 {
		t.Fatalf("requeued record TTL = %v, %v; want none", ttl, err)
	}
}

// ID-only status writers must not recreate a record that no longer exists.
func TestLegacyIDOnlyUpdateDoesNotRecreateMissingRecord(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	for name, write := range map[string]func(string) error{
		"running":    func(id string) error { return q.MarkRunning(ctx, id) },
		"dispatched": func(id string) error { return q.MarkDispatched(ctx, id) },
		"done":       func(id string) error { return q.MarkDone(ctx, id, 1, 0) },
		"failed":     func(id string) error { return q.MarkFailed(ctx, id, "x") },
		"canceled":   func(id string) error { return q.MarkCanceled(ctx, id, 0, 0) },
	} {
		id := "missing-" + name
		if err := write(id); !errors.Is(err, ErrClaimLost) {
			t.Fatalf("%s on a missing record = %v; want ErrClaimLost", name, err)
		}
		if n, err := q.rdb.Exists(ctx, JobHashPrefix+id).Result(); err != nil || n != 0 {
			t.Fatalf("%s recreated the missing record: exists=%d err=%v", name, n, err)
		}
	}
	job := legacyFenceJob("/jfs/legacy-retention/id-only.mov")
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := q.MarkDone(ctx, job.ID, 1, 0); err != nil {
		t.Fatal(err)
	}
	if ttl, err := q.rdb.TTL(ctx, JobHashPrefix+job.ID).Result(); err != nil || ttl <= 0 {
		t.Fatalf("ID-only terminal TTL = %v, %v; want JobTTL", ttl, err)
	}
}
