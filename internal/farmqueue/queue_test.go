package farmqueue

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestJobStatusRoundTrip guards the HASH<->struct field mapping (the wire
// contract a producer reads back). No Redis needed — exercises toMap/fromMap.
func TestJobStatusRoundTrip(t *testing.T) {
	in := JobStatus{
		ID: "abc123", Status: StatusRunning, Path: "/jfs/a/b", Kinds: "proxy,transcript",
		Producer: "manager", EnqueuedAt: "2026-06-25T12:00:00Z",
		StartedAt: "2026-06-25T12:00:05Z", Processed: 7, Failed: 1, Error: "boom",
		Worker: "gpu-1", TargetWorker: "gpu-1", Backend: "hevc_vaapi", QueueClass: QueueClassRender,
		Attempts: 3, HardwareFailures: 1,
		ParentID: "parent-123",
	}
	// toMap stores ints as strings (Redis HASH is all-strings); reconstitute
	// via a string map mirroring HGETALL.
	m := in.toMap()
	sm := make(map[string]string, len(m))
	for k, v := range m {
		s, ok := v.(string)
		if !ok {
			t.Fatalf("field %q is not a string in the HASH map: %T", k, v)
		}
		sm[k] = s
	}
	got := jobStatusFromMap(sm)
	if got != in {
		t.Fatalf("round-trip mismatch:\n in=%+v\nout=%+v", in, got)
	}
}

// TestJobStatusOptionalFieldsOmitted ensures empty optional fields don't get
// written (so HGETALL on a fresh queued job has no stray started_at/error).
func TestJobStatusOptionalFieldsOmitted(t *testing.T) {
	in := JobStatus{ID: "x", Status: StatusQueued, Path: "/jfs/x", Producer: "manager"}
	m := in.toMap()
	for _, k := range []string{"started_at", "finished_at", "error", "parent_id"} {
		if _, present := m[k]; present {
			t.Errorf("optional field %q should be omitted when empty", k)
		}
	}
	// processed/failed always present (counters default to 0).
	if m["processed"] != "0" || m["failed"] != "0" {
		t.Errorf("counters should default to 0, got processed=%v failed=%v", m["processed"], m["failed"])
	}
}

// TestNewJob stamps an id, timestamp, and carries kinds/producer.
func TestNewJob(t *testing.T) {
	j := NewJob("/jfs/clip.mov", []string{KindProxy, KindTranscript}, "openloupe")
	if j.ID == "" || j.EnqueuedAt == "" {
		t.Fatalf("NewJob must stamp id + enqueued_at, got %+v", j)
	}
	if j.Producer != "openloupe" || len(j.Kinds) != 2 {
		t.Fatalf("NewJob carried fields wrong: %+v", j)
	}
	if NewID() == NewID() {
		t.Error("NewID should not collide")
	}
}

func TestListJobsPrunesExpiredIndexMembers(t *testing.T) {
	metaURL := os.Getenv("JM_FARM_TEST_REDIS_URL")
	if metaURL == "" || os.Getenv("JM_FARM_TEST_REDIS_FLUSH") != "1" {
		t.Skip("set JM_FARM_TEST_REDIS_URL to an isolated Redis and JM_FARM_TEST_REDIS_FLUSH=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	q, err := Open(metaURL)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err := q.rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	missingID := "expired-index-member"
	liveID := "live-index-member"
	if err := q.rdb.ZAdd(ctx, JobIndexKey,
		redis.Z{Score: 2, Member: missingID},
		redis.Z{Score: 1, Member: liveID},
	).Err(); err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.HSet(ctx, JobHashPrefix+liveID, map[string]any{
		"id": liveID, "status": StatusQueued, "path": "/jfs/live",
	}).Err(); err != nil {
		t.Fatal(err)
	}

	jobs, err := q.ListJobs(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].ID != liveID {
		t.Fatalf("jobs = %+v, want only %q", jobs, liveID)
	}
	if n, err := q.rdb.ZScore(ctx, JobIndexKey, missingID).Result(); err != redis.Nil || n != 0 {
		t.Fatalf("expired index member remains: score=%v err=%v", n, err)
	}
}

func TestPruneExpiredJobIndexRemovesOnlyMissingHashes(t *testing.T) {
	metaURL := os.Getenv("JM_FARM_TEST_REDIS_URL")
	if metaURL == "" || os.Getenv("JM_FARM_TEST_REDIS_FLUSH") != "1" {
		t.Skip("set JM_FARM_TEST_REDIS_URL to an isolated Redis and JM_FARM_TEST_REDIS_FLUSH=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	q, err := Open(metaURL)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err := q.rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.ZAdd(ctx, JobIndexKey,
		redis.Z{Score: 1, Member: "missing-old"},
		redis.Z{Score: 2, Member: "live-old"},
	).Err(); err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.HSet(ctx, JobHashPrefix+"live-old", "status", StatusQueued).Err(); err != nil {
		t.Fatal(err)
	}
	removed, err := q.PruneExpiredJobIndex(ctx, 100)
	if err != nil || removed != 1 {
		t.Fatalf("removed=%d err=%v, want one missing member", removed, err)
	}
	if _, err := q.rdb.ZScore(ctx, JobIndexKey, "missing-old").Result(); err != redis.Nil {
		t.Fatalf("missing index member remains: %v", err)
	}
	if _, err := q.rdb.ZScore(ctx, JobIndexKey, "live-old").Result(); err != nil {
		t.Fatalf("live index member removed: %v", err)
	}
}
