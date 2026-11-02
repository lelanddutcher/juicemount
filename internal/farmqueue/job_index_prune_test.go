package farmqueue

import (
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9"
)

// Old live members at the front of the index must not stop the sweep from
// reaching expired members behind them.
func TestPruneExpiredJobIndexAdvancesPastLiveEntries(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("held-%03d", i)
		if err := q.rdb.HSet(ctx, JobHashPrefix+id, "status", StatusQueued, "held_token", "x").Err(); err != nil {
			t.Fatal(err)
		}
		if err := q.rdb.ZAdd(ctx, JobIndexKey, redis.Z{Score: float64(i), Member: id}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 500; i++ {
		id := fmt.Sprintf("expired-%03d", i)
		if err := q.rdb.ZAdd(ctx, JobIndexKey, redis.Z{Score: float64(1000 + i), Member: id}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	total := 0
	for k := 0; k < 10; k++ {
		n, err := q.PruneExpiredJobIndex(ctx, 100)
		if err != nil {
			t.Fatal(err)
		}
		total += n
	}
	if total != 500 {
		t.Fatalf("pruned %d expired members in 10 bounded calls; want 500", total)
	}
	members, err := q.rdb.ZRange(ctx, JobIndexKey, 0, -1).Result()
	if err != nil || len(members) != 100 {
		t.Fatalf("index after sweep = %d members, %v; want the 100 live ones", len(members), err)
	}
	for _, id := range members {
		if ttl, err := q.rdb.TTL(ctx, JobHashPrefix+id).Result(); err != nil || ttl != -1 {
			t.Fatalf("held record %s TTL = %v, %v; want untouched", id, ttl, err)
		}
	}
}

// In a mixed-version farm an older worker finishes a job without starting its
// retention. The index sweep gives that terminal record its JobTTL and leaves
// queued, held and tracked records alone.
func TestPruneExpiredJobIndexStartsRetentionForOlderTerminalRecords(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	records := map[string][]any{
		"old-done":     {"status", StatusDone},
		"old-dispatch": {"status", StatusDispatched},
		"queued":       {"status", StatusQueued},
		"held":         {"status", StatusFailed, "held_token", "x"},
		"tracked":      {"status", StatusDone, "tracked_epoch", "e1"},
	}
	score := 0.0
	for id, fields := range records {
		if err := q.rdb.HSet(ctx, JobHashPrefix+id, fields...).Err(); err != nil {
			t.Fatal(err)
		}
		score++
		if err := q.rdb.ZAdd(ctx, JobIndexKey, redis.Z{Score: score, Member: id}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := q.PruneExpiredJobIndex(ctx, 100); err != nil || n != 0 {
		t.Fatalf("prune = %d, %v; want nothing removed", n, err)
	}
	for id := range records {
		ttl, err := q.rdb.TTL(ctx, JobHashPrefix+id).Result()
		if err != nil {
			t.Fatal(err)
		}
		wantTTL := id == "old-done" || id == "old-dispatch"
		if wantTTL != (ttl > 0) {
			t.Fatalf("%s TTL = %v; want retention=%v", id, ttl, wantTTL)
		}
	}
}
