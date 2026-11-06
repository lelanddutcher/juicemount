package farmqueue

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
)

// testJobIndexPruneLock is spelled out so these tests also compile against
// code that predates the sweep lock.
const testJobIndexPruneLock = "juicefarm:jobs:prune-lock"

// sweepAgain releases the once-per-interval sweep lock so a test can run the
// next sweep immediately.
func sweepAgain(t *testing.T, ctx context.Context, q *Client) {
	t.Helper()
	if err := q.rdb.Del(ctx, testJobIndexPruneLock).Err(); err != nil {
		t.Fatal(err)
	}
}

func addExpiredIndexMembers(t *testing.T, ctx context.Context, q *Client, prefix string, n int, score float64) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := q.rdb.ZAdd(ctx, JobIndexKey, redis.Z{Score: score + float64(i), Member: fmt.Sprintf("%s-%03d", prefix, i)}).Err(); err != nil {
			t.Fatal(err)
		}
	}
}

// commandRecorder records every command name the client sends, including
// pipelined ones.
type commandRecorder struct {
	mu    sync.Mutex
	names []string
}

func (r *commandRecorder) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) { return next(ctx, network, addr) }
}

func (r *commandRecorder) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		r.record(cmd)
		return next(ctx, cmd)
	}
}

func (r *commandRecorder) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			r.record(cmd)
		}
		return next(ctx, cmds)
	}
}

func (r *commandRecorder) record(cmd redis.Cmder) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.names = append(r.names, strings.ToLower(cmd.Name()))
}

func (r *commandRecorder) reset() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	names := r.names
	r.names = nil
	return names
}

func countCommands(names []string, name string) int {
	n := 0
	for _, got := range names {
		if got == name {
			n++
		}
	}
	return n
}

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
		sweepAgain(t, ctx, q)
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

// A job-index member whose key is not a hash (written by something else, or a
// corrupted record) is dangling. It must not raise WRONGTYPE and pin the sweep
// on the same window forever, and the foreign key itself is left alone.
func TestPruneExpiredJobIndexTreatsForeignKeyAsDangling(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	if err := q.rdb.Set(ctx, JobHashPrefix+"foreign", "not a job", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.ZAdd(ctx, JobIndexKey, redis.Z{Score: 1, Member: "foreign"}).Err(); err != nil {
		t.Fatal(err)
	}
	addExpiredIndexMembers(t, ctx, q, "expired", 3, 10)
	n, err := q.PruneExpiredJobIndex(ctx, 100)
	if err != nil || n != 4 {
		t.Fatalf("prune with a foreign key = %d, %v; want 4 dangling members removed", n, err)
	}
	if left, err := q.rdb.ZCard(ctx, JobIndexKey).Result(); err != nil || left != 0 {
		t.Fatalf("index size = %d, %v; want 0", left, err)
	}
	if v, err := q.rdb.Get(ctx, JobHashPrefix+"foreign").Result(); err != nil || v != "not a job" {
		t.Fatalf("foreign key = %q, %v; want untouched", v, err)
	}
}

// A failed sweep still moves the shared cursor past its window, so one bad
// batch cannot make every caller re-scan the same members forever.
func TestPruneExpiredJobIndexAdvancesCursorWhenBatchFails(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	for i := 0; i < 150; i++ {
		id := fmt.Sprintf("live-%03d", i)
		if err := q.rdb.HSet(ctx, JobHashPrefix+id, "status", StatusQueued).Err(); err != nil {
			t.Fatal(err)
		}
		if err := q.rdb.ZAdd(ctx, JobIndexKey, redis.Z{Score: float64(i), Member: id}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	injected := errors.New("injected sweep failure")
	failing := &failFirstEval{err: injected}
	q.rdb.AddHook(failing)
	if _, err := q.PruneExpiredJobIndex(ctx, 100); !errors.Is(err, injected) {
		t.Fatalf("sweep with a failing batch = %v; want the injected error", err)
	}
	if cursor, err := q.rdb.Get(ctx, jobIndexPruneCursorKey).Result(); err != nil || cursor != "100" {
		t.Fatalf("cursor after failed batch = %q, %v; want 100", cursor, err)
	}
}

// failFirstEval fails the first script call (EVAL or EVALSHA, plain or
// pipelined) after Redis ran it, as a lost reply would.
type failFirstEval struct {
	err   error
	fired bool
	mu    sync.Mutex
}

func (f *failFirstEval) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) { return next(ctx, network, addr) }
}

func (f *failFirstEval) take(cmd redis.Cmder) bool {
	name := strings.ToLower(cmd.Name())
	if name != "eval" && name != "evalsha" {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fired {
		return false
	}
	f.fired = true
	return true
}

func (f *failFirstEval) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if err := next(ctx, cmd); err != nil {
			return err
		}
		if f.take(cmd) {
			cmd.SetErr(f.err)
			return f.err
		}
		return nil
	}
}

func (f *failFirstEval) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if err := next(ctx, cmds); err != nil {
			return err
		}
		for _, cmd := range cmds {
			if f.take(cmd) {
				cmd.SetErr(f.err)
				return f.err
			}
		}
		return nil
	}
}

// Every worker runs the sweep. Each sweep must cost one cached script call for
// its whole batch, not one plain EVAL (with the full script text) per member.
func TestPruneExpiredJobIndexUsesOneCachedScriptPerSweep(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	addExpiredIndexMembers(t, ctx, q, "first", 100, 0)
	if _, err := q.PruneExpiredJobIndex(ctx, 100); err != nil {
		t.Fatal(err)
	}
	addExpiredIndexMembers(t, ctx, q, "second", 100, 1000)
	sweepAgain(t, ctx, q)
	recorder := &commandRecorder{}
	q.rdb.AddHook(recorder)
	n, err := q.PruneExpiredJobIndex(ctx, 100)
	names := recorder.reset()
	if err != nil || n != 100 {
		t.Fatalf("second sweep = %d, %v; want 100", n, err)
	}
	if evals, shas := countCommands(names, "eval"), countCommands(names, "evalsha"); evals != 0 || shas != 1 {
		t.Fatalf("second sweep sent %d EVAL and %d EVALSHA (%v); want one EVALSHA", evals, shas, names)
	}
}

// Only one process sweeps per interval. Workers and the Manager all call the
// sweep; the rest skip without touching the index.
func TestPruneExpiredJobIndexSweepsOncePerInterval(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	addExpiredIndexMembers(t, ctx, q, "expired", 300, 0)
	if n, err := q.PruneExpiredJobIndex(ctx, 100); err != nil || n != 100 {
		t.Fatalf("first sweep = %d, %v; want 100", n, err)
	}
	recorder := &commandRecorder{}
	q.rdb.AddHook(recorder)
	if n, err := q.PruneExpiredJobIndex(ctx, 100); err != nil || n != 0 {
		t.Fatalf("second sweep in the same interval = %d, %v; want skipped", n, err)
	}
	if names := recorder.reset(); countCommands(names, "zrange") != 0 || countCommands(names, "evalsha")+countCommands(names, "eval") != 0 {
		t.Fatalf("skipped sweep still scanned the index: %v", names)
	}
	if left, err := q.rdb.ZCard(ctx, JobIndexKey).Result(); err != nil || left != 200 {
		t.Fatalf("index size = %d, %v; want 200", left, err)
	}
}
