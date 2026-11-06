package farmqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func claimScanTestQueue(t *testing.T) (*Client, context.Context) {
	t.Helper()
	metaURL := os.Getenv("JM_FARM_TEST_REDIS_URL")
	if metaURL == "" || os.Getenv("JM_FARM_TEST_REDIS_FLUSH") != "1" {
		t.Skip("set JM_FARM_TEST_REDIS_URL to an isolated Redis and JM_FARM_TEST_REDIS_FLUSH=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	q, err := Open(metaURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })
	if err := q.rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	return q, ctx
}

// recordClaimScripts logs every command into the slowlog for the rest of the
// test and returns a reader for the scripts executed since the last read: how
// many ran and the longest Redis-side execution time of any one of them.
func recordClaimScripts(t *testing.T, q *Client, ctx context.Context) func() (calls int, longest time.Duration) {
	t.Helper()
	previous, err := q.rdb.ConfigGet(ctx, "slowlog-*").Result()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, name := range []string{"slowlog-log-slower-than", "slowlog-max-len"} {
			if value, ok := previous[name]; ok {
				_ = q.rdb.ConfigSet(context.Background(), name, value).Err()
			}
		}
		_ = q.rdb.SlowLogReset(context.Background()).Err()
	})
	if err := q.rdb.ConfigSet(ctx, "slowlog-max-len", "100000").Err(); err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.ConfigSet(ctx, "slowlog-log-slower-than", "0").Err(); err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.SlowLogReset(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	return func() (int, time.Duration) {
		t.Helper()
		logs, err := q.rdb.SlowLogGet(ctx, -1).Result()
		if err != nil {
			t.Fatal(err)
		}
		if err := q.rdb.SlowLogReset(ctx).Err(); err != nil {
			t.Fatal(err)
		}
		calls, longest := 0, time.Duration(0)
		for _, entry := range logs {
			if len(entry.Args) == 0 || !strings.HasPrefix(strings.ToLower(entry.Args[0]), "eval") {
				continue
			}
			// The slowlog truncates long arguments, so match the script's start.
			if len(entry.Args) < 2 || len(entry.Args[1]) < 64 || entry.Args[1][:64] != claimForWorkerScript[:64] {
				continue
			}
			calls++
			longest = max(longest, entry.Duration)
		}
		return calls, longest
	}
}

// pushPinnedBacklog fills a CPU proxy lane with jobs pinned to a GPU that is not
// this worker, so none of them is eligible for it.
func pushPinnedBacklog(t *testing.T, q *Client, ctx context.Context, n int) {
	t.Helper()
	pipe := q.rdb.Pipeline()
	for i := 0; i < n; i++ {
		raw := fmt.Sprintf(`{"id":"pinned-%d","path":"/jfs/p/%d.mov","kinds":["proxy"],"queue_class":"cpu","required_capabilities":["cpu","worker-name:gpu1"]}`, i, i)
		pipe.LPush(ctx, classQueue(KindProxy, QueueClassCPU), raw)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
}

// A claim poll used to inspect every queued job in one script. An idle worker
// behind a backlog pinned to another GPU then held Redis for 7/98/291 ms at
// 1k/20k/50k jobs, every 250 ms, stalling every editor's metadata operation on
// the same Redis. Each script now inspects a bounded window, and the scan still
// reaches work at the far end of the backlog.
func TestClaimScanCostStaysBoundedAsBacklogGrows(t *testing.T) {
	q, ctx := claimScanTestQueue(t)
	scripts := recordClaimScripts(t, q, ctx)
	for _, backlog := range []int{1000, 20000} {
		// A fresh worker starts its scan at the claim end.
		worker := Worker{ID: fmt.Sprintf("worker-%d", backlog), Name: fmt.Sprintf("w%d", backlog),
			Role: QueueClassServer, Capabilities: []string{"cpu", "metadata"}}
		if err := q.Heartbeat(ctx, worker); err != nil {
			t.Fatal(err)
		}
		if err := q.rdb.Del(ctx, classQueue(KindProxy, QueueClassCPU)).Err(); err != nil {
			t.Fatal(err)
		}
		pushPinnedBacklog(t, q, ctx, backlog)
		// The only job this worker can run is the newest, at the far end.
		eligible := NewJob(fmt.Sprintf("/jfs/eligible-%d.mov", backlog), []string{KindProxy}, "manager")
		eligible.QueueClass = QueueClassCPU
		eligible.RequiredCapabilities = []string{"cpu"}
		raw, _ := json.Marshal(eligible)
		if err := q.rdb.LPush(ctx, classQueue(KindProxy, QueueClassCPU), raw).Err(); err != nil {
			t.Fatal(err)
		}
		_, _ = scripts()

		claim, ok, err := q.ClaimForWorker(ctx, 0, worker)
		calls, longest := scripts()
		t.Logf("backlog=%d: %d claim scripts, longest %v", backlog, calls, longest)
		if err != nil || !ok || claim.Job.ID != eligible.ID {
			t.Fatalf("backlog=%d: claim = %+v ok=%v err=%v; want the job behind the backlog", backlog, claim.Job, ok, err)
		}
		if longest > 20*time.Millisecond {
			t.Fatalf("backlog=%d: one claim script held Redis for %v; want a bounded scan", backlog, longest)
		}
		if minCalls := backlog / claimScanBatch; calls < minCalls {
			t.Fatalf("backlog=%d: %d claim scripts covered the backlog; want at least %d bounded steps", backlog, calls, minCalls)
		}
		if n, err := q.rdb.LLen(ctx, classQueue(KindProxy, QueueClassCPU)).Result(); err != nil || n != int64(backlog) {
			t.Fatalf("backlog=%d: ready depth after claim = %d, %v", backlog, n, err)
		}
		if err := q.AckClaim(ctx, claim); err != nil {
			t.Fatal(err)
		}
	}
}

// An idle worker used to poll every 250 ms forever. After a full pass that finds
// nothing it now backs off, and a claim is still immediate once work arrives.
func TestIdleClaimPollingBacksOff(t *testing.T) {
	q, ctx := claimScanTestQueue(t)
	worker := Worker{ID: "worker-idle", Name: "idle", Role: QueueClassServer, Capabilities: []string{"cpu", "metadata"}}
	if err := q.Heartbeat(ctx, worker); err != nil {
		t.Fatal(err)
	}
	scripts := recordClaimScripts(t, q, ctx)
	if _, ok, err := q.ClaimForWorker(ctx, 3*time.Second, worker); err != nil || ok {
		t.Fatalf("idle claim ok=%v err=%v", ok, err)
	}
	calls, _ := scripts()
	t.Logf("idle worker ran %d claim scripts in 3s", calls)
	// 250 ms polling is 12 or 13 scripts in 3 s; 250/500/1000/1250 ms is 5.
	if calls > 6 {
		t.Fatalf("idle worker ran %d claim scripts in 3s; want backoff", calls)
	}

	job := NewJob("/jfs/new.mov", []string{KindProxy}, "manager")
	job.QueueClass = QueueClassCPU
	job.RequiredCapabilities = []string{"cpu"}
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	claim, ok, err := q.ClaimForWorker(ctx, 5*time.Second, worker)
	if err != nil || !ok || claim.Job.ID != job.ID {
		t.Fatalf("claim after enqueue = %+v ok=%v err=%v", claim.Job, ok, err)
	}
	if waited := time.Since(start); waited > time.Second {
		t.Fatalf("claim after idle backoff waited %v", waited)
	}
	if scan := q.claimScanFor(worker.ID); scan.idle != 0 {
		t.Fatalf("idle backoff after a claim = %v; want reset", scan.idle)
	}
}

// A lane with owed entries is not covered yet. Pacing slows with each poll
// that claims nothing and only a claim resets it: new entries are inspected by
// the next poll anyway, and resetting on them kept an idle worker behind an
// ineligible backlog polling 38 times a second.
func TestClaimScanPacingResetsOnlyOnAClaim(t *testing.T) {
	scan := newClaimScanState()
	keys := []string{"a", "b"}
	if done := scan.observe(keys, "64:200:5:0,0:3:2:0"); done {
		t.Fatal("pass reported complete while lane a was mid-scan")
	}
	if done := scan.observe(keys, "0:200:5:0,0:3:2:7"); done {
		t.Fatal("pass reported complete while lane b still owed new entries")
	}
	if done := scan.observe(keys, "0:200:5:0,0:3:2:0"); !done {
		t.Fatal("pass not complete after every lane wrapped with nothing owed")
	}
	for _, want := range []time.Duration{25, 50, 100, 200, 250, 250} {
		if got := scan.stepDelay(); got != want*time.Millisecond {
			t.Fatalf("step delay = %v; want %v", got, want*time.Millisecond)
		}
	}
	if got := scan.idleDelay(); got != claimIdleBase {
		t.Fatalf("first idle delay = %v", got)
	}
	if got := scan.idleDelay(); got != 2*claimIdleBase {
		t.Fatalf("second idle delay = %v", got)
	}
	for i := 0; i < 10; i++ {
		scan.idleDelay()
	}
	if scan.idle != claimIdleMax {
		t.Fatalf("idle delay cap = %v", scan.idle)
	}
	// One push and one claim elsewhere: new entries do not reset the pacing.
	scan.observe(keys, "0:200:5:0,0:3:3:0")
	if got := scan.stepDelay(); got != claimScanStepMax {
		t.Fatalf("step delay after a new entry = %v; want %v", got, claimScanStepMax)
	}
	if got := scan.idleDelay(); got != claimIdleMax {
		t.Fatalf("idle delay after a new entry = %v; want %v", got, claimIdleMax)
	}
	scan.claimed()
	if got := scan.stepDelay(); got != claimScanStepDelay {
		t.Fatalf("step delay after a claim = %v; want %v", got, claimScanStepDelay)
	}
	if got := scan.idleDelay(); got != claimIdleBase {
		t.Fatalf("idle delay after a claim = %v; want %v", got, claimIdleBase)
	}
	cursors, seqs, owed := scan.scanArgs([]string{"b", "a", "c"})
	if cursors != "0,0,0" || seqs != "3,5,-1" || owed != "0,0,0" {
		t.Fatalf("scan args = %q %q %q", cursors, seqs, owed)
	}
}

// A candidate found deep in a tracked lane moves to the claim end and is then
// transferred with the same custody steps as the lane's head, without
// disturbing the entries it was found behind.
func TestTrackedClaimDeepInTheLaneKeepsCustody(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	worker := watchPlannerWorker()
	_, child := h1ReviewTrackedChild(t, q, ctx, worker)
	lane := QueueKeyFor(&child)
	var pinned []string
	for i := 0; i < 2*claimScanBatch+5; i++ {
		pinned = append(pinned, fmt.Sprintf(`{"id":"pinned-%d","path":"/jfs/p/%d.mov","kinds":["proxy"],"queue_class":"cpu","required_capabilities":["cpu","worker-name:gpu1"]}`, i, i))
	}
	// RPUSH puts the pinned raws on the claim end, ahead of the child.
	if err := q.rdb.RPush(ctx, lane, pinned).Err(); err != nil {
		t.Fatal(err)
	}
	before, err := q.rdb.LRange(ctx, lane, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	claim, ok, err := q.ClaimForWorker(ctx, 0, worker)
	if err != nil || !ok || claim.Job.ID != child.ID {
		t.Fatalf("claim = %+v ok=%v err=%v; want the tracked child", claim.Job, ok, err)
	}
	after, err := q.rdb.LRange(ctx, lane, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	want := slices.DeleteFunc(slices.Clone(before), func(raw string) bool { return raw == claim.Raw })
	if !slices.Equal(after, want) {
		t.Fatalf("ready lane after a deep claim changed beyond the claimed raw:\nbefore=%d after=%d", len(before), len(after))
	}
	status, err := q.rdb.HGetAll(ctx, JobHashPrefix+child.ID).Result()
	if err != nil || status["claim_phase"] != "reserved" || status["claim_token"] != claim.Token ||
		status["lease_owner"] != worker.ID || status["custody_raw"] != "" {
		t.Fatalf("tracked child status = %+v, %v", status, err)
	}
	receipts, err := q.rdb.LRange(ctx, claimProcessingKey(claim), 0, -1).Result()
	if err != nil || !slices.Contains(receipts, claim.Raw) {
		t.Fatalf("processing receipt = %v, %v", receipts, err)
	}
	if member, err := q.rdb.SIsMember(ctx, trackedCustodyIndexKey, child.ID).Result(); err != nil || member {
		t.Fatalf("custody index still holds the child: %v %v", member, err)
	}
}

// claimScriptDurations returns the Redis-side time of each claim script run
// since the slowlog was last reset, oldest first (recordClaimScripts must be
// active).
func claimScriptDurations(t *testing.T, q *Client, ctx context.Context) []time.Duration {
	t.Helper()
	logs, err := q.rdb.SlowLogGet(ctx, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.SlowLogReset(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	var durations []time.Duration
	for i := len(logs) - 1; i >= 0; i-- {
		entry := logs[i]
		if len(entry.Args) < 2 || !strings.HasPrefix(strings.ToLower(entry.Args[0]), "eval") ||
			len(entry.Args[1]) < 64 || entry.Args[1][:64] != claimForWorkerScript[:64] {
			continue
		}
		durations = append(durations, entry.Duration)
	}
	return durations
}

// Enqueue pushes to the far end of a lane. With a bounded window a new job
// behind a 50k backlog meant for another GPU waited for the cursor to walk
// there (about 20 s); a lane that grew now has its newest entries inspected
// first.
func TestNewJobBehindLargeIneligibleBacklogIsClaimedPromptly(t *testing.T) {
	q, ctx := claimScanTestQueue(t)
	worker := Worker{ID: "worker-fresh", Name: "fresh", Role: QueueClassServer, Capabilities: []string{"cpu", "metadata"}}
	if err := q.Heartbeat(ctx, worker); err != nil {
		t.Fatal(err)
	}
	pushPinnedBacklog(t, q, ctx, 50000)
	if _, ok, err := q.ClaimForWorker(ctx, 0, worker); err != nil || ok {
		t.Fatalf("backlog pass ok=%v err=%v; want nothing eligible", ok, err)
	}
	scripts := recordClaimScripts(t, q, ctx)
	job := NewJob("/jfs/new.mov", []string{KindProxy}, "manager")
	job.QueueClass = QueueClassCPU
	job.RequiredCapabilities = []string{"cpu"}
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	claim, ok, err := q.ClaimForWorker(ctx, 5*time.Second, worker)
	waited := time.Since(start)
	calls, _ := scripts()
	if err != nil || !ok || claim.Job.ID != job.ID {
		t.Fatalf("claim = %+v ok=%v err=%v after %v", claim.Job, ok, err, waited)
	}
	t.Logf("new job claimed after %v in %d claim scripts", waited, calls)
	if calls > 2 || waited > time.Second {
		t.Fatalf("new job behind the backlog took %d polls and %v; want one or two", calls, waited)
	}
}

// Claiming a job found deep in a lane removes it from the nearer end, so the
// worst case is the middle of the lane: half a lane walk, not a scan step (about
// 2.5 ms at 100k against a 0.25 ms step). LREM from the claim end walked the
// whole distance from that end. Bounds are relative to a full walk of the same
// lane in the same Redis, the minimum of three trials each, so they hold on any
// machine and still catch a walk from the wrong end or of the whole lane.
func TestDeepClaimCostIsAtMostHalfALaneWalk(t *testing.T) {
	q, ctx := claimScanTestQueue(t)
	lane := classQueue(KindProxy, QueueClassCPU)
	const backlog = 100000
	pushPinnedBacklog(t, q, ctx, backlog)
	_ = recordClaimScripts(t, q, ctx)

	fullWalk := time.Duration(1<<63 - 1)
	for trial := 0; trial < 3; trial++ {
		// LREM of a value that is not in the lane walks all of it.
		if err := q.rdb.LRem(ctx, lane, 0, "absent").Err(); err != nil {
			t.Fatal(err)
		}
		logs, err := q.rdb.SlowLogGet(ctx, -1).Result()
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range logs {
			if len(entry.Args) > 0 && strings.EqualFold(entry.Args[0], "lrem") {
				fullWalk = min(fullWalk, entry.Duration)
			}
		}
		if err := q.rdb.SlowLogReset(ctx).Err(); err != nil {
			t.Fatal(err)
		}
	}

	// claimAt places an eligible job at head index h and returns the cheapest
	// claim step and median scan step over three fresh workers. A fresh worker
	// has no lane sequence yet, so its cursor walks to the job.
	claimAt := func(name string, h int64) (claimStep, median time.Duration) {
		claimStep, median = time.Duration(1<<63-1), time.Duration(1<<63-1)
		for trial := 0; trial < 3; trial++ {
			worker := scanTestWorker(t, q, ctx, fmt.Sprintf("%s-%d", name, trial))
			job := newCPUJob(fmt.Sprintf("/jfs/%s-%d.mov", name, trial))
			raw, _ := json.Marshal(job)
			if err := q.rdb.LSet(ctx, lane, h, raw).Err(); err != nil {
				t.Fatal(err)
			}
			_ = claimScriptDurations(t, q, ctx)
			claim, ok, err := q.ClaimForWorker(ctx, 0, worker)
			if err != nil || !ok || claim.Job.ID != job.ID {
				t.Fatalf("%s: claim = %q ok=%v err=%v", name, claim.Job.ID, ok, err)
			}
			durations := claimScriptDurations(t, q, ctx)
			steps := slices.Clone(durations[:len(durations)-1])
			slices.Sort(steps)
			claimStep = min(claimStep, durations[len(durations)-1])
			median = min(median, steps[len(steps)/2])
		}
		return claimStep, median
	}

	// The middle: either end is half a walk away.
	step, median := claimAt("middle", backlog/2)
	limit := 2*median + fullWalk*3/4
	t.Logf("middle: claim step %v; median scan step %v; full lane walk %v; limit %v", step, median, fullWalk, limit)
	if step > limit {
		t.Fatalf("mid-lane claim step took %v; want at most %v (half a lane walk plus scan cost)", step, limit)
	}
	// A quarter from the enqueue end: removing from the claim end would walk
	// three quarters of the lane.
	step, median = claimAt("quarter", backlog/4)
	limit = 2*median + fullWalk/2
	t.Logf("quarter: claim step %v; median scan step %v; limit %v", step, median, limit)
	if step > limit {
		t.Fatalf("claim a quarter from the enqueue end took %v; want at most %v (a quarter walk plus scan cost)", step, limit)
	}
}

func newCPUJob(path string, capabilities ...string) Job {
	job := NewJob(path, []string{KindProxy}, "manager")
	job.QueueClass = QueueClassCPU
	job.RequiredCapabilities = append([]string{"cpu"}, capabilities...)
	return job
}

func scanTestWorker(t *testing.T, q *Client, ctx context.Context, name string) Worker {
	t.Helper()
	worker := Worker{ID: "worker-" + name, Name: name, Role: QueueClassServer, Capabilities: []string{"cpu", "metadata"}}
	if err := q.Heartbeat(ctx, worker); err != nil {
		t.Fatal(err)
	}
	return worker
}

// claimWithinTwoPolls claims for worker and requires the claim of want to take
// at most two claim scripts.
func claimWithinTwoPolls(t *testing.T, q *Client, ctx context.Context, worker Worker, want string, scripts func() (int, time.Duration)) Claim {
	t.Helper()
	_, _ = scripts()
	claim, ok, err := q.ClaimForWorker(ctx, 5*time.Second, worker)
	calls, _ := scripts()
	if err != nil || !ok || claim.Job.ID != want {
		t.Fatalf("claim = %q ok=%v err=%v after %d polls; want %q", claim.Job.ID, ok, err, calls, want)
	}
	if calls > 2 {
		t.Fatalf("claim of %q took %d polls; want one or two", want, calls)
	}
	return claim
}

// New entries are inspected after the cursor window, so older eligible work
// keeps its turn. Inspecting the newest entries first claimed only one of 200
// old jobs while one new job arrived per claim.
func TestNewEntriesDoNotStarveOlderEligibleWork(t *testing.T) {
	q, ctx := claimScanTestQueue(t)
	worker := scanTestWorker(t, q, ctx, "fair")
	if _, ok, err := q.ClaimForWorker(ctx, 0, worker); err != nil || ok {
		t.Fatalf("empty poll ok=%v err=%v", ok, err)
	}
	old := map[string]bool{}
	for i := 0; i < 200; i++ {
		job := newCPUJob(fmt.Sprintf("/jfs/old/%d.mov", i))
		if err := q.Enqueue(ctx, job); err != nil {
			t.Fatal(err)
		}
		old[job.ID] = true
	}
	claimedOld := 0
	for round := 0; round < 40; round++ {
		if err := q.Enqueue(ctx, newCPUJob(fmt.Sprintf("/jfs/new/%d.mov", round))); err != nil {
			t.Fatal(err)
		}
		claim, ok, err := q.ClaimForWorker(ctx, 0, worker)
		if err != nil || !ok {
			t.Fatalf("round %d: ok=%v err=%v", round, ok, err)
		}
		if old[claim.Job.ID] {
			claimedOld++
		}
		if err := q.AckClaim(ctx, claim); err != nil {
			t.Fatal(err)
		}
	}
	if claimedOld != 40 {
		t.Fatalf("claimed %d of 40 from the older jobs; new arrivals jumped the queue", claimedOld)
	}
}

// Several new jobs behind a large backlog are each claimed in a poll or two. A
// claim used to mark the lane's other new entries as seen.
func TestSeveralNewJobsBehindBacklogAreEachClaimedPromptly(t *testing.T) {
	q, ctx := claimScanTestQueue(t)
	worker := scanTestWorker(t, q, ctx, "multi")
	pushPinnedBacklog(t, q, ctx, 50000)
	if _, ok, err := q.ClaimForWorker(ctx, 0, worker); err != nil || ok {
		t.Fatalf("backlog pass ok=%v err=%v", ok, err)
	}
	scripts := recordClaimScripts(t, q, ctx)
	var jobs []Job
	for i := 0; i < 3; i++ {
		job := newCPUJob(fmt.Sprintf("/jfs/new-%d.mov", i))
		if err := q.Enqueue(ctx, job); err != nil {
			t.Fatal(err)
		}
		jobs = append(jobs, job)
	}
	for _, job := range jobs {
		claim := claimWithinTwoPolls(t, q, ctx, worker, job.ID, scripts)
		if err := q.AckClaim(ctx, claim); err != nil {
			t.Fatal(err)
		}
	}
}

// Another worker claiming in the same interval leaves the lane length
// unchanged; the new job must still be seen.
func TestNewJobIsSeenDespiteAConcurrentClaim(t *testing.T) {
	q, ctx := claimScanTestQueue(t)
	a := scanTestWorker(t, q, ctx, "a")
	b := scanTestWorker(t, q, ctx, "b")
	pushPinnedBacklog(t, q, ctx, 50000)
	forB := newCPUJob("/jfs/for-b.mov", "worker-name:b")
	raw, _ := json.Marshal(forB)
	if err := q.rdb.RPush(ctx, classQueue(KindProxy, QueueClassCPU), raw).Err(); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := q.ClaimForWorker(ctx, 0, a); err != nil || ok {
		t.Fatalf("backlog pass ok=%v err=%v", ok, err)
	}
	// One push and one claim: the lane length is what worker a last saw.
	forA := newCPUJob("/jfs/for-a.mov", "worker-name:a")
	if err := q.Enqueue(ctx, forA); err != nil {
		t.Fatal(err)
	}
	if claim, ok, err := q.ClaimForWorker(ctx, 0, b); err != nil || !ok || claim.Job.ID != forB.ID {
		t.Fatalf("worker b claim = %q ok=%v err=%v", claim.Job.ID, ok, err)
	}
	scripts := recordClaimScripts(t, q, ctx)
	claimWithinTwoPolls(t, q, ctx, a, forA.ID, scripts)
}

// A burst of more new entries than one poll inspects keeps the older new job
// in view: new entries are inspected oldest first, and the rest stay owed.
func TestBurstOfNewEntriesDoesNotHideAnOlderNewJob(t *testing.T) {
	q, ctx := claimScanTestQueue(t)
	worker := scanTestWorker(t, q, ctx, "burst")
	pushPinnedBacklog(t, q, ctx, 50000)
	if _, ok, err := q.ClaimForWorker(ctx, 0, worker); err != nil || ok {
		t.Fatalf("backlog pass ok=%v err=%v", ok, err)
	}
	eligible := newCPUJob("/jfs/eligible.mov")
	if err := q.Enqueue(ctx, eligible); err != nil {
		t.Fatal(err)
	}
	var later []Job
	for i := 0; i < 2*claimScanBatch+10; i++ {
		job := newCPUJob(fmt.Sprintf("/jfs/burst/%d.mov", i), "worker-name:gpu1")
		if err := q.Enqueue(ctx, job); err != nil {
			t.Fatal(err)
		}
		later = append(later, job)
	}
	scripts := recordClaimScripts(t, q, ctx)
	claimWithinTwoPolls(t, q, ctx, worker, eligible.ID, scripts)

	// The rest of the burst stays owed and is covered on the next polls: an
	// eligible job at its newest end is still found without walking the lane.
	last := newCPUJob("/jfs/last.mov")
	if err := q.Enqueue(ctx, last); err != nil {
		t.Fatal(err)
	}
	_, _ = scripts()
	claim, ok, err := q.ClaimForWorker(ctx, 5*time.Second, worker)
	calls, _ := scripts()
	if err != nil || !ok || claim.Job.ID != last.ID {
		t.Fatalf("claim after burst = %q ok=%v err=%v", claim.Job.ID, ok, err)
	}
	if limit := (len(later)+1)/claimScanBatch + 2; calls > limit {
		t.Fatalf("claim at the end of the burst took %d polls; want at most %d", calls, limit)
	}
}

// Every enqueue path counts its push in the lane's sequence, so a worker can
// tell how many entries arrived. A path that did not would leave its jobs to
// the cursor walk.
func TestEnqueuePathsCountNewEntries(t *testing.T) {
	q, ctx := claimScanTestQueue(t)
	seq := func(lane string) int64 {
		t.Helper()
		n, err := q.rdb.Get(ctx, LaneSeqKey(lane)).Int64()
		if err != nil && err != redis.Nil {
			t.Fatal(err)
		}
		return n
	}
	expectGrowth := func(name, lane string, want int64, push func()) {
		t.Helper()
		before := seq(lane)
		push()
		if got := seq(lane) - before; got != want {
			t.Fatalf("%s: lane %s sequence grew by %d; want %d", name, lane, got, want)
		}
	}
	server := scanTestWorker(t, q, ctx, "seq-server")
	cpuLane := classQueue(KindProxy, QueueClassCPU)

	job := newCPUJob("/jfs/seq/a.mov")
	expectGrowth("Enqueue", cpuLane, 1, func() {
		if err := q.Enqueue(ctx, job); err != nil {
			t.Fatal(err)
		}
	})
	claim, ok, err := q.ClaimForWorker(ctx, 0, server)
	if err != nil || !ok {
		t.Fatalf("claim ok=%v err=%v", ok, err)
	}
	expectGrowth("RequeueClaimSameRoute", cpuLane, 1, func() {
		if err := q.RequeueClaimSameRoute(ctx, claim, "test"); err != nil {
			t.Fatal(err)
		}
	})
	if err := q.MarkFailed(ctx, job.ID, "fixture failure"); err != nil {
		t.Fatal(err)
	}
	expectGrowth("RequeueFailed", cpuLane, 1, func() {
		if _, created, err := q.RequeueFailed(ctx, job.ID, "manager"); err != nil || !created {
			t.Fatalf("requeue created=%v err=%v", created, err)
		}
	})

	parent := NewJob("/jfs/seq/dir", []string{KindDerivatives}, "manager")
	parent.QueueClass = QueueClassServer
	parent.RequiredCapabilities = []string{"cpu"}
	expectGrowth("EnqueueTargetShards", classQueue(KindDerivatives, QueueClassServer), 3, func() {
		if _, created, err := q.EnqueueTargetShards(ctx, parent, []string{"/a", "/b", "/c", "/d", "/e"}, 2); err != nil || created != 3 {
			t.Fatalf("shards created=%d err=%v", created, err)
		}
	})

	root := t.TempDir()
	if leader, err := q.AcquireWatchLeadership(ctx, "leader", time.Minute); err != nil || !leader {
		t.Fatalf("watch leader: %v %v", leader, err)
	}
	expectGrowth("StartWatchEpoch", TrackedQueuePrefix+QueueClassServer+":"+KindProxy, 1, func() {
		if _, err := q.StartWatchEpoch(ctx, root, "leader", []string{KindProxy}, time.Now()); err != nil {
			t.Fatal(err)
		}
	})
}
