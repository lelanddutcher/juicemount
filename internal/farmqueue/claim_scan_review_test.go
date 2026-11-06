package farmqueue

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"time"
)

// A restarted worker's cursor sits on a stretch it cannot serve, the older
// eligible work lies beyond its window and is not owed, and new eligible work
// arrives before every claim. A claim among the owed entries left the cursor
// where it was, so the window never moved: 0 of 40 claims reached the older
// work. The cursor now moves past an inspected window, so the older work is
// reached once the window has walked the stretch ahead of it.
func TestOwedClaimsDoNotParkTheCursorBeforeOlderWork(t *testing.T) {
	q, ctx := claimScanTestQueue(t)
	lane := classQueue(KindProxy, QueueClassCPU)
	const ahead = 1000
	pushPinnedBacklog(t, q, ctx, ahead) // ineligible at the claim end
	old := map[string]bool{}
	for i := 0; i < 50; i++ {
		job := newCPUJob(fmt.Sprintf("/jfs/old/%d.mov", i))
		if err := q.Enqueue(ctx, job); err != nil {
			t.Fatal(err)
		}
		old[job.ID] = true
	}
	// A restarted worker that has seen the lane's sequence, with its cursor at
	// the claim end: the older jobs are neither owed nor in its window.
	worker := scanTestWorker(t, q, ctx, "restarted")
	seq, err := q.rdb.Get(ctx, LaneSeqKey(lane)).Int64()
	if err != nil {
		t.Fatal(err)
	}
	scan := q.claimScanFor(worker.ID)
	scan.mu.Lock()
	scan.seqs[lane], scan.owed[lane], scan.cursors[lane] = seq, 0, 0
	scan.mu.Unlock()
	const rounds = 40
	claimedOld := 0
	for round := 0; round < rounds; round++ {
		if err := q.Enqueue(ctx, newCPUJob(fmt.Sprintf("/jfs/new/%d.mov", round))); err != nil {
			t.Fatal(err)
		}
		claim, ok, err := q.ClaimForWorker(ctx, 5*time.Second, worker)
		if err != nil || !ok {
			t.Fatalf("round %d ok=%v err=%v", round, ok, err)
		}
		if old[claim.Job.ID] {
			claimedOld++
		}
		if err := q.AckClaim(ctx, claim); err != nil {
			t.Fatal(err)
		}
	}
	// One window per claim walks the stretch ahead; from then on the window,
	// inspected before the owed entries, holds the older work and wins.
	want := rounds - (ahead+claimScanBatch-1)/claimScanBatch - 1
	t.Logf("older jobs claimed %d of %d rounds (want at least %d)", claimedOld, rounds, want)
	if claimedOld < want {
		t.Fatalf("older jobs claimed in %d of %d rounds; want at least %d", claimedOld, rounds, want)
	}
}

// A window claim of an entry that is also owed takes it off what is owed, so
// the next new job is not left behind stale owed accounting.
func TestWindowClaimOfOwedEntryReducesOwed(t *testing.T) {
	q, ctx := claimScanTestQueue(t)
	lane := classQueue(KindProxy, QueueClassCPU)
	worker := scanTestWorker(t, q, ctx, "short-lane")
	if _, ok, err := q.ClaimForWorker(ctx, 0, worker); err != nil || ok {
		t.Fatalf("empty poll ok=%v err=%v", ok, err)
	}
	for i := 0; i < 3; i++ {
		if err := q.Enqueue(ctx, newCPUJob(fmt.Sprintf("/jfs/short/%d.mov", i))); err != nil {
			t.Fatal(err)
		}
	}
	// A short lane lies wholly in the window, so the oldest new job is claimed
	// from the window.
	claim, ok, err := q.ClaimForWorker(ctx, 0, worker)
	if err != nil || !ok {
		t.Fatalf("claim ok=%v err=%v", ok, err)
	}
	if err := q.AckClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	scan := q.claimScanFor(worker.ID)
	scan.mu.Lock()
	owed := scan.owed[lane]
	scan.mu.Unlock()
	if owed != 2 {
		t.Fatalf("owed after a window claim of one of 3 new entries = %d; want 2", owed)
	}
}

// With entries owed anywhere in a long lane, a poll still costs about one
// scan step: the owed entries are read from the nearer end.
func TestOwedInspectionCostStaysNearAScanStep(t *testing.T) {
	q, ctx := claimScanTestQueue(t)
	lane := classQueue(KindProxy, QueueClassCPU)
	const backlog = 100000
	pushPinnedBacklog(t, q, ctx, backlog)
	w := scanTestWorker(t, q, ctx, "owed-cost")
	scan := q.claimScanFor(w.ID)
	seq, _ := q.rdb.Get(ctx, LaneSeqKey(lane)).Int64()
	_ = recordClaimScripts(t, q, ctx)
	medianFor := func(owed int64) time.Duration {
		var durations []time.Duration
		for trial := 0; trial < 15; trial++ {
			scan.mu.Lock()
			scan.seqs[lane], scan.owed[lane], scan.cursors[lane] = seq, owed, backlog/2
			scan.mu.Unlock()
			_ = claimScriptDurations(t, q, ctx)
			_, _, _ = q.ClaimForWorker(ctx, time.Millisecond, w)
			if d := claimScriptDurations(t, q, ctx); len(d) > 0 {
				durations = append(durations, d[0])
			}
		}
		slices.Sort(durations)
		return durations[len(durations)/2]
	}
	base := medianFor(claimScanBatch)
	for _, owed := range []int64{backlog / 4, backlog / 2, backlog} {
		median := medianFor(owed)
		t.Logf("owed=%d: median poll %v (owed=64: %v)", owed, median, base)
		if limit := 3*base + 500*time.Microsecond; median > limit {
			t.Fatalf("owed=%d: median poll %v; want at most %v", owed, median, limit)
		}
	}
}

// Two workers behind an ineligible backlog, seeded random enqueues for A, B and
// neither, claims by both, and removals from anywhere in the lane. Every job for
// A that arrived while A was polling is claimed within a few polls, and no job
// is claimed twice.
func TestRandomizedOwedAccountingClaimsEachJobOncePromptly(t *testing.T) {
	q, ctx := claimScanTestQueue(t)
	lane := classQueue(KindProxy, QueueClassCPU)
	pushPinnedBacklog(t, q, ctx, 5000)
	a := scanTestWorker(t, q, ctx, "a")
	b := scanTestWorker(t, q, ctx, "b")
	if _, ok, _ := q.ClaimForWorker(ctx, 0, a); ok {
		t.Fatal("a claimed")
	}
	if _, ok, _ := q.ClaimForWorker(ctx, 0, b); ok {
		t.Fatal("b claimed")
	}
	rng := rand.New(rand.NewSource(7))
	seen := map[string]bool{}
	scripts := recordClaimScripts(t, q, ctx)
	worst := 0
	for round := 0; round < 150; round++ {
		pendingA := []string{}
		n := rng.Intn(150)
		for i := 0; i < n; i++ {
			var job Job
			switch rng.Intn(4) {
			case 0:
				job = newCPUJob(fmt.Sprintf("/jfs/r%d/a%d.mov", round, i), "worker-name:a")
				pendingA = append(pendingA, job.ID)
			case 1:
				job = newCPUJob(fmt.Sprintf("/jfs/r%d/b%d.mov", round, i), "worker-name:b")
			default:
				job = newCPUJob(fmt.Sprintf("/jfs/r%d/x%d.mov", round, i), "worker-name:gpu1")
			}
			if err := q.Enqueue(ctx, job); err != nil {
				t.Fatal(err)
			}
		}
		// Random removals from anywhere (operator cancel / demotion stand-in).
		if rng.Intn(3) == 0 {
			length, _ := q.rdb.LLen(ctx, lane).Result()
			for k := 0; k < 5; k++ {
				idx := rng.Int63n(length - int64(k))
				raw, err := q.rdb.LIndex(ctx, lane, idx).Result()
				if err == nil {
					var j Job
					_ = json.Unmarshal([]byte(raw), &j)
					_ = q.rdb.LRem(ctx, lane, 1, raw).Err()
					for p, id := range pendingA {
						if id == j.ID {
							pendingA = append(pendingA[:p], pendingA[p+1:]...)
							break
						}
					}
				}
			}
		}
		// B claims a random number (shrinks the lane between A polls).
		for k := rng.Intn(10); k > 0; k-- {
			claim, ok, err := q.ClaimForWorker(ctx, 50*time.Millisecond, b)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			if seen[claim.Job.ID] {
				t.Fatalf("duplicate claim %s", claim.Job.ID)
			}
			seen[claim.Job.ID] = true
			_ = q.AckClaim(ctx, claim)
		}
		// A drains its jobs; count polls per claim.
		for len(pendingA) > 0 {
			_, _ = scripts()
			claim, ok, err := q.ClaimForWorker(ctx, 5*time.Second, a)
			calls, _ := scripts()
			if err != nil || !ok {
				t.Fatalf("round %d: a claim ok=%v err=%v pending=%d", round, ok, err, len(pendingA))
			}
			if seen[claim.Job.ID] {
				t.Fatalf("duplicate claim %s", claim.Job.ID)
			}
			seen[claim.Job.ID] = true
			found := false
			for p, id := range pendingA {
				if id == claim.Job.ID {
					pendingA = append(pendingA[:p], pendingA[p+1:]...)
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("a claimed unexpected %s", claim.Job.ID)
			}
			worst = max(worst, calls)
			_ = q.AckClaim(ctx, claim)
		}
	}
	t.Logf("worst A polls for one claim: %d (owed-driven bound about 150/64+2 = 5)", worst)
	if worst > 6 {
		t.Errorf("an A job took %d polls", worst)
	}
}

// An idle worker behind a 50k ineligible backlog, with one ineligible enqueue a
// second. Resetting the pacing on every new entry kept it at 25 ms steps, 38
// claim scripts a second; only a claim resets it now.
func TestIdlePollRateStaysLowUnderIneligibleIngest(t *testing.T) {
	q, ctx := claimScanTestQueue(t)
	pushPinnedBacklog(t, q, ctx, 50000)
	w := scanTestWorker(t, q, ctx, "idle-ingest")
	if _, ok, _ := q.ClaimForWorker(ctx, 0, w); ok {
		t.Fatal("claimed")
	}
	scripts := recordClaimScripts(t, q, ctx)
	stop := make(chan struct{})
	go func() {
		i := 0
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				_ = q.Enqueue(ctx, newCPUJob(fmt.Sprintf("/jfs/ing/%d.mov", i), "worker-name:gpu1"))
				i++
			}
		}
	}()
	_, _ = scripts()
	start := time.Now()
	for time.Since(start) < 10*time.Second {
		_, _, _ = q.ClaimForWorker(ctx, 5*time.Second, w)
	}
	close(stop)
	calls, longest := scripts()
	t.Logf("idle worker, 1 ineligible enqueue/s, 50k backlog: %d scripts in 10s (%.1f/s), longest %v", calls, float64(calls)/10, longest)
	if calls > 60 {
		t.Fatalf("idle worker ran %d claim scripts in 10 s; want at most 60 (about 4 a second plus each call's first poll)", calls)
	}
}
