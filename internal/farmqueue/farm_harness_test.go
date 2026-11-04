//go:build linux

package farmqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// The farm harness runs several workers against one Redis, crashes them and
// cuts them off from Redis at every step of a delivery, restarts Redis from its
// append-only file, and then checks the queue's invariants over the whole run.
//
// The workers run in this process, each with its own Redis connection pool
// behind its own fault proxy. A subprocess built from cmd/jmfarm was not used:
// that package is a main package this one cannot import, its job runner needs
// ffmpeg and a JuiceFS-shaped mount, and nothing in it can stop between two
// queue writes on cue. What the invariants are about lives here instead: every
// lease, fence, requeue, receipt and retention rule is a Redis script in this
// package, and the workers call exactly those methods in the order
// cmd/jmfarm's runQueue does (claim, start, renew, publish outcome, then ACK;
// stop when an outcome cannot be published). A worker's death is its context
// cancelled and its proxy closed, which drops whatever it had not yet sent;
// nothing it does afterwards reaches Redis.
//
// Time. Leases run 45 s and heartbeats 30 s of Redis TIME, far longer than a
// test can wait. When a worker dies or is partitioned for "longer than a
// lease", the harness ages it instead: its heartbeat is deleted and the
// deadlines of the deliveries it holds are set into the past, which is the
// state Redis would reach once that time had passed with the worker silent.
// The worker's own clock is not moved, so a partitioned worker comes back
// believing it still holds its lease: a stricter adversary than a real clock.
// Reaper and prune locks are dropped at the same moments, as their 15 s TTLs
// would have run out.
//
// Seeds choose the workload and the faults; goroutine scheduling still varies,
// so a seed reproduces a failure statistically rather than exactly.
//
// Not covered here: watch-v2 tracked jobs and their coverage ledgers, render
// storage permits, and the derivative manifest's per-blob retry state, which
// lives in each worker's local store rather than in Redis.

const (
	harnessDefaultSeeds = 16
	harnessSlots        = 4
	// A transient task fault is requeued on its first two reports and fails
	// the job on the third (maxPendingBlobAttempts's bound), counted in the
	// hardware-failure budget that worker loss must never spend.
	harnessTransientBudget = 3
	harnessIneligible      = 80
	harnessChaosWindow     = 2500 * time.Millisecond
	// The farm settles once chaos ends; a loaded host only slows it. The
	// harness waits while jobs and receipts keep changing state and fails
	// once nothing has changed for harnessQuietStall, or at harnessQuietLimit
	// whatever happens (a delivery cycling forever).
	harnessQuietStall = 25 * time.Second
	harnessQuietLimit = 90 * time.Second
	// harnessTimeBudget stops the run from starting another seed, so a
	// uniformly slow host reports the seeds it ran instead of reaching go
	// test's 10-minute timeout. A seed is bounded by its 2-minute context.
	harnessTimeBudget = 5 * time.Minute
	// harnessAbsentCapability is a capability no harness worker has, so the
	// ineligible backlog stays queued in front of eligible work for the whole
	// run and every claim must look past it.
	harnessAbsentCapability = "decoder:farm-harness-absent"
)

// harnessPublishRetry scales cmd/jmfarm's defaultClaimPublishRetry (6
// attempts, 250 ms doubling to 4 s) down to the harness's time scale.
var harnessPublishRetry = struct {
	attempts   int
	backoff    time.Duration
	maxBackoff time.Duration
}{attempts: 6, backoff: 25 * time.Millisecond, maxBackoff: 200 * time.Millisecond}

func TestFarmHarnessCrashPartitionInvariants(t *testing.T) {
	metaURL := os.Getenv("JM_FARM_TEST_REDIS_URL")
	if metaURL == "" || os.Getenv("JM_FARM_TEST_REDIS_FLUSH") != "1" {
		t.Skip("set JM_FARM_TEST_REDIS_URL to isolated Redis and JM_FARM_TEST_REDIS_FLUSH=1")
	}
	provided, err := redis.ParseURL(metaURL)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	providedClient := redis.NewClient(provided)
	defer providedClient.Close()
	if err := providedClient.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	bin, reason, err := findHarnessRedisServer(ctx, providedClient)
	if err != nil {
		t.Fatal(err)
	}
	var backend harnessBackend
	if bin != "" {
		server, err := startHarnessRedis(bin)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(server.close)
		backend = harnessBackend{options: server.options, dial: server.dial, server: server}
		t.Logf("farm harness: private %s, appendonly yes, appendfsync always", bin)
	} else {
		// Without a binary of its own the harness cannot restart Redis, and
		// the invariants then rest on the provided server's own durability.
		addr := provided.Addr
		backend = harnessBackend{
			options: func() *redis.Options { copy := *provided; return &copy },
			dial:    func() (net.Conn, error) { return net.DialTimeout("tcp", addr, 500*time.Millisecond) },
		}
		t.Logf("farm harness: no Redis 7.4 server binary (%s); using JM_FARM_TEST_REDIS_URL without Redis restarts", reason)
	}

	seeds, base := harnessSeeds(t)
	t.Logf("farm harness: %d seed(s) from %d; rerun one with JM_FARM_HARNESS_SEED=<seed> JM_FARM_HARNESS_SEEDS=1", seeds, base)
	total := map[string]int{}
	started, ran := time.Now(), 0
	var ranSeeds []string
	for i := 0; i < seeds; i++ {
		if i > 0 && time.Since(started) > harnessTimeBudget {
			t.Logf("farm harness: stopped after %d of %d seed(s): the %s budget is spent", ran, seeds, harnessTimeBudget)
			break
		}
		ran++
		seed := base + int64(i)
		ranSeeds = append(ranSeeds, strconv.FormatInt(seed, 10))
		var stats map[string]int
		ok := t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			stats = runFarmHarnessSeed(t, backend, seed)
		})
		for key, value := range stats {
			total[key] += value
		}
		if !ok {
			t.Errorf("farm harness seed %d failed; rerun with JM_FARM_HARNESS_SEED=%d JM_FARM_HARNESS_SEEDS=1", seed, seed)
			break
		}
	}
	t.Logf("farm harness: %d seed(s) in %s (%s): %s", ran, time.Since(started).Round(time.Millisecond),
		strings.Join(ranSeeds, ","), formatHarnessStats(total))
	if t.Failed() || ran < harnessDefaultSeeds {
		// Fewer seeds than the default may not reach every fault.
		return
	}
	// The faults are random; make sure the run exercised what it claims to.
	required := []string{"kills", "partitions_long", "reaped", "stale_probes", "fenced_writes",
		"planner_replays", "transient_requeues", "files_added"}
	if backend.server != nil {
		required = append(required, "redis_restarts")
	}
	for _, key := range required {
		if total[key] == 0 {
			t.Errorf("farm harness never exercised %s over %d seeds: %s", key, ran, formatHarnessStats(total))
		}
	}
}

func harnessSeeds(t *testing.T) (count int, base int64) {
	count = harnessDefaultSeeds
	if raw := os.Getenv("JM_FARM_HARNESS_SEEDS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			t.Fatalf("JM_FARM_HARNESS_SEEDS=%q is not a positive count", raw)
		}
		count = n
	}
	base = time.Now().UnixNano() % 1_000_000_000
	if raw := os.Getenv("JM_FARM_HARNESS_SEED"); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			t.Fatalf("JM_FARM_HARNESS_SEED=%q is not an integer", raw)
		}
		base = n
	}
	return count, base
}

func formatHarnessStats(stats map[string]int) string {
	keys := make([]string, 0, len(stats))
	for key := range stats {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", key, stats[key]))
	}
	return strings.Join(parts, " ")
}

type harnessBackend struct {
	options func() *redis.Options // a direct, unproxied connection to Redis
	dial    func() (net.Conn, error)
	server  *harnessRedis // nil when the harness does not own Redis
}

// harnessGrant is one delivery the harness saw a worker receive.
type harnessGrant struct {
	serial              int
	worker, name, token string
	targets             []string
	hardwareFailures    int
}

// harnessCommit is a terminal or dispatched write that returned success.
type harnessCommit struct {
	serial              int
	token, status, name string
}

// harnessWalk is one planner pass over a parent's directory.
type harnessWalk struct {
	targets  []string
	children []Job
	created  int
	err      error
}

type farmHarness struct {
	t       *testing.T
	seed    int64
	backend harnessBackend
	admin   *Client
	ctx     context.Context
	cancel  context.CancelFunc
	root    string
	out     string

	mu         sync.Mutex
	rng        *rand.Rand
	serial     int
	grants     map[string][]harnessGrant
	commits    map[string][]harnessCommit
	walks      map[string][]harnessWalk
	parents    map[string]Job
	parentDirs []string
	children   map[string]Job
	// unconfirmed are the children only a failed walk named. Its write may or
	// may not have landed; one is a job only once its record exists.
	unconfirmed map[string]Job
	ineligible  map[string]bool
	live        map[int]*harnessWorker
	stats       map[string]int
	chaosOn     bool
	// probes are the scenarios every seed runs once while chaos is on, so
	// that no run passes without exercising them.
	probes   map[string]bool
	stopping bool

	faults      sync.WaitGroup
	supervisors sync.WaitGroup
}

func runFarmHarnessSeed(t *testing.T, backend harnessBackend, seed int64) map[string]int {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	h := &farmHarness{
		t: t, seed: seed, backend: backend, ctx: ctx, cancel: cancel,
		root: filepath.Join(t.TempDir(), "mount"), out: filepath.Join(t.TempDir(), "outputs"),
		rng:    rand.New(rand.NewSource(seed)),
		grants: map[string][]harnessGrant{}, commits: map[string][]harnessCommit{},
		walks: map[string][]harnessWalk{}, parents: map[string]Job{}, children: map[string]Job{},
		unconfirmed: map[string]Job{},
		ineligible:  map[string]bool{}, live: map[int]*harnessWorker{}, stats: map[string]int{},
	}
	defer cancel()
	admin := redis.NewClient(backend.options())
	h.admin = Wrap(admin)
	defer h.admin.Close()
	if err := h.retry("flush", func(ctx context.Context) error { return admin.FlushDB(ctx).Err() }); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{h.root, h.out} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// The backlog goes in first, so it sits at the claim end of the lane that
	// every eligible job joins later.
	h.enqueueIneligible(harnessIneligible)
	for i := 0; i < 6; i++ {
		h.enqueueParent()
	}
	h.mu.Lock()
	h.chaosOn = true
	h.probes = map[string]bool{"stale-publisher": true, "planner-replay": true}
	h.mu.Unlock()
	for slot := 0; slot < harnessSlots; slot++ {
		h.supervisors.Add(1)
		go h.supervise(slot)
	}
	h.chaos(harnessChaosWindow)
	converged := h.quiesce()
	h.stopWorkers()
	if !converged {
		return h.snapshotStats()
	}
	h.finalReap()
	h.check()
	h.checkRetentionAndPrune()
	return h.snapshotStats()
}

func (h *farmHarness) errorf(format string, args ...any) {
	h.t.Helper()
	h.t.Errorf("seed %d: "+format, append([]any{h.seed}, args...)...)
}

func (h *farmHarness) count(key string, n int) {
	if n == 0 {
		return
	}
	h.mu.Lock()
	h.stats[key] += n
	h.mu.Unlock()
}

func (h *farmHarness) snapshotStats() map[string]int {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[string]int, len(h.stats))
	for key, value := range h.stats {
		out[key] = value
	}
	return out
}

func (h *farmHarness) randn(n int) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.rng.Intn(n)
}

func (h *farmHarness) randDuration(lo, hi time.Duration) time.Duration {
	return lo + time.Duration(h.randn(int(hi-lo)/int(time.Millisecond)+1))*time.Millisecond
}

// retry runs a control-plane operation until it succeeds; Redis may be
// restarting. The harness's own writes never race a fence, so any error is
// availability.
func (h *farmHarness) retry(what string, op func(context.Context) error) error {
	deadline := time.Now().Add(20 * time.Second)
	for {
		err := op(h.ctx)
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) || h.ctx.Err() != nil {
			return fmt.Errorf("%s: %w", what, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// harnessOutcomeClass decides, from the seed and the job ID alone, how a
// child's synthetic task behaves on every execution: succeed, fail
// transiently until it has been requeued k times, or fail permanently.
func (h *farmHarness) outcomeClass(jobID string) (class string, k int) {
	sum := fnv.New64a()
	_, _ = fmt.Fprintf(sum, "%d/%s", h.seed, jobID)
	v := sum.Sum64()
	switch roll := v % 100; {
	case roll < 55:
		return "ok", 0
	case roll < 80:
		return "transient", 1 + int((v/100)%4)
	default:
		return "permanent", 0
	}
}

func harnessShardSize(jobID string) int {
	sum := fnv.New32a()
	_, _ = sum.Write([]byte(jobID))
	return 2 + int(sum.Sum32()%2)
}

func (h *farmHarness) enqueueParent() {
	h.mu.Lock()
	n := len(h.parents)
	files := 1 + h.rng.Intn(7)
	h.mu.Unlock()
	dir := filepath.Join(h.root, fmt.Sprintf("p%02d", n))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		h.errorf("fixture: %v", err)
		return
	}
	for i := 0; i < files; i++ {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("clip-%02d.mov", i)), []byte("m"), 0o644); err != nil {
			h.errorf("fixture: %v", err)
			return
		}
	}
	job := Job{ID: fmt.Sprintf("fh-%d-p%02d", h.seed, n), Path: dir, Kinds: []string{KindDerivatives},
		Producer: "farm-harness", EnqueuedAt: nowISO()}
	h.mu.Lock()
	h.parents[job.ID] = job
	h.parentDirs = append(h.parentDirs, dir)
	h.mu.Unlock()
	// A reply lost to a Redis crash makes the retry a second push of the same
	// job: a duplicate delivery the queue must absorb, as from a real producer.
	if err := h.retry("enqueue parent", func(ctx context.Context) error { return h.admin.Enqueue(ctx, job) }); err != nil {
		h.errorf("%v", err)
	}
}

func (h *farmHarness) enqueueIneligible(n int) {
	for i := 0; i < n; i++ {
		h.mu.Lock()
		id := fmt.Sprintf("fh-%d-inelig-%03d", h.seed, len(h.ineligible))
		h.ineligible[id] = true
		h.mu.Unlock()
		job := Job{ID: id, Path: h.root, Kinds: []string{KindDerivatives}, Producer: "farm-harness",
			QueueClass: QueueClassServer, RequiredCapabilities: []string{harnessAbsentCapability},
			ShardIndex: 1, ShardCount: 1, RetryTargets: []string{filepath.Join(h.root, "never")}}
		if err := h.retry("enqueue ineligible", func(ctx context.Context) error { return h.admin.Enqueue(ctx, job) }); err != nil {
			h.errorf("%v", err)
		}
	}
}

// addFile models a file copied into a directory while its plan may be
// replayed: a later walk sees it and must queue it.
func (h *farmHarness) addFile() {
	h.mu.Lock()
	if len(h.parentDirs) == 0 {
		h.mu.Unlock()
		return
	}
	dir := h.parentDirs[h.rng.Intn(len(h.parentDirs))]
	h.mu.Unlock()
	h.addFileTo(dir)
}

func (h *farmHarness) addFileTo(dir string) {
	h.mu.Lock()
	h.stats["files_added"]++
	name := fmt.Sprintf("late-%04d.mov", h.stats["files_added"])
	h.mu.Unlock()
	if err := os.WriteFile(filepath.Join(dir, name), []byte("m"), 0o644); err != nil {
		h.errorf("fixture: %v", err)
	}
}

// ---- supervision and faults ----------------------------------------------

// supervise keeps one worker slot occupied, the way a service manager
// restarts a crashed jmfarm: each incarnation is a new process with a new ID.
func (h *farmHarness) supervise(slot int) {
	defer h.supervisors.Done()
	for incarnation := 0; ; incarnation++ {
		h.mu.Lock()
		stopping := h.stopping
		h.mu.Unlock()
		if stopping {
			return
		}
		hw, err := h.startWorker(slot, incarnation)
		if err != nil {
			h.errorf("start worker: %v", err)
			return
		}
		if hw == nil {
			return // the run is stopping
		}
		<-hw.done
		h.retire(hw)
		time.Sleep(h.randDuration(10*time.Millisecond, 60*time.Millisecond))
	}
}

func (h *farmHarness) startWorker(slot, incarnation int) (*harnessWorker, error) {
	proxy, err := newHarnessProxy(h.backend.dial)
	if err != nil {
		return nil, err
	}
	id := "fh-" + NewID()
	opts := h.backend.options()
	opts.Network, opts.Addr = "tcp", proxy.Addr()
	opts.ClientName = id
	opts.DialTimeout = 300 * time.Millisecond
	opts.ReadTimeout = 250 * time.Millisecond
	opts.WriteTimeout = 250 * time.Millisecond
	opts.PoolSize = 4
	ctx, cancel := context.WithCancel(h.ctx)
	h.mu.Lock()
	if h.stopping {
		h.mu.Unlock()
		cancel()
		proxy.Close()
		return nil, nil
	}
	rng := rand.New(rand.NewSource(h.seed*1_000_003 + int64(slot)*10_007 + int64(incarnation)))
	var plan *harnessPointFault
	if rng.Intn(100) < 55 {
		points := []string{"claimed", "running", "progress", "outputs", "terminal", "acked", "planned", "planned"}
		actions := []string{"kill", "kill", "kill", "kill", "long", "long", "long", "short", "short", "reset"}
		plan = &harnessPointFault{point: points[rng.Intn(len(points))], job: 1 + rng.Intn(3),
			action: actions[rng.Intn(len(actions))]}
	}
	hw := &harnessWorker{
		h: h, slot: slot, proxy: proxy, q: Wrap(redis.NewClient(opts)), ctx: ctx, cancel: cancel,
		done: make(chan struct{}), rng: rng, plan: plan,
		w: Worker{ID: id, Name: fmt.Sprintf("fh-w%d-i%d", slot, incarnation), Role: QueueClassServer,
			Capabilities: []string{"cpu", "metadata"}},
	}
	h.live[slot] = hw
	h.mu.Unlock()
	go hw.run()
	return hw, nil
}

// retire finishes a dead worker: nothing of it may still reach Redis, then
// the time it would take for its heartbeat and leases to lapse passes.
func (h *farmHarness) retire(hw *harnessWorker) {
	hw.kill()
	_ = hw.q.Close()
	h.waitClientsGone(hw.w.ID)
	h.expireWorker(hw.w.ID)
	h.mu.Lock()
	if h.live[hw.slot] == hw {
		delete(h.live, hw.slot)
	}
	h.mu.Unlock()
}

type harnessPointFault struct {
	point  string
	job    int
	action string
}

// pointFault injects a worker's planned fault at the step it reached.
func (h *farmHarness) pointFault(hw *harnessWorker, action string) {
	h.mu.Lock()
	if !h.chaosOn {
		h.mu.Unlock()
		return
	}
	h.stats["point_faults"]++
	if action != "kill" {
		h.faults.Add(1)
	}
	h.mu.Unlock()
	switch action {
	case "kill":
		h.count("kills", 1)
		hw.kill()
	default:
		go func() {
			defer h.faults.Done()
			h.partition(hw, action)
		}()
	}
}

// partition cuts a worker off from Redis. A long partition outlasts the
// worker's lease: once nothing more is in flight the worker is aged, and its
// deliveries become the reaper's while it still runs.
func (h *farmHarness) partition(hw *harnessWorker, mode string) {
	if !hw.proxy.Partition(mode == "reset") {
		return
	}
	hold := h.randDuration(30*time.Millisecond, 250*time.Millisecond)
	switch mode {
	case "long":
		h.count("partitions_long", 1)
		hold = h.randDuration(400*time.Millisecond, 1500*time.Millisecond)
		h.waitClientsQuiet(hw.w.ID)
		h.expireWorker(hw.w.ID)
		if h.randn(2) == 0 {
			h.reap()
		}
	case "reset":
		h.count("partitions_reset", 1)
	default:
		h.count("partitions_short", 1)
	}
	select {
	case <-time.After(hold):
	case <-hw.done:
	}
	hw.proxy.Heal()
}

func (h *farmHarness) takeProbe(name string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.chaosOn || !h.probes[name] {
		return false
	}
	delete(h.probes, name)
	return true
}

// supersede freezes a worker that has passed its last lease check and is
// about to publish, as a long GC or VM pause would: it is cut off, aged and
// reaped, and stays frozen until another delivery has taken the job. It
// reports whether that happened; the frozen worker's write must then lose.
func (h *farmHarness) supersede(hw *harnessWorker, claim Claim) bool {
	if !hw.proxy.Partition(false) {
		return false
	}
	defer hw.proxy.Heal()
	h.count("partitions_long", 1)
	h.waitClientsQuiet(hw.w.ID)
	h.expireWorker(hw.w.ID)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		h.reap()
		token, err := h.admin.rdb.HGet(h.ctx, JobHashPrefix+claim.Job.ID, "delivery_token").Result()
		if err == nil && token != "" && token != claim.Token {
			h.count("stale_probes", 1)
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

func (h *farmHarness) liveWorker() *harnessWorker {
	h.mu.Lock()
	defer h.mu.Unlock()
	slots := make([]int, 0, len(h.live))
	for slot := range h.live {
		slots = append(slots, slot)
	}
	if len(slots) == 0 {
		return nil
	}
	sort.Ints(slots)
	return h.live[slots[h.rng.Intn(len(slots))]]
}

func (h *farmHarness) chaos(window time.Duration) {
	end := time.Now().Add(window)
	restarts := 0
	for time.Now().Before(end) && !h.t.Failed() {
		time.Sleep(h.randDuration(30*time.Millisecond, 120*time.Millisecond))
		switch roll := h.randn(100); {
		case roll < 16:
			if hw := h.liveWorker(); hw != nil {
				h.count("kills", 1)
				hw.kill()
			}
		case roll < 48:
			if hw := h.liveWorker(); hw != nil {
				mode := []string{"short", "short", "long", "long", "reset"}[h.randn(5)]
				h.mu.Lock()
				h.faults.Add(1)
				h.mu.Unlock()
				go func() {
					defer h.faults.Done()
					h.partition(hw, mode)
				}()
			}
		case roll < 52:
			if h.backend.server != nil && restarts < 2 {
				restarts++
				h.restartRedis()
			}
		case roll < 64:
			h.addFile()
		case roll < 72:
			h.mu.Lock()
			more := len(h.parents) < 12
			h.mu.Unlock()
			if more {
				h.enqueueParent()
			}
		case roll < 76:
			h.enqueueIneligible(4)
		}
	}
	h.mu.Lock()
	h.chaosOn = false
	h.mu.Unlock()
	h.faults.Wait()
}

// restartRedis crashes Redis with SIGKILL and starts it again from its
// append-only file. Every acknowledged write must survive; every connection
// drops, so replies in flight are lost.
func (h *farmHarness) restartRedis() {
	if err := h.backend.server.crash(); err != nil {
		h.errorf("crash redis: %v", err)
		return
	}
	time.Sleep(h.randDuration(20*time.Millisecond, 200*time.Millisecond))
	if err := h.backend.server.start(); err != nil {
		h.errorf("restart redis from its append-only file: %v", err)
		return
	}
	h.count("redis_restarts", 1)
}

// clientsNamed returns the CLIENT LIST lines of one worker's connections.
func (h *farmHarness) clientsNamed(name string) ([]string, error) {
	list, err := h.admin.rdb.ClientList(h.ctx).Result()
	if err != nil {
		return nil, err
	}
	var out []string
	for _, line := range strings.Split(list, "\n") {
		if strings.Contains(line, " name="+name+" ") {
			out = append(out, line)
		}
	}
	return out, nil
}

// waitClientsGone returns once Redis has freed every connection of a dead
// worker, so every command it ever sent has run. Only then may its heartbeat
// be deleted: a late heartbeat would make it look alive for 30 s.
func (h *farmHarness) waitClientsGone(name string) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		lines, err := h.clientsNamed(name)
		if err == nil && len(lines) == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.errorf("worker %s still has Redis connections 5 s after its death", name)
}

// waitClientsQuiet waits until a partitioned worker's connections have no
// unprocessed input, so a renewal that left before the partition has run.
func (h *farmHarness) waitClientsQuiet(name string) {
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		lines, err := h.clientsNamed(name)
		quiet := err == nil
		for _, line := range lines {
			if !strings.Contains(line, " qbuf=0 ") {
				quiet = false
			}
		}
		if quiet {
			time.Sleep(10 * time.Millisecond)
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// harnessExpireScript is the passage of a lease's worth of time for one
// silent worker: its heartbeat lapses, the deadlines of the deliveries it
// leases fall into the past, and the 15 s reaper and prune locks run out.
const harnessExpireScript = `
local worker = ARGV[1]
redis.call('DEL', 'juicefarm:worker:' .. worker)
local now = tonumber(redis.call('TIME')[1])
local aged = 0
for _, raw in ipairs(redis.call('LRANGE', 'juicefarm:processing:' .. worker, 0, -1)) do
  local ok, job = pcall(cjson.decode, raw)
  if ok and type(job) == 'table' and type(job.id) == 'string' then
    local key = 'juicefarm:job:' .. job.id
    if redis.call('HGET', key, 'lease_owner') == worker then
      for _, field in ipairs({'delivery_expires_unix', 'lease_expires_unix'}) do
        if redis.call('HEXISTS', key, field) == 1 then redis.call('HSET', key, field, now - 1) end
      end
      redis.call('ZADD', 'juicefarm:leases', 'XX', now - 1, job.id)
      aged = aged + 1
    end
  end
end
for _, lock in ipairs(redis.call('KEYS', 'juicefarm:reaper:*')) do redis.call('DEL', lock) end
redis.call('DEL', 'juicefarm:jobs:prune-lock')
return aged`

func (h *farmHarness) expireWorker(workerID string) {
	if err := h.retry("expire worker", func(ctx context.Context) error {
		return h.admin.rdb.Eval(ctx, harnessExpireScript, nil, workerID).Err()
	}); err != nil {
		h.errorf("%v", err)
	}
}

func (h *farmHarness) reap() {
	n, err := h.admin.RecoverAbandonedWorkers(h.ctx)
	if err == nil {
		h.count("reaped", n)
	}
}

func (h *farmHarness) stopWorkers() {
	h.mu.Lock()
	h.stopping = true
	live := make([]*harnessWorker, 0, len(h.live))
	for _, hw := range h.live {
		live = append(live, hw)
	}
	h.mu.Unlock()
	for _, hw := range live {
		hw.kill()
	}
	h.supervisors.Wait()
}

// ---- the worker -------------------------------------------------------------

type harnessWorker struct {
	h     *farmHarness
	slot  int
	w     Worker
	q     *Client
	proxy *harnessProxy
	ctx   context.Context
	// cancel and the proxy's Close together are the worker's SIGKILL.
	cancel context.CancelFunc
	done   chan struct{}
	rng    *rand.Rand // used only by the worker's own goroutine
	plan   *harnessPointFault
	jobs   int

	mu      sync.Mutex
	current string // the job this worker knows it holds
}

func (hw *harnessWorker) kill() {
	hw.cancel()
	hw.proxy.Close()
}

func (hw *harnessWorker) setCurrent(id string) {
	hw.mu.Lock()
	hw.current = id
	hw.mu.Unlock()
}

func (hw *harnessWorker) currentJob() string {
	hw.mu.Lock()
	defer hw.mu.Unlock()
	return hw.current
}

func (hw *harnessWorker) faultDue(point string) bool {
	return hw.plan != nil && hw.plan.point == point && hw.plan.job == hw.jobs
}

func (hw *harnessWorker) fault(point string) {
	if !hw.faultDue(point) {
		return
	}
	action := hw.plan.action
	hw.plan = nil
	hw.h.pointFault(hw, action)
}

func (hw *harnessWorker) sleep(d time.Duration) {
	select {
	case <-hw.ctx.Done():
	case <-time.After(d):
	}
}

// run is the queue loop of cmd/jmfarm's runQueue at harness scale: heartbeat,
// maintenance, claim, execute. Returning is the process exiting.
func (hw *harnessWorker) run() {
	defer close(hw.done)
	ctx := hw.ctx
	var nextMaintenance time.Time
	for ctx.Err() == nil {
		if err := hw.q.Heartbeat(ctx, hw.w); err != nil {
			hw.sleep(20 * time.Millisecond)
			continue
		}
		if time.Now().After(nextMaintenance) {
			if n, err := hw.q.RecoverAbandonedWorkers(ctx); err == nil {
				hw.h.count("reaped", n)
			}
			_, _ = hw.q.RecoverUnserviceableReady(ctx)
			_, _ = hw.q.PromoteServiceableReady(ctx)
			_, _ = hw.q.PruneExpiredJobIndex(ctx, 100)
			nextMaintenance = time.Now().Add(time.Duration(100+hw.rng.Intn(200)) * time.Millisecond)
		}
		claim, ok, err := hw.q.ClaimForWorker(ctx, 500*time.Millisecond, hw.w)
		if err != nil {
			hw.h.count("claim_errors", 1)
			hw.sleep(50 * time.Millisecond)
			continue
		}
		if !ok {
			continue
		}
		if stop := hw.execute(claim); stop != "" {
			hw.h.count("worker_stops", 1)
			return
		}
	}
}

type harnessResult struct {
	kind   string // dispatched, dispatch-retry, done, failed, transient
	errMsg string
}

// execute runs one delivery and publishes its outcome. A non-empty return
// is the reason the worker stops, as runQueue returns.
func (hw *harnessWorker) execute(claim Claim) (stop string) {
	h, ctx := hw.h, hw.ctx
	hw.jobs++
	hw.setCurrent(claim.Job.ID)
	defer hw.setCurrent("")
	serial := h.recordGrant(hw, claim)
	hw.fault("claimed")
	worker := hw.w
	worker.CurrentJob = claim.Job.ID
	_ = hw.q.Heartbeat(ctx, worker)
	if _, err := hw.q.MarkClaimRunningWithDeadline(ctx, claim, worker); err != nil {
		h.noteFence(err)
		return "mark-running: " + err.Error()
	}
	hw.fault("running")
	guard := hw.startLeaseGuard(claim)
	var result harnessResult
	if claim.Job.PlanOnly {
		result = hw.planChildren(guard.ctx, claim)
	} else {
		result = hw.work(guard.ctx, claim, serial)
	}
	guard.stop()
	if ctx.Err() != nil {
		return "killed"
	}
	if guard.lost.Load() {
		h.count("lease_lost", 1)
		return "lease lost during execution"
	}
	switch result.kind {
	case "dispatched":
		if err := hw.publishDispatched(claim, serial); err != nil {
			return "publish dispatched: " + err.Error()
		}
	case "dispatch-retry":
		if err := hw.q.RequeueClaimSameRoute(ctx, claim, result.errMsg); err != nil {
			h.noteFence(err)
			return "dispatch retry: " + err.Error()
		}
	case "transient":
		if claim.Job.HardwareFailures < harnessTransientBudget-1 {
			if err := hw.q.RequeueClaimAfterHardwareFailure(ctx, claim, false, result.errMsg); err != nil {
				h.noteFence(err)
				return "transient requeue: " + err.Error()
			}
			h.count("transient_requeues", 1)
			break
		}
		if err := hw.publishOutcome(claim, serial, StatusFailed,
			"transient fault outlasted its retry budget: "+result.errMsg); err != nil {
			return "publish failed: " + err.Error()
		}
	case "done", "failed":
		if err := hw.publishOutcome(claim, serial, result.kind, result.errMsg); err != nil {
			return "publish " + result.kind + ": " + err.Error()
		}
	default:
		return "task ended without an outcome: " + result.kind
	}
	hw.fault("acked")
	return ""
}

type harnessLeaseGuard struct {
	ctx    context.Context
	cancel context.CancelFunc
	lost   atomic.Bool
	done   chan struct{}
}

// startLeaseGuard renews the delivery and refreshes the heartbeat until the
// task ends, and cancels the task once Redis says another delivery owns the
// job, like cmd/jmfarm's claim lease guard.
func (hw *harnessWorker) startLeaseGuard(claim Claim) *harnessLeaseGuard {
	ctx, cancel := context.WithCancel(hw.ctx)
	g := &harnessLeaseGuard{ctx: ctx, cancel: cancel, done: make(chan struct{})}
	worker := hw.w
	worker.CurrentJob = claim.Job.ID
	go func() {
		defer close(g.done)
		tick := time.NewTicker(80 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			if _, err := hw.q.RenewClaimWithDeadline(ctx, claim); errors.Is(err, ErrClaimLost) {
				g.lost.Store(true)
				cancel()
				return
			}
			_ = hw.q.Heartbeat(ctx, worker)
		}
	}()
	return g
}

func (g *harnessLeaseGuard) stop() {
	g.cancel()
	<-g.done
}

// planChildren expands a directory parent into bounded children, the legacy
// planning path of runJob.
func (hw *harnessWorker) planChildren(ctx context.Context, claim Claim) harnessResult {
	entries, err := os.ReadDir(claim.Job.Path)
	if err != nil {
		return harnessResult{kind: "dispatch-retry", errMsg: err.Error()}
	}
	var targets []string
	for _, entry := range entries {
		targets = append(targets, filepath.Join(claim.Job.Path, entry.Name()))
	}
	sort.Strings(targets)
	children, created, err := hw.q.EnqueueTargetShards(ctx, claim.Job, targets, harnessShardSize(claim.Job.ID))
	hw.h.recordWalk(claim.Job.ID, harnessWalk{targets: targets, children: children, created: created, err: err})
	if err != nil {
		return harnessResult{kind: "dispatch-retry", errMsg: "enqueue bounded target shards: " + err.Error()}
	}
	if hw.h.takeProbe("planner-replay") {
		// The planner dies after publishing children and before marking the
		// parent dispatched, and a file is copied in before the replay walks.
		hw.h.addFileTo(claim.Job.Path)
		hw.h.count("kills", 1)
		hw.kill()
		return harnessResult{kind: "aborted"}
	}
	if hw.faultDue("planned") && hw.rng.Intn(2) == 0 {
		hw.h.addFileTo(claim.Job.Path)
	}
	hw.fault("planned")
	return harnessResult{kind: "dispatched"}
}

// work is a synthetic derivative pass: per target a little progress, then
// an output published by atomic rename, as the farm writes sidecars.
func (hw *harnessWorker) work(ctx context.Context, claim Claim, serial int) harnessResult {
	class, k := hw.h.outcomeClass(claim.Job.ID)
	targets := claim.Job.RetryTargets
	dir := filepath.Join(hw.h.out, claim.Job.ID)
	for i, target := range targets {
		select {
		case <-ctx.Done():
			return harnessResult{kind: "aborted"}
		case <-time.After(time.Duration(2+hw.rng.Intn(8)) * time.Millisecond):
		}
		if i == len(targets)/2 {
			hw.fault("progress")
		}
		if class == "permanent" {
			return harnessResult{kind: "failed", errMsg: "permanent: source cannot be decoded"}
		}
		if class == "transient" && claim.Job.HardwareFailures < k && i == len(targets)-1 {
			return harnessResult{kind: "transient", errMsg: "transient: derivative tree briefly unavailable"}
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return harnessResult{kind: "failed", errMsg: err.Error()}
		}
		tmp := filepath.Join(dir, fmt.Sprintf(".%s.%d.tmp", filepath.Base(target), serial))
		if err := os.WriteFile(tmp, []byte(strconv.Itoa(serial)), 0o644); err != nil {
			return harnessResult{kind: "failed", errMsg: err.Error()}
		}
		if err := os.Rename(tmp, filepath.Join(dir, filepath.Base(target))); err != nil {
			return harnessResult{kind: "failed", errMsg: err.Error()}
		}
	}
	hw.fault("outputs")
	return harnessResult{kind: "done"}
}

// harnessRetryableWrite is cmd/jmfarm's retryableClaimWriteError: retry
// network faults and the Redis replies that clear on their own, never a lost
// claim or a deterministic script error.
func harnessRetryableWrite(err error) bool {
	if err == nil || errors.Is(err, ErrClaimLost) {
		return false
	}
	var reply redis.Error
	if !errors.As(err, &reply) {
		return true
	}
	for _, prefix := range []string{"BUSY", "LOADING", "READONLY", "MASTERDOWN", "TRYAGAIN", "CLUSTERDOWN"} {
		if redis.HasErrorPrefix(err, prefix) {
			return true
		}
	}
	return false
}

// retryWrite is cmd/jmfarm's retryClaimWrite at harness scale.
func (hw *harnessWorker) retryWrite(op func(context.Context) error) error {
	delay := harnessPublishRetry.backoff
	for attempt := 1; ; attempt++ {
		err := op(hw.ctx)
		if !harnessRetryableWrite(err) || attempt >= harnessPublishRetry.attempts {
			if err != nil {
				hw.h.noteFence(err)
				if !errors.Is(err, ErrClaimLost) {
					hw.h.count("publish_stops", 1)
				}
			} else if attempt > 1 {
				hw.h.count("publish_retried", 1)
			}
			return err
		}
		select {
		case <-hw.ctx.Done():
			return err
		case <-time.After(delay):
		}
		if delay *= 2; delay > harnessPublishRetry.maxBackoff {
			delay = harnessPublishRetry.maxBackoff
		}
	}
}

// publishOutcome is cmd/jmfarm's publishClaimOutcome. The processed count
// carries the delivery's serial, so the final record names the delivery
// whose terminal write it holds.
func (hw *harnessWorker) publishOutcome(claim Claim, serial int, status, errMsg string) error {
	failed := 0
	if status == StatusFailed {
		failed = 1
	}
	stale := hw.h.takeProbe("stale-publisher") && hw.h.supersede(hw, claim)
	err := hw.retryWrite(func(ctx context.Context) error {
		return hw.q.CompleteClaim(ctx, claim, status, serial, failed, errMsg)
	})
	// The fence normally refuses it; a worker killed meanwhile, or a Redis
	// restart that outlasts the retry budget, refuses it too.
	if stale && err == nil {
		hw.h.errorf("superseded delivery %d of %s published %q over its replacement", serial, claim.Job.ID, status)
	}
	if err != nil {
		return fmt.Errorf("mark-%s: %w", status, err)
	}
	hw.h.recordCommit(claim.Job.ID, harnessCommit{serial: serial, token: claim.Token, status: status, name: hw.w.Name})
	hw.fault("terminal")
	if err := hw.retryWrite(func(ctx context.Context) error { return hw.q.AckClaim(ctx, claim) }); err != nil {
		return fmt.Errorf("ack %s: %w", status, err)
	}
	return nil
}

// publishDispatched is cmd/jmfarm's publishClaimDispatched for a legacy
// parent.
func (hw *harnessWorker) publishDispatched(claim Claim, serial int) error {
	if err := hw.retryWrite(func(ctx context.Context) error { return hw.q.MarkClaimDispatched(ctx, claim) }); err != nil {
		return fmt.Errorf("mark-dispatched: %w", err)
	}
	hw.h.recordCommit(claim.Job.ID, harnessCommit{serial: serial, token: claim.Token, status: StatusDispatched, name: hw.w.Name})
	hw.fault("terminal")
	if err := hw.retryWrite(func(ctx context.Context) error { return hw.q.AckClaim(ctx, claim) }); err != nil {
		return fmt.Errorf("ack dispatched: %w", err)
	}
	return nil
}

// ---- the ledger ----------------------------------------------------------

func (h *farmHarness) recordGrant(hw *harnessWorker, claim Claim) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.serial++
	h.grants[claim.Job.ID] = append(h.grants[claim.Job.ID], harnessGrant{
		serial: h.serial, worker: hw.w.ID, name: hw.w.Name, token: claim.Token,
		targets: append([]string(nil), claim.Job.RetryTargets...), hardwareFailures: claim.Job.HardwareFailures,
	})
	return h.serial
}

func (h *farmHarness) recordCommit(jobID string, commit harnessCommit) {
	h.mu.Lock()
	h.commits[jobID] = append(h.commits[jobID], commit)
	h.mu.Unlock()
}

func (h *farmHarness) recordWalk(parentID string, walk harnessWalk) {
	h.mu.Lock()
	defer h.mu.Unlock()
	prior := h.walks[parentID]
	if len(prior) > 0 {
		h.stats["planner_replays"]++
	}
	for _, earlier := range prior {
		if earlier.err == nil && walk.err == nil && slices.Equal(earlier.targets, walk.targets) && walk.created == 0 {
			h.stats["replays_created_none"]++
			break
		}
	}
	h.walks[parentID] = append(prior, walk)
	for _, child := range walk.children {
		if _, seen := h.children[child.ID]; seen {
			continue
		}
		// A walk whose enqueue failed may have written nothing; a file added
		// before the replay walks then names that shard differently, and the
		// child it would have been never exists.
		if walk.err != nil {
			h.unconfirmed[child.ID] = child
			continue
		}
		delete(h.unconfirmed, child.ID)
		h.children[child.ID] = child
	}
}

func (h *farmHarness) noteFence(err error) {
	if errors.Is(err, ErrClaimLost) {
		h.count("fenced_writes", 1)
	}
}

// ---- convergence and invariants ------------------------------------------

type harnessRecord struct {
	fields map[string]string
	ttl    time.Duration
}

// records reads every job status hash directly, not through the index the
// prune maintains.
func (h *farmHarness) records() (map[string]harnessRecord, error) {
	out := map[string]harnessRecord{}
	var cursor uint64
	for {
		keys, next, err := h.admin.rdb.Scan(h.ctx, cursor, JobHashPrefix+"*", 1000).Result()
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			kind, err := h.admin.rdb.Type(h.ctx, key).Result()
			if err != nil {
				return nil, err
			}
			if kind != "hash" {
				continue
			}
			fields, err := h.admin.rdb.HGetAll(h.ctx, key).Result()
			if err != nil {
				return nil, err
			}
			ttl, err := h.admin.rdb.TTL(h.ctx, key).Result()
			if err != nil {
				return nil, err
			}
			out[strings.TrimPrefix(key, JobHashPrefix)] = harnessRecord{fields: fields, ttl: ttl}
		}
		if cursor = next; cursor == 0 {
			return out, nil
		}
	}
}

func harnessTerminal(status string) bool {
	return status == StatusDone || status == StatusFailed || status == StatusCanceled || status == StatusDispatched
}

// knownJobs returns the jobs the harness enqueued or a planner returned. A
// child only a failed walk named counts once records holds it: that walk's
// write landed after all.
func (h *farmHarness) knownJobs(records map[string]harnessRecord) (parents, children map[string]Job, ineligible map[string]bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	parents, children, ineligible = map[string]Job{}, map[string]Job{}, map[string]bool{}
	for id, job := range h.parents {
		parents[id] = job
	}
	for id, job := range h.children {
		children[id] = job
	}
	for id, job := range h.unconfirmed {
		if _, landed := records[id]; landed {
			children[id] = job
		}
	}
	for id := range h.ineligible {
		ineligible[id] = true
	}
	return parents, children, ineligible
}

func (h *farmHarness) receiptLists() (map[string][]string, error) {
	out := map[string][]string{}
	for _, pattern := range []string{ProcessingPrefix + "*", TrackedProcessingPrefix + "*"} {
		var cursor uint64
		for {
			keys, next, err := h.admin.rdb.Scan(h.ctx, cursor, pattern, 1000).Result()
			if err != nil {
				return nil, err
			}
			for _, key := range keys {
				if kind, err := h.admin.rdb.Type(h.ctx, key).Result(); err != nil || kind != "list" {
					continue
				}
				raws, err := h.admin.rdb.LRange(h.ctx, key, 0, -1).Result()
				if err != nil {
					return nil, err
				}
				out[key] = raws
			}
			if cursor = next; cursor == 0 {
				break
			}
		}
	}
	return out, nil
}

func harnessRawID(raw string) string {
	var job struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(raw), &job); err != nil {
		return "<malformed>"
	}
	return job.ID
}

// quiesce waits, fault-free, until every eligible job is terminal and every
// receipt has been released. Dead workers have been aged, so the live
// workers' reapers can recover anything left behind. A job that never
// settles while its holder is alive and idle is stranded: the farm stops
// changing state. Progress, not a wall-clock deadline, keeps the wait going,
// so a slow host is not mistaken for a stuck farm.
func (h *farmHarness) quiesce() bool {
	start := time.Now()
	state, stalled := "", start.Add(harnessQuietStall)
	for {
		pending, receipts, err := h.unsettled()
		if err == nil && len(pending) == 0 && len(receipts) == 0 {
			return true
		}
		now := time.Now()
		if next := harnessFarmState(pending, receipts, err); next != state {
			state, stalled = next, now.Add(harnessQuietStall)
		}
		if now.After(stalled) || now.Sub(start) > harnessQuietLimit || h.ctx.Err() != nil {
			why := fmt.Sprintf("made no progress for %s", harnessQuietStall)
			if !now.After(stalled) {
				why = fmt.Sprintf("did not settle within %s", now.Sub(start).Round(time.Second))
			}
			h.diagnoseUnsettled(why, pending, receipts, err)
			return false
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// harnessFarmState summarises what quiesce waits on; any change in it is
// progress.
func harnessFarmState(pending map[string]harnessRecord, receipts map[string][]string, err error) string {
	if err != nil {
		return "read failed"
	}
	var parts []string
	for id, record := range pending {
		parts = append(parts, fmt.Sprintf("%s:%s:%s:%s:%s:%s", id, record.fields["status"], record.fields["delivery_token"],
			record.fields["lease_owner"], record.fields["processed"], record.fields["hardware_failures"]))
	}
	for key, raws := range receipts {
		for _, raw := range raws {
			parts = append(parts, key+"<"+harnessRawID(raw))
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

func (h *farmHarness) unsettled() (pending map[string]harnessRecord, receipts map[string][]string, err error) {
	records, err := h.records()
	if err != nil {
		return nil, nil, err
	}
	parents, children, _ := h.knownJobs(records)
	pending = map[string]harnessRecord{}
	for id := range parents {
		if record := records[id]; !harnessTerminal(record.fields["status"]) {
			pending[id] = record
		}
	}
	for id := range children {
		if record := records[id]; !harnessTerminal(record.fields["status"]) {
			pending[id] = record
		}
	}
	receipts, err = h.receiptLists()
	return pending, receipts, err
}

func (h *farmHarness) diagnoseUnsettled(why string, pending map[string]harnessRecord, receipts map[string][]string, err error) {
	if err != nil {
		h.errorf("farm %s; last read failed: %v", why, err)
		return
	}
	holders := map[string]string{}
	for key, raws := range receipts {
		for _, raw := range raws {
			holders[harnessRawID(raw)] = key
		}
	}
	h.mu.Lock()
	liveCurrent := map[string]string{}
	for _, hw := range h.live {
		liveCurrent[hw.w.ID] = hw.currentJob()
	}
	// A claim token the worker did receive, for another job, means the claim
	// script ran twice for one call: the first run's reply was lost and the
	// client resent it.
	tokenUse := map[string]string{}
	for jobID, grants := range h.grants {
		for _, grant := range grants {
			tokenUse[grant.token] = fmt.Sprintf("delivery %d of %s to %s", grant.serial, jobID, grant.name)
		}
	}
	namedBy := map[string][]string{}
	for parentID, walks := range h.walks {
		for i, walk := range walks {
			for _, child := range walk.children {
				namedBy[child.ID] = append(namedBy[child.ID], fmt.Sprintf("walk %d of %s (err=%v)", i+1, parentID, walk.err))
			}
		}
	}
	h.mu.Unlock()
	var lines []string
	for id, record := range pending {
		line := fmt.Sprintf("%s status=%q delivery_token=%q lease_owner=%q", id, record.fields["status"],
			record.fields["delivery_token"], record.fields["lease_owner"])
		if record.fields == nil {
			line += "; no record, named by " + strings.Join(namedBy[id], ", ")
		}
		if key := holders[id]; key != "" {
			owner := strings.TrimPrefix(strings.TrimPrefix(key, ProcessingPrefix), TrackedProcessingPrefix)
			current, alive := liveCurrent[owner]
			line += fmt.Sprintf(" receipt in %s (worker alive=%v, working on %q)", key, alive, current)
		}
		if use := tokenUse[record.fields["delivery_token"]]; use != "" {
			line += "; its delivery token was returned as " + use
		} else if record.fields["delivery_token"] != "" {
			line += "; its delivery token was never returned to a worker"
		}
		lines = append(lines, line)
	}
	for key, raws := range receipts {
		owner := strings.TrimPrefix(strings.TrimPrefix(key, ProcessingPrefix), TrackedProcessingPrefix)
		current, alive := liveCurrent[owner]
		for _, raw := range raws {
			if id := harnessRawID(raw); pending[id].fields == nil {
				lines = append(lines, fmt.Sprintf("receipt for settled job %s still in %s (worker alive=%v, working on %q)",
					id, key, alive, current))
			}
		}
	}
	sort.Strings(lines)
	h.errorf("farm %s with every worker healthy; %d job(s) and receipts stranded:\n  %s",
		why, len(pending), strings.Join(lines, "\n  "))
}

// finalReap lets the reaper see every worker dead and aged: it must find
// nothing left to recover.
func (h *farmHarness) finalReap() {
	n, err := h.admin.RecoverAbandonedWorkers(h.ctx)
	if err != nil {
		h.errorf("final recovery scan: %v", err)
	}
	if n != 0 {
		h.errorf("final recovery scan recovered %d receipt(s) after the farm had settled", n)
	}
}

func (h *farmHarness) check() {
	records, err := h.records()
	if err != nil {
		h.errorf("read job records: %v", err)
		return
	}
	parents, children, ineligible := h.knownJobs(records)
	h.mu.Lock()
	grants := make(map[string][]harnessGrant, len(h.grants))
	for id, list := range h.grants {
		grants[id] = append([]harnessGrant(nil), list...)
	}
	commits := make(map[string][]harnessCommit, len(h.commits))
	for id, list := range h.commits {
		commits[id] = append([]harnessCommit(nil), list...)
	}
	walks := make(map[string][]harnessWalk, len(h.walks))
	for id, list := range h.walks {
		walks[id] = append([]harnessWalk(nil), list...)
	}
	h.stats["jobs"] += len(parents) + len(children)
	for id := range h.unconfirmed {
		if _, landed := records[id]; !landed {
			h.stats["failed_walk_children_unwritten"]++
		}
	}
	h.mu.Unlock()

	// Every record belongs to a job the harness enqueued or a planner
	// returned, and every such job has a record.
	for id := range records {
		_, isParent := parents[id]
		_, isChild := children[id]
		if !isParent && !isChild && !ineligible[id] {
			h.errorf("record %s belongs to no enqueued or planned job: %v", id, records[id].fields)
		}
	}

	for id := range parents {
		record, ok := records[id]
		if !ok {
			h.errorf("parent %s has no record", id)
			continue
		}
		if status := record.fields["status"]; status != StatusDispatched {
			h.errorf("parent %s ended %q; want dispatched", id, status)
		}
		h.checkCommit(id, record, grants[id], commits[id], false)
		h.checkRetired(id, record)
		h.checkShards(id, walks[id], records, grants)
	}

	for id, child := range children {
		record, ok := records[id]
		if !ok {
			h.errorf("child %s of %s has no record", id, child.ParentID)
			continue
		}
		h.checkCommit(id, record, grants[id], commits[id], true)
		h.checkRetired(id, record)
		class, k := h.outcomeClass(id)
		status, failures := record.fields["status"], record.fields["hardware_failures"]
		switch {
		case class == "ok" && (status != StatusDone || failures != "0"):
			h.errorf("child %s (succeeds) ended %q with %s transient requeues; want done with 0", id, status, failures)
		case class == "permanent" && (status != StatusFailed || failures != "0" ||
			!strings.HasPrefix(record.fields["error"], "permanent:")):
			h.errorf("child %s (permanent fault) ended %q with %s requeues, error %q; want failed at once",
				id, status, failures, record.fields["error"])
		case class == "transient" && k < harnessTransientBudget && (status != StatusDone || failures != strconv.Itoa(k)):
			h.errorf("child %s (transient, clears after %d) ended %q with %s requeues; want done with %d",
				id, k, status, failures, k)
		case class == "transient" && k >= harnessTransientBudget && (status != StatusFailed ||
			failures != strconv.Itoa(harnessTransientBudget-1) ||
			!strings.HasPrefix(record.fields["error"], "transient fault outlasted")):
			h.errorf("child %s (transient, outlasts the budget) ended %q with %s requeues, error %q; want failed after %d",
				id, status, failures, record.fields["error"], harnessTransientBudget-1)
		}
		if status == StatusDone {
			h.checkOutputs(id, record, grants[id])
		}
	}

	for id := range ineligible {
		record, ok := records[id]
		if !ok {
			h.errorf("ineligible job %s lost its record", id)
			continue
		}
		if record.fields["status"] != StatusQueued || record.fields["delivery_token"] != "" || record.ttl != -1 {
			h.errorf("ineligible job %s ended status=%q delivery_token=%q ttl=%v; want queued, never delivered, no TTL",
				id, record.fields["status"], record.fields["delivery_token"], record.ttl)
		}
	}

	h.checkNoStranded(ineligible)
}

// checkCommit proves the terminal write came from the job's one delivery that
// still held it: no two deliveries committed, and the delivery the record
// names is the last one to have started the job.
func (h *farmHarness) checkCommit(id string, record harnessRecord, grants []harnessGrant, commits []harnessCommit, child bool) {
	tokens := map[string]bool{}
	for _, commit := range commits {
		tokens[commit.token] = true
	}
	if len(tokens) > 1 {
		h.errorf("job %s has terminal writes from %d deliveries: %+v", id, len(tokens), commits)
	}
	committer := ""
	if child {
		serial, err := strconv.Atoi(record.fields["processed"])
		var grant *harnessGrant
		for i := range grants {
			if grants[i].serial == serial {
				grant = &grants[i]
			}
		}
		if err != nil || grant == nil {
			h.errorf("job %s record names delivery %q, which never claimed it", id, record.fields["processed"])
			return
		}
		committer = grant.name
		if len(commits) > 0 && commits[0].serial != serial {
			h.errorf("job %s record holds delivery %d's outcome; delivery %d's write succeeded", id, serial, commits[0].serial)
		}
	} else if len(commits) > 0 {
		committer = commits[0].name
	}
	// MarkClaimRunning stamps the worker; a later delivery's start would have
	// overwritten it. The committing delivery must be the last to start.
	if committer != "" && record.fields["worker"] != committer {
		h.errorf("job %s committed by %s but last started by %s: a superseded delivery's write landed",
			id, committer, record.fields["worker"])
	}
}

// checkRetired proves a terminal record released its delivery and expires.
func (h *farmHarness) checkRetired(id string, record harnessRecord) {
	if !harnessTerminal(record.fields["status"]) {
		return
	}
	for _, field := range []string{"delivery_token", "lease_owner", "delivery_expires_unix", "lease_expires_unix"} {
		if value := record.fields[field]; value != "" {
			h.errorf("terminal job %s still carries %s=%q", id, field, value)
		}
	}
	if record.ttl <= 0 || record.ttl > JobTTL {
		h.errorf("terminal job %s has TTL %v; want retention in (0, %v]", id, record.ttl, JobTTL)
	}
}

// checkShards proves the deterministic child IDs: the same target list
// always names the same child, replays never create a child twice, and every
// file a successful walk saw is covered by a finished child.
func (h *farmHarness) checkShards(parentID string, walks []harnessWalk, records map[string]harnessRecord, grants map[string][]harnessGrant) {
	if len(walks) == 0 {
		h.errorf("parent %s was dispatched without a recorded walk", parentID)
		return
	}
	byTargets := map[string]string{}
	created := 0
	for _, walk := range walks {
		for _, child := range walk.children {
			if want := parentID + "-shard-" + targetShardDigest(child.RetryTargets); child.ID != want {
				h.errorf("child %s of %s is not named by its targets (want %s)", child.ID, parentID, want)
			}
			key := strings.Join(child.RetryTargets, "\x00")
			if other, ok := byTargets[key]; ok && other != child.ID {
				h.errorf("targets %q of %s are queued as both %s and %s", child.RetryTargets, parentID, other, child.ID)
			}
			byTargets[key] = child.ID
		}
		if walk.err != nil {
			continue
		}
		created += walk.created
		// Each file must be in a child that finished with exactly those targets.
		for _, target := range walk.targets {
			covered := false
			for _, child := range walk.children {
				if !slices.Contains(child.RetryTargets, target) {
					continue
				}
				record := records[child.ID]
				serial, _ := strconv.Atoi(record.fields["processed"])
				for _, grant := range grants[child.ID] {
					if grant.serial == serial && slices.Contains(grant.targets, target) && harnessTerminal(record.fields["status"]) {
						covered = true
					}
				}
			}
			if !covered {
				h.errorf("file %s seen by a successful walk of %s is in no finished child", target, parentID)
			}
		}
	}
	// Each child is created by the walk that first finds its record missing.
	// Concurrent planners (a reaped one and its replacement) can return in a
	// different order than they committed, so count creations, not order: a
	// replay over unchanged files adds nothing, and no child is created twice.
	if created > len(byTargets) {
		h.errorf("walks of %s created %d children but name only %d distinct ones: a replay duplicated children",
			parentID, created, len(byTargets))
	}
}

func (h *farmHarness) checkOutputs(id string, record harnessRecord, grants []harnessGrant) {
	serial, _ := strconv.Atoi(record.fields["processed"])
	for _, grant := range grants {
		if grant.serial != serial {
			continue
		}
		for _, target := range grant.targets {
			if _, err := os.Stat(filepath.Join(h.out, id, filepath.Base(target))); err != nil {
				h.errorf("done child %s has no published output for %s: %v", id, target, err)
			}
		}
	}
}

// checkNoStranded proves nothing is left claimed or leased once every worker
// is gone and aged, and only the ineligible backlog is still ready.
func (h *farmHarness) checkNoStranded(ineligible map[string]bool) {
	receipts, err := h.receiptLists()
	if err != nil {
		h.errorf("read receipts: %v", err)
		return
	}
	for key, raws := range receipts {
		if len(raws) > 0 {
			ids := make([]string, 0, len(raws))
			for _, raw := range raws {
				ids = append(ids, harnessRawID(raw))
			}
			h.errorf("receipts left in %s: %v", key, ids)
		}
	}
	for _, index := range []string{ProcessingIndexKey, TrackedProcessingIndexKey} {
		if members, err := h.admin.rdb.SMembers(h.ctx, index).Result(); err != nil || len(members) != 0 {
			h.errorf("%s = %v, %v; want empty", index, members, err)
		}
	}
	if leases, err := h.admin.rdb.ZRange(h.ctx, LeaseIndexKey, 0, -1).Result(); err != nil || len(leases) != 0 {
		h.errorf("lease index = %v, %v; want empty", leases, err)
	}
	ready := map[string]int{}
	for _, key := range allQueueKeys() {
		raws, err := h.admin.rdb.LRange(h.ctx, key, 0, -1).Result()
		if err != nil {
			h.errorf("read lane %s: %v", key, err)
			return
		}
		for _, raw := range raws {
			id := harnessRawID(raw)
			ready[id]++
			if !ineligible[id] {
				h.errorf("lane %s still holds job %s after the farm settled", key, id)
			}
		}
	}
	for id := range ineligible {
		if ready[id] == 0 {
			h.errorf("ineligible job %s left its ready lane", id)
		}
	}
}

// checkRetentionAndPrune proves the index keeps every record until it
// expires and then sheds it: after the run every record is still indexed;
// once the terminal records have expired (simulated: a week cannot pass),
// sweeps leave exactly the still-queued jobs.
func (h *farmHarness) checkRetentionAndPrune() {
	records, err := h.records()
	if err != nil {
		h.errorf("read job records: %v", err)
		return
	}
	indexed, err := h.admin.rdb.ZRange(h.ctx, JobIndexKey, 0, -1).Result()
	if err != nil {
		h.errorf("read job index: %v", err)
		return
	}
	inIndex := map[string]bool{}
	for _, id := range indexed {
		inIndex[id] = true
	}
	live := map[string]bool{}
	for id, record := range records {
		if !inIndex[id] {
			h.errorf("job %s (%s) has a record but no index entry: a sweep removed a live entry", id, record.fields["status"])
		}
		if harnessTerminal(record.fields["status"]) {
			if err := h.admin.rdb.PExpire(h.ctx, JobHashPrefix+id, time.Millisecond).Err(); err != nil {
				h.errorf("expire %s: %v", id, err)
			}
		} else {
			live[id] = true
		}
	}
	time.Sleep(20 * time.Millisecond)
	var remaining []string
	for sweep := 0; sweep < len(indexed)/maxJobIndexPruneBatch+5; sweep++ {
		if err := h.admin.rdb.Del(h.ctx, jobIndexPruneLockKey).Err(); err != nil {
			h.errorf("drop prune lock: %v", err)
			return
		}
		if _, err := h.admin.PruneExpiredJobIndex(h.ctx, maxJobIndexPruneBatch); err != nil {
			h.errorf("prune: %v", err)
			return
		}
		if remaining, err = h.admin.rdb.ZRange(h.ctx, JobIndexKey, 0, -1).Result(); err != nil {
			h.errorf("read job index: %v", err)
			return
		}
		if len(remaining) == len(live) {
			break
		}
	}
	got := map[string]bool{}
	for _, id := range remaining {
		got[id] = true
		if !live[id] {
			h.errorf("expired job %s is still indexed after the sweeps", id)
		}
	}
	for id := range live {
		if !got[id] {
			h.errorf("live job %s lost its index entry to the sweep", id)
		}
	}
}
