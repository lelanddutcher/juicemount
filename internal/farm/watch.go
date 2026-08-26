package farm

import (
	"bufio"
	"context"
	"os"
	"os/exec"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Wave-3 auto-discovery: the farm worker used to generate derivatives only
// when something ENQUEUED a job (a manager sweep click or OpenLoupe). New
// media landing in the volume by any other path — a JuiceMount client drain,
// an SMB ingest, a server-side copy — sat posterless until the next manual
// sweep. The Watcher closes that gap at the source of truth: JuiceFS commits
// every namespace change to Redis, and with notify-keyspace-events enabled
// (KEA — the volume ships it in redis.conf) each directory mutation fires a
// `__keyspace@<db>__:d<inode>` event. The farm sits next to Redis, so it
// subscribes, settles, resolves the directory, and enqueues itself a normal
// derivatives job — the SAME job shape a manager sweep produces, drained by
// the SAME worker loop, deduped by the SAME per-file currency checks the
// sweep already does. No new pipeline: just a new producer.
//
// Discipline:
//   - FILTER FIRST: the farm's own derivative writes land in .juicemount/…
//     and fire keyspace events too. Anything under a dot-directory (or the
//     trash) is dropped before any further work — otherwise the watcher
//     feeds itself forever.
//   - SETTLE: a directory receiving events is probably mid-copy. It is
//     processed only after its event stream has been quiet for a settle
//     window (default 60s), so ffmpeg never opens a half-drained file. A
//     racing writer just re-dirties the dir; the job's per-file currency
//     check makes the second pass cheap.
//   - BOUNDED: per-tick enqueue cap (leftovers stay dirty and carry over),
//     bounded dirty-set (overflow drops with a counter — the manager sweep
//     is the catch-up path), bounded exec per resolve.
//   - Inode→path resolution shells out to `juicefs info -i` against the
//     worker's OWN mount: battle-tested reverse lookup, no hand-decoding of
//     JuiceFS attr blobs. Failures drop the dir silently (raced deletes).
//
// Kill switch: JM_FARM_WATCH=0. Tunables: JM_FARM_WATCH_SETTLE_SEC,
// JM_FARM_WATCH_TICK_SEC, JM_FARM_WATCH_MAX_PER_TICK, JM_FARM_WATCH_KINDS.

// WatchConfig wires a Watcher. Enqueue and (optionally) Resolve are
// injected so the decision core is unit-testable without Redis, a mount,
// or the juicefs binary.
type WatchConfig struct {
	MetaURL string // redis:// URL, same JM_META the worker drains
	Mount   string // the worker's local FUSE mount root

	// Enqueue submits a settled, filtered, mount-relative DIRECTORY for
	// derivative generation. Required.
	Enqueue func(ctx context.Context, relDir string) error

	// Enabled is evaluated at the start of every tick. Returning false keeps
	// dirty events queued in memory so discovery can resume without losing the
	// work that landed while the operator paused the farm. Nil means enabled.
	Enabled func(ctx context.Context) bool

	// Resolve maps a directory inode to its mount-relative path. Nil means
	// the juicefs-info resolver against Mount.
	Resolve func(ctx context.Context, inode uint64) (string, bool)

	Settle     time.Duration // quiet time before a dirty dir is processed (default 60s)
	Tick       time.Duration // processing cadence (default 15s)
	MaxPerTick int           // enqueue budget per tick (default 200)
	DedupeTTL  time.Duration // suppress re-enqueue of the same dir (default 10m)
	MaxDirty   int           // dirty-set bound (default 10k)

	Logf func(format string, a ...any) // default: drop
}

// Watcher accumulates keyspace-dirty directory inodes and turns them into
// farm jobs once they settle. All state is guarded by mu; Note and Tick are
// safe from different goroutines.
type Watcher struct {
	cfg WatchConfig

	mu       sync.Mutex
	dirty    map[uint64]time.Time // inode → last event time
	recent   map[string]time.Time // relDir → last enqueue (dedupe)
	overflow int                  // dirty-set overflow drops since last log
}

// NewWatcher applies defaults and env tunables.
func NewWatcher(cfg WatchConfig) *Watcher {
	if cfg.Settle <= 0 {
		cfg.Settle = envDuration("JM_FARM_WATCH_SETTLE_SEC", 60*time.Second)
	}
	if cfg.Tick <= 0 {
		cfg.Tick = envDuration("JM_FARM_WATCH_TICK_SEC", 15*time.Second)
	}
	if cfg.MaxPerTick <= 0 {
		cfg.MaxPerTick = envInt("JM_FARM_WATCH_MAX_PER_TICK", 200)
	}
	if cfg.DedupeTTL <= 0 {
		cfg.DedupeTTL = 10 * time.Minute
	}
	if cfg.MaxDirty <= 0 {
		cfg.MaxDirty = 10000
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	if cfg.Resolve == nil {
		mount := cfg.Mount
		cfg.Resolve = func(ctx context.Context, inode uint64) (string, bool) {
			return juicefsInfoResolve(ctx, mount, inode)
		}
	}
	return &Watcher{
		cfg:    cfg,
		dirty:  make(map[uint64]time.Time),
		recent: make(map[string]time.Time),
	}
}

// WatchEnabled reports the JM_FARM_WATCH kill switch (default ON).
func WatchEnabled() bool { return os.Getenv("JM_FARM_WATCH") != "0" }

// WatchKindsFromEnv returns the job kinds the watcher enqueues.
// The default covers the complete ingest pipeline. Routing splits these kinds
// into independent jobs so server metadata stays local while proxy/transcript
// work is sent to the best eligible render node.
func WatchKindsFromEnv() []string {
	raw := strings.TrimSpace(os.Getenv("JM_FARM_WATCH_KINDS"))
	if raw == "" {
		return []string{"derivatives", "proxy", "transcript"}
	}
	var kinds []string
	for _, k := range strings.Split(raw, ",") {
		if k = strings.TrimSpace(k); k != "" {
			kinds = append(kinds, k)
		}
	}
	if len(kinds) == 0 {
		return []string{"derivatives", "proxy", "transcript"}
	}
	return kinds
}

// Note records a keyspace event for a directory inode. Root (inode 1) is
// deliberately ignored: enqueuing the volume root on every top-level event
// would schedule a full-tree walk per blip; new top-level directories still
// discover fine (their own d<inode> fires as content lands inside), and a
// stray file created directly at the root is the manager sweep's job.
func (w *Watcher) Note(inode uint64, now time.Time) {
	if inode <= 1 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, known := w.dirty[inode]; !known && len(w.dirty) >= w.cfg.MaxDirty {
		w.overflow++
		return
	}
	w.dirty[inode] = now
}

// Tick processes settled directories: resolve → filter → dedupe → enqueue,
// up to MaxPerTick. Unresolved-yet-unsettled inodes stay dirty; resolved or
// dropped ones leave the set. Returns the number of jobs enqueued.
func (w *Watcher) Tick(ctx context.Context, now time.Time) int {
	if w.cfg.Enabled != nil && !w.cfg.Enabled(ctx) {
		return 0
	}
	// Snapshot the settled candidates under the lock, then resolve/enqueue
	// outside it (resolution shells out; enqueue talks to Redis).
	w.mu.Lock()
	var settled []uint64
	for ino, last := range w.dirty {
		if now.Sub(last) >= w.cfg.Settle {
			settled = append(settled, ino)
			if len(settled) >= w.cfg.MaxPerTick {
				break
			}
		}
	}
	for _, ino := range settled {
		delete(w.dirty, ino)
	}
	if w.overflow > 0 {
		w.cfg.Logf("farm watch: dirty-set overflow, dropped %d dir events (manager sweep is the catch-up)", w.overflow)
		w.overflow = 0
	}
	w.mu.Unlock()

	type resolvedTarget struct {
		inode uint64
		path  string
	}
	resolved := make([]resolvedTarget, 0, len(settled))
	for _, ino := range settled {
		if ctx.Err() != nil {
			break
		}
		rel, ok := w.cfg.Resolve(ctx, ino)
		if !ok {
			continue // raced delete / unresolvable — drop silently
		}
		if !WatchPathAllowed(rel) {
			continue
		}
		resolved = append(resolved, resolvedTarget{inode: ino, path: path.Clean(rel)})
	}

	// One file creation commonly dirties its directory and several ancestors.
	// Keep the deepest disjoint targets in this settled batch; recursively
	// walking both "Project" and "Project/Reel" duplicates every derivative
	// and can turn a single ingest into a full project scan.
	sort.SliceStable(resolved, func(i, j int) bool {
		return len(resolved[i].path) > len(resolved[j].path)
	})
	pruned := resolved[:0]
	for _, candidate := range resolved {
		overlapped := false
		prefix := strings.TrimSuffix(candidate.path, "/") + "/"
		for _, kept := range pruned {
			if kept.path == candidate.path || strings.HasPrefix(kept.path, prefix) {
				overlapped = true
				break
			}
		}
		if !overlapped {
			pruned = append(pruned, candidate)
		}
	}

	enqueued := 0
	for _, target := range pruned {
		ino, rel := target.inode, target.path
		w.mu.Lock()
		if t, seen := w.recent[rel]; seen && now.Sub(t) < w.cfg.DedupeTTL {
			w.mu.Unlock()
			continue
		}
		w.recent[rel] = now
		// Opportunistic sweep so the dedupe map stays bounded.
		if len(w.recent) > 4096 {
			for p, t := range w.recent {
				if now.Sub(t) >= w.cfg.DedupeTTL {
					delete(w.recent, p)
				}
			}
		}
		w.mu.Unlock()

		if err := w.cfg.Enqueue(ctx, rel); err != nil {
			w.cfg.Logf("farm watch: enqueue %q failed: %v", rel, err)
			// Put it back so the next tick retries after the dedupe window.
			w.mu.Lock()
			delete(w.recent, rel)
			w.dirty[ino] = now
			w.mu.Unlock()
			continue
		}
		w.cfg.Logf("farm watch: enqueued %q (inode %d)", rel, ino)
		enqueued++
	}
	return enqueued
}

// WatchPathAllowed is the feedback-loop guard: the farm's own derivative
// writes (<mount>/.juicemount/…), trash churn, and any dot-directory are
// never enqueued — the watcher must not schedule work in response to its own
// output. Root and empty paths are refused too (Note already drops inode 1;
// this keeps the guard self-sufficient for tests and future callers).
//
// It also honors the derivative-exclusion policy (proxies in any spelling,
// NLE ephemeral/cache dirs — see DirIsExcluded) so the live watcher never
// enqueues what a sweep's collectTargets would skip. Size isn't checked here
// (the watcher has only the path); the collectTargets size floor still catches
// a too-small file if the sweep reaches it.
func WatchPathAllowed(rel string) bool {
	rel = strings.Trim(strings.TrimSpace(rel), "/")
	if rel == "" || rel == "." {
		return false
	}
	for _, seg := range strings.Split(rel, "/") {
		if strings.HasPrefix(seg, ".") {
			return false // .juicemount, .trash, .Spotlight-V100, any dot-dir
		}
	}
	if DirIsExcluded(rel) {
		return false // proxy / NLE-cache path — matches collectTargets exclusion
	}
	return true
}

// Run subscribes to the volume's keyspace events and drives the tick loop
// until ctx is done. Resolution is deliberately isolated in its own goroutine:
// `juicefs info -i` can take close to a second on a large live volume, and a
// 200-directory burst used to stop this goroutine from reading PubSub long
// enough for go-redis' channel to fill and drop exactly the events we need.
// go-redis reconnects and resubscribes internally; the recursive backstop
// covers events missed during a real connection gap.
func (w *Watcher) Run(ctx context.Context) error {
	opt, err := redis.ParseURL(w.cfg.MetaURL)
	if err != nil {
		return err
	}
	rdb := redis.NewClient(opt)
	defer rdb.Close()

	pattern := "__keyspace@" + strconv.Itoa(opt.DB) + "__:d*"
	prefix := "__keyspace@" + strconv.Itoa(opt.DB) + "__:"
	sub := rdb.PSubscribe(ctx, pattern)
	defer sub.Close()

	w.cfg.Logf("farm watch: engaged (db=%d settle=%s tick=%s cap=%d kinds via job) — gaps during reconnects are covered by manager sweeps",
		opt.DB, w.cfg.Settle, w.cfg.Tick, w.cfg.MaxPerTick)

	ch := sub.Channel(redis.WithChannelSize(4096))
	tickCtx, stopTicks := context.WithCancel(ctx)
	var tickWG sync.WaitGroup
	tickWG.Add(1)
	go func() {
		defer tickWG.Done()
		ticker := time.NewTicker(w.cfg.Tick)
		defer ticker.Stop()
		for {
			select {
			case <-tickCtx.Done():
				return
			case now := <-ticker.C:
				// Ticks are intentionally serialized. time.Ticker coalesces while a
				// slow batch is resolving, which bounds work without blocking event
				// intake or launching an unbounded resolver backlog.
				w.Tick(tickCtx, now)
			}
		}
	}()
	defer func() {
		stopTicks()
		tickWG.Wait()
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return nil // subscription torn down (Close during shutdown)
			}
			key := strings.TrimPrefix(msg.Channel, prefix)
			if len(key) < 2 || key[0] != 'd' {
				continue
			}
			ino, err := strconv.ParseUint(key[1:], 10, 64)
			if err != nil {
				continue
			}
			w.Note(ino, time.Now())
		}
	}
}

// juicefsInfoResolve maps an inode to its mount-relative path by shelling
// out to `juicefs info -i <inode> <mount>` — the same reverse lookup the
// juicefs CLI ships, run against the worker's own mount. Output parsing is
// deliberately loose (the exact layout varies by version): the first
// volume-absolute path found on any line wins. Multi-path (hardlink) output
// resolves to the first path, which is fine — the job walks a directory.
func juicefsInfoResolve(ctx context.Context, mount string, inode uint64) (string, bool) {
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	// With -i the positional args are INODES, and the mount is implied by the
	// working directory (juicefs finds the .control file relative to cwd) —
	// verified live against juicefs 1.3.1: `juicefs info -i N <mount>` parses
	// the mount path as an inode and errors.
	cmd := exec.CommandContext(cctx, "juicefs", "info", "-i", strconv.FormatUint(inode, 10))
	cmd.Dir = mount
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", false
	}
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		// Accept "path: /a/b", "paths: /a/b", or a bare "/a/b" line.
		if i := strings.Index(line, ": /"); i >= 0 {
			line = line[i+2:]
		}
		if !strings.HasPrefix(line, "/") {
			continue
		}
		rel := strings.Trim(path.Clean(line), "/")
		if rel == "" || rel == "." {
			return "", false // the root itself — never a job target
		}
		return rel, true
	}
	return "", false
}

func envDuration(name string, def time.Duration) time.Duration {
	if raw := os.Getenv(name); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return def
}

func envInt(name string, def int) int {
	if raw := os.Getenv(name); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			return n
		}
	}
	return def
}
