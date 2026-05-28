package manager

// SLICE 6 — Storage maintenance.
//
// Five operational levers wrapping `juicefs` CLI subprocesses (GC,
// FSCK, Warmup, Cache flush, Compact metadata). Each lever:
//
//   - Runs as a background goroutine spawning an exec.CommandContext.
//   - Streams the subprocess's stderr+stdout line-by-line into the
//     op's Output slice AND into any live SSE subscribers.
//   - Permits at most one op of the SAME kind at a time (per-kind
//     mutex); different kinds run concurrently (gc + fsck OK).
//   - Caps Output at 1000 lines with a "[truncated]" marker when the
//     cap is exceeded.
//
// The pattern intentionally mirrors JobManager (jobs.go) — subscribe-
// style fan-out for SSE, in-process state-only persistence, runner
// indirection for tests — but stays a separate struct because the
// constraints differ (one-per-kind vs. one-active-job-globally,
// line-oriented output vs. typed progress events).

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// maintenanceOutputCap is the per-op output ring limit. The SLICE 6
// spec: "Output capped at 1000 lines per op; older lines truncated
// with a '[truncated]' marker line." We implement this as a simple
// length check on append — once the cap is reached we keep only the
// most recent (cap-1) lines plus a single marker at the head.
const maintenanceOutputCap = 1000

// maintenanceTruncMarker is the sentinel line inserted at the head of
// Output the first time the cap is exceeded. It carries no semantic
// meaning for the subprocess; it tells the UI "older lines dropped".
const maintenanceTruncMarker = "[truncated]"

// MaintenanceKind enumerates the five levers exposed by the
// Maintenance tab. Strings (not iota) because they are surfaced over
// the wire as the URL suffix and as MaintenanceOp.Kind in JSON — a
// schema change here is a UI break, so the literal values are part
// of the contract.
type MaintenanceKind string

const (
	MaintenanceGC          MaintenanceKind = "gc"
	MaintenanceFSCK        MaintenanceKind = "fsck"
	MaintenanceWarmup      MaintenanceKind = "warmup"
	MaintenanceCacheFlush  MaintenanceKind = "cache-flush"
	MaintenanceCompactMeta MaintenanceKind = "compact-meta"
)

// allMaintenanceKinds is the iteration-friendly list. Kept in slice
// form (not just the const block above) so handler registration and
// the per-kind mutex map can walk it deterministically.
var allMaintenanceKinds = []MaintenanceKind{
	MaintenanceGC,
	MaintenanceFSCK,
	MaintenanceWarmup,
	MaintenanceCacheFlush,
	MaintenanceCompactMeta,
}

// MaintenanceState is the lifecycle state of one MaintenanceOp.
// Mirrors JobState but uses the SLICE-6-spec literals so JSON
// consumers (the UI, jmctl) don't have to disambiguate.
type MaintenanceState string

const (
	MaintenancePending MaintenanceState = "pending"
	MaintenanceRunning MaintenanceState = "running"
	MaintenanceDone    MaintenanceState = "done"
	MaintenanceError   MaintenanceState = "error"
)

// MaintenanceOp is one invocation of a maintenance lever. Output is
// the captured subprocess output (line-oriented, capped). Error is
// non-empty only when State == MaintenanceError.
//
// Field order matches the SLICE-6 spec verbatim. Adding fields is
// allowed; renaming or reordering is a schema change.
type MaintenanceOp struct {
	Kind       MaintenanceKind  `json:"kind"`
	State      MaintenanceState `json:"state"`
	StartedAt  int64            `json:"started_at"`  // unix-ms
	FinishedAt int64            `json:"finished_at"` // unix-ms (0 while running)
	Output     []string         `json:"output"`
	Error      string           `json:"error,omitempty"`

	// runtime-only — not serialized
	mu        sync.Mutex          `json:"-"`
	cancel    context.CancelFunc  `json:"-"`
	listeners []chan string       `json:"-"`
	truncated bool                `json:"-"`
}

// MaintenanceRunner is the subprocess-execution indirection. Default
// is execMaintenance; tests override via SetMaintenanceRunner to feed
// canned output without spawning a real juicefs binary.
//
// argv is the full command + args the runner should exec; op is the
// op to mutate (append to Output, notify listeners). Returns the
// subprocess's exit error (nil on success).
type MaintenanceRunner func(ctx context.Context, argv []string, op *MaintenanceOp) error

// MaintenanceManager owns the five per-kind mutexes, the in-memory
// "last op per kind" snapshot, and the SSE fan-out plumbing.
//
// One MaintenanceManager per API instance. Constructed in
// newMaintenanceManager and stored on the API struct so handlers
// have a stable receiver.
type MaintenanceManager struct {
	juicefsBin string
	fuseMount  string // empty in standalone mode (warmup / cache-flush degrade to 501)
	metaURL    string // empty when not configured (gc / fsck / compact-meta degrade to 501)
	destMount  string // user-facing /jfs prefix for warmup-path rewriting

	runner MaintenanceRunner

	// kindMu is the per-kind mutex map. Locking kindMu[<kind>] is the
	// gate that enforces "one op per kind at a time"; a second
	// request for the same kind sees the lock held and returns 409.
	// Different kinds have different mutexes so gc + fsck can run
	// concurrently.
	kindMu map[MaintenanceKind]*sync.Mutex

	// mu protects the active/last maps. Held only briefly — never
	// across subprocess waits.
	mu     sync.Mutex
	active map[MaintenanceKind]*MaintenanceOp // running op per kind, nil when idle
	last   map[MaintenanceKind]*MaintenanceOp // most recent finished op per kind (kept for /api/maintenance/{kind} GET status)
}

// newMaintenanceManager constructs a MaintenanceManager with the
// default execMaintenance runner. juicefsBin / fuseMount / metaURL
// mirror the same-named API fields; destMount is the user-facing
// prefix the warmup handler rewrites against.
func newMaintenanceManager(juicefsBin, fuseMount, metaURL, destMount string) *MaintenanceManager {
	mm := &MaintenanceManager{
		juicefsBin: juicefsBin,
		fuseMount:  fuseMount,
		metaURL:    metaURL,
		destMount:  destMount,
		runner:     execMaintenance,
		kindMu:     make(map[MaintenanceKind]*sync.Mutex, len(allMaintenanceKinds)),
		active:     make(map[MaintenanceKind]*MaintenanceOp, len(allMaintenanceKinds)),
		last:       make(map[MaintenanceKind]*MaintenanceOp, len(allMaintenanceKinds)),
	}
	for _, k := range allMaintenanceKinds {
		mm.kindMu[k] = &sync.Mutex{}
	}
	return mm
}

// SetRunner swaps the underlying subprocess runner. Test-only — the
// production path always uses execMaintenance.
func (mm *MaintenanceManager) SetRunner(fn MaintenanceRunner) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	mm.runner = fn
}

// errKindBusy is the sentinel returned by tryStart when the per-kind
// mutex is already held. The HTTP handler translates it to 409.
var errKindBusy = errors.New("an op of this kind is already running")

// tryStart attempts to claim the per-kind mutex non-blockingly. On
// success the caller owns the slot and MUST call finish() when done
// (typically via defer). On failure the kind is already running and
// the caller should return 409 to the HTTP client.
//
// The argv is the command line the runner will exec — built by the
// handler so kind-specific flag construction (e.g. --dry-run) stays
// out of this function. We accept argv here rather than letting the
// caller invoke runner directly because tryStart owns the lifecycle:
// state transitions, listener cleanup, last/active bookkeeping.
func (mm *MaintenanceManager) tryStart(kind MaintenanceKind, argv []string) (*MaintenanceOp, error) {
	mu, ok := mm.kindMu[kind]
	if !ok {
		return nil, fmt.Errorf("unknown maintenance kind: %s", kind)
	}
	// TryLock is non-blocking — returns false immediately if held.
	// Available since Go 1.18; the project's go.mod targets 1.21+.
	if !mu.TryLock() {
		return nil, errKindBusy
	}
	op := &MaintenanceOp{
		Kind:      kind,
		State:     MaintenancePending,
		StartedAt: time.Now().UnixMilli(),
		Output:    make([]string, 0, 64),
	}
	mm.mu.Lock()
	mm.active[kind] = op
	mm.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	op.mu.Lock()
	op.cancel = cancel
	op.State = MaintenanceRunning
	op.mu.Unlock()

	// Run the subprocess on a goroutine so the HTTP handler can
	// return immediately and the client polls /stream for live
	// output. Mutex is released in the goroutine's defer chain so a
	// second same-kind request between accept and exec correctly
	// returns 409.
	go func() {
		defer mu.Unlock()
		// Release context resources as soon as the runner returns.
		// Without this, ctx + its internal channels stay live until GC
		// runs on the op snapshot — bounded (≤5 ops in mm.last) but
		// avoidable. Code-reviewer flagged this; fix is cheap.
		defer cancel()
		err := mm.runner(ctx, argv, op)
		op.mu.Lock()
		op.FinishedAt = time.Now().UnixMilli()
		switch {
		case ctx.Err() != nil && err != nil:
			op.State = MaintenanceError
			op.Error = "canceled"
		case err != nil:
			op.State = MaintenanceError
			op.Error = err.Error()
		default:
			op.State = MaintenanceDone
		}
		op.closeListenersLocked()
		op.mu.Unlock()
		mm.mu.Lock()
		delete(mm.active, kind)
		mm.last[kind] = op
		mm.mu.Unlock()
	}()
	return op, nil
}

// maintenanceOpSnapshot is the wire-shape DTO used by the HTTP
// handlers. We do NOT return MaintenanceOp by value because it
// embeds a sync.Mutex; `go vet` flags any pass-by-value of a struct
// holding a Mutex (and rightly so — a copied mutex is a bug
// generator). The DTO carries only the serializable fields.
type maintenanceOpSnapshot struct {
	Kind       MaintenanceKind  `json:"kind"`
	State      MaintenanceState `json:"state"`
	StartedAt  int64            `json:"started_at"`
	FinishedAt int64            `json:"finished_at"`
	Output     []string         `json:"output"`
	Error      string           `json:"error,omitempty"`
}

// snapshot returns a JSON-safe copy of the op's externally-visible
// state. Output is copied (not aliased) so a concurrent runner
// goroutine appending mid-marshal can't trigger a "concurrent map /
// slice mutation" panic in encoding/json.
func (op *MaintenanceOp) snapshot() maintenanceOpSnapshot {
	op.mu.Lock()
	defer op.mu.Unlock()
	out := maintenanceOpSnapshot{
		Kind:       op.Kind,
		State:      op.State,
		StartedAt:  op.StartedAt,
		FinishedAt: op.FinishedAt,
		Error:      op.Error,
	}
	if len(op.Output) > 0 {
		out.Output = make([]string, len(op.Output))
		copy(out.Output, op.Output)
	}
	return out
}

// appendOutputLocked appends one line to the op's output, applying
// the maintenanceOutputCap ring trim. Caller MUST hold op.mu.
//
// Trim policy: when len(Output) reaches the cap, drop the oldest
// non-marker line and insert a single "[truncated]" marker at the
// head on the first overflow. Subsequent overflows just shift the
// window without re-inserting the marker.
func (op *MaintenanceOp) appendOutputLocked(line string) {
	if !op.truncated {
		if len(op.Output) < maintenanceOutputCap {
			op.Output = append(op.Output, line)
			return
		}
		// First overflow: install the [truncated] marker at index 0 and
		// slide the live window. NOTE: this drops TWO lines at the
		// boundary — original Output[0] is overwritten by the marker,
		// and Output[1] is lost in the copy(Output, Output[1:]) shift
		// (the last index gets clobbered by the new line below). Two-
		// line drop is intentional and tested for — keeps the cap
		// invariant exactly equal to maintenanceOutputCap without
		// growing the slice. Subsequent overflows drop exactly one
		// line (the (oldest-after-marker) line at index 1).
		op.truncated = true
		copy(op.Output, op.Output[1:])
		op.Output[0] = maintenanceTruncMarker
		op.Output[len(op.Output)-1] = line
		return
	}
	// Already truncated: keep the marker at index 0, slide the rest.
	// Indices 1..cap-1 are the live ring; we drop op.Output[1] and
	// append the new line at the tail.
	copy(op.Output[1:], op.Output[2:])
	op.Output[len(op.Output)-1] = line
}

// notifyListenersLocked broadcasts a single output line to every SSE
// subscriber. Drops on a full buffer rather than blocking the runner
// goroutine — matches the JobManager.notifyListenersLocked pattern.
// Caller MUST hold op.mu.
func (op *MaintenanceOp) notifyListenersLocked(line string) {
	for _, c := range op.listeners {
		select {
		case c <- line:
		default:
			// slow consumer; drop
		}
	}
}

// closeListenersLocked closes every subscriber channel. Called at
// op termination. Caller MUST hold op.mu.
func (op *MaintenanceOp) closeListenersLocked() {
	for _, c := range op.listeners {
		close(c)
	}
	op.listeners = nil
}

// subscribe registers an SSE channel for live output from a still-
// running op. Returns the channel + a cleanup func + ok=true; ok=false
// when the op is already in a terminal state (caller should fetch the
// final snapshot and skip the stream).
//
// We do NOT replay the existing Output to a late subscriber here. The
// stream endpoint is for live tailing; clients that need the full
// captured output read GET /api/maintenance/{kind} which returns the
// full snapshot (including the truncated head marker if any).
func (op *MaintenanceOp) subscribe() (<-chan string, func(), bool) {
	op.mu.Lock()
	defer op.mu.Unlock()
	if op.State != MaintenanceRunning && op.State != MaintenancePending {
		return nil, nil, false
	}
	ch := make(chan string, 64)
	op.listeners = append(op.listeners, ch)
	cleanup := func() {
		op.mu.Lock()
		defer op.mu.Unlock()
		for i, c := range op.listeners {
			if c == ch {
				op.listeners = append(op.listeners[:i], op.listeners[i+1:]...)
				break
			}
		}
	}
	return ch, cleanup, true
}

// execMaintenance is the production runner. Spawns argv via
// exec.CommandContext, captures stdout+stderr line-by-line, and feeds
// each line into op.appendOutputLocked + op.notifyListenersLocked.
//
// We use a single combined pipe (stderr → stdout) because the juicefs
// CLI mixes progress notes between the two streams and the UI treats
// them as one log. The order across streams is best-effort; within a
// single stream it's preserved.
func execMaintenance(ctx context.Context, argv []string, op *MaintenanceOp) error {
	if len(argv) == 0 {
		return errors.New("empty argv")
	}
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("stdout pipe: %w", err)
	}
	cmd.Stderr = cmd.Stdout // merge streams — see func doc
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start: %w", err)
	}
	// Drain the combined pipe on a goroutine so the runner survives a
	// subprocess that closes its pipes before exit. The scanner stops
	// at EOF or the first read error.
	done := make(chan struct{})
	go func() {
		defer close(done)
		scanner := bufio.NewScanner(stdout)
		// Allow long lines (juicefs sometimes prints status lines
		// >64KB when listing thousands of slices). 1MB is plenty.
		scanner.Buffer(make([]byte, 64*1024), 1<<20)
		for scanner.Scan() {
			line := scanner.Text()
			op.mu.Lock()
			op.appendOutputLocked(line)
			op.notifyListenersLocked(line)
			op.mu.Unlock()
		}
	}()
	// Wait for both the scanner to finish AND the subprocess to exit.
	<-done
	waitErr := cmd.Wait()
	// Drain whatever the scanner missed (e.g. final line without
	// trailing newline) by re-reading what's still buffered. The
	// scanner already consumed Stdout to EOF, so this is a no-op for
	// well-behaved children but a safety net otherwise.
	_, _ = io.Copy(io.Discard, stdout)
	return waitErr
}

// ─── HTTP handlers ─────────────────────────────────────────────────

// handleMaintenanceGC is POST /api/maintenance/gc. Supports
// ?dry_run=true to add --dry-run to the juicefs gc invocation. The
// dry-run mode reports bytes that WOULD be reclaimed without
// modifying anything.
//
// Returns:
//   - 202 + MaintenanceOp on accept (op runs in the background)
//   - 409 if a gc is already running (per-kind mutex held)
//   - 501 in standalone mode without a metaURL configured
func (a *API) handleMaintenanceGC(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		a.maintenanceStatus(w, MaintenanceGC)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "POST or GET", http.StatusMethodNotAllowed)
		return
	}
	if a.maintenance == nil || a.maintenance.metaURL == "" {
		http.Error(w, "gc requires metaURL (configure OverviewMetaURL or run in standalone mode)", http.StatusNotImplemented)
		return
	}
	dryRun := r.URL.Query().Get("dry_run") == "true"
	argv := []string{a.maintenance.binOrDefault(), "gc", a.maintenance.metaURL}
	if !dryRun {
		// juicefs 1.3.x convention: `gc` without flags = scan-only
		// dry-run (reports leaked objects without touching storage);
		// `gc --delete` = actually reclaim. There is NO --dry-run flag
		// (passing it FATALs). Inverted: dryRun is the default, we
		// only add --delete when the user explicitly opts in.
		argv = append(argv, "--delete")
	}
	op, err := a.maintenance.tryStart(MaintenanceGC, argv)
	if err != nil {
		a.writeMaintenanceStartErr(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, op.snapshot())
}

// handleMaintenanceFSCK is POST /api/maintenance/fsck. Streams
// `juicefs fsck` output. Defects are reported in the captured
// Output; the State stays MaintenanceDone unless the subprocess
// itself returns non-zero (rare — fsck reports issues on stdout
// while still exiting 0 in many juicefs versions).
func (a *API) handleMaintenanceFSCK(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		a.maintenanceStatus(w, MaintenanceFSCK)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "POST or GET", http.StatusMethodNotAllowed)
		return
	}
	if a.maintenance == nil || a.maintenance.metaURL == "" {
		http.Error(w, "fsck requires metaURL", http.StatusNotImplemented)
		return
	}
	argv := []string{a.maintenance.binOrDefault(), "fsck", a.maintenance.metaURL}
	op, err := a.maintenance.tryStart(MaintenanceFSCK, argv)
	if err != nil {
		a.writeMaintenanceStartErr(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, op.snapshot())
}

// handleMaintenanceWarmup is POST /api/maintenance/warmup. Accepts
// ?path=<user-facing path inside /jfs>; defaults to the volume root
// (a.destMount). Validates with jfsPathAllowed — the same gate the
// browse and migration handlers use — so the user cannot pass a
// path outside the FUSE mount.
//
// Warmup requires the FUSE mount (juicefs warmup operates on the
// kernel mount path, not the metaURL). Standalone mode returns 501.
func (a *API) handleMaintenanceWarmup(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		a.maintenanceStatus(w, MaintenanceWarmup)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "POST or GET", http.StatusMethodNotAllowed)
		return
	}
	if a.fuseMount == "" {
		http.Error(w, "warmup requires the FUSE mount (embedded mode)", http.StatusNotImplemented)
		return
	}
	userPath := strings.TrimSpace(r.URL.Query().Get("path"))
	if userPath == "" {
		// Default to the volume root so the UI can hit the button
		// without a path picker. Matches the "defaults to volume
		// root" requirement in the SLICE-6 spec.
		userPath = a.destMount
	}
	if !a.jfsPathAllowed(userPath) {
		http.Error(w, "warmup path outside the JuiceFS volume", http.StatusForbidden)
		return
	}
	// Rewrite the user-facing /jfs/... path to the on-disk FUSE
	// mount path before exec. juicefs warmup expects a path on the
	// local filesystem, not a /jfs/... virtual.
	dmClean := filepath.Clean(a.destMount)
	rel := strings.TrimPrefix(filepath.Clean(userPath), dmClean)
	rel = strings.TrimPrefix(rel, "/")
	fuse := strings.TrimSuffix(a.fuseMount, "/")
	realPath := fuse
	if rel != "" {
		realPath = fuse + "/" + rel
	}
	argv := []string{a.maintenance.binOrDefault(), "warmup", realPath}
	op, err := a.maintenance.tryStart(MaintenanceWarmup, argv)
	if err != nil {
		a.writeMaintenanceStartErr(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, op.snapshot())
}

// handleMaintenanceCacheFlush is POST /api/maintenance/cache-flush.
// Runs `juicefs warmup --evict <fuseMount>` to evict every cached
// chunk for the volume. Requires the FUSE mount; standalone mode
// returns 501.
//
// Implementation note: there is no single "cache-flush" subcommand in
// juicefs CLI. The supported way to drop the local cache is `juicefs
// warmup --evict <mountPath>` which iterates the tree and evicts each
// chunk. We invoke it that way so the UI button has a single, well-
// understood effect.
func (a *API) handleMaintenanceCacheFlush(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		a.maintenanceStatus(w, MaintenanceCacheFlush)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "POST or GET", http.StatusMethodNotAllowed)
		return
	}
	if a.fuseMount == "" {
		http.Error(w, "cache flush requires the FUSE mount (embedded mode)", http.StatusNotImplemented)
		return
	}
	argv := []string{a.maintenance.binOrDefault(), "warmup", "--evict", strings.TrimSuffix(a.fuseMount, "/")}
	op, err := a.maintenance.tryStart(MaintenanceCacheFlush, argv)
	if err != nil {
		a.writeMaintenanceStartErr(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, op.snapshot())
}

// handleMaintenanceCompactMeta is POST /api/maintenance/compact-meta.
// Runs `juicefs gc --compact` which rewrites slice chunks into denser
// objects. Requires metaURL; standalone mode returns 501.
func (a *API) handleMaintenanceCompactMeta(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		a.maintenanceStatus(w, MaintenanceCompactMeta)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "POST or GET", http.StatusMethodNotAllowed)
		return
	}
	if a.maintenance == nil || a.maintenance.metaURL == "" {
		http.Error(w, "compact-meta requires metaURL", http.StatusNotImplemented)
		return
	}
	argv := []string{a.maintenance.binOrDefault(), "gc", "--compact", a.maintenance.metaURL}
	op, err := a.maintenance.tryStart(MaintenanceCompactMeta, argv)
	if err != nil {
		a.writeMaintenanceStartErr(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, op.snapshot())
}

// handleMaintenanceStream is GET /api/maintenance/{kind}/stream. SSE
// fan-out of live subprocess output for the active op of that kind.
// Returns 404 when no op of that kind is currently running.
//
// The handler is dispatched by handleMaintenanceRoute — this method
// receives the resolved kind. The wire shape per event is the raw
// output line as the SSE `data:` field; multi-line lines do not
// occur because the runner splits at newlines.
func (a *API) handleMaintenanceStream(w http.ResponseWriter, r *http.Request, kind MaintenanceKind) {
	if a.maintenance == nil {
		http.Error(w, "maintenance not configured", http.StatusNotImplemented)
		return
	}
	a.maintenance.mu.Lock()
	op := a.maintenance.active[kind]
	a.maintenance.mu.Unlock()
	if op == nil {
		http.Error(w, "no op of this kind is running", http.StatusNotFound)
		return
	}
	ch, cleanup, ok := op.subscribe()
	if !ok {
		// Race: op finished between active-lookup and subscribe.
		// Just return the final snapshot via the regular status
		// endpoint shape so the client can still display the result.
		writeJSON(w, http.StatusOK, op.snapshot())
		return
	}
	defer cleanup()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher, _ := w.(http.Flusher)

	for {
		select {
		case line, more := <-ch:
			if !more {
				// Op finished — listener channel closed by
				// closeListenersLocked.
				return
			}
			// Encode the line as JSON so embedded newlines/quotes are
			// safe over the wire. The UI parses the JSON to recover
			// the raw text.
			b, _ := json.Marshal(line)
			fmt.Fprintf(w, "data: %s\n\n", b)
			if flusher != nil {
				flusher.Flush()
			}
		case <-r.Context().Done():
			return
		}
	}
}

// Per-kind SSE stream handlers. Registered as separate ServeMux
// entries so the existing exact-path /api/maintenance/{kind} POST
// routes don't accidentally swallow /stream requests. Each is a thin
// wrapper around handleMaintenanceStream with the kind baked in.
func (a *API) handleMaintenanceStreamGC(w http.ResponseWriter, r *http.Request) {
	a.handleMaintenanceStream(w, r, MaintenanceGC)
}
func (a *API) handleMaintenanceStreamFSCK(w http.ResponseWriter, r *http.Request) {
	a.handleMaintenanceStream(w, r, MaintenanceFSCK)
}
func (a *API) handleMaintenanceStreamWarmup(w http.ResponseWriter, r *http.Request) {
	a.handleMaintenanceStream(w, r, MaintenanceWarmup)
}
func (a *API) handleMaintenanceStreamCacheFlush(w http.ResponseWriter, r *http.Request) {
	a.handleMaintenanceStream(w, r, MaintenanceCacheFlush)
}
func (a *API) handleMaintenanceStreamCompactMeta(w http.ResponseWriter, r *http.Request) {
	a.handleMaintenanceStream(w, r, MaintenanceCompactMeta)
}

// maintenanceStatus is the shared GET handler — returns the active
// or last-known op for `kind`, or 404 if neither exists yet.
func (a *API) maintenanceStatus(w http.ResponseWriter, kind MaintenanceKind) {
	if a.maintenance == nil {
		http.Error(w, "maintenance not configured", http.StatusNotImplemented)
		return
	}
	a.maintenance.mu.Lock()
	op := a.maintenance.active[kind]
	if op == nil {
		op = a.maintenance.last[kind]
	}
	a.maintenance.mu.Unlock()
	if op == nil {
		http.Error(w, "no op of this kind has run yet", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, op.snapshot())
}

// writeMaintenanceStartErr maps the few error returns of tryStart to
// HTTP status codes. errKindBusy → 409 (the "same-kind already
// running" gate); anything else → 400.
func (a *API) writeMaintenanceStartErr(w http.ResponseWriter, err error) {
	if errors.Is(err, errKindBusy) {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	log.Printf("manager: maintenance start: %v", err)
	http.Error(w, err.Error(), http.StatusBadRequest)
}

// binOrDefault returns the configured juicefsBin or "juicefs" when
// empty (relying on PATH lookup). Lets the unit tests run with an
// unset bin path without inventing a fake one.
func (mm *MaintenanceManager) binOrDefault() string {
	if mm.juicefsBin == "" {
		return "juicefs"
	}
	return mm.juicefsBin
}
