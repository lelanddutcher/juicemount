package manager

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// overviewBackendTimeout caps any single backend probe so a hung Redis,
// stalled MinIO, or runaway `juicefs status` subprocess cannot block the
// aggregate /api/overview response. Picked at 3s — Redis INFO is sub-
// millisecond on a healthy box; MinIO /minio/health/live returns in <50ms;
// `juicefs status` walks the metadata once and typically returns in
// <500ms but can spike when Redis is under load. 3s is well clear of all
// happy-path numbers while keeping the worst-case dashboard poll snappy.
const overviewBackendTimeout = 3 * time.Second

// recentJobsLimit is the cap on how many recent jobs the Overview tab
// surfaces. The Migrations tab is the canonical job list; Overview's job
// section is a "what happened lately" glance, not a full history view.
const recentJobsLimit = 10

// OverviewSnapshot is the JSON shape returned by /api/overview. Each
// per-backend section carries its own Error string so a single failing
// source (e.g. Redis unreachable mid-poll) degrades only that card
// rather than 5xx'ing the whole dashboard. Frontend renders the card
// in an "error" style when Error != "".
//
// CollectedAt is the unix-ms timestamp the snapshot was assembled at;
// frontend uses it to detect a stale response after a network blip.
type OverviewSnapshot struct {
	CollectedAt int64               `json:"collected_at"`
	Volume      VolumeStatusSection `json:"volume"`
	Redis       RedisStatusSection  `json:"redis"`
	MinIO       MinIOStatusSection  `json:"minio"`
	Jobs        RecentJobsSection   `json:"jobs"`
}

// VolumeStatusSection captures the `juicefs status` output: volume
// name, used/total bytes, file count. Source is the JSON form of
// `juicefs status <metaURL>`; parser is tolerant of upstream field
// renames (used → used_space, etc.) by trying common aliases.
type VolumeStatusSection struct {
	Name      string `json:"name,omitempty"`
	UsedBytes int64  `json:"used_bytes,omitempty"`
	// Files is the total inode count reported by juicefs. Reported as
	// a separate number from UsedBytes — juicefs decouples logical file
	// count from the slice/object footprint that backs them.
	Files int64  `json:"files,omitempty"`
	Error string `json:"error,omitempty"`
}

// RedisStatusSection captures the Redis INFO probe result. LatencyMs
// is the wall-clock round-trip for the INFO command (not the Redis
// internal latency stat) — that's what the operator actually feels
// when the manager talks to the metadata store.
type RedisStatusSection struct {
	Reachable    bool   `json:"reachable"`
	LatencyMs    int64  `json:"latency_ms,omitempty"`
	Version      string `json:"version,omitempty"`
	UptimeSec    int64  `json:"uptime_sec,omitempty"`
	UsedMemoryMB int64  `json:"used_memory_mb,omitempty"`
	Error        string `json:"error,omitempty"`
}

// MinIOStatusSection captures the MinIO admin liveness probe. We use
// /minio/health/live which is the documented unauthenticated liveness
// endpoint — no admin credentials needed in this scope (matches
// health.checkMinIO's pattern).
type MinIOStatusSection struct {
	Reachable bool   `json:"reachable"`
	LatencyMs int64  `json:"latency_ms,omitempty"`
	Endpoint  string `json:"endpoint,omitempty"`
	Error     string `json:"error,omitempty"`
}

// RecentJobsSection mirrors the Migrations tab's last-N jobs in a
// stripped-down shape suitable for the Overview's compact summary.
// Reverse-chronological so the most recent job is first.
type RecentJobsSection struct {
	Items []RecentJob `json:"items"`
	Error string      `json:"error,omitempty"`
}

// RecentJob is one entry in RecentJobsSection. Fields chosen for an
// at-a-glance display: ID + state + duration + total bytes copied.
// Source/destination paths are included so the card can render a
// "→" arrow without a second round-trip to /api/jobs/<id>.
type RecentJob struct {
	ID          string   `json:"id"`
	Source      string   `json:"source"`
	Destination string   `json:"destination"`
	State       JobState `json:"state"`
	DurationMs  int64    `json:"duration_ms"`
	Bytes       int64    `json:"bytes"`
	Files       int64    `json:"files"`
}

// overviewSource bundles the external-system addresses the aggregator
// reaches into. Keeping them on a struct (rather than reading globals)
// makes the unit tests deterministic — test harness wires fakes via
// the *Func hooks.
type overviewSource struct {
	mgr        *JobManager
	juicefsBin string
	metaURL    string
	minioURL   string

	// Probe hooks. Default implementations call real backends; tests
	// swap these for mocked sync.Func-style closures. Each MUST honor
	// its ctx (which the aggregator caps at overviewBackendTimeout).
	probeVolume func(ctx context.Context) VolumeStatusSection
	probeRedis  func(ctx context.Context) RedisStatusSection
	probeMinIO  func(ctx context.Context) MinIOStatusSection
	probeJobs   func(ctx context.Context) RecentJobsSection
}

// newOverviewSource constructs an overviewSource wired up to the real
// backends. Tests build their own struct directly with mocked probe
// fields rather than going through this constructor.
func newOverviewSource(mgr *JobManager, juicefsBin, metaURL, minioURL string) *overviewSource {
	s := &overviewSource{
		mgr:        mgr,
		juicefsBin: juicefsBin,
		metaURL:    metaURL,
		minioURL:   minioURL,
	}
	s.probeVolume = s.realProbeVolume
	s.probeRedis = s.realProbeRedis
	s.probeMinIO = s.realProbeMinIO
	s.probeJobs = s.realProbeJobs
	return s
}

// collectOverview fans out to every backend concurrently, capping each
// probe at overviewBackendTimeout. Returns a single OverviewSnapshot
// where any per-backend failure is captured on that section's Error
// field rather than aborting the aggregate.
//
// Concurrency is the whole point: serial probes would compound the
// 3-second timeout into a 12-second worst-case response on a fully-
// stuck cluster. The errgroup-style fan-out keeps the response bounded
// by the SLOWEST single probe (3s), not the sum.
//
// We don't use golang.org/x/sync/errgroup directly because every probe
// is expected to succeed-with-an-error-field (never return an err to
// bubble up) — there's nothing to short-circuit on. A plain sync.WaitGroup
// matches the semantic and keeps the dep list at zero.
func (s *overviewSource) collectOverview(ctx context.Context) OverviewSnapshot {
	snap := OverviewSnapshot{
		CollectedAt: time.Now().UnixMilli(),
	}
	var wg sync.WaitGroup

	// Each probe gets its OWN bounded ctx — they're independent and
	// shouldn't share a timeout (otherwise a fast Redis cancellation
	// would also kill the in-flight volume probe). Cancel funcs are
	// invoked via defer in each goroutine so we don't leak timers.
	run := func(probe func(ctx context.Context)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pctx, cancel := context.WithTimeout(ctx, overviewBackendTimeout)
			defer cancel()
			probe(pctx)
		}()
	}

	run(func(pctx context.Context) { snap.Volume = s.probeVolume(pctx) })
	run(func(pctx context.Context) { snap.Redis = s.probeRedis(pctx) })
	run(func(pctx context.Context) { snap.MinIO = s.probeMinIO(pctx) })
	run(func(pctx context.Context) { snap.Jobs = s.probeJobs(pctx) })

	wg.Wait()
	return snap
}

// handleOverview is the HTTP entry point for /api/overview. Always
// returns 200 with the snapshot JSON — per-backend failures surface in
// the per-section Error string instead of an HTTP error code. This
// matches the "polling endpoint that never breaks the dashboard"
// contract: a partial-data response is more useful to the operator
// than a 5xx that hides what IS working.
func (a *API) handleOverview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", http.StatusMethodNotAllowed)
		return
	}
	if a.overview == nil {
		// Defensive: Register() always wires this, but a hand-constructed
		// API in a test that skips overview wiring would otherwise NPE.
		writeJSON(w, http.StatusOK, OverviewSnapshot{
			CollectedAt: time.Now().UnixMilli(),
			Volume:      VolumeStatusSection{Error: "overview not configured"},
			Redis:       RedisStatusSection{Error: "overview not configured"},
			MinIO:       MinIOStatusSection{Error: "overview not configured"},
			Jobs:        RecentJobsSection{Error: "overview not configured"},
		})
		return
	}
	snap := a.overview.collectOverview(r.Context())
	writeJSON(w, http.StatusOK, snap)
}

// realProbeVolume runs `juicefs status --json <metaURL>` and parses
// the resulting JSON. juicefs's stable status command supports --json
// (or `-json` in older versions); we pass both shapes by trying --json
// first and only falling back on flag-parse failure.
//
// Skipped when metaURL is empty (embedded mode without an explicit
// MetaURL passed to Config — the section reports an actionable error
// rather than emitting all-zero fields that look like a healthy empty
// volume).
func (s *overviewSource) realProbeVolume(ctx context.Context) VolumeStatusSection {
	if s.metaURL == "" {
		return VolumeStatusSection{Error: "metaURL not configured"}
	}
	bin := s.juicefsBin
	if bin == "" {
		bin = "juicefs"
	}
	// `juicefs status` emits JSON on stdout by default in 1.3.x — it
	// does NOT accept --json (verified against ce-v1.3.1: FATAL
	// "unknown option: --json"). The INFO banner lines go to stderr;
	// we only consume stdout. parseJuicefsStatusJSON tolerantly finds
	// the first `{` in case a future version prepends anything.
	cmd := exec.CommandContext(ctx, bin, "status", s.metaURL)
	out, err := cmd.Output()
	if err != nil {
		// Surface a short, operator-friendly message — the full stderr
		// is logged at the call site if a future slice wires logging,
		// but we don't echo it into the HTTP response because juicefs
		// occasionally prints the resolved Redis URL with credentials.
		return VolumeStatusSection{Error: fmt.Sprintf("juicefs status failed: %v", trimErr(err))}
	}
	return parseJuicefsStatusJSON(out)
}

// parseJuicefsStatusJSON tolerantly decodes the JSON document juicefs
// emits. The shape has shifted across versions; we accept several
// field name aliases. Exposed (lowercased) for testability.
func parseJuicefsStatusJSON(raw []byte) VolumeStatusSection {
	// juicefs status emits a top-level object with a "Setting"
	// sub-object (volume metadata) and "Sessions" list. The fields
	// we surface live on the root: UsedSpace, AvailableSpace, FilesCount
	// in 1.3.x; some 1.2.x builds named them used_space etc.
	//
	// Tolerantly skip any leading non-JSON output (some juicefs
	// versions interleave warnings on stdout despite the INFO banner
	// being on stderr). Find the first `{` and parse from there.
	if idx := bytes.IndexByte(raw, '{'); idx > 0 {
		raw = raw[idx:]
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil {
		return VolumeStatusSection{Error: "parse juicefs status JSON: " + err.Error()}
	}
	out := VolumeStatusSection{}
	// Setting block has the volume name.
	if setting, ok := doc["Setting"]; ok {
		var s struct {
			Name string `json:"Name"`
		}
		_ = json.Unmarshal(setting, &s)
		out.Name = s.Name
	}
	// Used bytes — try common aliases.
	for _, k := range []string{"UsedSpace", "used_space", "Used", "used"} {
		if v, ok := doc[k]; ok {
			n, perr := strconv.ParseInt(strings.TrimSpace(string(v)), 10, 64)
			if perr == nil {
				out.UsedBytes = n
				break
			}
		}
	}
	// File count.
	for _, k := range []string{"FilesCount", "files_count", "Files", "files"} {
		if v, ok := doc[k]; ok {
			n, perr := strconv.ParseInt(strings.TrimSpace(string(v)), 10, 64)
			if perr == nil {
				out.Files = n
				break
			}
		}
	}
	return out
}

// realProbeRedis runs INFO via a short-lived go-redis client. We
// deliberately don't reuse the long-lived health monitor's client:
// overview probes should be self-contained and impose no ordering
// constraints on the health subsystem (the health monitor's client
// is mid-pipeline-state at any given moment).
//
// Empty metaURL returns an Error so the operator sees the missing
// configuration rather than a falsely-green "reachable" row.
func (s *overviewSource) realProbeRedis(ctx context.Context) RedisStatusSection {
	if s.metaURL == "" {
		return RedisStatusSection{Error: "metaURL not configured"}
	}
	// We parse via a tiny inline helper to avoid pulling metadata
	// package's full client surface into the manager (would create
	// an import cycle once future slices land settings.go that imports
	// other manager primitives).
	addr, db, err := parseRedisAddr(s.metaURL)
	if err != nil {
		return RedisStatusSection{Error: "parse metaURL: " + err.Error()}
	}
	start := time.Now()
	info, err := redisInfo(ctx, addr, db)
	latency := time.Since(start).Milliseconds()
	if err != nil {
		return RedisStatusSection{Error: "redis INFO: " + err.Error(), LatencyMs: latency}
	}
	out := RedisStatusSection{
		Reachable: true,
		LatencyMs: latency,
	}
	parseRedisInfoFields(info, &out)
	return out
}

// realProbeMinIO does a GET against /minio/health/live — the
// documented unauthenticated MinIO liveness endpoint. We don't try
// admin endpoints here because the overview slice doesn't have admin
// credentials wired (that lands in slice-4 with Destinations); the
// liveness check is enough to tell the operator whether the storage
// backend is reachable at all.
func (s *overviewSource) realProbeMinIO(ctx context.Context) MinIOStatusSection {
	if s.minioURL == "" {
		return MinIOStatusSection{Error: "minioURL not configured"}
	}
	url := strings.TrimSuffix(s.minioURL, "/") + "/minio/health/live"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return MinIOStatusSection{Endpoint: s.minioURL, Error: err.Error()}
	}
	client := &http.Client{Timeout: overviewBackendTimeout}
	start := time.Now()
	resp, err := client.Do(req)
	latency := time.Since(start).Milliseconds()
	if err != nil {
		return MinIOStatusSection{Endpoint: s.minioURL, LatencyMs: latency, Error: err.Error()}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return MinIOStatusSection{
			Endpoint:  s.minioURL,
			LatencyMs: latency,
			Error:     fmt.Sprintf("unexpected status %d", resp.StatusCode),
		}
	}
	return MinIOStatusSection{
		Reachable: true,
		LatencyMs: latency,
		Endpoint:  s.minioURL,
	}
}

// realProbeJobs surfaces the last recentJobsLimit jobs from the
// JobManager in reverse insertion order. Bounded under read of the
// JobManager's mutex via List() — no separate locks needed.
func (s *overviewSource) realProbeJobs(ctx context.Context) RecentJobsSection {
	if s.mgr == nil {
		return RecentJobsSection{Error: "no job manager attached"}
	}
	all := s.mgr.List()
	// Reverse so the most recent submission is first. Cheap given the
	// recentJobsLimit cap — we copy at most 10 pointers.
	n := len(all)
	limit := recentJobsLimit
	if n < limit {
		limit = n
	}
	out := RecentJobsSection{Items: make([]RecentJob, 0, limit)}
	for i := 0; i < limit; i++ {
		j := all[n-1-i]
		snap := j.GetSnapshot()
		var duration int64
		if snap.FinishedAt > 0 && snap.StartedAt > 0 {
			duration = snap.FinishedAt - snap.StartedAt
		} else if snap.StartedAt > 0 {
			// Still running — duration is now − StartedAt.
			duration = time.Now().UnixMilli() - snap.StartedAt
		}
		out.Items = append(out.Items, RecentJob{
			ID:          snap.ID,
			Source:      snap.Source,
			Destination: snap.Destination,
			State:       snap.State,
			DurationMs:  duration,
			Bytes:       snap.Last.Bytes,
			Files:       snap.Last.Files,
		})
	}
	return out
}

// trimErr collapses an exec.ExitError's stderr tail into the error
// message. *exec.ExitError doesn't include stderr by default unless the
// caller wired CombinedOutput — for the overview probes we want a
// short, single-line error that's safe to surface in the JSON response.
func trimErr(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	if i := strings.IndexByte(msg, '\n'); i >= 0 {
		msg = msg[:i]
	}
	return msg
}

// parseRedisAddr extracts host:port and db from a redis:// URL. Local
// re-implementation (rather than importing metadata.ParseRedisURL) so
// the manager package stays free of upstream import cycles as future
// slices add more cross-package state.
func parseRedisAddr(rawURL string) (addr string, db int, err error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", 0, err
	}
	if u.Scheme != "redis" && u.Scheme != "rediss" {
		return "", 0, fmt.Errorf("unsupported scheme %q (want redis://)", u.Scheme)
	}
	addr = u.Host
	if addr == "" {
		return "", 0, fmt.Errorf("missing host in %q", rawURL)
	}
	if !strings.Contains(addr, ":") {
		addr += ":6379"
	}
	if p := strings.TrimPrefix(u.Path, "/"); p != "" {
		n, perr := strconv.Atoi(p)
		if perr != nil {
			return "", 0, fmt.Errorf("invalid db %q", p)
		}
		db = n
	}
	return addr, db, nil
}

// redisInfo runs the INFO command against a Redis at addr/db and
// returns the raw output. Short-lived client (built per call) — we
// don't want a long-lived connection here because the overview
// endpoint is polled and the lifecycle is unbounded; the cost of TCP
// setup is dominated by the INFO command itself anyway. Honors ctx
// for timeout / cancellation.
func redisInfo(ctx context.Context, addr string, db int) (string, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		DB:           db,
		DialTimeout:  overviewBackendTimeout,
		ReadTimeout:  overviewBackendTimeout,
		WriteTimeout: overviewBackendTimeout,
	})
	defer rdb.Close()
	res, err := rdb.Info(ctx, "server", "memory").Result()
	if err != nil {
		return "", err
	}
	return res, nil
}

// parseRedisInfoFields extracts the handful of INFO fields we surface
// on the Overview dashboard. INFO output is line-based "key:value" with
// section headers prefixed by "# ". We don't need a real INI parser —
// linear scan is fine and matches what go-redis itself does internally.
func parseRedisInfoFields(info string, out *RedisStatusSection) {
	scanner := bufio.NewScanner(strings.NewReader(info))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		idx := strings.Index(line, ":")
		if idx <= 0 {
			continue
		}
		key := line[:idx]
		val := strings.TrimSpace(line[idx+1:])
		switch key {
		case "redis_version":
			out.Version = val
		case "uptime_in_seconds":
			if n, err := strconv.ParseInt(val, 10, 64); err == nil {
				out.UptimeSec = n
			}
		case "used_memory":
			if n, err := strconv.ParseInt(val, 10, 64); err == nil {
				// Convert bytes → MB for compact display. Floor division
				// is fine — operators don't care about sub-MB precision.
				out.UsedMemoryMB = n / (1024 * 1024)
			}
		}
	}
}
