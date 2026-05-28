package manager

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// juicefsMetricsAddr is the host:port juicefs sync exposes Prometheus
// metrics on. The default is 127.0.0.1:9567; we pin it explicitly via
// --metrics so the parent process can poll a known address. Only one
// juicefs sync runs at a time (JobManager serializes), so a fixed port
// is safe.
const juicefsMetricsAddr = "127.0.0.1:9567"

// Direction is the migration direction selected in the UI's Migrations
// tab. SLICE 1 of the manager roadmap adds Out-of-JuiceFS and JuiceFS-
// to-JuiceFS migrations alongside the existing Into-JuiceFS behavior.
//
// Direction governs:
//   - which root the source browser starts in (/sources for In, /jfs
//     for Out and Between)
//   - whether the destination must be (DirectionIn) or must not be
//     (DirectionOut) inside the JuiceFS volume — see
//     validateDirectionPair
//   - whether the source URI is run through the file:// helper (In/Out)
//     or the JuiceFS-volume helper (Between, once slice-4 lands)
//
// DirectionIn is the default for backwards compatibility with clients
// that don't set the field on POST /api/migrate.
type Direction string

const (
	// DirectionIn — source is a host path under /sources/..., dest is
	// under /jfs/.... This is the pre-SLICE-1 behavior.
	DirectionIn Direction = "in"
	// DirectionOut — source is under /jfs/... (the FUSE mount tree),
	// dest is a host path outside /jfs. Used for exporting JuiceFS
	// contents to an external disk or share.
	DirectionOut Direction = "out"
	// DirectionBetween — both source and dest are JuiceFS volumes.
	// SLICE 1 stubs this with a "configure a second destination first"
	// message; lights up fully in SLICE 4 once the Destinations tab
	// can identify a second JuiceFS volume by profile name.
	DirectionBetween Direction = "between"
)

// SyncOptions controls per-job behavior. Tier-1/2/3 features in the
// foolproofing roadmap map directly onto these fields.
type SyncOptions struct {
	PreserveStructure bool     `json:"preserve_structure"` // if false, copy flat (basename only) [tier-1]
	PreserveTimes     bool     `json:"preserve_times"`     // --preserve mtime,uid,gid [tier-1]
	DryRun            bool     `json:"dry_run"`            // --dry [tier-1]
	SkipJunk          bool     `json:"skip_junk"`          // auto-exclude .DS_Store, Thumbs.db, ._* [tier-1]
	BWLimit           int      `json:"bw_limit"`           // --bwlimit MB/s (0 = unlimited) [tier-2]
	Threads           int      `json:"threads"`            // --threads N (default 10) [tier-2]
	Excludes          []string `json:"excludes"`           // user-provided --exclude patterns [tier-2]
	Includes          []string `json:"includes"`           // user-provided --include patterns [tier-2]
	Verify            bool     `json:"verify"`             // --check-all (byte-level post-sync verify, slow) [tier-3]
}

// DefaultSyncOptions returns the safe defaults applied when the user
// hasn't customized via the UI.
func DefaultSyncOptions() SyncOptions {
	return SyncOptions{
		PreserveStructure: true,
		// PreserveTimes=false by default: source files often carry uids
		// and modes that don't map cleanly to the JuiceFS volume on a
		// Mac client (we hit mode 070 with uid 100 from the prior
		// dataset — Mac users couldn't open the resulting files). Users
		// who actually want fidelity can opt in via the UI toggle.
		PreserveTimes: false,
		DryRun:        false,
		SkipJunk:      true,
		BWLimit:       0,
		Threads:       10,
		Verify:        false,
	}
}

// Mode controls how destinations are written.
type Mode int

const (
	// ModeEmbedded writes via file:///<FUSEMount>/<path>. The container
	// must have the JuiceFS volume FUSE-mounted at FUSEMount before
	// RunSync is called.
	ModeEmbedded Mode = iota
	// ModeStandalone writes via jfs://<VolName>/<path> with the env
	// var named <VolName> set to MetaURL. No FUSE mount required;
	// juicefs sync connects to Redis + MinIO directly. Requires
	// network reachability to both from inside the container.
	ModeStandalone
)

// RunSyncSpec bundles destination-resolution config so RunSync can
// pick the right URL form without re-deriving it from a half-set
// JobManager.
type RunSyncSpec struct {
	Mode      Mode
	FUSEMount string // ModeEmbedded
	MetaURL   string // ModeStandalone
	VolName   string // ModeStandalone
}

// RunSync invokes `juicefs sync` against the given source and
// destination, with the supplied options. The spec determines which
// destination-URL form is used. See Mode constants for details.
//
// Source is always given a trailing slash when PreserveStructure is
// true (rsync "copy contents" semantic).
//
// Emits ProgressEvent on `progress` each time juicefs writes a
// recognized progress line. Blocks until juicefs exits or ctx cancels.
func RunSync(ctx context.Context, juicefsBin string, spec RunSyncSpec, source, destination string, opts SyncOptions, progress chan<- ProgressEvent) error {
	// SLICE 1: route the source through normalizeAnyURI so /jfs/...
	// sources (Out direction) get rewritten to file:///<FUSEMount>/...
	// — same kernel-mount path the destination side uses for In. For
	// the pre-SLICE-1 case (source under /sources/...) normalizeAnyURI
	// falls through to the file:// branch, identical to what
	// normalizeSourceURI returned.
	src := normalizeAnyURI(source, spec.FUSEMount, opts.PreserveStructure)

	var dst string
	var extraEnv []string
	switch spec.Mode {
	case ModeStandalone:
		// SLICE 1: an Out-direction destination is a host path (e.g.
		// /external/backups/...), so route it through normalizeAnyURI
		// rather than the JuiceFS-specific helper. Standalone-mode
		// In destinations under /jfs/... still get the jfs:// scheme
		// because that branch of normalizeAnyURI passes them through
		// to the file:// rewrite path only when FUSEMount is set
		// (which standalone mode does not set) — so the helper falls
		// through to file:// for /external/... paths, and we keep
		// the explicit jfs:// helper for /jfs/... destinations.
		if strings.HasPrefix(strings.TrimSpace(destination), "/jfs") {
			dst = normalizeDestURIJFS(destination, spec.VolName, opts.PreserveStructure)
			// juicefs sync's URL syntax: env var name = URL alias. So
			// for jfs://zpool/... we set zpool=redis://...
			extraEnv = append(extraEnv, spec.VolName+"="+spec.MetaURL)
		} else {
			dst = normalizeAnyURI(destination, "", opts.PreserveStructure)
		}
	default: // ModeEmbedded
		// SLICE 1 split: /jfs/... destinations go through the FUSE-
		// mount rewrite (normalizeDestURIEmbedded), Out-direction
		// host-path destinations go through normalizeAnyURI which
		// passes them through as file://<host-path>. Without this
		// split, normalizeDestURIEmbedded would prepend the FUSE
		// mount to an Out-direction destination like /external/foo
		// and silently write outside the intended target.
		trimmed := strings.TrimSpace(destination)
		if strings.HasPrefix(trimmed, "/jfs") {
			dst = normalizeDestURIEmbedded(destination, spec.FUSEMount, opts.PreserveStructure)
		} else {
			dst = normalizeAnyURI(destination, "", opts.PreserveStructure)
		}
	}

	threads := opts.Threads
	if threads <= 0 {
		threads = 10
	}
	args := []string{
		"sync",
		"--list-threads", strconv.Itoa(threads),
		"--threads", strconv.Itoa(threads),
		"--update",
		"--check-change",
		// Pin metrics port so the parent can poll for accurate progress
		// counters (juicefs's stderr progress bar is TTY-only; non-TTY
		// stderr emits sparse logs that the regex parser barely catches).
		"--metrics", juicefsMetricsAddr,
	}
	if opts.PreserveTimes {
		// juicefs sync 1.3.1 only exposes --perms (mode bits). mtime
		// is preserved by default for file:// → file:// transfers but
		// can't be carried through S3 cleanly. The UI labels this
		// "Preserve permissions" accordingly.
		args = append(args, "--perms")
	}
	if opts.DryRun {
		args = append(args, "--dry")
	}
	if opts.BWLimit > 0 {
		args = append(args, "--bwlimit", strconv.Itoa(opts.BWLimit))
	}
	if opts.SkipJunk {
		for _, p := range junkPatterns {
			args = append(args, "--exclude", p)
		}
	}
	for _, p := range opts.Excludes {
		args = append(args, "--exclude", p)
	}
	for _, p := range opts.Includes {
		args = append(args, "--include", p)
	}
	if opts.Verify {
		// --check-all re-reads every file in source AND destination and
		// compares byte-by-byte. Doubles the read I/O of the sync but
		// gives true byte-level confidence the copy succeeded. The
		// older --check-new only verified newly-copied files (~free)
		// which barely caught anything useful — bumped to --check-all
		// since "Verify after sync" implies real verification.
		args = append(args, "--check-all")
	}
	args = append(args, src, dst)

	cmd := exec.CommandContext(ctx, juicefsBin, args...)
	// Inherit parent env (PATH, HOME, TMPDIR) then add any mode-
	// specific vars (the URL-alias env for ModeStandalone). Replacing
	// Env outright would leave juicefs without PATH for its child
	// processes (e.g. the temp-dir resolution it does on macOS).
	cmd.Env = append(os.Environ(), extraEnv...)

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("pipe stderr: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start juicefs sync: %w", err)
	}

	// Tee stderr into both the progress parser (extracts numeric
	// counters) and a tail-buffer that captures the most recent log
	// lines verbatim. On non-zero exit we surface those tail lines in
	// the returned error so the operator can see WHY juicefs failed
	// — exit-1-with-no-stderr-context was the original UX of this
	// path, and it was useless. The tail buffer keeps the last 32 KB
	// to bound memory; juicefs FATAL lines are usually one-shot near
	// the end of the run anyway.
	//
	// Two progress sources run concurrently:
	//   1. parseSyncProgress reads stderr line-by-line. juicefs's
	//      non-TTY logs are sparse (≈ one summary line at the end), so
	//      this mostly catches the final-flush counts and the
	//      "Copying <path>" current-file markers when present.
	//   2. pollJuicefsMetrics scrapes the juicefs Prometheus endpoint
	//      every 2s for the live `juicefs_sync_copied` /
	//      `..._copied_bytes` / `..._failed` counters. This is what
	//      drives the UI's live progress bar — without it the UI shows
	//      "0 files" for the duration of any non-trivial copy.
	stderrTail := newRingBuffer(32 * 1024)
	teed := io.TeeReader(stderr, stderrTail)

	metricsCtx, stopMetrics := context.WithCancel(ctx)
	metricsDone := make(chan struct{})
	go func() {
		defer close(metricsDone)
		pollJuicefsMetrics(metricsCtx, juicefsMetricsAddr, progress)
	}()
	parseSyncProgress(teed, progress)
	stopMetrics()
	<-metricsDone

	if err := cmd.Wait(); err != nil {
		if ctx.Err() == context.Canceled {
			return context.Canceled
		}
		tail := stderrTail.String()
		if tail == "" {
			return fmt.Errorf("juicefs sync exited: %w (no stderr output)", err)
		}
		return fmt.Errorf("juicefs sync exited: %w — last stderr:\n%s", err, tail)
	}
	return nil
}

// pollJuicefsMetrics scrapes the juicefs sync Prometheus endpoint every
// 2s and translates the relevant counters into ProgressEvents. Returns
// when ctx is canceled — typically as soon as parseSyncProgress sees
// the stderr EOF that signals juicefs has exited.
//
// We only emit when counters change so the SSE consumer doesn't get a
// flood of identical heartbeats during long no-op stretches (e.g. a
// 30-second stat-pass that finds nothing to copy).
func pollJuicefsMetrics(ctx context.Context, addr string, progress chan<- ProgressEvent) {
	client := &http.Client{Timeout: 3 * time.Second}
	url := "http://" + addr + "/metrics"

	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	// First scrape after a short delay so juicefs has time to bind
	// the metrics port (it logs "Prometheus metrics listening on ..."
	// during startup but the listener appears within ~50ms; the 200ms
	// initial delay is generous and keeps the first scrape from
	// racing the bind).
	firstScrape := time.NewTimer(200 * time.Millisecond)
	defer firstScrape.Stop()

	var last ProgressEvent
	scrape := func() {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return
		}
		resp, err := client.Do(req)
		if err != nil {
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return
		}
		ev := parseJuicefsMetrics(body)
		// Only emit on change so we don't spam the SSE stream during
		// long idle periods.
		if ev.Files == last.Files && ev.Bytes == last.Bytes && ev.Errors == last.Errors {
			return
		}
		ev.UpdatedAt = time.Now().UnixMilli()
		select {
		case progress <- ev:
			last = ev
		default:
			// Channel full — caller is behind. Skip this update; the
			// counters are cumulative so the next scrape will carry
			// the latest values anyway.
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-firstScrape.C:
			scrape()
		case <-tick.C:
			scrape()
		}
	}
}

// metricNameRegex captures any line of the form
//   juicefs_sync_<name>{...} <number>
// and pulls out the counter name + value. We don't strip labels — the
// juicefs sync metrics have only {cmd="sync",pid="..."} so the simplest
// thing is to scan for known counter names by prefix.
var metricLineRegex = regexp.MustCompile(`^(juicefs_sync_[a-z_]+)\{[^}]*\}\s+([0-9.eE+-]+)`)

// parseJuicefsMetrics extracts the counters we surface to the UI from a
// /metrics scrape body. Unrecognized counters are ignored. Bytes are
// rounded down (juicefs reports bytes as float64; we keep int64 for the
// SSE wire format consistency with the regex-parser path).
func parseJuicefsMetrics(body []byte) ProgressEvent {
	var ev ProgressEvent
	scanner := bufio.NewScanner(strings.NewReader(string(body)))
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "juicefs_sync_") {
			continue
		}
		m := metricLineRegex.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		val, err := strconv.ParseFloat(m[2], 64)
		if err != nil {
			continue
		}
		switch m[1] {
		case "juicefs_sync_copied":
			ev.Files = int64(val)
		case "juicefs_sync_copied_bytes":
			ev.Bytes = int64(val)
		case "juicefs_sync_failed":
			ev.Errors = int64(val)
		}
	}
	return ev
}

// ringBuffer is a tiny io.Writer that keeps only the last N bytes.
// Used to capture the tail of juicefs sync's stderr for inclusion in
// error messages without unbounded memory growth.
type ringBuffer struct {
	buf  []byte
	max  int
	full bool
	pos  int
}

func newRingBuffer(max int) *ringBuffer {
	return &ringBuffer{buf: make([]byte, 0, max), max: max}
}

func (r *ringBuffer) Write(p []byte) (int, error) {
	n := len(p)
	if n >= r.max {
		// Source line bigger than buffer: keep just the tail.
		copy(r.buf[:cap(r.buf)], p[n-r.max:])
		r.buf = r.buf[:r.max]
		r.full = true
		r.pos = 0
		return n, nil
	}
	if !r.full && len(r.buf)+n <= r.max {
		r.buf = append(r.buf, p...)
		return n, nil
	}
	// Wrap-around: extend or evict from the front.
	r.full = true
	if len(r.buf) < r.max {
		r.buf = r.buf[:r.max]
	}
	for _, b := range p {
		r.buf[r.pos] = b
		r.pos = (r.pos + 1) % r.max
	}
	return n, nil
}

func (r *ringBuffer) String() string {
	if !r.full {
		return string(r.buf)
	}
	// Reorder ring → linear, starting at the oldest byte.
	out := make([]byte, r.max)
	copy(out, r.buf[r.pos:])
	copy(out[r.max-r.pos:], r.buf[:r.pos])
	return string(out)
}

// progressRegex matches a juicefs sync progress line. Example formats
// the parser handles (juicefs's output has evolved across versions):
//   Scanned: 1234, copied: 567 (12.3 MiB/s), skipped: 0, failed: 0, eta: 12s
//   2026/05/26 14:33:21 <INFO> Scanned 100 entries, copied 50 (1.5 GiB), failed 0
//
// Any unmatched line is ignored. Robust to format drift — emits a
// partial event with whatever fields parsed.
var progressRegex = regexp.MustCompile(
	`(?i)\b(?:scanned|copied|skipped|failed|bytes|eta)[:\s]+([0-9]+(?:\.[0-9]+)?)\s*([KMGTPE]i?B)?(/s)?`,
)

// parseSyncProgress reads stderr line-by-line, extracts progress
// counters, and emits ProgressEvent. Closes nothing — caller owns
// the `progress` channel lifecycle.
func parseSyncProgress(r io.Reader, progress chan<- ProgressEvent) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024) // 1 MiB max line
	lastEmit := time.Now()
	var current ProgressEvent
	for scanner.Scan() {
		line := scanner.Text()
		updated := false
		matches := progressRegex.FindAllStringSubmatch(line, -1)
		for _, m := range matches {
			label := strings.ToLower(extractLabel(m[0]))
			valStr := m[1]
			val, err := strconv.ParseFloat(valStr, 64)
			if err != nil {
				continue
			}
			unit := strings.ToUpper(m[2])
			// Convert to bytes if a size unit was present.
			if unit != "" {
				val = applyUnit(val, unit)
			}
			switch label {
			case "scanned":
				// scanned ≠ copied; we don't surface scanned count
				// directly but it could be useful for progress %.
				updated = true
			case "copied":
				if unit == "" {
					current.Files = int64(val)
				} else {
					current.Bytes = int64(val)
				}
				updated = true
			case "skipped":
				// not currently surfaced
				updated = true
			case "failed":
				current.Errors = int64(val)
				updated = true
			case "bytes":
				current.Bytes = int64(val)
				updated = true
			case "eta":
				current.ETASec = int64(val)
				updated = true
			}
		}
		// Look for a "current file" marker — juicefs prints
		// "Copying <path>" or "Processed <path>" lines.
		if strings.Contains(line, "Copying ") {
			if idx := strings.Index(line, "Copying "); idx >= 0 {
				current.Current = strings.TrimSpace(line[idx+len("Copying "):])
				updated = true
			}
		}

		if updated {
			now := time.Now()
			// Throttle event emission to ~5 Hz so a flood of
			// progress lines doesn't overwhelm SSE consumers.
			if now.Sub(lastEmit) >= 200*time.Millisecond {
				current.UpdatedAt = now.UnixMilli()
				// Best-effort send; drop if buffer is full.
				select {
				case progress <- current:
				default:
				}
				lastEmit = now
			}
		}
	}
	// Final flush — emit only if we actually parsed something. juicefs
	// non-TTY stderr is sparse, so `current` is often all-zero here.
	// pollJuicefsMetrics is the authoritative source for counters; if
	// we emit a zeroed event after it stopped, last-write-wins in jobs.go
	// would clobber the accurate metrics-derived final state and the UI
	// would show 0 files / 0 bytes for a successful copy.
	if current.Files == 0 && current.Bytes == 0 && current.Errors == 0 && current.Current == "" {
		return
	}
	current.UpdatedAt = time.Now().UnixMilli()
	select {
	case progress <- current:
	default:
	}
}

// extractLabel returns the label part of a match like "copied: 50"
// → "copied". The regex captures the value separately so we re-parse
// the first word here.
func extractLabel(match string) string {
	for i, r := range match {
		if r == ':' || r == ' ' || r == '\t' {
			return match[:i]
		}
	}
	return match
}

// applyUnit converts a value + IEC/SI unit string to bytes.
// Examples: ("12.3", "MIB") → 12.3 * 1024 * 1024.
func applyUnit(v float64, unit string) float64 {
	switch unit {
	case "KB", "KIB":
		return v * 1024
	case "MB", "MIB":
		return v * 1024 * 1024
	case "GB", "GIB":
		return v * 1024 * 1024 * 1024
	case "TB", "TIB":
		return v * 1024 * 1024 * 1024 * 1024
	case "PB", "PIB":
		return v * 1024 * 1024 * 1024 * 1024 * 1024
	case "EB", "EIB":
		return v * 1024 * 1024 * 1024 * 1024 * 1024 * 1024
	}
	return v
}

// junkPatterns are the common OS-metadata files we auto-exclude when
// SyncOptions.SkipJunk is true. Saves users from polluting the
// destination with .DS_Store / Thumbs.db / AppleDouble sidecar litter.
var junkPatterns = []string{
	".DS_Store",
	"._*",
	".Spotlight-V100",
	".Trashes",
	".fseventsd",
	".TemporaryItems",
	"Thumbs.db",
	"desktop.ini",
	".sync.ffs_db", // FreeFileSync database sidecar
}

// normalizeSourceURI converts a user-provided source string into the
// form juicefs sync expects. Routes through matchSlash so src and dst
// agree on the rsync trailing-slash convention.
//
// NOTE: this helper only knows the In-direction shape (any absolute
// path → file://). SLICE 1 adds normalizeAnyURI as the higher-level
// dispatcher that picks the right helper based on the path's scheme
// or prefix; normalizeSourceURI is kept as the In-direction back-end
// (and the SLICE-0 callers that didn't yet need direction awareness).
func normalizeSourceURI(s string, preserveStructure bool) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "/") {
		s = "file://" + s
	}
	return matchSlash(s, preserveStructure)
}

// normalizeAnyURI dispatches a user-supplied URI to the correct
// scheme-specific helper. SLICE 1 needs this because the migration
// source can now be:
//
//   - a host path under /sources/... (In or Between's host-stub side)
//     → file:///sources/...
//   - a path under /jfs/... (Out / Between)
//     → file:///<FUSEMount>/... when running in embedded mode
//   - a raw URL with a scheme (file://, s3://, jfs://, ...)
//     → passed through verbatim, only trailing-slash adjusted
//
// The /jfs branch is intentionally rewritten to file:///<FUSEMount>/...
// rather than jfs://<vol>/... so juicefs sync sees the in-process FUSE
// mount the manager already has open. This matches what
// normalizeDestURIEmbedded does on the destination side — both ends
// of a same-volume sync therefore go through the same kernel mount,
// which is the cheapest path (no Redis round-trip per file lookup).
//
// fuseMount is the in-process FUSE mount path; pass "" if the manager
// is running in standalone mode (in which case /jfs/... is treated as
// an unrecognized path and falls back to the file:// helper). Callers
// in embedded mode should always pass cfg.FUSEMount.
//
// preserveStructure controls trailing slash, same semantics as
// normalizeSourceURI and the destination helpers.
func normalizeAnyURI(s string, fuseMount string, preserveStructure bool) string {
	s = strings.TrimSpace(s)
	// Scheme passthrough — any "<word>://..." string is treated as an
	// already-formed URL. matchSlash still applies so src/dst agree.
	if i := strings.Index(s, "://"); i > 0 && i < 10 {
		return matchSlash(s, preserveStructure)
	}
	// /jfs/... is the user-facing prefix for the JuiceFS volume tree.
	// Rewrite to file:///<FUSEMount>/... so the existing embedded-mode
	// machinery (no Redis round-trip) handles the read. Falls back to
	// the file:// helper if fuseMount is empty (standalone mode) — in
	// which case the caller likely shouldn't be giving us a /jfs path
	// in the first place; the destination helpers already document
	// this asymmetry.
	if fuseMount != "" {
		fuse := strings.TrimSuffix(fuseMount, "/")
		if strings.HasPrefix(s, "/jfs/") {
			rel := s[len("/jfs"):]
			return matchSlash("file://"+fuse+rel, preserveStructure)
		}
		if s == "/jfs" {
			return matchSlash("file://"+fuse, preserveStructure)
		}
	}
	// Everything else with a leading slash → file:// host path.
	if strings.HasPrefix(s, "/") {
		return matchSlash("file://"+s, preserveStructure)
	}
	// Bare relative paths get passed through; juicefs sync will reject
	// them with a clear error. Defensive — handleMigrate already
	// requires absolute paths so this branch is unreachable in normal
	// flow.
	return matchSlash(s, preserveStructure)
}

// validateDirectionPair enforces the source/destination shape rules
// for each Direction. Called by handleMigrate before submission so
// the user gets a clear 4xx rather than discovering the bad combo
// when juicefs sync FATALs.
//
// Rules:
//
//   - DirectionIn: source is a host path (already validated by
//     pathAllowed against sourceRoots), destination MUST be inside
//     the JuiceFS volume (/jfs/...). Rejecting external destinations
//     keeps the In semantics ("import" — data lands in JuiceFS).
//
//   - DirectionOut: source is /jfs/..., destination MUST NOT be
//     inside /jfs. A dest under /jfs would actually be a same-volume
//     copy (logically an In or Between, depending on second-volume
//     setup) and is rejected with a clear message rather than
//     silently doing the wrong thing.
//
//   - DirectionBetween: both sides must be /jfs/... paths. SLICE 1
//     surfaces a stub error pointing the user at the Destinations
//     tab; SLICE 4 wires this through to a named second-volume
//     profile.
//
// Path-traversal guards (filepath.Clean check against `..` segments)
// are applied by the caller via the same logic that already protects
// /api/migrate's destination — that's not in scope for this helper,
// which only enforces *direction* shape.
func validateDirectionPair(dir Direction, src, dst string) error {
	src = strings.TrimSpace(src)
	dst = strings.TrimSpace(dst)
	// pathUnderJFS returns true if p starts with /jfs (treating "/jfs"
	// alone and "/jfs/..." as both "inside the volume").
	pathUnderJFS := func(p string) bool {
		return p == "/jfs" || strings.HasPrefix(p, "/jfs/")
	}
	switch dir {
	case DirectionIn, "":
		// Empty Direction defaults to In for backwards compat with
		// pre-SLICE-1 clients (the JS field is new).
		if !pathUnderJFS(dst) {
			return fmt.Errorf("direction=in requires destination under /jfs (got %q)", dst)
		}
		if pathUnderJFS(src) {
			return fmt.Errorf("direction=in source must be a host path, not /jfs/... (got %q) — use direction=between for JuiceFS-to-JuiceFS", src)
		}
	case DirectionOut:
		if !pathUnderJFS(src) {
			return fmt.Errorf("direction=out requires source under /jfs (got %q)", src)
		}
		if pathUnderJFS(dst) {
			return fmt.Errorf("direction=out destination cannot be under /jfs (got %q) — that's an Into-JuiceFS copy, switch to direction=in", dst)
		}
	case DirectionBetween:
		if !pathUnderJFS(src) || !pathUnderJFS(dst) {
			return fmt.Errorf("direction=between requires both source and destination under /jfs (got src=%q dst=%q)", src, dst)
		}
		// SLICE-1 stub: a single-volume JuiceFS install can't
		// distinguish two distinct /jfs roots. This message lights
		// the user at the right path forward — SLICE 4 turns the
		// Destinations tab into the place to register the second
		// volume by name.
		return fmt.Errorf("Configure a second JuiceFS destination first (Destinations tab)")
	default:
		return fmt.Errorf("unknown direction %q (expected in|out|between)", dir)
	}
	return nil
}

// normalizeDestURIJFS translates a UI-supplied destination into a
// jfs://<volName>/<path> URL. Used by ModeStandalone.
//
// preserveStructure controls the trailing slash: juicefs sync REQUIRES
// src and dst to agree on the trailing-slash convention (else FATAL:
// "SRC and DST should both end with path separator or not!"). Caller
// must pass the SAME value used for normalizeSourceURI.
func normalizeDestURIJFS(dest, volName string, preserveStructure bool) string {
	dest = strings.TrimSpace(dest)
	if i := strings.Index(dest, "://"); i > 0 && i < 10 {
		return matchSlash(dest, preserveStructure)
	}
	rel := dest
	if strings.HasPrefix(rel, "/jfs/") {
		rel = rel[len("/jfs"):]
	} else if rel == "/jfs" {
		rel = ""
	}
	if !strings.HasPrefix(rel, "/") && rel != "" {
		rel = "/" + rel
	}
	return matchSlash("jfs://"+volName+rel, preserveStructure)
}

// matchSlash applies the rsync trailing-slash convention based on the
// preserveStructure flag. Caller-managed slash discipline; both
// normalizeSourceURI and the dest helpers route through this so src
// and dst can never disagree.
func matchSlash(s string, preserveStructure bool) string {
	if preserveStructure {
		if !strings.HasSuffix(s, "/") {
			s = s + "/"
		}
	} else {
		s = strings.TrimSuffix(s, "/")
	}
	return s
}

// normalizeDestURIEmbedded translates a UI-supplied destination into
// a juicefs-sync-compatible URL using the in-process FUSE mount.
//
// preserveStructure controls the trailing slash so src and dst agree
// — see matchSlash + normalizeDestURIJFS for why.
func normalizeDestURIEmbedded(dest, fuseMount string, preserveStructure bool) string {
	dest = strings.TrimSpace(dest)
	if i := strings.Index(dest, "://"); i > 0 && i < 10 {
		return matchSlash(dest, preserveStructure)
	}
	fuse := strings.TrimSuffix(fuseMount, "/")
	rel := dest
	if strings.HasPrefix(rel, "/jfs/") {
		rel = rel[len("/jfs"):]
	} else if rel == "/jfs" {
		rel = ""
	}
	if !strings.HasPrefix(rel, "/") && rel != "" {
		rel = "/" + rel
	}
	return matchSlash("file://"+fuse+rel, preserveStructure)
}

// randHex returns n random bytes as a hex string. Used for job IDs.
func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
