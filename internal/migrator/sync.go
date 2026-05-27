package migrator

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
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
	Verify            bool     `json:"verify"`             // --check-new (post-sync verify) [tier-3]
}

// DefaultSyncOptions returns the safe defaults applied when the user
// hasn't customized via the UI.
func DefaultSyncOptions() SyncOptions {
	return SyncOptions{
		PreserveStructure: true,
		PreserveTimes:     true,
		DryRun:            false,
		SkipJunk:          true,
		BWLimit:           0,
		Threads:           10,
		Verify:            false,
	}
}

// RunSync invokes `juicefs sync` against the given source and
// destination, with the supplied options.
//
// fuseMount is the in-process FUSE mount of the JuiceFS volume (e.g.
// /mnt/juicefs). Destinations are written via file:///<fuse-mount>/<path>
// so chunks flow through the same juicefs daemon as the NFS gateway —
// no jfs:// env-var-named-after-volume dance, no separate Redis/S3
// client.
//
// Source is always given a trailing slash when PreserveStructure is
// true (rsync "copy contents" semantic).
//
// Emits ProgressEvent on `progress` each time juicefs writes a
// recognized progress line. Blocks until juicefs exits or ctx cancels.
func RunSync(ctx context.Context, juicefsBin, fuseMount, source, destination string, opts SyncOptions, progress chan<- ProgressEvent) error {
	src := normalizeSourceURI(source, opts.PreserveStructure)
	dst := normalizeDestURIEmbedded(destination, fuseMount)

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
	}
	if opts.PreserveTimes {
		args = append(args, "--preserve", "mtime,uid,gid")
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
		args = append(args, "--check-new")
	}
	args = append(args, src, dst)

	cmd := exec.CommandContext(ctx, juicefsBin, args...)

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
	stderrTail := newRingBuffer(32 * 1024)
	teed := io.TeeReader(stderr, stderrTail)
	parseSyncProgress(teed, progress)

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
	// Final flush — emit whatever we have so the listener sees the
	// last state even if the throttle window was active.
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
}

// normalizeSourceURI converts a user-provided source string into the
// form juicefs sync expects. Trailing slash → "copy contents" rsync
// semantic. Caller controls whether we apply that semantic (via
// preserveStructure=true).
func normalizeSourceURI(s string, preserveStructure bool) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "/") {
		s = "file://" + s
	}
	if preserveStructure && !strings.HasSuffix(s, "/") {
		s = s + "/"
	}
	return s
}

// normalizeDestURIEmbedded translates a UI-supplied destination into
// a juicefs-sync-compatible URL using the in-process FUSE mount.
//
// In-process means the JuiceFS volume is already mounted in the same
// container at fuseMount (e.g. /mnt/juicefs). We just rewrite the
// user-visible path (e.g. /jfs/imported/foo) to a file:// URL under
// the actual mount point.
//
// Rules:
//   - Already-scheme'd URL: pass through (+ trailing slash)
//   - "/jfs/PATH" (the UI's destMount convention) → file:///<fuseMount>/PATH/
//   - bare /PATH → file:///<fuseMount>/PATH/
func normalizeDestURIEmbedded(dest, fuseMount string) string {
	dest = strings.TrimSpace(dest)
	if i := strings.Index(dest, "://"); i > 0 && i < 10 {
		if !strings.HasSuffix(dest, "/") {
			dest = dest + "/"
		}
		return dest
	}
	fuse := strings.TrimSuffix(fuseMount, "/")
	// Strip /jfs prefix if the UI sent that.
	rel := dest
	if strings.HasPrefix(rel, "/jfs/") {
		rel = rel[len("/jfs"):]
	} else if rel == "/jfs" {
		rel = "/"
	}
	if !strings.HasPrefix(rel, "/") {
		rel = "/" + rel
	}
	if !strings.HasSuffix(rel, "/") {
		rel = rel + "/"
	}
	return "file://" + fuse + rel
}

// randHex returns n random bytes as a hex string. Used for job IDs.
func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
